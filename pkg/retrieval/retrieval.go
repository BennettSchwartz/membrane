package retrieval

import (
	"context"
	"errors"
	"fmt"

	"github.com/GustyCube/membrane/pkg/schema"
	"github.com/GustyCube/membrane/pkg/storage"
)

// Common retrieval errors.
var (
	// ErrAccessDenied is returned when a trust context denies access to a record.
	ErrAccessDenied = errors.New("access denied by trust context")

	// ErrNilTrust is returned when a nil trust context is provided.
	ErrNilTrust = errors.New("trust context is required")
)

// Service is the main retrieval service implementing layered memory retrieval
// per RFC 15.8.
type Service struct {
	store     storage.Store
	selector  *Selector
	embedding EmbeddingService
}

// EmbeddingService generates query embeddings for retrieval-time applicability scoring.
type EmbeddingService interface {
	EmbedQuery(ctx context.Context, taskDescriptor string) ([]float32, error)
}

// RetrieveRequest specifies parameters for a layered retrieval query.
type RetrieveRequest struct {
	// TaskDescriptor describes the current task for contextual retrieval.
	TaskDescriptor string

	// Trust is the trust context that gates what records can be returned.
	Trust *TrustContext

	// MemoryTypes optionally restricts retrieval to specific memory types.
	// If empty, all types are queried in layered order.
	MemoryTypes []schema.MemoryType

	// MinSalience filters out records below this salience threshold.
	MinSalience float64

	// Limit caps the total number of returned records. 0 means no limit.
	Limit int
}

// RetrieveResponse contains the results of a layered retrieval query.
type RetrieveResponse struct {
	// Records contains the filtered, sorted, and limited results.
	Records []*schema.MemoryRecord

	// Selection is non-nil when competence or plan_graph candidates were
	// evaluated through the multi-solution selector (RFC 15A.11).
	Selection *SelectionResult
}

// layerOrder defines the canonical retrieval order per RFC 15.8:
// working -> semantic -> competence -> plan_graph -> episodic.
var layerOrder = []schema.MemoryType{
	schema.MemoryTypeWorking,
	schema.MemoryTypeSemantic,
	schema.MemoryTypeCompetence,
	schema.MemoryTypePlanGraph,
	schema.MemoryTypeEpisodic,
}

// NewService creates a new retrieval Service backed by the given store and selector.
func NewService(store storage.Store, selector *Selector) *Service {
	return &Service{
		store:    store,
		selector: selector,
	}
}

// NewServiceWithEmbedding creates a new retrieval Service with embedding support.
func NewServiceWithEmbedding(store storage.Store, selector *Selector, embedding EmbeddingService) *Service {
	return &Service{
		store:     store,
		selector:  selector,
		embedding: embedding,
	}
}

// Retrieve performs layered retrieval as specified in RFC 15.8.
// It queries the store for each memory type layer in order, applies trust and
// salience filtering, runs competence/plan_graph results through the selector,
// sorts by salience descending, and applies the limit.
func (svc *Service) Retrieve(ctx context.Context, req *RetrieveRequest) (*RetrieveResponse, error) {
	if req.Trust == nil {
		return nil, ErrNilTrust
	}

	// Determine which memory types to query.
	layers := layerOrder
	if len(req.MemoryTypes) > 0 {
		layers = req.MemoryTypes
	}

	var allRecords []*schema.MemoryRecord
	var selectionCandidates []*schema.MemoryRecord
	var selection *SelectionResult
	var queryEmbedding []float32

	if svc.embedding != nil && req.TaskDescriptor != "" {
		queryEmbedding, _ = svc.embedding.EmbedQuery(ctx, req.TaskDescriptor)
	}

	for _, memType := range layers {
		records, err := svc.store.ListByType(ctx, memType)
		if err != nil {
			return nil, fmt.Errorf("failed to list records for memory type %s: %w", memType, err)
		}

		// Apply trust context filtering.
		records = FilterByTrust(records, req.Trust)

		// Apply salience filtering.
		if req.MinSalience > 0 {
			records = FilterBySalience(records, req.MinSalience)
		}

		// Collect competence and plan_graph records for selection.
		if memType == schema.MemoryTypeCompetence || memType == schema.MemoryTypePlanGraph {
			selectionCandidates = append(selectionCandidates, records...)
		}

		allRecords = append(allRecords, records...)
	}

	// Run selector on competence/plan_graph candidates if any exist.
	if len(selectionCandidates) > 0 && svc.selector != nil {
		selection = svc.selector.Select(ctx, selectionCandidates, queryEmbedding)
	}

	if selection != nil && len(selection.Selected) > 0 && req.TaskDescriptor != "" {
		allRecords = rankRecordsWithSelection(allRecords, selection)
	} else {
		SortBySalience(allRecords)
	}

	// Apply limit.
	if req.Limit > 0 && len(allRecords) > req.Limit {
		allRecords = allRecords[:req.Limit]
	}

	return &RetrieveResponse{
		Records:   allRecords,
		Selection: selection,
	}, nil
}

func rankRecordsWithSelection(records []*schema.MemoryRecord, selection *SelectionResult) []*schema.MemoryRecord {
	ranked := make([]*schema.MemoryRecord, 0, len(records))
	seen := make(map[string]struct{}, len(records))
	selectedIDs := make(map[string]struct{}, len(selection.Selected))
	for _, rec := range selection.Selected {
		selectedIDs[rec.ID] = struct{}{}
		ranked = append(ranked, rec)
		seen[rec.ID] = struct{}{}
	}

	remaining := make([]*schema.MemoryRecord, 0, len(records))
	for _, rec := range records {
		if _, ok := seen[rec.ID]; ok {
			continue
		}
		if _, ok := selectedIDs[rec.ID]; ok {
			continue
		}
		remaining = append(remaining, rec)
	}
	SortBySalience(remaining)
	return append(ranked, remaining...)
}

// RetrieveByID fetches a single record by ID and checks it against the trust context.
// Returns storage.ErrNotFound if the record does not exist, or ErrAccessDenied
// if the trust context does not allow access.
func (svc *Service) RetrieveByID(ctx context.Context, id string, trust *TrustContext) (*schema.MemoryRecord, error) {
	if trust == nil {
		return nil, ErrNilTrust
	}

	record, err := svc.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}

	if !trust.Allows(record) {
		return nil, ErrAccessDenied
	}

	return record, nil
}

// RetrieveByType fetches all records of a given type that pass the trust check.
func (svc *Service) RetrieveByType(ctx context.Context, memType schema.MemoryType, trust *TrustContext) ([]*schema.MemoryRecord, error) {
	if trust == nil {
		return nil, ErrNilTrust
	}

	records, err := svc.store.ListByType(ctx, memType)
	if err != nil {
		return nil, err
	}

	records = FilterByTrust(records, trust)
	SortBySalience(records)

	return records, nil
}
