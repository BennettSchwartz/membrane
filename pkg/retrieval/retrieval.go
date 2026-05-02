package retrieval

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
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
	store        storage.Store
	selector     *Selector
	embedding    EmbeddingService
	vectorRanker VectorRanker
}

// EmbeddingService generates query embeddings for retrieval-time applicability scoring.
type EmbeddingService interface {
	EmbedQuery(ctx context.Context, taskDescriptor string) ([]float32, error)
}

// VectorRanker searches stored embeddings by cosine similarity and returns
// record IDs ordered by relevance. Used to rank ALL record types when a
// query embedding is available.
type VectorRanker interface {
	SearchByEmbedding(ctx context.Context, query []float32, limit int) ([]string, error)
}

// RetrieveRequest specifies parameters for a layered retrieval query.
type RetrieveRequest struct {
	// TaskDescriptor describes the current task for contextual retrieval.
	TaskDescriptor string

	// QueryEmbedding is an optional pre-computed embedding for the query.
	// When set, Retrieve uses this instead of calling EmbedQuery, avoiding
	// an extra API round-trip and potential rate-limit failures.
	QueryEmbedding []float32

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
	schema.MemoryTypeEntity,
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

// NewServiceWithVectorRanker creates a retrieval Service that uses vector
// similarity to rank ALL record types, not just competence/plan_graph.
func NewServiceWithVectorRanker(store storage.Store, selector *Selector, embedding EmbeddingService, ranker VectorRanker) *Service {
	return &Service{
		store:        store,
		selector:     selector,
		embedding:    embedding,
		vectorRanker: ranker,
	}
}

// Retrieve performs layered retrieval as specified in RFC 15.8.
// It queries the store for each memory type layer in order, applies trust and
// salience filtering, runs competence/plan_graph results through the selector,
// sorts by salience descending, and applies the limit.
func (svc *Service) Retrieve(ctx context.Context, req *RetrieveRequest) (*RetrieveResponse, error) {
	if req == nil || req.Trust == nil {
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

	// Use pre-computed embedding if provided, otherwise generate one.
	queryEmbedding := req.QueryEmbedding
	if len(queryEmbedding) == 0 && svc.embedding != nil && req.TaskDescriptor != "" {
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

	// Rank results. Priority:
	// 1. Vector similarity ranking for ALL records when a vector ranker
	//    and query embedding are available. Competence/plan_graph selection
	//    results are still promoted to the front.
	// 2. Selection-based ranking (competence/plan_graph scored by selector,
	//    remaining records by salience).
	// 3. Pure salience ranking (fallback when no embeddings are available).
	if svc.vectorRanker != nil && len(queryEmbedding) > 0 {
		allRecords = rankByVector(ctx, allRecords, svc.vectorRanker, queryEmbedding, selection)
	} else if selection != nil && len(selection.Selected) > 0 && req.TaskDescriptor != "" {
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
	for _, rec := range selection.Selected {
		ranked = append(ranked, rec)
		seen[rec.ID] = struct{}{}
	}

	remaining := make([]*schema.MemoryRecord, 0, len(records))
	for _, rec := range records {
		if _, ok := seen[rec.ID]; ok {
			continue
		}
		remaining = append(remaining, rec)
	}
	SortBySalience(remaining)
	return append(ranked, remaining...)
}

// rankByVector re-orders records using vector similarity from the VectorRanker,
// while keeping selector-promoted competence/plan_graph records at the front.
func rankByVector(ctx context.Context, records []*schema.MemoryRecord, ranker VectorRanker, query []float32, selection *SelectionResult) []*schema.MemoryRecord {
	if len(records) == 0 {
		return records
	}

	// Search all stored embeddings so that records of the requested type(s)
	// aren't pushed out by globally-similar records of other types.
	// The intersection with byID below keeps only relevant records.
	searchLimit := len(records) * 10
	if searchLimit < 500 {
		searchLimit = 500
	}
	rankedIDs, err := ranker.SearchByEmbedding(ctx, query, searchLimit)
	if err != nil || len(rankedIDs) == 0 {
		// Fall back to salience if vector search fails.
		SortBySalience(records)
		return records
	}

	// Build an index of records by ID.
	byID := make(map[string]*schema.MemoryRecord, len(records))
	for _, rec := range records {
		byID[rec.ID] = rec
	}

	// Assign a vector rank score: top result gets 1.0, linearly decreasing.
	// Then blend with salience to produce a hybrid score.
	type scored struct {
		rec   *schema.MemoryRecord
		score float64
	}
	var matched []scored
	for rank, id := range rankedIDs {
		rec, ok := byID[id]
		if !ok {
			continue
		}
		// Vector similarity score: 1.0 for rank 0, decreasing.
		vecScore := 1.0
		if len(rankedIDs) > 1 {
			vecScore = 1.0 - float64(rank)/float64(len(rankedIDs))
		}
		// Hybrid: 70% vector similarity, 30% salience.
		hybrid := 0.7*vecScore + 0.3*rec.Salience
		matched = append(matched, scored{rec, hybrid})
	}

	// Sort by hybrid score descending.
	sort.Slice(matched, func(i, j int) bool {
		return matched[i].score > matched[j].score
	})

	result := make([]*schema.MemoryRecord, 0, len(records))
	seen := make(map[string]struct{}, len(matched))
	if selection != nil {
		for _, rec := range selection.Selected {
			if _, ok := byID[rec.ID]; !ok {
				continue
			}
			if _, ok := seen[rec.ID]; ok {
				continue
			}
			result = append(result, rec)
			seen[rec.ID] = struct{}{}
		}
	}
	for _, s := range matched {
		if _, ok := seen[s.rec.ID]; ok {
			continue
		}
		result = append(result, s.rec)
		seen[s.rec.ID] = struct{}{}
	}

	// Any records not in the vector search results go last, sorted by salience.
	var remainder []*schema.MemoryRecord
	for _, rec := range records {
		if _, ok := seen[rec.ID]; !ok {
			remainder = append(remainder, rec)
		}
	}
	sort.Slice(remainder, func(i, j int) bool {
		return remainder[i].Salience > remainder[j].Salience
	})
	result = append(result, remainder...)

	return result
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
