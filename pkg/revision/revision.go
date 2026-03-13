package revision

import (
	"context"
	"errors"
	"fmt"

	"github.com/GustyCube/membrane/pkg/schema"
	"github.com/GustyCube/membrane/pkg/storage"
)

// Common errors returned by revision operations.
var (
	// ErrEpisodicImmutable is returned when attempting to revise an episodic record.
	// RFC Section 5: Episodic memory is append-only and MUST NOT be revised.
	ErrEpisodicImmutable = errors.New("episodic memory is immutable and cannot be revised")

	// ErrRecordNotFound is returned when a referenced record does not exist.
	ErrRecordNotFound = storage.ErrNotFound
)

// Service provides revision operations for memory records.
// All operations are atomic — partial revisions are never externally visible (RFC 15.7).
type Service struct {
	store    storage.Store
	embedder Embedder
}

// NewService creates a new revision Service backed by the given Store.
func NewService(store storage.Store) *Service {
	return &Service{store: store}
}

// Embedder stores embeddings for durable records when available.
type Embedder interface {
	EmbedRecord(ctx context.Context, rec *schema.MemoryRecord) error
}

// NewServiceWithEmbedder creates a new revision Service with best-effort embedding support.
func NewServiceWithEmbedder(store storage.Store, embedder Embedder) *Service {
	return &Service{store: store, embedder: embedder}
}

// ensureRevisable checks that a record exists and is not episodic.
// Returns an error if the record cannot be revised.
func ensureRevisable(rec *schema.MemoryRecord) error {
	if rec.Type == schema.MemoryTypeEpisodic {
		return fmt.Errorf("%w: record %s", ErrEpisodicImmutable, rec.ID)
	}
	return nil
}

// ensureEvidence checks that semantic records being created via revision
// include at least one evidence reference in their payload.
func ensureEvidence(rec *schema.MemoryRecord) error {
	if rec.Type != schema.MemoryTypeSemantic {
		return nil
	}
	sp, ok := rec.Payload.(*schema.SemanticPayload)
	if !ok {
		return nil
	}
	if len(sp.Evidence) == 0 && len(rec.Provenance.Sources) == 0 {
		return fmt.Errorf("semantic revision requires evidence: record %s has no evidence references or provenance sources", rec.ID)
	}
	return nil
}

// retractRecord marks a record as retracted by setting salience to 0 and,
// for semantic records, setting the revision status to "retracted".
func retractRecord(rec *schema.MemoryRecord) {
	rec.Salience = 0
	if sp, ok := rec.Payload.(*schema.SemanticPayload); ok {
		if sp.Revision == nil {
			sp.Revision = &schema.RevisionState{}
		}
		sp.Revision.Status = schema.RevisionStatusRetracted
	}
}

func (s *Service) embedRecord(ctx context.Context, rec *schema.MemoryRecord) {
	if s.embedder == nil || rec == nil {
		return
	}
	_ = s.embedder.EmbedRecord(ctx, rec)
}
