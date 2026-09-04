package revision

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

// Contest marks a record as contested, indicating conflicting evidence exists.
// The contestingRef identifies the conflicting record or evidence.
func (s *Service) Contest(ctx context.Context, id string, contestingRef string, actor, rationale string) error {
	return s.contest(ctx, id, contestingRef, actor, rationale, nil)
}

// ContestWithAccess treats a stored contesting reference as graph material
// only when canLink authorizes that existing record. Denied records retain the
// same external-evidence behavior as missing opaque references, preventing an
// authorization oracle while protecting the hidden record's graph.
func (s *Service) ContestWithAccess(ctx context.Context, id string, contestingRef string, actor, rationale string, canLink func(*schema.MemoryRecord) bool) error {
	return s.contest(ctx, id, contestingRef, actor, rationale, canLink)
}

func (s *Service) contest(ctx context.Context, id string, contestingRef string, actor, rationale string, canLink func(*schema.MemoryRecord) bool) error {
	return storage.WithTransaction(ctx, s.store, func(tx storage.Transaction) error {
		now := time.Now().UTC()
		record, err := tx.Get(ctx, id)
		if err != nil {
			return fmt.Errorf("contest: get record: %w", err)
		}
		if err := ensureRevisable(record); err != nil {
			return err
		}
		// Mark semantic records as contested, initializing revision state when needed.
		if sp, ok := record.Payload.(*schema.SemanticPayload); ok {
			if sp.Revision == nil {
				sp.Revision = &schema.RevisionState{}
			}
			sp.Revision.Status = schema.RevisionStatusContested
		}
		record.UpdatedAt = now
		if err := tx.Update(ctx, record); err != nil {
			return fmt.Errorf("contest: update record: %w", err)
		}
		// Add graph relations when the contesting reference names a stored record.
		// External evidence refs still contest the record, but relations require
		// MemoryRecord targets in Postgres.
		if contestingRef != "" {
			contestingRecord, err := tx.Get(ctx, contestingRef)
			if err != nil && !errors.Is(err, storage.ErrNotFound) {
				return fmt.Errorf("contest: get contesting record: %w", err)
			}
			if contestingRecord != nil && (canLink == nil || canLink(contestingRecord)) {
				if err := tx.AddRelation(ctx, id, schema.Relation{
					Predicate: schema.GraphPredicateContestedBy,
					TargetID:  contestingRef,
					Weight:    1.0,
					CreatedAt: now,
				}); err != nil {
					return fmt.Errorf("contest: add relation: %w", err)
				}
				if err := tx.AddRelation(ctx, contestingRef, schema.Relation{
					Predicate: schema.GraphPredicateContests,
					TargetID:  id,
					Weight:    1.0,
					CreatedAt: now,
				}); err != nil {
					return fmt.Errorf("contest: add inverse relation: %w", err)
				}
			}
		}
		return tx.AddAuditEntry(ctx, id, schema.AuditEntry{
			Action:    schema.AuditActionRevise,
			Actor:     actor,
			Timestamp: now,
			Rationale: rationale,
		})
	})
}
