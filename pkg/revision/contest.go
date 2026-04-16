package revision

import (
	"context"
	"fmt"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

// Contest marks a record as contested, indicating conflicting evidence exists.
// The contestingRef identifies the conflicting record or evidence.
func (s *Service) Contest(ctx context.Context, id string, contestingRef string, actor, rationale string) error {
	return storage.WithTransaction(ctx, s.store, func(tx storage.Transaction) error {
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
		record.UpdatedAt = time.Now().UTC()
		if err := tx.Update(ctx, record); err != nil {
			return fmt.Errorf("contest: update record: %w", err)
		}
		// Add relation to the contesting record
		if contestingRef != "" {
			if err := tx.AddRelation(ctx, id, schema.Relation{
				Predicate: "contested_by",
				TargetID:  contestingRef,
				Weight:    1.0,
			}); err != nil {
				return fmt.Errorf("contest: add relation: %w", err)
			}
		}
		return tx.AddAuditEntry(ctx, id, schema.AuditEntry{
			Action:    schema.AuditActionRevise,
			Actor:     actor,
			Timestamp: time.Now().UTC(),
			Rationale: rationale,
		})
	})
}
