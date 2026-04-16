package revision

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

// Fork creates a new record derived from an existing source record.
// Unlike Supersede, both the source and the forked record remain active —
// this is intended for conditional variants, not replacement.
//
// Episodic records cannot be forked (RFC Section 5).
// The entire operation is performed within a single transaction so that partial
// revisions are never externally visible (RFC 15.7).
func (s *Service) Fork(ctx context.Context, sourceID string, forkedRecord *schema.MemoryRecord, actor, rationale string) (*schema.MemoryRecord, error) {
	err := storage.WithTransaction(ctx, s.store, func(tx storage.Transaction) error {
		// Consolidate timestamp for entire transaction.
		now := time.Now().UTC()

		// 1. Get the source record and verify it is revisable.
		sourceRec, err := tx.Get(ctx, sourceID)
		if err != nil {
			return fmt.Errorf("get source record %s: %w", sourceID, err)
		}
		if err := ensureRevisable(sourceRec); err != nil {
			return err
		}

		// 1a. Ensure the forked record has evidence if it's semantic.
		if err := ensureEvidence(forkedRecord); err != nil {
			return err
		}

		// 2. Assign a new ID if not already set.
		if forkedRecord.ID == "" {
			forkedRecord.ID = uuid.New().String()
		}

		// 3. Create "derived_from" relation to source.
		forkedRecord.Relations = append(forkedRecord.Relations, schema.Relation{
			Predicate: "derived_from",
			TargetID:  sourceID,
			Weight:    1.0,
			CreatedAt: now,
		})

		// 4. Set timestamps on forked record.
		forkedRecord.CreatedAt = now
		forkedRecord.UpdatedAt = now

		// 5. Add audit entries to both records.
		if err := tx.AddAuditEntry(ctx, sourceID, newAuditEntry(
			schema.AuditActionFork,
			actor,
			fmt.Sprintf("forked to %s: %s", forkedRecord.ID, rationale),
			now,
		)); err != nil {
			return fmt.Errorf("add audit entry to source record %s: %w", sourceID, err)
		}

		forkedRecord.AuditLog = append(forkedRecord.AuditLog, newAuditEntry(
			schema.AuditActionCreate,
			actor,
			fmt.Sprintf("forked from %s: %s", sourceID, rationale),
			now,
		))

		// 6. Store the forked record.
		if err := tx.Create(ctx, forkedRecord); err != nil {
			return fmt.Errorf("create forked record %s: %w", forkedRecord.ID, err)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("fork: %w", err)
	}
	s.embedRecord(ctx, forkedRecord)
	return forkedRecord, nil
}
