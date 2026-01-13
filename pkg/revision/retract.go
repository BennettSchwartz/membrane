package revision

import (
	"context"
	"fmt"
	"time"

	"github.com/GustyCube/membrane/pkg/schema"
	"github.com/GustyCube/membrane/pkg/storage"
)

// Retract marks a record as retracted without deleting it, preserving auditability.
// For semantic records, the revision status is set to "retracted".
// Salience is set to 0 for all record types.
//
// Episodic records cannot be retracted (RFC Section 5).
// The entire operation is performed within a single transaction so that partial
// revisions are never externally visible (RFC 15.7).
func (s *Service) Retract(ctx context.Context, id, actor, rationale string) error {
	err := storage.WithTransaction(ctx, s.store, func(tx storage.Transaction) error {
		// Consolidate timestamp for entire transaction.
		now := time.Now().UTC()

		// 1. Get the record and verify it is revisable.
		rec, err := tx.Get(ctx, id)
		if err != nil {
			return fmt.Errorf("get record %s: %w", id, err)
		}
		if err := ensureRevisable(rec); err != nil {
			return err
		}

		// 2-3. Mark as retracted: set salience to 0 and semantic status to retracted.
		retractRecord(rec)

		rec.UpdatedAt = now
		if err := tx.Update(ctx, rec); err != nil {
			return fmt.Errorf("update record %s: %w", id, err)
		}

		// 4. Add audit entry with "delete" action.
		if err := tx.AddAuditEntry(ctx, id, newAuditEntry(
			schema.AuditActionDelete,
			actor,
			rationale,
			now,
		)); err != nil {
			return fmt.Errorf("add audit entry to record %s: %w", id, err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("retract: %w", err)
	}
	return nil
}
