package revision

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

// Supersede atomically replaces an old record with a new one.
// The old record is retracted (salience set to 0, semantic status set to "retracted"),
// and the new record is linked to the old via provenance and a "supersedes" relation.
//
// Episodic records cannot be superseded (RFC Section 5).
// The entire operation is performed within a single transaction so that partial
// revisions are never externally visible (RFC 15.7).
func (s *Service) Supersede(ctx context.Context, oldID string, newRecord *schema.MemoryRecord, actor, rationale string) (*schema.MemoryRecord, error) {
	err := storage.WithTransaction(ctx, s.store, func(tx storage.Transaction) error {
		// Consolidate timestamp for entire transaction.
		now := time.Now().UTC()

		// 1. Get the old record and verify it is revisable.
		oldRec, err := tx.Get(ctx, oldID)
		if err != nil {
			return fmt.Errorf("get old record %s: %w", oldID, err)
		}
		if err := ensureRevisable(oldRec); err != nil {
			return err
		}

		// 1a. Ensure the new record has evidence if it's semantic.
		if err := ensureEvidence(newRecord); err != nil {
			return err
		}

		// Assign a new ID before linking the old record to the replacement.
		if newRecord.ID == "" {
			newRecord.ID = uuid.New().String()
		}

		// 2. Retract the old record (set salience to 0, mark semantic as retracted).
		retractRecord(oldRec)

		// For semantic payloads, also record the superseded-by link.
		if sp, ok := oldRec.Payload.(*schema.SemanticPayload); ok {
			sp.Revision.SupersededBy = newRecord.ID
		}

		oldRec.UpdatedAt = now
		if err := tx.Update(ctx, oldRec); err != nil {
			return fmt.Errorf("update old record %s: %w", oldID, err)
		}

		// 4. Set provenance to reference old record.
		newRecord.Provenance.Sources = append(newRecord.Provenance.Sources, schema.ProvenanceSource{
			Kind:      schema.ProvenanceKindEvent,
			Ref:       oldID,
			CreatedBy: actor,
			Timestamp: now,
		})

		// For semantic payloads, set the supersedes link.
		if sp, ok := newRecord.Payload.(*schema.SemanticPayload); ok {
			if sp.Revision == nil {
				sp.Revision = &schema.RevisionState{}
			}
			sp.Revision.Supersedes = oldID
			sp.Revision.Status = schema.RevisionStatusActive
		}

		// 5. Add "supersedes" relation from new -> old.
		newRecord.Relations = append(newRecord.Relations, schema.Relation{
			Predicate: "supersedes",
			TargetID:  oldID,
			Weight:    1.0,
			CreatedAt: now,
		})

		// 6. Add audit entries.
		newRecord.CreatedAt = now
		newRecord.UpdatedAt = now

		if err := tx.AddAuditEntry(ctx, oldID, newAuditEntry(
			schema.AuditActionRevise,
			actor,
			fmt.Sprintf("superseded by %s: %s", newRecord.ID, rationale),
			now,
		)); err != nil {
			return fmt.Errorf("add audit entry to old record %s: %w", oldID, err)
		}

		newRecord.AuditLog = append(newRecord.AuditLog, newAuditEntry(
			schema.AuditActionCreate,
			actor,
			fmt.Sprintf("supersedes %s: %s", oldID, rationale),
			now,
		))

		// 7. Store new record.
		if err := tx.Create(ctx, newRecord); err != nil {
			return fmt.Errorf("create new record %s: %w", newRecord.ID, err)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("supersede: %w", err)
	}
	s.embedRecord(ctx, newRecord)
	return newRecord, nil
}
