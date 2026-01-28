package revision

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/GustyCube/membrane/pkg/schema"
	"github.com/GustyCube/membrane/pkg/storage"
)

// Merge atomically combines multiple source records into a single merged record.
// All source records are retracted (salience set to 0, semantic status set to "retracted"),
// and the merged record is linked to each source via "derived_from" relations.
//
// Episodic records cannot be merged (RFC Section 5).
// The entire operation is performed within a single transaction so that partial
// revisions are never externally visible (RFC 15.7).
func (s *Service) Merge(ctx context.Context, recordIDs []string, mergedRecord *schema.MemoryRecord, actor, rationale string) (*schema.MemoryRecord, error) {
	if len(recordIDs) == 0 {
		return nil, fmt.Errorf("merge: no source record IDs provided")
	}

	err := storage.WithTransaction(ctx, s.store, func(tx storage.Transaction) error {
		// Consolidate timestamp for entire transaction.
		now := time.Now().UTC()

		// 1. Get all source records and verify they are revisable.
		sourceRecords := make([]*schema.MemoryRecord, 0, len(recordIDs))
		for _, id := range recordIDs {
			rec, err := tx.Get(ctx, id)
			if err != nil {
				return fmt.Errorf("get source record %s: %w", id, err)
			}
			if err := ensureRevisable(rec); err != nil {
				return err
			}
			sourceRecords = append(sourceRecords, rec)
		}

		// Validate evidence for semantic records.
		if err := ensureEvidence(mergedRecord); err != nil {
			return err
		}

		// 2. Retract all source records.
		for _, rec := range sourceRecords {
			retractRecord(rec)
			rec.UpdatedAt = now
			if err := tx.Update(ctx, rec); err != nil {
				return fmt.Errorf("update source record %s: %w", rec.ID, err)
			}
		}

		// 3. Assign a new ID to the merged record if not already set.
		if mergedRecord.ID == "" {
			mergedRecord.ID = uuid.New().String()
		}

		// Create "derived_from" relations to all source records.
		for _, id := range recordIDs {
			mergedRecord.Relations = append(mergedRecord.Relations, schema.Relation{
				Predicate: "derived_from",
				TargetID:  id,
				Weight:    1.0,
				CreatedAt: now,
			})
		}

		// 4. Add audit entries to all source records.
		for _, id := range recordIDs {
			if err := tx.AddAuditEntry(ctx, id, newAuditEntry(
				schema.AuditActionMerge,
				actor,
				fmt.Sprintf("merged into %s: %s", mergedRecord.ID, rationale),
				now,
			)); err != nil {
				return fmt.Errorf("add audit entry to source record %s: %w", id, err)
			}
		}

		// Set timestamps on merged record.
		mergedRecord.CreatedAt = now
		mergedRecord.UpdatedAt = now

		// Add "create" audit entry to merged record.
		mergedRecord.AuditLog = append(mergedRecord.AuditLog, newAuditEntry(
			schema.AuditActionCreate,
			actor,
			fmt.Sprintf("merged from %v: %s", recordIDs, rationale),
			now,
		))

		// 5. Store merged record.
		if err := tx.Create(ctx, mergedRecord); err != nil {
			return fmt.Errorf("create merged record %s: %w", mergedRecord.ID, err)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("merge: %w", err)
	}
	return mergedRecord, nil
}
