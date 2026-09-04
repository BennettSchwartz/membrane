package revision

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

// Merge atomically combines multiple source records into a single merged record.
// All source records are retracted (salience set to 0, semantic status set to "retracted"),
// and the merged record is linked to each source via derived_from relations.
//
// Episodic records cannot be merged (RFC Section 5).
// The entire operation is performed within a single transaction so that partial
// revisions are never externally visible (RFC 15.7).
func (s *Service) Merge(ctx context.Context, recordIDs []string, mergedRecord *schema.MemoryRecord, actor, rationale string) (*schema.MemoryRecord, error) {
	if err := validateMergeSourceIDs(recordIDs); err != nil {
		return nil, err
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
		normalizeNewRecordMetadata(mergedRecord, actor, now, sourceRecords[0].Lifecycle)

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

		// Create derived_from relations to all source records.
		for _, id := range recordIDs {
			mergedRecord.Relations = append(mergedRecord.Relations, schema.Relation{
				Predicate: schema.GraphPredicateDerivedFrom,
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
		markSemanticActive(mergedRecord)

		// Add "create" audit entry to merged record.
		mergedRecord.AuditLog = []schema.AuditEntry{newAuditEntry(
			schema.AuditActionCreate,
			actor,
			fmt.Sprintf("merged from %v: %s", recordIDs, rationale),
			now,
		)}

		// 5. Store merged record.
		if err := tx.Create(ctx, mergedRecord); err != nil {
			return fmt.Errorf("create merged record %s: %w", mergedRecord.ID, err)
		}
		for _, id := range recordIDs {
			if err := tx.AddRelation(ctx, id, schema.Relation{
				Predicate: schema.GraphPredicateDerivedSemantic,
				TargetID:  mergedRecord.ID,
				Weight:    1.0,
				CreatedAt: now,
			}); err != nil {
				return fmt.Errorf("add derived_semantic relation to source record %s: %w", id, err)
			}
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("merge: %w", err)
	}
	s.embedRecord(ctx, mergedRecord)
	return mergedRecord, nil
}

func validateMergeSourceIDs(recordIDs []string) error {
	if len(recordIDs) == 0 {
		return fmt.Errorf("merge: no source record IDs provided")
	}
	seen := make(map[string]struct{}, len(recordIDs))
	for i, id := range recordIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			return fmt.Errorf("merge: source record ID at index %d is required", i)
		}
		if _, ok := seen[id]; ok {
			return fmt.Errorf("merge: duplicate source record ID %q", id)
		}
		seen[id] = struct{}{}
	}
	return nil
}
