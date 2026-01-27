package consolidation

import (
	"context"
	"time"

	"github.com/GustyCube/membrane/pkg/schema"
	"github.com/GustyCube/membrane/pkg/storage"
)

// compressionAgeThreshold is the minimum age an episodic record must
// reach before it becomes eligible for compression. Records older than
// this have their salience reduced so that more recent experience is
// preferred during retrieval.
const compressionAgeThreshold = 24 * time.Hour

// compressedSalienceMultiplier is the factor applied to an episodic
// record's salience when it is compressed.
const compressedSalienceMultiplier = 0.5

// compressedSalienceFloor prevents salience from dropping below a
// minimum after compression so the record is not immediately pruned.
const compressedSalienceFloor = 0.05

// EpisodicConsolidator compresses old episodic records by reducing
// their salience. This ensures that raw experience fades over time
// while still remaining available for later retrieval or extraction.
type EpisodicConsolidator struct {
	store storage.Store
}

// NewEpisodicConsolidator creates an EpisodicConsolidator backed by store.
func NewEpisodicConsolidator(store storage.Store) *EpisodicConsolidator {
	return &EpisodicConsolidator{store: store}
}

// Consolidate finds episodic records older than the compression age
// threshold and reduces their salience. It returns the number of
// records that were compressed.
func (c *EpisodicConsolidator) Consolidate(ctx context.Context) (int, error) {
	records, err := c.store.ListByType(ctx, schema.MemoryTypeEpisodic)
	if err != nil {
		return 0, err
	}

	now := time.Now().UTC()
	compressed := 0

	for _, rec := range records {
		// Skip records that are not old enough.
		if now.Sub(rec.CreatedAt) < compressionAgeThreshold {
			continue
		}

		// Skip already-compressed records (salience at or below floor).
		if rec.Salience <= compressedSalienceFloor {
			continue
		}

		newSalience := rec.Salience * compressedSalienceMultiplier
		if newSalience < compressedSalienceFloor {
			newSalience = compressedSalienceFloor
		}

		err := storage.WithTransaction(ctx, c.store, func(tx storage.Transaction) error {
			if err := tx.UpdateSalience(ctx, rec.ID, newSalience); err != nil {
				return err
			}
			entry := schema.AuditEntry{
				Action:    schema.AuditActionDecay,
				Actor:     "consolidation/episodic",
				Timestamp: now,
				Rationale: "Episodic compression: record exceeded age threshold",
			}
			return tx.AddAuditEntry(ctx, rec.ID, entry)
		})
		if err != nil {
			return compressed, err
		}

		compressed++
	}

	return compressed, nil
}
