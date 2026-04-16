// Package metrics collects observability metrics from the memory substrate.
package metrics

import (
	"context"
	"fmt"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

// Collector gathers metrics from the underlying store.
type Collector struct {
	store storage.Store
}

// Snapshot is a point-in-time view of memory substrate metrics.
type Snapshot struct {
	CollectedAt          time.Time      `json:"collected_at"`
	TotalRecords         int            `json:"total_records"`
	RecordsByType        map[string]int `json:"records_by_type"`
	AvgSalience          float64        `json:"avg_salience"`
	AvgConfidence        float64        `json:"avg_confidence"`
	SalienceDistribution map[string]int `json:"salience_distribution"`
	ActiveRecords        int            `json:"active_records"`
	PinnedRecords        int            `json:"pinned_records"`
	TotalAuditEntries    int            `json:"total_audit_entries"`

	// RFC 15.10 metrics
	MemoryGrowthRate      float64 `json:"memory_growth_rate"`
	RetrievalUsefulness   float64 `json:"retrieval_usefulness"`
	CompetenceSuccessRate float64 `json:"competence_success_rate"`
	PlanReuseFrequency    float64 `json:"plan_reuse_frequency"`
	RevisionRate          float64 `json:"revision_rate"`
}

// NewCollector creates a new Collector backed by the given store.
func NewCollector(store storage.Store) *Collector {
	return &Collector{store: store}
}

// Collect queries the store and returns a metrics Snapshot.
func (c *Collector) Collect(ctx context.Context) (*Snapshot, error) {
	records, err := c.store.List(ctx, storage.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("metrics: list records: %w", err)
	}

	snap := &Snapshot{
		CollectedAt:   time.Now().UTC(),
		RecordsByType: make(map[string]int),
		SalienceDistribution: map[string]int{
			"0.0-0.2": 0,
			"0.2-0.4": 0,
			"0.4-0.6": 0,
			"0.6-0.8": 0,
			"0.8-1.0": 0,
		},
	}

	var totalSalience, totalConfidence float64

	// RFC 15.10 accumulators
	cutoff24h := time.Now().UTC().Add(-24 * time.Hour)
	var recentRecords int
	var reinforceCount, revisionCount, totalAuditCount int
	var competenceSuccessSum float64
	var competenceCount int
	var planExecSum float64
	var planCount int

	for _, rec := range records {
		snap.TotalRecords++
		snap.RecordsByType[string(rec.Type)]++

		totalSalience += rec.Salience
		totalConfidence += rec.Confidence

		// Active records have salience > 0.
		if rec.Salience > 0 {
			snap.ActiveRecords++
		}

		// Pinned records.
		if rec.Lifecycle.Pinned {
			snap.PinnedRecords++
		}

		// Salience distribution buckets.
		switch {
		case rec.Salience < 0.2:
			snap.SalienceDistribution["0.0-0.2"]++
		case rec.Salience < 0.4:
			snap.SalienceDistribution["0.2-0.4"]++
		case rec.Salience < 0.6:
			snap.SalienceDistribution["0.4-0.6"]++
		case rec.Salience < 0.8:
			snap.SalienceDistribution["0.6-0.8"]++
		default:
			snap.SalienceDistribution["0.8-1.0"]++
		}

		// Count audit entries.
		snap.TotalAuditEntries += len(rec.AuditLog)

		// RFC 15.10: Memory growth rate – count records created in last 24h.
		if rec.CreatedAt.After(cutoff24h) {
			recentRecords++
		}

		// RFC 15.10: Retrieval usefulness & revision rate – scan audit log.
		for _, entry := range rec.AuditLog {
			totalAuditCount++
			if entry.Action == schema.AuditActionReinforce {
				reinforceCount++
			}
			if entry.Action == schema.AuditActionRevise || entry.Action == schema.AuditActionFork || entry.Action == schema.AuditActionMerge {
				revisionCount++
			}
		}

		// RFC 15.10: Competence success rate.
		if cp, ok := rec.Payload.(*schema.CompetencePayload); ok && cp.Performance != nil {
			competenceSuccessSum += cp.Performance.SuccessRate
			competenceCount++
		}

		// RFC 15.10: Plan reuse frequency.
		if pg, ok := rec.Payload.(*schema.PlanGraphPayload); ok && pg.Metrics != nil {
			planExecSum += float64(pg.Metrics.ExecutionCount)
			planCount++
		}
	}

	if snap.TotalRecords > 0 {
		snap.AvgSalience = totalSalience / float64(snap.TotalRecords)
		snap.AvgConfidence = totalConfidence / float64(snap.TotalRecords)
		snap.MemoryGrowthRate = float64(recentRecords) / float64(snap.TotalRecords)
	}

	if totalAuditCount > 0 {
		snap.RetrievalUsefulness = float64(reinforceCount) / float64(totalAuditCount)
		snap.RevisionRate = float64(revisionCount) / float64(totalAuditCount)
	}

	if competenceCount > 0 {
		snap.CompetenceSuccessRate = competenceSuccessSum / float64(competenceCount)
	}

	if planCount > 0 {
		snap.PlanReuseFrequency = planExecSum / float64(planCount)
	}

	return snap, nil
}
