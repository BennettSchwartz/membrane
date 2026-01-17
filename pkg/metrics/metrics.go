// Package metrics collects observability metrics from the memory substrate.
package metrics

import (
	"context"
	"fmt"

	"github.com/GustyCube/membrane/pkg/storage"
)

// Collector gathers metrics from the underlying store.
type Collector struct {
	store storage.Store
}

// Snapshot is a point-in-time view of memory substrate metrics.
type Snapshot struct {
	TotalRecords         int            `json:"total_records"`
	RecordsByType        map[string]int `json:"records_by_type"`
	AvgSalience          float64        `json:"avg_salience"`
	AvgConfidence        float64        `json:"avg_confidence"`
	SalienceDistribution map[string]int `json:"salience_distribution"`
	ActiveRecords        int            `json:"active_records"`
	PinnedRecords        int            `json:"pinned_records"`
	TotalAuditEntries    int            `json:"total_audit_entries"`
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
	}

	if snap.TotalRecords > 0 {
		snap.AvgSalience = totalSalience / float64(snap.TotalRecords)
		snap.AvgConfidence = totalConfidence / float64(snap.TotalRecords)
	}

	return snap, nil
}
