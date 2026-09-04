package membrane

import (
	"context"
	"errors"
	"testing"

	"github.com/BennettSchwartz/membrane/pkg/retrieval"
	"github.com/BennettSchwartz/membrane/pkg/schema"
)

func TestGetMetricsForTrustRejectsNilTrust(t *testing.T) {
	m := newTestMembrane(t)
	if _, err := m.GetMetricsForTrust(context.Background(), nil); !errors.Is(err, retrieval.ErrNilTrust) {
		t.Fatalf("GetMetricsForTrust nil error = %v, want ErrNilTrust", err)
	}
}

func TestGetMetricsForTrustExcludesUnauthorizedRecords(t *testing.T) {
	m := newTestMembrane(t)
	ctx := context.Background()

	for _, record := range []*schema.MemoryRecord{
		metricsTestRecord("allowed-low", "project:alpha", schema.SensitivityLow),
		metricsTestRecord("wrong-scope", "project:beta", schema.SensitivityLow),
		metricsTestRecord("too-sensitive", "project:alpha", schema.SensitivityHigh),
	} {
		if err := m.store.Create(ctx, record); err != nil {
			t.Fatalf("Create %s: %v", record.ID, err)
		}
	}

	snapshot, err := m.GetMetricsForTrust(ctx, retrieval.NewTrustContext(
		schema.SensitivityLow,
		true,
		"grpc",
		[]string{"project:alpha"},
	))
	if err != nil {
		t.Fatalf("GetMetricsForTrust: %v", err)
	}
	if snapshot.TotalRecords != 1 || snapshot.RecordsByType[string(schema.MemoryTypeEpisodic)] != 1 {
		t.Fatalf("policy-filtered snapshot = %+v, want only allowed-low", snapshot)
	}
}

func metricsTestRecord(id, scope string, sensitivity schema.Sensitivity) *schema.MemoryRecord {
	record := schema.NewMemoryRecord(id, schema.MemoryTypeEpisodic, sensitivity, &schema.EpisodicPayload{
		Kind: "episodic",
	})
	record.Scope = scope
	return record
}
