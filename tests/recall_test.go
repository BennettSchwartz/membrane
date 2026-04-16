package tests_test

import (
	"context"
	"testing"

	"github.com/BennettSchwartz/membrane/pkg/ingestion"
	"github.com/BennettSchwartz/membrane/pkg/membrane"
	"github.com/BennettSchwartz/membrane/pkg/retrieval"
	"github.com/BennettSchwartz/membrane/pkg/schema"
)

type recallCase struct {
	name        string
	trust       *retrieval.TrustContext
	minSalience float64
	limit       int
	expected    []string
}

func TestRetrievalRecallAtK(t *testing.T) {
	ctx := context.Background()
	m := newTestMembrane(t)

	recA := mustIngestObservation(t, ctx, m, "project:alpha", schema.SensitivityLow)
	recB := mustIngestObservation(t, ctx, m, "project:alpha", schema.SensitivityLow)
	recC := mustIngestObservation(t, ctx, m, "project:alpha", schema.SensitivityHigh)
	recD := mustIngestObservation(t, ctx, m, "project:beta", schema.SensitivityLow)
	recE := mustIngestObservation(t, ctx, m, "", schema.SensitivityLow)
	recF := mustIngestObservation(t, ctx, m, "project:alpha", schema.SensitivityLow)

	// Shape salience to produce deterministic ordering in retrieval results.
	mustPenalize(t, ctx, m, recB.ID, 0.10)
	mustPenalize(t, ctx, m, recC.ID, 0.05)
	mustPenalize(t, ctx, m, recD.ID, 0.15)
	mustPenalize(t, ctx, m, recE.ID, 0.80)
	mustPenalize(t, ctx, m, recF.ID, 0.95)

	cases := []recallCase{
		{
			name:        "scoped-low-sensitivity",
			trust:       retrieval.NewTrustContext(schema.SensitivityLow, true, "test-actor", []string{"project:alpha"}),
			minSalience: 0.2,
			limit:       2,
			expected:    []string{recA.ID, recB.ID},
		},
		{
			name:        "scoped-high-sensitivity",
			trust:       retrieval.NewTrustContext(schema.SensitivityHigh, true, "test-actor", []string{"project:alpha"}),
			minSalience: 0,
			limit:       3,
			expected:    []string{recA.ID, recC.ID, recB.ID},
		},
		{
			name:        "global-min-salience",
			trust:       retrieval.NewTrustContext(schema.SensitivityLow, true, "test-actor", nil),
			minSalience: 0.8,
			limit:       3,
			expected:    []string{recA.ID, recB.ID, recD.ID},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := retrieveRecords(ctx, m, &retrieval.RetrieveRequest{
				Trust:       tc.trust,
				MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
				MinSalience: tc.minSalience,
				Limit:       tc.limit,
			})
			if err != nil {
				t.Fatalf("Retrieve: %v", err)
			}

			got := recordIDs(resp.Records)
			if len(got) != tc.limit {
				t.Fatalf("expected %d records, got %d", tc.limit, len(got))
			}

			if !equalStrings(got, tc.expected) {
				t.Fatalf("unexpected ordering: got=%v expected=%v", got, tc.expected)
			}

			recall := recallAtK(got, tc.expected, tc.limit)
			precision := precisionAtK(got, tc.expected, tc.limit)
			if recall < 1.0 || precision < 1.0 {
				t.Fatalf("expected perfect recall/precision, got recall=%.2f precision=%.2f", recall, precision)
			}
		})
	}
}

func mustIngestObservation(t *testing.T, ctx context.Context, m *membrane.Membrane, scope string, sensitivity schema.Sensitivity) *schema.MemoryRecord {
	t.Helper()
	rec, err := captureObservationRecord(ctx, m, ingestion.IngestObservationRequest{
		Source:      "test",
		Subject:     "system",
		Predicate:   "observed",
		Object:      "signal",
		Tags:        []string{"recall"},
		Scope:       scope,
		Sensitivity: sensitivity,
	})
	if err != nil {
		t.Fatalf("IngestObservation: %v", err)
	}
	return rec
}

func mustPenalize(t *testing.T, ctx context.Context, m *membrane.Membrane, id string, amount float64) {
	t.Helper()
	if err := m.Penalize(ctx, id, amount, "test", "salience shaping"); err != nil {
		t.Fatalf("Penalize(%s): %v", id, err)
	}
}

func recordIDs(records []*schema.MemoryRecord) []string {
	ids := make([]string, 0, len(records))
	for _, rec := range records {
		ids = append(ids, rec.ID)
	}
	return ids
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func recallAtK(got, expected []string, k int) float64 {
	if len(expected) == 0 {
		return 1
	}
	relevant := make(map[string]struct{}, len(expected))
	for _, id := range expected {
		relevant[id] = struct{}{}
	}

	found := 0
	for i := 0; i < len(got) && i < k; i++ {
		if _, ok := relevant[got[i]]; ok {
			found++
		}
	}
	return float64(found) / float64(len(expected))
}

func precisionAtK(got, expected []string, k int) float64 {
	if k == 0 {
		return 1
	}
	relevant := make(map[string]struct{}, len(expected))
	for _, id := range expected {
		relevant[id] = struct{}{}
	}

	found := 0
	limit := k
	if limit > len(got) {
		limit = len(got)
	}
	for i := 0; i < limit; i++ {
		if _, ok := relevant[got[i]]; ok {
			found++
		}
	}
	return float64(found) / float64(limit)
}
