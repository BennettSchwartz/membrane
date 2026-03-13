package tests_test

import (
	"context"
	"testing"
	"time"

	"github.com/GustyCube/membrane/pkg/retrieval"
	"github.com/GustyCube/membrane/pkg/schema"
)

// makeCompetenceRecord builds a competence MemoryRecord with the given parameters.
func makeCompetenceRecord(t *testing.T, id string, confidence float64, successCount, failureCount int64, reinforcedAt time.Time) *schema.MemoryRecord {
	t.Helper()
	rec := schema.NewMemoryRecord(id, schema.MemoryTypeCompetence, schema.SensitivityLow,
		&schema.CompetencePayload{
			Kind:      "competence",
			SkillName: "skill-" + id,
			Triggers: []schema.Trigger{
				{Signal: "test-signal"},
			},
			Recipe: []schema.RecipeStep{
				{Step: "do the thing"},
			},
			Performance: &schema.PerformanceStats{
				SuccessCount: successCount,
				FailureCount: failureCount,
				SuccessRate:  float64(successCount) / float64(successCount+failureCount),
			},
		},
	)
	rec.Confidence = confidence
	rec.Lifecycle.LastReinforcedAt = reinforcedAt
	return rec
}

func TestSelectorRanksByApplicabilitySuccessRecency(t *testing.T) {
	now := time.Now().UTC()
	selector := retrieval.NewSelector(0.3)

	// Candidate A: high confidence, high success rate, recently reinforced.
	candA := makeCompetenceRecord(t, "cand-a", 0.95, 90, 10, now.Add(-1*time.Hour))

	// Candidate B: medium confidence, medium success rate, older.
	candB := makeCompetenceRecord(t, "cand-b", 0.70, 50, 50, now.Add(-15*24*time.Hour))

	// Candidate C: low confidence, low success rate, very old.
	candC := makeCompetenceRecord(t, "cand-c", 0.40, 20, 80, now.Add(-60*24*time.Hour))

	result := selector.Select(context.Background(), []*schema.MemoryRecord{candC, candA, candB}, nil)

	// Verify candidate A is ranked first (best overall score).
	if len(result.Selected) != 3 {
		t.Fatalf("expected 3 selected, got %d", len(result.Selected))
	}
	if result.Selected[0].ID != "cand-a" {
		t.Errorf("expected cand-a ranked first, got %s", result.Selected[0].ID)
	}

	// Verify ordering: A > B > C.
	if result.Selected[1].ID != "cand-b" {
		t.Errorf("expected cand-b ranked second, got %s", result.Selected[1].ID)
	}
	if result.Selected[2].ID != "cand-c" {
		t.Errorf("expected cand-c ranked third, got %s", result.Selected[2].ID)
	}

	// Verify confidence is positive when there's a clear winner.
	if result.Confidence <= 0 {
		t.Errorf("expected positive confidence when candidates are well-separated, got %.4f", result.Confidence)
	}
}

func TestSelectorNeedsMoreBelowThreshold(t *testing.T) {
	now := time.Now().UTC()

	// Use a high threshold.
	selector := retrieval.NewSelector(0.9)

	// Two nearly identical candidates - the gap should be small, producing low confidence.
	candA := makeCompetenceRecord(t, "close-a", 0.80, 70, 30, now.Add(-2*time.Hour))
	candB := makeCompetenceRecord(t, "close-b", 0.78, 68, 32, now.Add(-3*time.Hour))

	result := selector.Select(context.Background(), []*schema.MemoryRecord{candA, candB}, nil)

	// With near-identical scores, confidence should be below the high threshold.
	if !result.NeedsMore {
		t.Errorf("expected NeedsMore=true when candidates are close and threshold is high, confidence=%.4f", result.Confidence)
	}
}

func TestSelectorSingleCandidate(t *testing.T) {
	now := time.Now().UTC()
	selector := retrieval.NewSelector(0.3)

	cand := makeCompetenceRecord(t, "solo", 0.90, 80, 20, now.Add(-1*time.Hour))
	result := selector.Select(context.Background(), []*schema.MemoryRecord{cand}, nil)

	if len(result.Selected) != 1 {
		t.Fatalf("expected 1 selected, got %d", len(result.Selected))
	}
	if result.Selected[0].ID != "solo" {
		t.Errorf("expected solo, got %s", result.Selected[0].ID)
	}

	// Single candidate should have confidence based on its score.
	if result.Confidence <= 0 {
		t.Errorf("expected positive confidence for single candidate, got %.4f", result.Confidence)
	}
}

func TestSelectorEmpty(t *testing.T) {
	selector := retrieval.NewSelector(0.3)
	result := selector.Select(context.Background(), []*schema.MemoryRecord{}, nil)

	if len(result.Selected) != 0 {
		t.Errorf("expected 0 selected for empty input, got %d", len(result.Selected))
	}
	if result.Confidence != 0 {
		t.Errorf("expected confidence=0 for empty input, got %.4f", result.Confidence)
	}
	if !result.NeedsMore {
		t.Error("expected NeedsMore=true for empty input")
	}
}

func TestSelectorPlanGraphRecords(t *testing.T) {
	now := time.Now().UTC()
	selector := retrieval.NewSelector(0.5)

	planA := schema.NewMemoryRecord("plan-a", schema.MemoryTypePlanGraph, schema.SensitivityLow,
		&schema.PlanGraphPayload{
			Kind:    "plan_graph",
			PlanID:  "plan-a",
			Version: "1.0",
			Nodes:   []schema.PlanNode{{ID: "n1", Op: "build"}},
			Edges:   []schema.PlanEdge{},
			Metrics: &schema.PlanMetrics{
				ExecutionCount: 100,
				FailureRate:    0.05,
			},
		},
	)
	planA.Confidence = 0.9
	planA.Lifecycle.LastReinforcedAt = now.Add(-1 * time.Hour)

	planB := schema.NewMemoryRecord("plan-b", schema.MemoryTypePlanGraph, schema.SensitivityLow,
		&schema.PlanGraphPayload{
			Kind:    "plan_graph",
			PlanID:  "plan-b",
			Version: "1.0",
			Nodes:   []schema.PlanNode{{ID: "n1", Op: "test"}},
			Edges:   []schema.PlanEdge{},
			Metrics: &schema.PlanMetrics{
				ExecutionCount: 10,
				FailureRate:    0.5,
			},
		},
	)
	planB.Confidence = 0.5
	planB.Lifecycle.LastReinforcedAt = now.Add(-30 * 24 * time.Hour)

	result := selector.Select(context.Background(), []*schema.MemoryRecord{planB, planA}, nil)

	if len(result.Selected) != 2 {
		t.Fatalf("expected 2 selected, got %d", len(result.Selected))
	}

	// plan-a should be ranked first: higher confidence, better success rate, more recent.
	if result.Selected[0].ID != "plan-a" {
		t.Errorf("expected plan-a ranked first, got %s", result.Selected[0].ID)
	}
}

type stubEmbeddingProvider struct {
	scores map[string]float64
}

func (s stubEmbeddingProvider) Similarity(_ context.Context, recordID string, _ []float32) (float64, bool) {
	score, ok := s.scores[recordID]
	return score, ok
}

func TestSelectorUsesEmbeddingApplicabilityWhenAvailable(t *testing.T) {
	now := time.Now().UTC()
	selector := retrieval.NewSelectorWithEmbedding(0.2, stubEmbeddingProvider{
		scores: map[string]float64{
			"better-match": 0.95,
			"worse-match":  0.10,
		},
	})

	betterMatch := makeCompetenceRecord(t, "better-match", 0.40, 70, 30, now.Add(-2*time.Hour))
	worseMatch := makeCompetenceRecord(t, "worse-match", 0.95, 70, 30, now.Add(-2*time.Hour))

	result := selector.Select(context.Background(), []*schema.MemoryRecord{worseMatch, betterMatch}, []float32{1, 0, 0})
	if result.Selected[0].ID != "better-match" {
		t.Fatalf("expected embedding match to outrank confidence proxy, got %s", result.Selected[0].ID)
	}
}
