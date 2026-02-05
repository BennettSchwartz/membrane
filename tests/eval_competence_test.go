package tests_test

import (
	"testing"
	"time"

	"github.com/GustyCube/membrane/pkg/retrieval"
	"github.com/GustyCube/membrane/pkg/schema"
)

func TestEvalCompetenceSelection(t *testing.T) {
	now := time.Now().UTC()
	selector := retrieval.NewSelector(0.2)

	best := evalCompetenceRecord("best", 0.95, 90, 10, now.Add(-2*time.Hour))
	mid := evalCompetenceRecord("mid", 0.70, 60, 40, now.Add(-7*24*time.Hour))
	worst := evalCompetenceRecord("worst", 0.40, 20, 80, now.Add(-30*24*time.Hour))

	result := selector.Select([]*schema.MemoryRecord{mid, worst, best})
	if len(result.Selected) != 3 {
		t.Fatalf("expected 3 selected, got %d", len(result.Selected))
	}
	if result.Selected[0].ID != best.ID {
		t.Fatalf("expected best ranked first, got %s", result.Selected[0].ID)
	}
	if result.NeedsMore {
		t.Fatalf("expected NeedsMore=false for clearly separated candidates (confidence=%.2f)", result.Confidence)
	}
}

func TestEvalPlanGraphSelection(t *testing.T) {
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
				ExecutionCount: 50,
				FailureRate:    0.1,
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
				ExecutionCount: 5,
				FailureRate:    0.6,
			},
		},
	)
	planB.Confidence = 0.5
	planB.Lifecycle.LastReinforcedAt = now.Add(-14 * 24 * time.Hour)

	result := selector.Select([]*schema.MemoryRecord{planB, planA})
	if len(result.Selected) != 2 {
		t.Fatalf("expected 2 selected, got %d", len(result.Selected))
	}
	if result.Selected[0].ID != planA.ID {
		t.Fatalf("expected plan-a ranked first, got %s", result.Selected[0].ID)
	}
}

func evalCompetenceRecord(id string, confidence float64, successCount, failureCount int64, reinforcedAt time.Time) *schema.MemoryRecord {
	rec := schema.NewMemoryRecord(id, schema.MemoryTypeCompetence, schema.SensitivityLow,
		&schema.CompetencePayload{
			Kind:      "competence",
			SkillName: "skill-" + id,
			Triggers: []schema.Trigger{
				{Signal: "eval"},
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
