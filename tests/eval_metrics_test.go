package tests_test

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/BennettSchwartz/membrane/pkg/metrics"
	"github.com/BennettSchwartz/membrane/pkg/schema"
)

func TestEvalMetricsSnapshot(t *testing.T) {
	ctx := context.Background()
	store := newEvalStore(t)
	now := time.Now().UTC()

	rec1 := schema.NewMemoryRecord(uuid.NewString(), schema.MemoryTypeSemantic, schema.SensitivityLow,
		&schema.SemanticPayload{
			Kind:      "semantic",
			Subject:   "service",
			Predicate: "uses",
			Object:    "postgres",
			Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
		},
	)
	rec1.Salience = 1.0
	rec1.Confidence = 0.9
	rec1.AuditLog = []schema.AuditEntry{
		{Action: schema.AuditActionCreate, Actor: "eval", Timestamp: now, Rationale: "create"},
		{Action: schema.AuditActionReinforce, Actor: "eval", Timestamp: now, Rationale: "reinforce"},
	}

	rec2 := schema.NewMemoryRecord(uuid.NewString(), schema.MemoryTypeCompetence, schema.SensitivityLow,
		&schema.CompetencePayload{
			Kind:          "competence",
			SkillName:     "skill:build",
			Triggers:      []schema.Trigger{{Signal: "build"}},
			Recipe:        []schema.RecipeStep{{Step: "do build"}},
			RequiredTools: []string{"bash"},
			Performance: &schema.PerformanceStats{
				SuccessCount: 8,
				FailureCount: 2,
				SuccessRate:  0.8,
			},
			Version: "1",
		},
	)
	rec2.Salience = 0.5
	rec2.Confidence = 0.8
	rec2.AuditLog = []schema.AuditEntry{
		{Action: schema.AuditActionCreate, Actor: "eval", Timestamp: now, Rationale: "create"},
		{Action: schema.AuditActionMerge, Actor: "eval", Timestamp: now, Rationale: "merge"},
	}

	rec3 := schema.NewMemoryRecord(uuid.NewString(), schema.MemoryTypePlanGraph, schema.SensitivityLow,
		&schema.PlanGraphPayload{
			Kind:    "plan_graph",
			PlanID:  "plan-1",
			Version: "1",
			Nodes:   []schema.PlanNode{{ID: "n1", Op: "deploy"}},
			Edges:   []schema.PlanEdge{},
			Metrics: &schema.PlanMetrics{ExecutionCount: 3, FailureRate: 0.0},
		},
	)
	rec3.Salience = 0.8
	rec3.Confidence = 0.85
	rec3.AuditLog = []schema.AuditEntry{
		{Action: schema.AuditActionCreate, Actor: "eval", Timestamp: now, Rationale: "create"},
	}

	for _, rec := range []*schema.MemoryRecord{rec1, rec2, rec3} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("store.Create: %v", err)
		}
	}

	collector := metrics.NewCollector(store)
	snap, err := collector.Collect(ctx)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}

	if snap.TotalRecords != 3 {
		t.Fatalf("expected total_records=3, got %d", snap.TotalRecords)
	}
	if snap.RecordsByType["semantic"] != 1 || snap.RecordsByType["competence"] != 1 || snap.RecordsByType["plan_graph"] != 1 {
		t.Fatalf("unexpected records_by_type: %#v", snap.RecordsByType)
	}
	if snap.ActiveRecords != 3 {
		t.Fatalf("expected active_records=3, got %d", snap.ActiveRecords)
	}

	expectedAvgSalience := (1.0 + 0.5 + 0.8) / 3.0
	if math.Abs(snap.AvgSalience-expectedAvgSalience) > 0.0001 {
		t.Fatalf("expected avg_salience %.4f, got %.4f", expectedAvgSalience, snap.AvgSalience)
	}

	// Total audit entries: 5, with 1 reinforce and 1 merge.
	if snap.TotalAuditEntries != 5 {
		t.Fatalf("expected total_audit_entries=5, got %d", snap.TotalAuditEntries)
	}
	if math.Abs(snap.RetrievalUsefulness-0.2) > 0.0001 {
		t.Fatalf("expected retrieval_usefulness=0.2, got %.4f", snap.RetrievalUsefulness)
	}
	if math.Abs(snap.RevisionRate-0.2) > 0.0001 {
		t.Fatalf("expected revision_rate=0.2, got %.4f", snap.RevisionRate)
	}
	if math.Abs(snap.CompetenceSuccessRate-0.8) > 0.0001 {
		t.Fatalf("expected competence_success_rate=0.8, got %.4f", snap.CompetenceSuccessRate)
	}
	if math.Abs(snap.PlanReuseFrequency-3.0) > 0.0001 {
		t.Fatalf("expected plan_reuse_frequency=3.0, got %.4f", snap.PlanReuseFrequency)
	}
}
