package tests_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/BennettSchwartz/membrane/pkg/consolidation"
	"github.com/BennettSchwartz/membrane/pkg/ingestion"
	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
	"github.com/BennettSchwartz/membrane/pkg/storage/sqlite"
)

func TestEvalConsolidationSemantic(t *testing.T) {
	ctx := context.Background()
	store := newEvalStore(t)
	ingest := newEvalIngestionService(store)

	episode, err := ingest.IngestEvent(ctx, ingestion.IngestEventRequest{
		Source:    "eval",
		EventKind: "latency_check",
		Ref:       "evt-1",
		Summary:   "p95 latency 180ms",
		Tags:      []string{"eval"},
	})
	if err != nil {
		t.Fatalf("IngestEvent: %v", err)
	}

	_, err = ingest.IngestOutcome(ctx, ingestion.IngestOutcomeRequest{
		Source:         "eval",
		TargetRecordID: episode.ID,
		OutcomeStatus:  schema.OutcomeStatusSuccess,
	})
	if err != nil {
		t.Fatalf("IngestOutcome: %v", err)
	}

	consol := consolidation.NewSemanticConsolidator(store)
	created, reinforced, err := consol.Consolidate(ctx)
	if err != nil {
		t.Fatalf("Consolidate: %v", err)
	}
	if created != 1 || reinforced != 0 {
		t.Fatalf("expected created=1 reinforced=0, got created=%d reinforced=%d", created, reinforced)
	}

	semantics, err := store.ListByType(ctx, schema.MemoryTypeSemantic)
	if err != nil {
		t.Fatalf("ListByType semantic: %v", err)
	}
	if len(semantics) != 1 {
		t.Fatalf("expected 1 semantic record, got %d", len(semantics))
	}
	sp, ok := semantics[0].Payload.(*schema.SemanticPayload)
	if !ok {
		t.Fatalf("semantic payload missing")
	}
	if sp.Subject != "latency_check" || sp.Predicate != "observed_in" {
		t.Fatalf("unexpected semantic payload: subject=%s predicate=%s", sp.Subject, sp.Predicate)
	}

	rels, err := store.GetRelations(ctx, semantics[0].ID)
	if err != nil {
		t.Fatalf("GetRelations: %v", err)
	}
	if !hasRelation(rels, "derived_from", episode.ID) {
		t.Fatalf("expected derived_from relation to %s", episode.ID)
	}
}

func TestEvalConsolidationSemanticReinforce(t *testing.T) {
	ctx := context.Background()
	store := newEvalStore(t)
	ingest := newEvalIngestionService(store)

	ep1, err := ingest.IngestEvent(ctx, ingestion.IngestEventRequest{
		Source:    "eval",
		EventKind: "deploy",
		Ref:       "evt-1",
		Summary:   "deployed service",
		Tags:      []string{"eval"},
	})
	if err != nil {
		t.Fatalf("IngestEvent 1: %v", err)
	}
	_, err = ingest.IngestOutcome(ctx, ingestion.IngestOutcomeRequest{
		Source:         "eval",
		TargetRecordID: ep1.ID,
		OutcomeStatus:  schema.OutcomeStatusSuccess,
	})
	if err != nil {
		t.Fatalf("IngestOutcome 1: %v", err)
	}

	ep2, err := ingest.IngestEvent(ctx, ingestion.IngestEventRequest{
		Source:    "eval",
		EventKind: "deploy",
		Ref:       "evt-2",
		Summary:   "deployed service",
		Tags:      []string{"eval"},
	})
	if err != nil {
		t.Fatalf("IngestEvent 2: %v", err)
	}
	_, err = ingest.IngestOutcome(ctx, ingestion.IngestOutcomeRequest{
		Source:         "eval",
		TargetRecordID: ep2.ID,
		OutcomeStatus:  schema.OutcomeStatusSuccess,
	})
	if err != nil {
		t.Fatalf("IngestOutcome 2: %v", err)
	}

	consol := consolidation.NewSemanticConsolidator(store)
	created, reinforced, err := consol.Consolidate(ctx)
	if err != nil {
		t.Fatalf("Consolidate: %v", err)
	}
	if created != 1 || reinforced != 1 {
		t.Fatalf("expected created=1 reinforced=1, got created=%d reinforced=%d", created, reinforced)
	}

	semantics, err := store.ListByType(ctx, schema.MemoryTypeSemantic)
	if err != nil {
		t.Fatalf("ListByType semantic: %v", err)
	}
	if len(semantics) != 1 {
		t.Fatalf("expected 1 semantic record, got %d", len(semantics))
	}
	if !auditContains(semantics[0].AuditLog, schema.AuditActionReinforce) {
		t.Fatalf("expected reinforce audit entry after duplicate consolidation")
	}
}

func TestEvalConsolidationCompetence(t *testing.T) {
	ctx := context.Background()
	store := newEvalStore(t)
	ingest := newEvalIngestionService(store)

	rec1, err := ingest.IngestToolOutput(ctx, ingestion.IngestToolOutputRequest{
		Source:   "eval",
		ToolName: "bash",
		Args:     map[string]any{"cmd": "go test ./..."},
		Result:   "ok",
		Tags:     []string{"eval"},
	})
	if err != nil {
		t.Fatalf("IngestToolOutput 1: %v", err)
	}
	_, err = ingest.IngestOutcome(ctx, ingestion.IngestOutcomeRequest{
		Source:         "eval",
		TargetRecordID: rec1.ID,
		OutcomeStatus:  schema.OutcomeStatusSuccess,
	})
	if err != nil {
		t.Fatalf("IngestOutcome 1: %v", err)
	}

	rec2, err := ingest.IngestToolOutput(ctx, ingestion.IngestToolOutputRequest{
		Source:   "eval",
		ToolName: "bash",
		Args:     map[string]any{"cmd": "go test ./..."},
		Result:   "ok",
		Tags:     []string{"eval"},
	})
	if err != nil {
		t.Fatalf("IngestToolOutput 2: %v", err)
	}
	_, err = ingest.IngestOutcome(ctx, ingestion.IngestOutcomeRequest{
		Source:         "eval",
		TargetRecordID: rec2.ID,
		OutcomeStatus:  schema.OutcomeStatusSuccess,
	})
	if err != nil {
		t.Fatalf("IngestOutcome 2: %v", err)
	}

	consol := consolidation.NewCompetenceConsolidator(store)
	created, reinforced, err := consol.Consolidate(ctx)
	if err != nil {
		t.Fatalf("Consolidate: %v", err)
	}
	if created != 1 || reinforced != 0 {
		t.Fatalf("expected created=1 reinforced=0, got created=%d reinforced=%d", created, reinforced)
	}

	competences, err := store.ListByType(ctx, schema.MemoryTypeCompetence)
	if err != nil {
		t.Fatalf("ListByType competence: %v", err)
	}
	if len(competences) != 1 {
		t.Fatalf("expected 1 competence record, got %d", len(competences))
	}
	cp, ok := competences[0].Payload.(*schema.CompetencePayload)
	if !ok {
		t.Fatalf("competence payload missing")
	}
	if cp.SkillName != "skill:bash" {
		t.Fatalf("unexpected skill name: %s", cp.SkillName)
	}
	if cp.Performance == nil || cp.Performance.SuccessCount != 2 {
		t.Fatalf("unexpected performance stats: %#v", cp.Performance)
	}
}

func TestEvalConsolidationCompetenceUsesMaxSensitivity(t *testing.T) {
	ctx := context.Background()
	store := newEvalStore(t)
	ingest := newEvalIngestionService(store)

	for _, sensitivity := range []schema.Sensitivity{schema.SensitivityLow, schema.SensitivityHigh} {
		rec, err := ingest.IngestToolOutput(ctx, ingestion.IngestToolOutputRequest{
			Source:      "eval",
			ToolName:    "bash",
			Args:        map[string]any{"cmd": "go test ./..."},
			Result:      "ok",
			Scope:       "project:alpha",
			Sensitivity: sensitivity,
			Tags:        []string{"eval"},
		})
		if err != nil {
			t.Fatalf("IngestToolOutput: %v", err)
		}
		_, err = ingest.IngestOutcome(ctx, ingestion.IngestOutcomeRequest{
			Source:         "eval",
			TargetRecordID: rec.ID,
			OutcomeStatus:  schema.OutcomeStatusSuccess,
		})
		if err != nil {
			t.Fatalf("IngestOutcome: %v", err)
		}
	}

	consol := consolidation.NewCompetenceConsolidator(store)
	created, reinforced, err := consol.Consolidate(ctx)
	if err != nil {
		t.Fatalf("Consolidate: %v", err)
	}
	if created != 1 || reinforced != 0 {
		t.Fatalf("expected created=1 reinforced=0, got created=%d reinforced=%d", created, reinforced)
	}

	competences, err := store.ListByType(ctx, schema.MemoryTypeCompetence)
	if err != nil {
		t.Fatalf("ListByType competence: %v", err)
	}
	if len(competences) != 1 {
		t.Fatalf("expected 1 competence record, got %d", len(competences))
	}
	if competences[0].Sensitivity != schema.SensitivityHigh {
		t.Fatalf("expected derived sensitivity high, got %s", competences[0].Sensitivity)
	}
	if competences[0].Scope != "project:alpha" {
		t.Fatalf("expected preserved scope project:alpha, got %q", competences[0].Scope)
	}
	if !strings.Contains(competences[0].AuditLog[0].Rationale, "sensitivity=max(high)") {
		t.Fatalf("expected audit rationale to record sensitivity policy, got %q", competences[0].AuditLog[0].Rationale)
	}
}

func TestEvalConsolidationCompetenceUsesRestrictedScopeForMixedSources(t *testing.T) {
	ctx := context.Background()
	store := newEvalStore(t)
	ingest := newEvalIngestionService(store)

	for _, scope := range []string{"project:alpha", "project:beta"} {
		rec, err := ingest.IngestToolOutput(ctx, ingestion.IngestToolOutputRequest{
			Source:      "eval",
			ToolName:    "bash",
			Args:        map[string]any{"cmd": "go test ./..."},
			Result:      "ok",
			Scope:       scope,
			Sensitivity: schema.SensitivityMedium,
			Tags:        []string{"eval"},
		})
		if err != nil {
			t.Fatalf("IngestToolOutput: %v", err)
		}
		_, err = ingest.IngestOutcome(ctx, ingestion.IngestOutcomeRequest{
			Source:         "eval",
			TargetRecordID: rec.ID,
			OutcomeStatus:  schema.OutcomeStatusSuccess,
		})
		if err != nil {
			t.Fatalf("IngestOutcome: %v", err)
		}
	}

	consol := consolidation.NewCompetenceConsolidator(store)
	created, reinforced, err := consol.Consolidate(ctx)
	if err != nil {
		t.Fatalf("Consolidate: %v", err)
	}
	if created != 1 || reinforced != 0 {
		t.Fatalf("expected created=1 reinforced=0, got created=%d reinforced=%d", created, reinforced)
	}

	competences, err := store.ListByType(ctx, schema.MemoryTypeCompetence)
	if err != nil {
		t.Fatalf("ListByType competence: %v", err)
	}
	if len(competences) != 1 {
		t.Fatalf("expected 1 competence record, got %d", len(competences))
	}
	if competences[0].Scope != "consolidated:mixed-scope" {
		t.Fatalf("expected restricted mixed scope, got %q", competences[0].Scope)
	}
	if !strings.Contains(competences[0].AuditLog[0].Rationale, "scope=consolidated:mixed-scope from project:alpha, project:beta") {
		t.Fatalf("expected audit rationale to record scope policy, got %q", competences[0].AuditLog[0].Rationale)
	}
}

func TestEvalConsolidationPlanGraph(t *testing.T) {
	ctx := context.Background()
	store := newEvalStore(t)

	now := time.Now().UTC()
	payload := &schema.EpisodicPayload{
		Kind: "episodic",
		Timeline: []schema.TimelineEvent{
			{T: now, EventKind: "deploy", Ref: "t1", Summary: "deploy step 1"},
		},
		ToolGraph: []schema.ToolNode{
			{ID: "t1", Tool: "build", Timestamp: now},
			{ID: "t2", Tool: "test", Timestamp: now, DependsOn: []string{"t1"}},
			{ID: "t3", Tool: "deploy", Timestamp: now, DependsOn: []string{"t2"}},
		},
	}

	rec := schema.NewMemoryRecord(uuid.NewString(), schema.MemoryTypeEpisodic, schema.SensitivityLow, payload)
	if err := store.Create(ctx, rec); err != nil {
		t.Fatalf("store.Create episodic: %v", err)
	}

	consol := consolidation.NewPlanGraphConsolidator(store)
	created, err := consol.Consolidate(ctx)
	if err != nil {
		t.Fatalf("Consolidate: %v", err)
	}
	if created != 1 {
		t.Fatalf("expected created=1, got %d", created)
	}

	plans, err := store.ListByType(ctx, schema.MemoryTypePlanGraph)
	if err != nil {
		t.Fatalf("ListByType plan_graph: %v", err)
	}
	if len(plans) != 1 {
		t.Fatalf("expected 1 plan graph, got %d", len(plans))
	}
	pp, ok := plans[0].Payload.(*schema.PlanGraphPayload)
	if !ok {
		t.Fatalf("plan graph payload missing")
	}
	if len(pp.Nodes) != 3 {
		t.Fatalf("expected 3 plan nodes, got %d", len(pp.Nodes))
	}
	if len(pp.Edges) != 2 {
		t.Fatalf("expected 2 plan edges, got %d", len(pp.Edges))
	}
}

func newEvalStore(t *testing.T) storage.Store {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "eval.db")
	store, err := sqlite.Open(path, "")
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			_ = err
		}
		_ = os.Remove(path)
	})
	return store
}

func newEvalIngestionService(store storage.Store) *ingestion.Service {
	classifier := ingestion.NewClassifier()
	defaults := ingestion.DefaultPolicyDefaults()
	policy := ingestion.NewPolicyEngine(defaults)
	return ingestion.NewService(store, classifier, policy)
}

func hasRelation(rels []schema.Relation, predicate, targetID string) bool {
	for _, rel := range rels {
		if rel.Predicate == predicate && rel.TargetID == targetID {
			return true
		}
	}
	return false
}

func auditContains(entries []schema.AuditEntry, action schema.AuditAction) bool {
	for _, entry := range entries {
		if entry.Action == action {
			return true
		}
	}
	return false
}
