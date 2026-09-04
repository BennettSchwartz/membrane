package tests_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/BennettSchwartz/membrane/internal/teststore"
	"github.com/BennettSchwartz/membrane/pkg/consolidation"
	"github.com/BennettSchwartz/membrane/pkg/ingestion"
	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

func TestEvalConsolidationSemantic(t *testing.T) {
	ctx := context.Background()
	store := newEvalStore(t)
	ingest := newEvalIngestionService(store)

	episode, err := createConsolidationEvent(ctx, store, eventCaptureFixture{
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

	ep1, err := createConsolidationEvent(ctx, store, eventCaptureFixture{
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

	ep2, err := createConsolidationEvent(ctx, store, eventCaptureFixture{
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

	rec1, err := captureToolOutputRecord(ctx, ingest, toolCaptureFixture{
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

	rec2, err := captureToolOutputRecord(ctx, ingest, toolCaptureFixture{
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
		rec, err := captureToolOutputRecord(ctx, ingest, toolCaptureFixture{
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

func TestEvalConsolidationCompetenceIsolatesSourceScopes(t *testing.T) {
	ctx := context.Background()
	store := newEvalStore(t)
	ingest := newEvalIngestionService(store)

	for _, scope := range []string{"project:alpha", "project:beta"} {
		rec, err := captureToolOutputRecord(ctx, ingest, toolCaptureFixture{
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
	if created != 0 || reinforced != 0 {
		t.Fatalf("separate scopes must not combine: created=%d reinforced=%d", created, reinforced)
	}

	competences, err := store.ListByType(ctx, schema.MemoryTypeCompetence)
	if err != nil {
		t.Fatalf("ListByType competence: %v", err)
	}
	if len(competences) != 0 {
		t.Fatalf("expected no competence from one source per scope, got %d", len(competences))
	}

	// A second observation in alpha forms a valid same-scope pattern while beta
	// remains isolated and contributes no evidence to the alpha competence.
	rec, err := captureToolOutputRecord(ctx, ingest, toolCaptureFixture{
		Source: "eval", ToolName: "bash", Args: map[string]any{"cmd": "go test ./..."}, Result: "ok",
		Scope: "project:alpha", Sensitivity: schema.SensitivityMedium, Tags: []string{"eval"},
	})
	if err != nil {
		t.Fatalf("IngestToolOutput control: %v", err)
	}
	if _, err := ingest.IngestOutcome(ctx, ingestion.IngestOutcomeRequest{Source: "eval", TargetRecordID: rec.ID, OutcomeStatus: schema.OutcomeStatusSuccess}); err != nil {
		t.Fatalf("IngestOutcome control: %v", err)
	}
	created, reinforced, err = consol.Consolidate(ctx)
	if err != nil || created != 1 || reinforced != 0 {
		t.Fatalf("same-scope control: created=%d reinforced=%d err=%v", created, reinforced, err)
	}
	competences, err = store.ListByType(ctx, schema.MemoryTypeCompetence)
	if err != nil {
		t.Fatalf("ListByType competence control: %v", err)
	}
	if len(competences) != 1 || competences[0].Scope != "project:alpha" {
		t.Fatalf("expected one alpha competence, got %+v", competences)
	}
	if cp := competences[0].Payload.(*schema.CompetencePayload); cp.Performance == nil || cp.Performance.SuccessCount != 2 {
		t.Fatalf("same-scope performance = %+v", cp.Performance)
	}
	for _, evidence := range competences[0].Provenance.Sources {
		source, err := store.Get(ctx, evidence.Ref)
		if err != nil || source.Scope != "project:alpha" {
			t.Fatalf("competence incorporated incompatible source %s: %+v, %v", evidence.Ref, source, err)
		}
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

// These consolidation fixtures exercise stored timeline event kinds, including
// custom kinds predating CaptureMemory's source-kind normalization.
func createConsolidationEvent(ctx context.Context, store storage.Store, fixture eventCaptureFixture) (*schema.MemoryRecord, error) {
	now := fixture.Timestamp
	if now.IsZero() {
		now = time.Now().UTC()
	}
	sensitivity := fixture.Sensitivity
	if sensitivity == "" {
		sensitivity = schema.SensitivityLow
	}
	rec := schema.NewMemoryRecord(uuid.NewString(), schema.MemoryTypeEpisodic, sensitivity, &schema.EpisodicPayload{
		Kind: "episodic",
		Timeline: []schema.TimelineEvent{{
			T: now, EventKind: fixture.EventKind, Ref: fixture.Ref, Summary: fixture.Summary,
		}},
	})
	rec.Scope = fixture.Scope
	rec.Tags = append([]string(nil), fixture.Tags...)
	rec.Provenance.CreatedBy = fixture.Source
	rec.CreatedAt = now
	rec.UpdatedAt = now
	if err := store.Create(ctx, rec); err != nil {
		return nil, err
	}
	return rec, nil
}

func newEvalStore(t *testing.T) storage.Store {
	t.Helper()
	store := teststore.NewMemoryStore()
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			_ = err
		}
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
