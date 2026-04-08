package ingestion

import (
	"context"
	"testing"
	"time"

	"github.com/GustyCube/membrane/pkg/schema"
	sqlitestore "github.com/GustyCube/membrane/pkg/storage/sqlite"
)

type stubInterpreter struct {
	interpretation *schema.Interpretation
}

func (s *stubInterpreter) Interpret(_ context.Context, _ InterpretRequest) (*schema.Interpretation, error) {
	return s.interpretation, nil
}

func newCaptureTestService(t *testing.T, interpreter Interpreter) (*Service, *sqlitestore.SQLiteStore) {
	t.Helper()

	store, err := sqlitestore.Open(":memory:", "")
	if err != nil {
		t.Fatalf("Open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	classifier := NewClassifier()
	policy := NewPolicyEngine(DefaultPolicyDefaults())
	if interpreter != nil {
		return NewServiceWithInterpreter(store, classifier, policy, interpreter), store
	}
	return NewService(store, classifier, policy), store
}

func TestCaptureMemoryCreatesPrimaryRecordEntityAndEdges(t *testing.T) {
	svc, store := newCaptureTestService(t, nil)
	ctx := context.Background()
	ts := time.Date(2026, 4, 8, 12, 0, 0, 0, time.UTC)

	resp, err := svc.CaptureMemory(ctx, CaptureMemoryRequest{
		Source:           "tester",
		SourceKind:       "event",
		Content:          map[string]any{"ref": "evt-1", "text": "Remember Orchid", "project": "Orchid"},
		Context:          map[string]any{"thread_id": "thread-1"},
		ReasonToRemember: "Deploy vocabulary should be recoverable",
		Summary:          "Remember Orchid",
		Tags:             []string{"deploy"},
		Scope:            "project:alpha",
		Sensitivity:      schema.SensitivityLow,
		Timestamp:        ts,
	})
	if err != nil {
		t.Fatalf("CaptureMemory: %v", err)
	}

	if resp.PrimaryRecord == nil || resp.PrimaryRecord.Type != schema.MemoryTypeEpisodic {
		t.Fatalf("PrimaryRecord.Type = %v, want episodic", resp.PrimaryRecord.Type)
	}
	if len(resp.CreatedRecords) != 1 || resp.CreatedRecords[0].Type != schema.MemoryTypeEntity {
		t.Fatalf("CreatedRecords = %+v, want one entity record", resp.CreatedRecords)
	}
	if len(resp.Edges) != 2 {
		t.Fatalf("Edges len = %d, want 2 bidirectional entity edges", len(resp.Edges))
	}

	got, err := store.Get(ctx, resp.PrimaryRecord.ID)
	if err != nil {
		t.Fatalf("Get primary: %v", err)
	}
	if got.Interpretation == nil {
		t.Fatalf("Interpretation = nil, want populated interpretation")
	}
	if got.Interpretation.Status != schema.InterpretationStatusResolved {
		t.Fatalf("Interpretation.Status = %q, want resolved", got.Interpretation.Status)
	}
	if len(got.Interpretation.Mentions) != 1 || got.Interpretation.Mentions[0].CanonicalEntityID != resp.CreatedRecords[0].ID {
		t.Fatalf("Interpretation mentions = %+v, want canonical entity %q", got.Interpretation.Mentions, resp.CreatedRecords[0].ID)
	}
	ep, ok := got.Payload.(*schema.EpisodicPayload)
	if !ok {
		t.Fatalf("Payload type = %T, want episodic payload", got.Payload)
	}
	if ep.Environment == nil || ep.Environment.Context["reason_to_remember"] != "Deploy vocabulary should be recoverable" {
		t.Fatalf("Capture context = %+v, want reason_to_remember persisted", ep.Environment)
	}
}

func TestCaptureMemoryWorkingStateCreatesWorkingPrimaryRecord(t *testing.T) {
	svc, _ := newCaptureTestService(t, nil)
	ctx := context.Background()

	resp, err := svc.CaptureMemory(ctx, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "working_state",
		Content:     map[string]any{"thread_id": "thread-1", "state": "planning", "context_summary": "Investigating Orchid"},
		Scope:       "project:alpha",
		Sensitivity: schema.SensitivityLow,
	})
	if err != nil {
		t.Fatalf("CaptureMemory: %v", err)
	}
	if resp.PrimaryRecord == nil || resp.PrimaryRecord.Type != schema.MemoryTypeWorking {
		t.Fatalf("PrimaryRecord.Type = %v, want working", resp.PrimaryRecord.Type)
	}
	if len(resp.CreatedRecords) != 0 {
		t.Fatalf("CreatedRecords len = %d, want 0 for plain working state capture", len(resp.CreatedRecords))
	}
}

func TestCaptureMemoryInterpreterResolvesExistingEntity(t *testing.T) {
	interpreter := &stubInterpreter{
		interpretation: &schema.Interpretation{
			Status:       schema.InterpretationStatusTentative,
			Summary:      "Resolved Orchid mention",
			ProposedType: schema.MemoryTypeSemantic,
			Mentions: []schema.Mention{{
				Surface:    "Orchid",
				EntityKind: schema.EntityKindProject,
				Aliases:    []string{"orchid"},
				Confidence: 0.9,
			}},
			ExtractionConfidence: 0.9,
		},
	}
	svc, store := newCaptureTestService(t, interpreter)
	ctx := context.Background()
	existing := schema.NewMemoryRecord("entity-existing", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Orchid",
		EntityKind:    schema.EntityKindProject,
		Aliases:       []string{"orchid"},
		Summary:       "Existing Orchid entity",
	})
	if err := store.Create(ctx, existing); err != nil {
		t.Fatalf("Create existing entity: %v", err)
	}

	resp, err := svc.CaptureMemory(ctx, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "event",
		Content:     map[string]any{"ref": "evt-2", "text": "Use Orchid for rollout verification"},
		Scope:       "project:alpha",
		Sensitivity: schema.SensitivityLow,
	})
	if err != nil {
		t.Fatalf("CaptureMemory: %v", err)
	}

	if len(resp.CreatedRecords) != 0 {
		t.Fatalf("CreatedRecords len = %d, want 0 when existing entity resolves", len(resp.CreatedRecords))
	}
	if len(resp.Edges) == 0 || resp.Edges[0].TargetID != "entity-existing" {
		t.Fatalf("Edges = %+v, want edge to existing entity", resp.Edges)
	}
	got, err := store.Get(ctx, resp.PrimaryRecord.ID)
	if err != nil {
		t.Fatalf("Get primary: %v", err)
	}
	if got.Interpretation == nil || got.Interpretation.Mentions[0].CanonicalEntityID != "entity-existing" {
		t.Fatalf("Interpretation = %+v, want canonical_entity_id entity-existing", got.Interpretation)
	}
}

func TestCaptureMemoryCreatesSecondarySemanticRecordForExplicitFact(t *testing.T) {
	svc, _ := newCaptureTestService(t, nil)
	ctx := context.Background()

	resp, err := svc.CaptureMemory(ctx, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "observation",
		Content:     map[string]any{"subject": "Orchid", "predicate": "deploy_target_for", "object": "staging"},
		Scope:       "project:alpha",
		Sensitivity: schema.SensitivityLow,
		Tags:        []string{"deploy"},
	})
	if err != nil {
		t.Fatalf("CaptureMemory: %v", err)
	}

	foundSemantic := false
	entityID := ""
	foundDerivedEdge := false
	for _, rec := range resp.CreatedRecords {
		if rec.Type == schema.MemoryTypeEntity {
			entityID = rec.ID
		}
		if rec.Type != schema.MemoryTypeSemantic {
			continue
		}
		foundSemantic = true
		payload, ok := rec.Payload.(*schema.SemanticPayload)
		if !ok {
			t.Fatalf("Semantic created record payload = %T, want *schema.SemanticPayload", rec.Payload)
		}
		if payload.Predicate != "deploy_target_for" || payload.Object != "staging" {
			t.Fatalf("Semantic payload = %+v, want explicit fact predicate/object", payload)
		}
	}
	for _, edge := range resp.Edges {
		if edge.Predicate == "derived_semantic" {
			foundDerivedEdge = true
			break
		}
	}
	if !foundSemantic {
		t.Fatalf("CreatedRecords = %+v, want derived semantic record", resp.CreatedRecords)
	}
	if entityID == "" {
		t.Fatalf("CreatedRecords = %+v, want linked entity record", resp.CreatedRecords)
	}
	for _, rec := range resp.CreatedRecords {
		payload, ok := rec.Payload.(*schema.SemanticPayload)
		if !ok {
			continue
		}
		if payload.Subject != entityID {
			t.Fatalf("Semantic subject = %q, want canonical entity id %q", payload.Subject, entityID)
		}
	}
	if !foundDerivedEdge {
		t.Fatalf("Edges = %+v, want derived_semantic edge", resp.Edges)
	}
}
