package ingestion

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/internal/teststore"
	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

type stubInterpreter struct {
	interpretation *schema.Interpretation
	resolved       *schema.Interpretation
}

func (s *stubInterpreter) Interpret(_ context.Context, _ InterpretRequest) (*schema.Interpretation, error) {
	return s.interpretation, nil
}

func (s *stubInterpreter) Resolve(_ context.Context, _ ResolveRequest) (*schema.Interpretation, error) {
	return s.resolved, nil
}

type failingRelationStore struct {
	boundedCaptureTestStore
}

func (s *failingRelationStore) Begin(ctx context.Context) (storage.Transaction, error) {
	tx, err := s.boundedCaptureTestStore.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &failingRelationTx{Transaction: tx}, nil
}

func (s *failingRelationStore) FindEntitiesByTerm(ctx context.Context, term, scope string, limit int) ([]*schema.MemoryRecord, error) {
	if lookup, ok := s.boundedCaptureTestStore.(storage.EntityLookup); ok {
		return lookup.FindEntitiesByTerm(ctx, term, scope, limit)
	}
	return nil, nil
}

func (s *failingRelationStore) FindEntityByIdentifier(ctx context.Context, namespace, value, scope string) (*schema.MemoryRecord, error) {
	if lookup, ok := s.boundedCaptureTestStore.(storage.EntityLookup); ok {
		return lookup.FindEntityByIdentifier(ctx, namespace, value, scope)
	}
	return nil, storage.ErrNotFound
}

type failingRelationTx struct {
	storage.Transaction
}

func (tx *failingRelationTx) AddRelation(context.Context, string, schema.Relation) error {
	return errors.New("forced relation write failure")
}

func newCaptureTestService(t *testing.T, interpreter Interpreter) (*Service, *teststore.MemoryStore) {
	t.Helper()

	store := teststore.NewMemoryStore()
	t.Cleanup(func() { _ = store.Close() })

	classifier := NewClassifier()
	policy := NewPolicyEngine(DefaultPolicyDefaults())
	if interpreter != nil {
		return NewServiceWithInterpreter(store, classifier, policy, interpreter), store
	}
	return NewService(store, classifier, policy), store
}

func TestCaptureMemoryDefaultsSensitivityAndPropagatesPrepareErrors(t *testing.T) {
	store := &listCaptureStore{errAt: 1}
	svc := &Service{store: store}

	_, err := svc.CaptureMemory(context.Background(), CaptureMemoryRequest{
		Source:     "tester",
		SourceKind: "event",
		Content:    map[string]any{"summary": "remember this"},
	})
	if err == nil || !strings.Contains(err.Error(), "ingestion: fetch candidates") {
		t.Fatalf("CaptureMemory prepare error = %v, want fetch candidates error", err)
	}
}

func TestCaptureMemoryRejectsInvalidSourceKind(t *testing.T) {
	svc, _ := newCaptureTestService(t, nil)

	_, err := svc.CaptureMemory(context.Background(), CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "transcript",
		Content:     map[string]any{"text": "remember this"},
		Sensitivity: schema.SensitivityLow,
	})
	if err == nil || !strings.Contains(err.Error(), "invalid source_kind") {
		t.Fatalf("CaptureMemory invalid source_kind error = %v, want invalid source_kind", err)
	}
}

func TestCaptureMemoryRejectsInvalidProposedType(t *testing.T) {
	svc, _ := newCaptureTestService(t, nil)

	_, err := svc.CaptureMemory(context.Background(), CaptureMemoryRequest{
		Source:       "tester",
		SourceKind:   "event",
		ProposedType: schema.MemoryType("transcript"),
		Content:      map[string]any{"text": "remember this"},
		Sensitivity:  schema.SensitivityLow,
	})
	if err == nil || !strings.Contains(err.Error(), "invalid proposed_type") {
		t.Fatalf("CaptureMemory invalid proposed_type error = %v, want invalid proposed_type", err)
	}
}

func TestCaptureMemoryIgnoresInvalidInterpreterProposedType(t *testing.T) {
	svc, _ := newCaptureTestService(t, &stubInterpreter{
		interpretation: &schema.Interpretation{
			ProposedType: schema.MemoryType("transcript"),
			Summary:      "interpreter summary",
		},
	})

	resp, err := svc.CaptureMemory(context.Background(), CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "event",
		Content:     map[string]any{"ref": "evt-invalid-llm-type", "text": "remember this"},
		Sensitivity: schema.SensitivityLow,
	})
	if err != nil {
		t.Fatalf("CaptureMemory invalid interpreter proposed_type: %v", err)
	}
	if got := resp.PrimaryRecord.Interpretation.ProposedType; got != schema.MemoryTypeEpisodic {
		t.Fatalf("interpreter proposed_type = %q, want inferred %q", got, schema.MemoryTypeEpisodic)
	}
	if got := resp.PrimaryRecord.Interpretation.Summary; got != "interpreter summary" {
		t.Fatalf("interpreter summary = %q, want preserved valid summary", got)
	}
}

func TestCaptureMemoryDirectNilInterpretationAndErrorBranches(t *testing.T) {
	ctx := context.Background()
	ts := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	svc, _ := newCaptureTestService(t, nil)
	resp, err := svc.captureMemory(ctx, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "event",
		Sensitivity: schema.SensitivityLow,
		Content:     map[string]any{"ref": "nil-interpretation", "summary": "nil interpretation"},
	}, ts, nil, nil)
	if err != nil {
		t.Fatalf("captureMemory nil interpretation: %v", err)
	}
	if resp.PrimaryRecord == nil || resp.PrimaryRecord.Interpretation == nil {
		t.Fatalf("response = %+v, want primary record with fallback interpretation", resp)
	}

	if _, err := svc.captureMemory(ctx, CaptureMemoryRequest{SourceKind: "event"}, ts, nil, nil); err == nil {
		t.Fatalf("captureMemory invalid primary error = nil")
	}

	_, base := newCaptureTestService(t, nil)
	updateErr := errors.New("update failed")
	updateSvc := NewService(&updateFailStore{boundedCaptureTestStore: base, err: updateErr}, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))
	if _, err := updateSvc.captureMemory(ctx, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "working_state",
		Sensitivity: schema.SensitivityLow,
		Content:     map[string]any{"thread_id": "thread", "state": string(schema.TaskStatePlanning)},
	}, ts, &schema.Interpretation{}, nil); !errors.Is(err, updateErr) {
		t.Fatalf("captureMemory update primary error = %v, want %v", err, updateErr)
	}
}

func TestCaptureMemoryDirectReferenceBranchesAndFinalizeError(t *testing.T) {
	ctx := context.Background()
	ts := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	svc, store := newCaptureTestService(t, nil)
	entity := newObservationEntity("entity-reference-target", "Orchid", "")
	if err := store.Create(ctx, entity); err != nil {
		t.Fatalf("Create entity: %v", err)
	}
	resp, err := svc.captureMemory(ctx, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "event",
		Sensitivity: schema.SensitivityLow,
		Content:     map[string]any{"id": "evt-entity-ref", "text": "references entity"},
	}, ts, &schema.Interpretation{
		Mentions:            []schema.Mention{{Surface: "New Entity", EntityKind: schema.EntityKindProject}},
		ReferenceCandidates: []schema.ReferenceCandidate{{TargetEntityID: entity.ID}},
	}, []*schema.MemoryRecord{entity})
	if err != nil {
		t.Fatalf("captureMemory entity reference: %v", err)
	}
	ref := resp.PrimaryRecord.Interpretation.ReferenceCandidates[0]
	if !ref.Resolved || ref.TargetEntityID != entity.ID || ref.TargetRecordID != "" || ref.Confidence != 1 {
		t.Fatalf("entity reference candidate = %+v, want resolved entity target with default confidence", ref)
	}
	mention := resp.PrimaryRecord.Interpretation.Mentions[0]
	if mention.CanonicalEntityID == "" || mention.Confidence != 1 {
		t.Fatalf("mention = %+v, want resolved entity with default confidence", mention)
	}

	selfResp, err := svc.captureMemory(ctx, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "event",
		Sensitivity: schema.SensitivityLow,
		Content:     map[string]any{"id": "evt-self", "text": "references itself"},
	}, ts, &schema.Interpretation{
		ReferenceCandidates: []schema.ReferenceCandidate{{Ref: "evt-self"}},
	}, nil)
	if err != nil {
		t.Fatalf("captureMemory self reference: %v", err)
	}
	selfRef := selfResp.PrimaryRecord.Interpretation.ReferenceCandidates[0]
	if !selfRef.Resolved || selfRef.TargetRecordID != selfResp.PrimaryRecord.ID {
		t.Fatalf("self reference candidate = %+v, want resolved to primary record", selfRef)
	}

	_, base := newCaptureTestService(t, nil)
	finalErr := errors.New("final update failed")
	finalSvc := NewService(&updateFailAtStore{boundedCaptureTestStore: base, failAt: 3, err: finalErr}, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))
	_, err = finalSvc.captureMemory(ctx, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "event",
		Sensitivity: schema.SensitivityLow,
		Content:     map[string]any{"id": "evt-final-fails", "text": "final update fails"},
	}, ts, &schema.Interpretation{}, nil)
	if !errors.Is(err, finalErr) {
		t.Fatalf("captureMemory final update error = %v, want %v", err, finalErr)
	}
}

func TestCaptureMemoryReferenceAndSemanticErrorBranches(t *testing.T) {
	ctx := context.Background()
	ts := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	target := newHelperSemanticRecord("reference-target", "target")

	for _, tc := range []struct {
		name   string
		failAt int
	}{
		{name: "reference forward edge", failAt: 1},
		{name: "reference inverse edge", failAt: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, base := newCaptureTestService(t, nil)
			if err := base.Create(ctx, target); err != nil {
				t.Fatalf("Create target: %v", err)
			}
			svc := NewService(&relationFailAtStore{boundedCaptureTestStore: base, failAt: tc.failAt}, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))
			_, err := svc.captureMemory(ctx, CaptureMemoryRequest{
				Source:      "tester",
				SourceKind:  "event",
				Sensitivity: schema.SensitivityLow,
				Content:     map[string]any{"id": "evt-reference-fails", "text": "reference write fails"},
			}, ts, &schema.Interpretation{
				ReferenceCandidates: []schema.ReferenceCandidate{{TargetRecordID: target.ID}},
			}, []*schema.MemoryRecord{target})
			if err == nil || !strings.Contains(err.Error(), "forced relation write failure") {
				t.Fatalf("captureMemory reference write error = %v, want forced relation failure", err)
			}
		})
	}

	_, relationBase := newCaptureTestService(t, nil)
	relationTarget := newHelperSemanticRecord("relation-target", "target")
	if err := relationBase.Create(ctx, relationTarget); err != nil {
		t.Fatalf("Create relation target: %v", err)
	}
	relationSvc := NewService(&relationFailAtStore{boundedCaptureTestStore: relationBase, failAt: 1}, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))
	_, err := relationSvc.captureMemory(ctx, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "event",
		Sensitivity: schema.SensitivityLow,
		Content:     map[string]any{"id": "evt-relation-fails", "text": "relation write fails"},
	}, ts, &schema.Interpretation{
		RelationCandidates: []schema.RelationCandidate{{Predicate: "depends_on", TargetRecordID: relationTarget.ID}},
	}, []*schema.MemoryRecord{relationTarget})
	if err == nil || !strings.Contains(err.Error(), "forced relation write failure") {
		t.Fatalf("captureMemory relation candidate error = %v, want forced relation failure", err)
	}

	_, base := newCaptureTestService(t, nil)
	svc := NewService(&relationFailAtStore{boundedCaptureTestStore: base, failAt: 1}, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))
	_, err = svc.captureMemory(ctx, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "event",
		Sensitivity: schema.SensitivityLow,
		Content:     map[string]any{"subject": "Orchid", "predicate": "runs_on", "object": "staging"},
	}, ts, &schema.Interpretation{ProposedType: schema.MemoryTypeSemantic, ExtractionConfidence: 1}, nil)
	if err == nil || !strings.Contains(err.Error(), "forced relation write failure") {
		t.Fatalf("captureMemory semantic creation error = %v, want forced relation failure", err)
	}
}

func TestCreatePrimaryRecordPropagatesBranchErrors(t *testing.T) {
	ctx := context.Background()
	ts := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	svc, _ := newCaptureTestService(t, nil)

	for _, tc := range []struct {
		name string
		req  CaptureMemoryRequest
	}{
		{name: "working_state", req: CaptureMemoryRequest{SourceKind: "working_state"}},
		{name: "tool_output", req: CaptureMemoryRequest{SourceKind: "tool_output"}},
		{name: "event", req: CaptureMemoryRequest{SourceKind: "event"}},
	} {
		t.Run("validation "+tc.name, func(t *testing.T) {
			if _, err := svc.createPrimaryRecord(ctx, tc.req, ts); err == nil {
				t.Fatalf("createPrimaryRecord %s validation error = nil", tc.name)
			}
		})
	}

	_, base := newCaptureTestService(t, nil)
	updateErr := errors.New("update failed")
	failSvc := NewService(&updateFailStore{boundedCaptureTestStore: base, err: updateErr}, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))
	if _, err := failSvc.createPrimaryRecord(ctx, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "tool_output",
		Sensitivity: schema.SensitivityLow,
		Content:     map[string]any{"tool_name": "rg", "result": "ok"},
	}, ts); !errors.Is(err, updateErr) {
		t.Fatalf("createPrimaryRecord tool_output update error = %v, want %v", err, updateErr)
	}
	if _, err := failSvc.createPrimaryRecord(ctx, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "event",
		Sensitivity: schema.SensitivityLow,
		Content:     map[string]any{"ref": "evt-update-fails", "summary": "event update fails"},
	}, ts); !errors.Is(err, updateErr) {
		t.Fatalf("createPrimaryRecord event update error = %v, want %v", err, updateErr)
	}
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

func TestCreatePrimaryRecordUsesContentIDAsEventRef(t *testing.T) {
	svc, _ := newCaptureTestService(t, nil)
	ctx := context.Background()
	ts := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	rec, err := svc.createPrimaryRecord(ctx, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "event",
		Content:     map[string]any{"id": "evt-123", "text": "lowercase event"},
		Summary:     "event summary",
		Sensitivity: schema.SensitivityLow,
	}, ts)
	if err != nil {
		t.Fatalf("createPrimaryRecord with id: %v", err)
	}
	payload := rec.Payload.(*schema.EpisodicPayload)
	if len(payload.Timeline) != 1 || payload.Timeline[0].Ref != "evt-123" {
		t.Fatalf("Timeline = %+v, want ref evt-123", payload.Timeline)
	}

	rec, err = svc.createPrimaryRecord(ctx, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "event",
		Content:     map[string]any{"text": "lowercase event without id"},
		Summary:     "event summary",
		Sensitivity: schema.SensitivityLow,
	}, ts)
	if err != nil {
		t.Fatalf("createPrimaryRecord without id: %v", err)
	}
	payload = rec.Payload.(*schema.EpisodicPayload)
	if len(payload.Timeline) != 1 || payload.Timeline[0].Ref == "" || payload.Timeline[0].Ref == "id" {
		t.Fatalf("Timeline = %+v, want generated non-literal ref", payload.Timeline)
	}
}

func TestCreatePrimaryRecordCapturesWorkingState(t *testing.T) {
	svc, _ := newCaptureTestService(t, nil)
	ctx := context.Background()
	ts := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	rec, err := svc.createPrimaryRecord(ctx, CaptureMemoryRequest{
		Source:     "tester",
		SourceKind: "working_state",
		Content: map[string]any{
			"thread_id":          "thread-1",
			"state":              "not-a-valid-state",
			"next_actions":       []any{"run tests"},
			"open_questions":     []any{"which env?"},
			"active_constraints": []any{map[string]any{"type": "env", "key": "target", "value": "staging", "required": true}},
		},
		ReasonToRemember: "track current task",
		Tags:             []string{"working"},
		Scope:            "project:alpha",
		Sensitivity:      schema.SensitivityLow,
	}, ts)
	if err != nil {
		t.Fatalf("createPrimaryRecord working_state: %v", err)
	}
	payload, ok := rec.Payload.(*schema.WorkingPayload)
	if !ok {
		t.Fatalf("Payload type = %T, want working payload", rec.Payload)
	}
	if payload.ThreadID != "thread-1" || payload.State != schema.TaskStateExecuting {
		t.Fatalf("working payload thread/state = %q/%q, want thread-1/executing", payload.ThreadID, payload.State)
	}
	if payload.ContextSummary != "track current task" || len(payload.NextActions) != 1 || len(payload.OpenQuestions) != 1 || len(payload.ActiveConstraints) != 1 {
		t.Fatalf("working payload details = %+v, want captured context/action/question/constraint", payload)
	}
}

func TestCreatePrimaryRecordCapturesToolOutputContext(t *testing.T) {
	svc, store := newCaptureTestService(t, nil)
	ctx := context.Background()
	ts := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	rec, err := svc.createPrimaryRecord(ctx, CaptureMemoryRequest{
		Source:     "tester",
		SourceKind: "tool_output",
		Content: map[string]any{
			"tool_name":  "rg",
			"args":       map[string]any{"pattern": "TODO"},
			"result":     "found matches",
			"depends_on": []string{"tool-prev"},
		},
		Context:          map[string]any{"cwd": "/repo"},
		ReasonToRemember: "search result explains the next edit",
		Tags:             []string{"search"},
		Scope:            "project:alpha",
		Sensitivity:      schema.SensitivityLow,
	}, ts)
	if err != nil {
		t.Fatalf("createPrimaryRecord tool_output: %v", err)
	}

	stored, err := store.Get(ctx, rec.ID)
	if err != nil {
		t.Fatalf("Get stored tool output: %v", err)
	}
	payload, ok := stored.Payload.(*schema.EpisodicPayload)
	if !ok {
		t.Fatalf("Payload type = %T, want episodic", stored.Payload)
	}
	if len(payload.ToolGraph) != 1 || payload.ToolGraph[0].Tool != "rg" || payload.ToolGraph[0].DependsOn[0] != "tool-prev" {
		t.Fatalf("ToolGraph = %+v, want captured tool node", payload.ToolGraph)
	}
	if payload.Environment == nil || payload.Environment.Context["reason_to_remember"] != "search result explains the next edit" {
		t.Fatalf("Environment context = %+v, want capture context", payload.Environment)
	}
}

func TestCaptureMemoryRollsBackWhenRelationWriteFails(t *testing.T) {
	base := teststore.NewMemoryStore()
	t.Cleanup(func() { _ = base.Close() })

	store := &failingRelationStore{boundedCaptureTestStore: base}
	svc := NewService(store, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))
	ctx := context.Background()

	_, err := svc.CaptureMemory(ctx, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "event",
		Content:     map[string]any{"ref": "evt-rollback", "text": "Remember Orchid", "project": "Orchid"},
		Scope:       "project:alpha",
		Sensitivity: schema.SensitivityLow,
	})
	if err == nil {
		t.Fatalf("CaptureMemory err = nil, want relation write failure")
	}

	records, listErr := base.List(ctx, storage.ListOptions{})
	if listErr != nil {
		t.Fatalf("List records after failed capture: %v", listErr)
	}
	if len(records) != 0 {
		t.Fatalf("Records after failed capture = %+v, want rollback to leave store empty", records)
	}
}

func TestCaptureMemoryWorkingStateCreatesWorkingPrimaryRecord(t *testing.T) {
	svc, _ := newCaptureTestService(t, nil)
	ctx := context.Background()

	resp, err := svc.CaptureMemory(ctx, CaptureMemoryRequest{
		Source:     "tester",
		SourceKind: "working_state",
		Content: map[string]any{
			"thread_id":       "thread-1",
			"state":           "planning",
			"context_summary": "Investigating Orchid",
			"active_constraints": []schema.Constraint{{
				Type:     "environment",
				Key:      "region",
				Value:    "us-east",
				Required: true,
			}},
		},
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
	payload := resp.PrimaryRecord.Payload.(*schema.WorkingPayload)
	if len(payload.ActiveConstraints) != 1 || payload.ActiveConstraints[0].Key != "region" {
		t.Fatalf("ActiveConstraints = %+v, want typed constraint preserved", payload.ActiveConstraints)
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
		PrimaryType:   schema.EntityTypeProject,
		Types:         []string{schema.EntityTypeProject},
		Aliases:       []schema.EntityAlias{{Value: "orchid"}},
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

func TestCaptureMemoryInterpreterResolvesExistingEntityByIdentifier(t *testing.T) {
	interpreter := &stubInterpreter{
		interpretation: &schema.Interpretation{
			Status:       schema.InterpretationStatusTentative,
			Summary:      "Resolved repository identifier mention",
			ProposedType: schema.MemoryTypeSemantic,
			Mentions: []schema.Mention{{
				Surface:    "GitHub:BennettSchwartz/orchid",
				EntityKind: schema.EntityKindProject,
				Confidence: 0.9,
			}},
			ExtractionConfidence: 0.9,
		},
	}
	svc, store := newCaptureTestService(t, interpreter)
	ctx := context.Background()
	existing := schema.NewMemoryRecord("entity-orchid-repo", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Project Orchid",
		PrimaryType:   schema.EntityTypeRepository,
		Types:         []string{schema.EntityTypeRepository},
		Identifiers: []schema.EntityIdentifier{{
			Namespace: "github",
			Value:     "BennettSchwartz/orchid",
		}},
		Summary: "Existing repository entity",
	})
	existing.Scope = "project:alpha"
	if err := store.Create(ctx, existing); err != nil {
		t.Fatalf("Create existing entity: %v", err)
	}

	resp, err := svc.CaptureMemory(ctx, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "event",
		Content:     map[string]any{"ref": "evt-repo", "text": "Use github:BennettSchwartz/orchid for rollout verification"},
		Scope:       "project:alpha",
		Sensitivity: schema.SensitivityLow,
	})
	if err != nil {
		t.Fatalf("CaptureMemory: %v", err)
	}

	if len(resp.CreatedRecords) != 0 {
		t.Fatalf("CreatedRecords len = %d, want 0 when identifier resolves existing entity", len(resp.CreatedRecords))
	}
	if !graphEdgesContain(resp.Edges, resp.PrimaryRecord.ID, schema.GraphPredicateMentionsEntity, existing.ID) {
		t.Fatalf("Edges = %+v, want mentions_entity edge to %s", resp.Edges, existing.ID)
	}
	got, err := store.Get(ctx, resp.PrimaryRecord.ID)
	if err != nil {
		t.Fatalf("Get primary: %v", err)
	}
	if got.Interpretation == nil || got.Interpretation.Mentions[0].CanonicalEntityID != existing.ID {
		t.Fatalf("Interpretation = %+v, want canonical_entity_id %s", got.Interpretation, existing.ID)
	}
}

func TestCaptureMemoryCoalescesDescriptorMentionsWithinSameCapture(t *testing.T) {
	interpreter := &stubInterpreter{
		interpretation: &schema.Interpretation{
			Status:       schema.InterpretationStatusTentative,
			Summary:      "Project Orchid and Orchid refer to the same project",
			ProposedType: schema.MemoryTypeSemantic,
			Mentions: []schema.Mention{
				{
					Surface:    "Project Orchid",
					EntityKind: schema.EntityKindProject,
					Confidence: 0.9,
				},
				{
					Surface:    "Orchid",
					EntityKind: schema.EntityKindProject,
					Confidence: 0.9,
				},
			},
			ExtractionConfidence: 0.9,
		},
	}
	svc, store := newCaptureTestService(t, interpreter)
	ctx := context.Background()

	resp, err := svc.CaptureMemory(ctx, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "event",
		Content:     map[string]any{"ref": "evt-descriptor-mentions", "text": "Project Orchid is also called Orchid"},
		Scope:       "project:alpha",
		Sensitivity: schema.SensitivityLow,
	})
	if err != nil {
		t.Fatalf("CaptureMemory: %v", err)
	}
	if len(resp.CreatedRecords) != 1 {
		t.Fatalf("CreatedRecords len = %d, want one coalesced entity", len(resp.CreatedRecords))
	}
	if len(resp.Edges) != 2 {
		t.Fatalf("Edges len = %d, want one bidirectional entity edge pair", len(resp.Edges))
	}

	got, err := store.Get(ctx, resp.PrimaryRecord.ID)
	if err != nil {
		t.Fatalf("Get primary: %v", err)
	}
	if got.Interpretation == nil || len(got.Interpretation.Mentions) != 2 {
		t.Fatalf("Interpretation = %+v, want two resolved mentions", got.Interpretation)
	}
	entityID := resp.CreatedRecords[0].ID
	for _, mention := range got.Interpretation.Mentions {
		if mention.CanonicalEntityID != entityID {
			t.Fatalf("Mention = %+v, want canonical entity %q", mention, entityID)
		}
	}
	if len(got.Relations) != 1 || got.Relations[0].TargetID != entityID {
		t.Fatalf("Relations = %+v, want one relation to coalesced entity", got.Relations)
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
		if !provenanceSourcesContain(rec.Provenance.Sources, schema.ProvenanceKindObservation, resp.PrimaryRecord.ID) {
			t.Fatalf("Semantic provenance sources = %+v, want observation source %s", rec.Provenance.Sources, resp.PrimaryRecord.ID)
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
	hasSemanticEntityLink := false
	for _, edge := range resp.Edges {
		if edge.Predicate == "subject_entity" && edge.SourceID != resp.PrimaryRecord.ID && edge.TargetID == entityID {
			hasSemanticEntityLink = true
			break
		}
	}
	if !hasSemanticEntityLink {
		t.Fatalf("Edges = %+v, want semantic record linked to canonical entity", resp.Edges)
	}
}

func TestCaptureMemoryCanonicalizesExplicitFactObjectThroughGlobalEntity(t *testing.T) {
	svc, store := newCaptureTestService(t, nil)
	ctx := context.Background()

	subject := newObservationEntity("entity-global-orchid", "Orchid", "")
	object := newObservationEntity("entity-global-borealis", "Borealis", "")
	for _, rec := range []*schema.MemoryRecord{subject, object} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	resp, err := svc.CaptureMemory(ctx, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "observation",
		Content:     map[string]any{"subject": "Orchid", "predicate": "depends_on", "object": "Borealis"},
		Scope:       "project:alpha",
		Sensitivity: schema.SensitivityLow,
	})
	if err != nil {
		t.Fatalf("CaptureMemory: %v", err)
	}

	var semantic *schema.MemoryRecord
	for _, rec := range resp.CreatedRecords {
		if rec.Type == schema.MemoryTypeSemantic {
			semantic = rec
			break
		}
	}
	if semantic == nil {
		t.Fatalf("CreatedRecords = %+v, want semantic record", resp.CreatedRecords)
	}
	payload := semantic.Payload.(*schema.SemanticPayload)
	if payload.Subject != subject.ID || payload.Object != object.ID {
		t.Fatalf("semantic payload = %+v, want global subject %q and object %q", payload, subject.ID, object.ID)
	}
	if !graphEdgesContain(resp.Edges, semantic.ID, schema.GraphPredicateSubjectEntity, subject.ID) ||
		!graphEdgesContain(resp.Edges, semantic.ID, schema.GraphPredicateObjectEntity, object.ID) ||
		!graphEdgesContain(resp.Edges, object.ID, schema.GraphPredicateFactObjectOf, semantic.ID) {
		t.Fatalf("Edges = %+v, want semantic subject/object links to global entities", resp.Edges)
	}
}

func TestCaptureMemoryReusesExistingSemanticFactForRepeatedObservation(t *testing.T) {
	svc, store := newCaptureTestService(t, nil)
	ctx := context.Background()
	content := map[string]any{"subject": "Orchid", "predicate": "deploy_target_for", "object": "staging"}

	first, err := svc.CaptureMemory(ctx, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "observation",
		Content:     content,
		Scope:       "project:alpha",
		Sensitivity: schema.SensitivityLow,
	})
	if err != nil {
		t.Fatalf("first CaptureMemory: %v", err)
	}
	var semanticID string
	for _, rec := range first.CreatedRecords {
		if rec.Type == schema.MemoryTypeSemantic {
			semanticID = rec.ID
			break
		}
	}
	if semanticID == "" {
		t.Fatalf("first CreatedRecords = %+v, want semantic record", first.CreatedRecords)
	}

	second, err := svc.CaptureMemory(ctx, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "observation",
		Content:     map[string]any{"subject": "Orchid", "predicate": "deployTargetFor", "object": "staging"},
		Scope:       "project:alpha",
		Sensitivity: schema.SensitivityLow,
	})
	if err != nil {
		t.Fatalf("second CaptureMemory: %v", err)
	}
	for _, rec := range second.CreatedRecords {
		if rec.Type == schema.MemoryTypeSemantic {
			t.Fatalf("second CreatedRecords = %+v, want no duplicate semantic record", second.CreatedRecords)
		}
	}
	if !graphEdgesContain(second.Edges, second.PrimaryRecord.ID, "derived_semantic", semanticID) ||
		!graphEdgesContain(second.Edges, semanticID, "derived_from", second.PrimaryRecord.ID) {
		t.Fatalf("second Edges = %+v, want provenance links to existing semantic %s", second.Edges, semanticID)
	}

	semantics, err := store.ListByType(ctx, schema.MemoryTypeSemantic)
	if err != nil {
		t.Fatalf("ListByType semantic: %v", err)
	}
	if len(semantics) != 1 {
		t.Fatalf("semantic count = %d, want one reused semantic fact", len(semantics))
	}
	got, err := store.Get(ctx, semanticID)
	if err != nil {
		t.Fatalf("Get semantic: %v", err)
	}
	payload := got.Payload.(*schema.SemanticPayload)
	if len(payload.Evidence) != 2 ||
		payload.Evidence[0].SourceID != first.PrimaryRecord.ID ||
		payload.Evidence[1].SourceID != second.PrimaryRecord.ID {
		t.Fatalf("semantic evidence = %+v, want first and second capture sources", payload.Evidence)
	}
	if !provenanceSourcesContain(got.Provenance.Sources, schema.ProvenanceKindObservation, first.PrimaryRecord.ID) ||
		!provenanceSourcesContain(got.Provenance.Sources, schema.ProvenanceKindObservation, second.PrimaryRecord.ID) {
		t.Fatalf("semantic provenance sources = %+v, want first and second capture sources", got.Provenance.Sources)
	}
	foundReinforce := false
	for _, entry := range got.AuditLog {
		if entry.Action == schema.AuditActionReinforce && entry.Actor == "ingestion/capture" {
			foundReinforce = true
			break
		}
	}
	if !foundReinforce {
		t.Fatalf("semantic audit log = %+v, want capture reinforcement", got.AuditLog)
	}
}

func TestCaptureMemoryKeepsSemanticFactReuseScoped(t *testing.T) {
	svc, store := newCaptureTestService(t, nil)
	ctx := context.Background()
	globalEntity := schema.NewMemoryRecord("entity-orchid-global", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Orchid",
		PrimaryType:   schema.EntityTypeProject,
		Types:         []string{schema.EntityTypeProject},
		Aliases:       []schema.EntityAlias{{Value: "Orchid"}},
	})
	if err := store.Create(ctx, globalEntity); err != nil {
		t.Fatalf("Create global entity: %v", err)
	}
	content := map[string]any{"subject": "Orchid", "predicate": "deploy_target_for", "object": "staging"}

	first, err := svc.CaptureMemory(ctx, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "observation",
		Content:     content,
		Scope:       "project:alpha",
		Sensitivity: schema.SensitivityLow,
	})
	if err != nil {
		t.Fatalf("first CaptureMemory: %v", err)
	}
	alphaSemantic := createdSemanticID(first.CreatedRecords)
	if alphaSemantic == "" {
		t.Fatalf("first CreatedRecords = %+v, want semantic record", first.CreatedRecords)
	}

	second, err := svc.CaptureMemory(ctx, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "observation",
		Content:     content,
		Scope:       "project:beta",
		Sensitivity: schema.SensitivityLow,
	})
	if err != nil {
		t.Fatalf("second CaptureMemory: %v", err)
	}
	betaSemantic := createdSemanticID(second.CreatedRecords)
	if betaSemantic == "" {
		t.Fatalf("second CreatedRecords = %+v, want separate scoped semantic record", second.CreatedRecords)
	}
	if betaSemantic == alphaSemantic {
		t.Fatalf("beta semantic ID = alpha semantic ID %s, want separate scoped facts", alphaSemantic)
	}
	if graphEdgesContain(second.Edges, second.PrimaryRecord.ID, "derived_semantic", alphaSemantic) {
		t.Fatalf("second Edges = %+v, leaked link to alpha semantic %s", second.Edges, alphaSemantic)
	}

	semantics, err := store.ListByType(ctx, schema.MemoryTypeSemantic)
	if err != nil {
		t.Fatalf("ListByType semantic: %v", err)
	}
	if len(semantics) != 2 {
		t.Fatalf("semantic count = %d, want separate semantic facts per scope", len(semantics))
	}
	beta, err := store.Get(ctx, betaSemantic)
	if err != nil {
		t.Fatalf("Get beta semantic: %v", err)
	}
	if beta.Scope != "project:beta" {
		t.Fatalf("beta semantic scope = %q, want project:beta", beta.Scope)
	}
}

func createdSemanticID(records []*schema.MemoryRecord) string {
	for _, rec := range records {
		if rec != nil && rec.Type == schema.MemoryTypeSemantic {
			return rec.ID
		}
	}
	return ""
}

func graphEdgesContain(edges []schema.GraphEdge, sourceID, predicate, targetID string) bool {
	for _, edge := range edges {
		if edge.SourceID == sourceID && edge.Predicate == predicate && edge.TargetID == targetID {
			return true
		}
	}
	return false
}

func provenanceSourcesContain(sources []schema.ProvenanceSource, kind schema.ProvenanceKind, ref string) bool {
	for _, source := range sources {
		if source.Kind == kind && source.Ref == ref {
			return true
		}
	}
	return false
}

func TestCaptureMemoryInterpreterMaterializesResolvedRelationCandidates(t *testing.T) {
	interpreter := &stubInterpreter{
		interpretation: &schema.Interpretation{
			Status:       schema.InterpretationStatusTentative,
			Summary:      "Orchid depends on rollout playbook",
			ProposedType: schema.MemoryTypeSemantic,
			Mentions: []schema.Mention{{
				Surface:    "Orchid",
				EntityKind: schema.EntityKindProject,
				Confidence: 0.9,
			}},
			RelationCandidates: []schema.RelationCandidate{{
				Predicate:  "depends_on",
				Confidence: 0.8,
			}},
			ReferenceCandidates: []schema.ReferenceCandidate{{
				Ref:        "playbook-ref",
				Confidence: 0.7,
			}},
			ExtractionConfidence: 0.9,
		},
		resolved: &schema.Interpretation{
			Mentions: []schema.Mention{{
				Surface:           "Orchid",
				EntityKind:        schema.EntityKindProject,
				CanonicalEntityID: "entity-existing",
				Confidence:        0.9,
			}},
			RelationCandidates: []schema.RelationCandidate{{
				Predicate:      "depends_on",
				TargetRecordID: "semantic-playbook",
				Confidence:     0.8,
				Resolved:       true,
			}},
			ReferenceCandidates: []schema.ReferenceCandidate{{
				Ref:            "playbook-ref",
				TargetRecordID: "semantic-playbook",
				Confidence:     0.7,
				Resolved:       true,
			}},
			ExtractionConfidence: 0.9,
		},
	}
	svc, store := newCaptureTestService(t, interpreter)
	ctx := context.Background()

	entity := schema.NewMemoryRecord("entity-existing", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Orchid",
		PrimaryType:   schema.EntityTypeProject,
		Types:         []string{schema.EntityTypeProject},
		Aliases:       []schema.EntityAlias{{Value: "orchid"}},
		Summary:       "Existing Orchid entity",
	})
	if err := store.Create(ctx, entity); err != nil {
		t.Fatalf("Create entity: %v", err)
	}
	playbook := schema.NewMemoryRecord("semantic-playbook", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "rollout",
		Predicate: "documented_in",
		Object:    "playbook",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	playbook.Relations = []schema.Relation{}
	if err := store.Create(ctx, playbook); err != nil {
		t.Fatalf("Create playbook: %v", err)
	}

	resp, err := svc.CaptureMemory(ctx, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "event",
		Content:     map[string]any{"ref": "evt-3", "text": "Orchid uses the rollout playbook"},
		Scope:       "project:alpha",
		Sensitivity: schema.SensitivityLow,
	})
	if err != nil {
		t.Fatalf("CaptureMemory: %v", err)
	}

	wantPredicates := map[string]bool{
		"mentions_entity":   false,
		"depends_on":        false,
		"dependency_of":     false,
		"references_record": false,
		"referenced_by":     false,
	}
	for _, edge := range resp.Edges {
		if _, ok := wantPredicates[edge.Predicate]; ok {
			wantPredicates[edge.Predicate] = true
		}
	}
	for predicate, seen := range wantPredicates {
		if !seen {
			t.Fatalf("Edges = %+v, missing %q", resp.Edges, predicate)
		}
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
	if len(got.Interpretation.RelationCandidates) != 1 || !got.Interpretation.RelationCandidates[0].Resolved {
		t.Fatalf("RelationCandidates = %+v, want resolved relation candidate", got.Interpretation.RelationCandidates)
	}
	if got.Interpretation.RelationCandidates[0].TargetRecordID != "semantic-playbook" {
		t.Fatalf("RelationCandidates = %+v, want target_record_id semantic-playbook", got.Interpretation.RelationCandidates)
	}
	if len(got.Interpretation.ReferenceCandidates) != 1 || !got.Interpretation.ReferenceCandidates[0].Resolved {
		t.Fatalf("ReferenceCandidates = %+v, want resolved reference candidate", got.Interpretation.ReferenceCandidates)
	}
}

func TestCaptureMemoryKeepsInterpretationTentativeWhenRelationsRemainUnresolved(t *testing.T) {
	interpreter := &stubInterpreter{
		interpretation: &schema.Interpretation{
			Status:       schema.InterpretationStatusTentative,
			Summary:      "Unresolved relation",
			ProposedType: schema.MemoryTypeSemantic,
			RelationCandidates: []schema.RelationCandidate{{
				Predicate:  "depends_on",
				Confidence: 0.4,
			}},
		},
	}
	svc, store := newCaptureTestService(t, interpreter)
	ctx := context.Background()

	resp, err := svc.CaptureMemory(ctx, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "event",
		Content:     map[string]any{"ref": "evt-4", "text": "Orchid depends on something unresolved"},
		Sensitivity: schema.SensitivityLow,
	})
	if err != nil {
		t.Fatalf("CaptureMemory: %v", err)
	}

	got, err := store.Get(ctx, resp.PrimaryRecord.ID)
	if err != nil {
		t.Fatalf("Get primary: %v", err)
	}
	if got.Interpretation == nil || got.Interpretation.Status != schema.InterpretationStatusTentative {
		t.Fatalf("Interpretation = %+v, want tentative status", got.Interpretation)
	}
}

func TestCaptureMemoryClearsResolverTargetsThatDoNotExist(t *testing.T) {
	interpreter := &stubInterpreter{
		interpretation: &schema.Interpretation{
			Status:       schema.InterpretationStatusTentative,
			Summary:      "Invalid resolver target",
			ProposedType: schema.MemoryTypeSemantic,
		},
		resolved: &schema.Interpretation{
			Status: schema.InterpretationStatusResolved,
			RelationCandidates: []schema.RelationCandidate{{
				Predicate:      "depends_on",
				TargetRecordID: "missing-record",
				Confidence:     0.9,
				Resolved:       true,
			}},
			ReferenceCandidates: []schema.ReferenceCandidate{{
				Ref:            "missing-ref",
				TargetRecordID: "missing-record",
				Confidence:     0.9,
				Resolved:       true,
			}},
			ExtractionConfidence: 0.9,
		},
	}
	svc, store := newCaptureTestService(t, interpreter)
	ctx := context.Background()

	resp, err := svc.CaptureMemory(ctx, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "event",
		Content:     map[string]any{"ref": "evt-invalid-resolver", "text": "Orchid uses something missing"},
		Sensitivity: schema.SensitivityLow,
	})
	if err != nil {
		t.Fatalf("CaptureMemory: %v", err)
	}
	for _, edge := range resp.Edges {
		if edge.TargetID == "missing-record" {
			t.Fatalf("Edges = %+v, want no edge to missing resolver target", resp.Edges)
		}
	}

	got, err := store.Get(ctx, resp.PrimaryRecord.ID)
	if err != nil {
		t.Fatalf("Get primary: %v", err)
	}
	relation := got.Interpretation.RelationCandidates[0]
	if relation.Resolved || relation.TargetRecordID != "" {
		t.Fatalf("RelationCandidate = %+v, want unresolved with cleared target", relation)
	}
	reference := got.Interpretation.ReferenceCandidates[0]
	if reference.Resolved || reference.TargetRecordID != "" {
		t.Fatalf("ReferenceCandidate = %+v, want unresolved with cleared target", reference)
	}
}
