package ingestion

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
	sqlitestore "github.com/BennettSchwartz/membrane/pkg/storage/sqlite"
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
	storage.Store
}

func (s *failingRelationStore) Begin(ctx context.Context) (storage.Transaction, error) {
	tx, err := s.Store.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &failingRelationTx{Transaction: tx}, nil
}

func (s *failingRelationStore) FindEntitiesByTerm(ctx context.Context, term, scope string, limit int) ([]*schema.MemoryRecord, error) {
	if lookup, ok := s.Store.(storage.EntityLookup); ok {
		return lookup.FindEntitiesByTerm(ctx, term, scope, limit)
	}
	return nil, nil
}

func (s *failingRelationStore) FindEntityByIdentifier(ctx context.Context, namespace, value, scope string) (*schema.MemoryRecord, error) {
	if lookup, ok := s.Store.(storage.EntityLookup); ok {
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
	updateSvc := NewService(&updateFailStore{Store: base, err: updateErr}, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))
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
	finalSvc := NewService(&updateFailAtStore{Store: base, failAt: 3, err: finalErr}, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))
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
			svc := NewService(&relationFailAtStore{Store: base, failAt: tc.failAt}, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))
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
	relationSvc := NewService(&relationFailAtStore{Store: relationBase, failAt: 1}, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))
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
	svc := NewService(&relationFailAtStore{Store: base, failAt: 1}, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))
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
	failSvc := NewService(&updateFailStore{Store: base, err: updateErr}, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))
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
	base, err := sqlitestore.Open(":memory:", "")
	if err != nil {
		t.Fatalf("Open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = base.Close() })

	store := &failingRelationStore{Store: base}
	svc := NewService(store, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))
	ctx := context.Background()

	_, err = svc.CaptureMemory(ctx, CaptureMemoryRequest{
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
