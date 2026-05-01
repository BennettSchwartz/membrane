package ingestion

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	sqlitestore "github.com/BennettSchwartz/membrane/pkg/storage/sqlite"
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
