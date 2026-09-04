package ingestion

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/internal/teststore"
	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

type recordingResolver struct {
	interpretation *schema.Interpretation
	resolved       *schema.Interpretation
	candidates     []*schema.MemoryRecord
}

type captureWorkCountingStore struct {
	boundedCaptureTestStore
	entityLookups int
}

type countingCaptureTerm struct {
	value string
	calls *int
}

func (t countingCaptureTerm) String() string {
	(*t.calls)++
	return t.value
}

func (s *captureWorkCountingStore) FindEntitiesByTerm(ctx context.Context, term, scope string, limit int) ([]*schema.MemoryRecord, error) {
	s.entityLookups++
	lookup, _ := s.boundedCaptureTestStore.(storage.EntityLookup)
	return lookup.FindEntitiesByTerm(ctx, term, scope, limit)
}

func (s *captureWorkCountingStore) FindEntityByIdentifier(ctx context.Context, namespace, value, scope string) (*schema.MemoryRecord, error) {
	s.entityLookups++
	lookup, _ := s.boundedCaptureTestStore.(storage.EntityLookup)
	return lookup.FindEntityByIdentifier(ctx, namespace, value, scope)
}

type unboundedOnlyCaptureStore struct {
	storage.Store
}

type scriptedBoundedCaptureStore struct {
	storage.Store
	calls   []storage.ListOptions
	results []storage.BoundedListResult
}

type noFullGetCaptureStore struct {
	boundedCaptureTestStore
	fullGetCalls          int
	semanticLookupCalls   int
	entityLookupCalls     int
	metadataOverrideSet   bool
	metadataOverride      []storage.RecordAuthorizationMetadata
	metadataOverrideError error
}

type noFullGetCaptureTx struct {
	storage.Transaction
	parent *noFullGetCaptureStore
}

func (s *noFullGetCaptureStore) Get(context.Context, string) (*schema.MemoryRecord, error) {
	s.fullGetCalls++
	return nil, errors.New("full Get is forbidden during access-restricted capture")
}

func (s *noFullGetCaptureStore) Begin(ctx context.Context) (storage.Transaction, error) {
	tx, err := s.boundedCaptureTestStore.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &noFullGetCaptureTx{Transaction: tx, parent: s}, nil
}

func (s *noFullGetCaptureStore) FindSemanticExactInScope(ctx context.Context, subject, predicate, object, scope string) (*schema.MemoryRecord, error) {
	s.semanticLookupCalls++
	lookup, ok := s.boundedCaptureTestStore.(storage.SemanticLookupInScope)
	if !ok {
		return nil, nil
	}
	return lookup.FindSemanticExactInScope(ctx, subject, predicate, object, scope)
}

func (s *noFullGetCaptureStore) FindEntitiesByTerm(ctx context.Context, term, scope string, limit int) ([]*schema.MemoryRecord, error) {
	s.entityLookupCalls++
	lookup, ok := s.boundedCaptureTestStore.(storage.EntityLookup)
	if !ok {
		return nil, nil
	}
	return lookup.FindEntitiesByTerm(ctx, term, scope, limit)
}

func (s *noFullGetCaptureStore) FindEntityByIdentifier(ctx context.Context, namespace, value, scope string) (*schema.MemoryRecord, error) {
	s.entityLookupCalls++
	lookup, ok := s.boundedCaptureTestStore.(storage.EntityLookup)
	if !ok {
		return nil, storage.ErrNotFound
	}
	return lookup.FindEntityByIdentifier(ctx, namespace, value, scope)
}

func (t *noFullGetCaptureTx) Get(context.Context, string) (*schema.MemoryRecord, error) {
	t.parent.fullGetCalls++
	return nil, errors.New("full transaction Get is forbidden during access-restricted capture")
}

func (t *noFullGetCaptureTx) GetAuthorizationMetadata(ctx context.Context, ids []string) ([]storage.RecordAuthorizationMetadata, error) {
	if t.parent.metadataOverrideSet {
		return append([]storage.RecordAuthorizationMetadata(nil), t.parent.metadataOverride...), t.parent.metadataOverrideError
	}
	lookup, ok := t.Transaction.(storage.AuthorizationMetadataStore)
	if !ok {
		return nil, storage.ErrAuthorizationMetadataUnsupported
	}
	return lookup.GetAuthorizationMetadata(ctx, ids)
}

func (s *scriptedBoundedCaptureStore) ListBounded(_ context.Context, opts storage.ListOptions) (storage.BoundedListResult, error) {
	s.calls = append(s.calls, opts)
	if len(s.results) == 0 {
		return storage.BoundedListResult{}, nil
	}
	result := s.results[0]
	s.results = s.results[1:]
	return result, nil
}

func TestCaptureMemoryWithAccessResolverTargetsNeverUseFullGet(t *testing.T) {
	base := teststore.NewMemoryStore()
	t.Cleanup(func() { _ = base.Close() })
	target := schema.NewMemoryRecord("bounded-resolver-target", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind: "semantic", Subject: "target", Predicate: "is", Object: "bounded",
		Validity: schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	target.Scope = "project:alpha"
	for i := 0; i < 2_000; i++ {
		target.AuditLog = append(target.AuditLog, schema.AuditEntry{Actor: fmt.Sprintf("actor-%04d", i), Rationale: strings.Repeat("history", 16)})
		target.Provenance.Sources = append(target.Provenance.Sources, schema.ProvenanceSource{Ref: fmt.Sprintf("source-%04d", i)})
	}
	if err := base.Create(context.Background(), target); err != nil {
		t.Fatalf("Create target: %v", err)
	}

	resolved := &schema.Interpretation{
		ReferenceCandidates: make([]schema.ReferenceCandidate, MaxCaptureReferenceCandidates),
		RelationCandidates:  make([]schema.RelationCandidate, MaxCaptureRelationCandidates),
	}
	for i := range resolved.ReferenceCandidates {
		id := target.ID
		if i%2 == 1 {
			id = fmt.Sprintf("absent-reference-%02d", i)
		}
		resolved.ReferenceCandidates[i] = schema.ReferenceCandidate{Ref: id, TargetRecordID: id, Resolved: true}
	}
	for i := range resolved.RelationCandidates {
		id := target.ID
		if i%2 == 1 {
			id = fmt.Sprintf("absent-relation-%02d", i)
		}
		resolved.RelationCandidates[i] = schema.RelationCandidate{Predicate: "related_to", TargetRecordID: id, Resolved: true}
	}
	store := &noFullGetCaptureStore{boundedCaptureTestStore: base}
	svc := NewServiceWithInterpreter(store, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()), &recordingResolver{resolved: resolved})
	resp, err := svc.CaptureMemoryWithAccess(context.Background(), CaptureMemoryRequest{
		Source: "tester", SourceKind: "event", Content: map[string]any{"note": "bounded resolver targets"},
		Scope: "project:alpha", Sensitivity: schema.SensitivityLow,
	}, lowCaptureAccess())
	if err != nil {
		t.Fatalf("CaptureMemoryWithAccess: %v", err)
	}
	if store.fullGetCalls != 0 {
		t.Fatalf("access-restricted resolver issued %d full Get calls across max references/relations, want 0", store.fullGetCalls)
	}
	got := resp.PrimaryRecord.Interpretation
	if len(got.ReferenceCandidates) != MaxCaptureReferenceCandidates || len(got.RelationCandidates) != MaxCaptureRelationCandidates {
		t.Fatalf("retained resolver candidates = references:%d relations:%d, want %d/%d",
			len(got.ReferenceCandidates), len(got.RelationCandidates), MaxCaptureReferenceCandidates, MaxCaptureRelationCandidates)
	}
	for i, candidate := range got.ReferenceCandidates {
		if i%2 == 0 && (!candidate.Resolved || candidate.TargetRecordID != target.ID) {
			t.Fatalf("authorized reference %d = %+v, want resolved target", i, candidate)
		}
		if i%2 == 1 && (candidate.Resolved || candidate.TargetRecordID != "" || candidate.TargetEntityID != "") {
			t.Fatalf("absent reference %d = %+v, want unresolved and cleared", i, candidate)
		}
	}
	for i, candidate := range got.RelationCandidates {
		if i%2 == 0 && (!candidate.Resolved || candidate.TargetRecordID != target.ID) {
			t.Fatalf("authorized relation %d = %+v, want resolved target", i, candidate)
		}
		if i%2 == 1 && (candidate.Resolved || candidate.TargetRecordID != "" || candidate.TargetEntityID != "") {
			t.Fatalf("absent relation %d = %+v, want unresolved and cleared", i, candidate)
		}
	}
	if len(resp.Edges) == 0 {
		t.Fatal("authorized bounded resolver target produced no graph edges")
	}
}

func TestCaptureMemoryWithAccessMetadataRecheckMismatchPreventsMutation(t *testing.T) {
	for _, tc := range []struct {
		name        string
		metadata    []storage.RecordAuthorizationMetadata
		metadataErr error
	}{
		{name: "row disappeared"},
		{name: "policy changed", metadata: []storage.RecordAuthorizationMetadata{{
			ID: "recheck-target", Scope: "project:secret", Sensitivity: schema.SensitivityHigh,
		}}},
		{name: "metadata unsupported", metadataErr: storage.ErrAuthorizationMetadataUnsupported},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := teststore.NewMemoryStore()
			t.Cleanup(func() { _ = base.Close() })
			target := schema.NewMemoryRecord("recheck-target", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
				Kind: "semantic", Subject: "target", Predicate: "is", Object: "initially visible",
				Validity: schema.Validity{Mode: schema.ValidityModeGlobal},
			})
			target.Scope = "project:alpha"
			if err := base.Create(context.Background(), target); err != nil {
				t.Fatalf("Create target: %v", err)
			}
			resolved := &schema.Interpretation{
				ReferenceCandidates: []schema.ReferenceCandidate{{Ref: target.ID, TargetRecordID: target.ID, Resolved: true}},
				RelationCandidates:  []schema.RelationCandidate{{Predicate: "related_to", TargetRecordID: target.ID, Resolved: true}},
			}
			store := &noFullGetCaptureStore{
				boundedCaptureTestStore: base,
				metadataOverrideSet:     true,
				metadataOverride:        tc.metadata,
				metadataOverrideError:   tc.metadataErr,
			}
			svc := NewServiceWithInterpreter(store, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()), &recordingResolver{resolved: resolved})
			resp, err := svc.CaptureMemoryWithAccess(context.Background(), CaptureMemoryRequest{
				Source: "tester", SourceKind: "event", Content: map[string]any{"note": "recheck current metadata"},
				Scope: "project:alpha", Sensitivity: schema.SensitivityLow,
			}, lowCaptureAccess())
			if err != nil {
				t.Fatalf("CaptureMemoryWithAccess: %v", err)
			}
			got := resp.PrimaryRecord.Interpretation
			if got.ReferenceCandidates[0].Resolved || got.ReferenceCandidates[0].TargetRecordID != "" ||
				got.RelationCandidates[0].Resolved || got.RelationCandidates[0].TargetRecordID != "" || len(resp.Edges) != 0 {
				t.Fatalf("metadata mismatch materialized target: interpretation=%+v edges=%+v", got, resp.Edges)
			}
			stored, err := base.Get(context.Background(), target.ID)
			if err != nil {
				t.Fatalf("Get target: %v", err)
			}
			if len(stored.Relations) != 0 || store.fullGetCalls != 0 {
				t.Fatalf("metadata mismatch mutated target or used full Get: relations=%+v getCalls=%d", stored.Relations, store.fullGetCalls)
			}
		})
	}
}

func TestCaptureMemoryWithAccessEntityResolutionUsesBoundedIndexOnly(t *testing.T) {
	base := teststore.NewMemoryStore()
	t.Cleanup(func() { _ = base.Close() })
	entity := schema.NewMemoryRecord("entity-bounded-orchid", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind: "entity", CanonicalName: "Orchid", PrimaryType: schema.EntityTypeProject,
		Types: []string{schema.EntityTypeProject}, Summary: "Orchid",
	})
	entity.Scope = "project:alpha"
	if err := base.Create(context.Background(), entity); err != nil {
		t.Fatalf("Create entity: %v", err)
	}
	store := &noFullGetCaptureStore{boundedCaptureTestStore: base}
	resolver := &recordingResolver{resolved: &schema.Interpretation{Mentions: []schema.Mention{{Surface: "Orchid"}}}}
	svc := NewServiceWithInterpreter(store, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()), resolver)
	resp, err := svc.CaptureMemoryWithAccess(context.Background(), CaptureMemoryRequest{
		Source: "tester", SourceKind: "observation",
		Content: map[string]any{"subject": "Orchid", "predicate": "deploy_target_for", "object": "staging"},
		Scope:   "project:alpha", Sensitivity: schema.SensitivityLow,
	}, lowCaptureAccess())
	if err != nil {
		t.Fatalf("CaptureMemoryWithAccess: %v", err)
	}
	if store.fullGetCalls != 0 || store.entityLookupCalls != 0 {
		t.Fatalf("restricted entity resolution used full Get/legacy lookup = %d/%d, want 0/0", store.fullGetCalls, store.entityLookupCalls)
	}
	mentions := resp.PrimaryRecord.Interpretation.Mentions
	if len(mentions) != 1 || mentions[0].CanonicalEntityID != entity.ID {
		t.Fatalf("mentions = %+v, want bounded entity %s", mentions, entity.ID)
	}
	var semantic *schema.MemoryRecord
	for _, record := range resp.CreatedRecords {
		if record.Type == schema.MemoryTypeSemantic {
			semantic = record
			break
		}
	}
	if semantic == nil {
		t.Fatalf("CreatedRecords = %+v, want derived semantic", resp.CreatedRecords)
	}
	payload, _ := semantic.Payload.(*schema.SemanticPayload)
	if payload == nil || payload.Subject != entity.ID {
		t.Fatalf("derived semantic = %+v, want canonical bounded entity subject", semantic)
	}
}

func TestCaptureMemoryWithAccessExactSemanticPathNeverUsesFullGet(t *testing.T) {
	base := teststore.NewMemoryStore()
	t.Cleanup(func() { _ = base.Close() })
	existing := schema.NewMemoryRecord("high-history-exact-semantic", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind: "semantic", Subject: "Orchid", Predicate: "deploy_target_for", Object: "staging",
		Validity: schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	existing.Scope = "project:alpha"
	for i := 0; i < 2_000; i++ {
		existing.AuditLog = append(existing.AuditLog, schema.AuditEntry{Actor: fmt.Sprintf("actor-%04d", i), Rationale: strings.Repeat("history", 16)})
		existing.Provenance.Sources = append(existing.Provenance.Sources, schema.ProvenanceSource{Ref: fmt.Sprintf("source-%04d", i)})
	}
	if err := base.Create(context.Background(), existing); err != nil {
		t.Fatalf("Create existing semantic: %v", err)
	}
	before, err := base.Get(context.Background(), existing.ID)
	if err != nil {
		t.Fatalf("Get existing semantic before capture: %v", err)
	}
	store := &noFullGetCaptureStore{boundedCaptureTestStore: base}
	svc := NewService(store, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))
	resp, err := svc.CaptureMemoryWithAccess(context.Background(), CaptureMemoryRequest{
		Source: "tester", SourceKind: "observation",
		Content: map[string]any{"subject": "Orchid", "predicate": "deploy_target_for", "object": "staging"},
		Scope:   "project:alpha", Sensitivity: schema.SensitivityLow,
	}, lowCaptureAccess())
	if err != nil {
		t.Fatalf("CaptureMemoryWithAccess: %v", err)
	}
	if store.fullGetCalls != 0 || store.semanticLookupCalls != 0 {
		t.Fatalf("access-restricted exact semantic path used full Get/legacy lookup = %d/%d, want 0/0", store.fullGetCalls, store.semanticLookupCalls)
	}
	if len(resp.CreatedRecords) == 0 {
		t.Fatal("access-restricted exact semantic path created no finite replacement record")
	}
	if resp.CreatedRecords[0].ID == existing.ID {
		t.Fatalf("restricted exact semantic reused projected record %q instead of creating a finite replacement", existing.ID)
	}
	stored, err := base.Get(context.Background(), existing.ID)
	if err != nil {
		t.Fatalf("Get existing semantic: %v", err)
	}
	if !reflect.DeepEqual(stored.AuditLog, before.AuditLog) || !reflect.DeepEqual(stored.Provenance.Sources, before.Provenance.Sources) || !reflect.DeepEqual(stored.Relations, before.Relations) {
		t.Fatalf("existing high-history semantic was overwritten: audit=%d/%d provenance=%d/%d relations=%d/%d",
			len(stored.AuditLog), len(before.AuditLog), len(stored.Provenance.Sources), len(before.Provenance.Sources), len(stored.Relations), len(before.Relations))
	}
}

func (r *recordingResolver) Interpret(context.Context, InterpretRequest) (*schema.Interpretation, error) {
	return r.interpretation, nil
}

func (r *recordingResolver) Resolve(_ context.Context, req ResolveRequest) (*schema.Interpretation, error) {
	r.candidates = append([]*schema.MemoryRecord(nil), req.Candidates...)
	return r.resolved, nil
}

func lowCaptureAccess() CaptureAccess {
	allowed := func(rec *schema.MemoryRecord) bool {
		return rec != nil && rec.Sensitivity == schema.SensitivityLow
	}
	return CaptureAccess{CanRead: allowed, CanWrite: allowed}
}

func TestCaptureMemoryCapsInterpreterDerivedWorkBeforeLookupsAndWrites(t *testing.T) {
	const oversized = 150
	interpretation := &schema.Interpretation{
		Status:              schema.InterpretationStatusTentative,
		Mentions:            make([]schema.Mention, oversized),
		ReferenceCandidates: make([]schema.ReferenceCandidate, oversized),
		RelationCandidates:  make([]schema.RelationCandidate, oversized),
	}
	for i := 0; i < oversized; i++ {
		aliases := make([]string, 20)
		for j := range aliases {
			aliases[j] = fmt.Sprintf("alias-%03d-%03d", i, j)
		}
		interpretation.Mentions[i] = schema.Mention{
			Surface: fmt.Sprintf("entity-%03d", i),
			Aliases: aliases,
		}
		interpretation.ReferenceCandidates[i] = schema.ReferenceCandidate{Ref: fmt.Sprintf("ref-%03d", i)}
		interpretation.RelationCandidates[i] = schema.RelationCandidate{Predicate: "related_to"}
	}

	svc, _ := newCaptureTestService(t, &stubInterpreter{interpretation: interpretation})
	counting := &captureWorkCountingStore{boundedCaptureTestStore: svc.store.(boundedCaptureTestStore)}
	svc.store = counting

	resp, err := svc.CaptureMemory(context.Background(), CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "event",
		Content:     map[string]any{"note": "bounded derived work"},
		Scope:       "project:alpha",
		Sensitivity: schema.SensitivityLow,
	})
	if err != nil {
		t.Fatalf("CaptureMemory: %v", err)
	}
	got := resp.PrimaryRecord.Interpretation
	if got == nil {
		t.Fatal("Interpretation = nil")
	}
	if total := len(got.Mentions) + len(got.ReferenceCandidates) + len(got.RelationCandidates); total > 128 {
		t.Fatalf("derived interpretation work items = %d, want <= 128", total)
	}
	if len(got.Mentions) > 64 || len(got.ReferenceCandidates) > 64 || len(got.RelationCandidates) > 64 {
		t.Fatalf("derived interpretation collection sizes = mentions:%d references:%d relations:%d, want each <= 64",
			len(got.Mentions), len(got.ReferenceCandidates), len(got.RelationCandidates))
	}
	if counting.entityLookups > 256 {
		t.Fatalf("entity lookup calls = %d, want request-wide cap <= 256", counting.entityLookups)
	}
	if len(resp.CreatedRecords) > 65 || len(resp.Edges) > 260 {
		t.Fatalf("derived writes = records:%d edges:%d, want bounded by retained work", len(resp.CreatedRecords), len(resp.Edges))
	}
}

func TestBoundCaptureInterpretationAppliesDeterministicPerCollectionAndAggregateCaps(t *testing.T) {
	const oversized = 70
	input := &schema.Interpretation{
		TopicalLabels:       make([]string, oversized),
		Mentions:            make([]schema.Mention, oversized),
		ReferenceCandidates: make([]schema.ReferenceCandidate, oversized),
		RelationCandidates:  make([]schema.RelationCandidate, oversized),
	}
	for i := 0; i < oversized; i++ {
		input.TopicalLabels[i] = fmt.Sprintf("label-%02d", i)
		input.Mentions[i] = schema.Mention{
			Surface: fmt.Sprintf("mention-%02d", i),
			Aliases: make([]string, MaxCaptureAliasesPerMention+1),
		}
		input.ReferenceCandidates[i] = schema.ReferenceCandidate{Ref: fmt.Sprintf("reference-%02d", i)}
		input.RelationCandidates[i] = schema.RelationCandidate{Predicate: fmt.Sprintf("relation-%02d", i)}
	}

	got := boundCaptureInterpretation(input)
	if len(got.Mentions) != 43 || len(got.ReferenceCandidates) != 43 || len(got.RelationCandidates) != 42 {
		t.Fatalf("bounded collection sizes = mentions:%d references:%d relations:%d, want 43/43/42",
			len(got.Mentions), len(got.ReferenceCandidates), len(got.RelationCandidates))
	}
	if len(got.TopicalLabels) != MaxCaptureTopicalLabels || len(got.Mentions[0].Aliases) != MaxCaptureAliasesPerMention {
		t.Fatalf("nested bounds = labels:%d aliases:%d, want %d/%d",
			len(got.TopicalLabels), len(got.Mentions[0].Aliases), MaxCaptureTopicalLabels, MaxCaptureAliasesPerMention)
	}
	if got.Mentions[42].Surface != "mention-42" || got.ReferenceCandidates[42].Ref != "reference-42" || got.RelationCandidates[41].Predicate != "relation-41" {
		t.Fatalf("bounded order changed: last mention=%q reference=%q relation=%q",
			got.Mentions[42].Surface, got.ReferenceCandidates[42].Ref, got.RelationCandidates[41].Predicate)
	}
	if len(input.Mentions[0].Aliases) != MaxCaptureAliasesPerMention+1 {
		t.Fatal("boundCaptureInterpretation mutated interpreter-owned aliases")
	}

	onlyMentions := boundCaptureInterpretation(&schema.Interpretation{Mentions: make([]schema.Mention, oversized)})
	if len(onlyMentions.Mentions) != MaxCaptureMentions {
		t.Fatalf("single collection cap = %d, want %d", len(onlyMentions.Mentions), MaxCaptureMentions)
	}
}

func TestFallbackInterpretationTruncationIsDeterministic(t *testing.T) {
	content := make(map[string]any, MaxCaptureMentions+10)
	for i := 0; i < MaxCaptureMentions+10; i++ {
		content[fmt.Sprintf("entity_%03d", i)] = fmt.Sprintf("surface-%03d", i)
	}

	for attempt := 0; attempt < 10; attempt++ {
		got := inferMentionsFromContent(content)
		if len(got) != MaxCaptureMentions {
			t.Fatalf("attempt %d mentions = %d, want %d", attempt, len(got), MaxCaptureMentions)
		}
		if got[0].Surface != "surface-000" || got[len(got)-1].Surface != "surface-063" {
			t.Fatalf("attempt %d retained range = %q..%q, want surface-000..surface-063",
				attempt, got[0].Surface, got[len(got)-1].Surface)
		}
	}
}

func TestFetchCaptureCandidatesCapsAndPrecomputesActiveEntityTerms(t *testing.T) {
	svc, store := newCaptureTestService(t, nil)
	ctx := context.Background()
	for i := 0; i < captureCandidateLimit; i++ {
		record := schema.NewMemoryRecord(fmt.Sprintf("candidate-%02d", i), schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
			Kind:      "semantic",
			Subject:   "candidate",
			Predicate: "is",
			Object:    i,
			Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
		})
		if err := store.Create(ctx, record); err != nil {
			t.Fatalf("Create candidate %d: %v", i, err)
		}
	}

	const oversized = 4096
	conversions := 0
	activeEntities := make([]any, oversized)
	for i := range activeEntities {
		activeEntities[i] = countingCaptureTerm{
			// Repetition verifies that the work budget counts examined inputs,
			// not only retained unique terms.
			value: "repeated-active-entity",
			calls: &conversions,
		}
	}

	got, err := svc.fetchCaptureCandidates(ctx, CaptureMemoryRequest{
		Context: map[string]any{"active_entities": activeEntities},
	}, nil)
	if err != nil {
		t.Fatalf("fetchCaptureCandidates: %v", err)
	}
	if len(got) != captureCandidateLimit {
		t.Fatalf("candidate count = %d, want %d", len(got), captureCandidateLimit)
	}
	if conversions > MaxCaptureCandidateQueryTerms {
		t.Fatalf("active-entity conversions = %d, want at most one precomputed %d-term budget", conversions, MaxCaptureCandidateQueryTerms)
	}
}

func TestPrepareCaptureResolutionCapsResolverOverrideBeforeTransaction(t *testing.T) {
	const oversized = 100
	resolver := &stubInterpreter{
		interpretation: &schema.Interpretation{Summary: "initial"},
		resolved: &schema.Interpretation{
			Mentions:            make([]schema.Mention, oversized),
			ReferenceCandidates: make([]schema.ReferenceCandidate, oversized),
			RelationCandidates:  make([]schema.RelationCandidate, oversized),
		},
	}
	svc, _ := newCaptureTestService(t, resolver)
	got, _, err := svc.prepareCaptureResolution(context.Background(), CaptureMemoryRequest{
		Source: "tester", SourceKind: "event", Content: map[string]any{"note": "resolver bound"},
	}, time.Time{})
	if err != nil {
		t.Fatalf("prepareCaptureResolution: %v", err)
	}
	if total := len(got.Mentions) + len(got.ReferenceCandidates) + len(got.RelationCandidates); total != MaxCaptureInterpretationWorkItems {
		t.Fatalf("resolver work items = %d, want %d", total, MaxCaptureInterpretationWorkItems)
	}
}

func TestCaptureMemoryWithAccessFiltersResolverCandidates(t *testing.T) {
	resolver := &recordingResolver{}
	svc, store := newCaptureTestService(t, resolver)
	ctx := context.Background()

	for _, rec := range []*schema.MemoryRecord{
		schema.NewMemoryRecord("candidate-low", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
			Kind: "semantic", Subject: "visible", Predicate: "is", Object: "safe",
			Validity: schema.Validity{Mode: schema.ValidityModeGlobal},
		}),
		schema.NewMemoryRecord("candidate-high", schema.MemoryTypeSemantic, schema.SensitivityHigh, &schema.SemanticPayload{
			Kind: "semantic", Subject: "secret", Predicate: "is", Object: "classified",
			Validity: schema.Validity{Mode: schema.ValidityModeGlobal},
		}),
	} {
		rec.Scope = "project:alpha"
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	if _, err := svc.CaptureMemoryWithAccess(ctx, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "event",
		Content:     map[string]any{"ref": "evt-access-candidates", "text": "remember this"},
		Scope:       "project:alpha",
		Sensitivity: schema.SensitivityLow,
	}, lowCaptureAccess()); err != nil {
		t.Fatalf("CaptureMemoryWithAccess: %v", err)
	}

	got := make(map[string]bool, len(resolver.candidates))
	for _, rec := range resolver.candidates {
		got[rec.ID] = true
	}
	if !got["candidate-low"] {
		t.Fatalf("resolver candidates = %v, want visible candidate", got)
	}
	if got["candidate-high"] {
		t.Fatalf("resolver candidates = %v, leaked unauthorized candidate", got)
	}
}

func TestCaptureMemoryWithAccessLeavesUnauthorizedInferredTargetsUnresolved(t *testing.T) {
	resolver := &recordingResolver{
		resolved: &schema.Interpretation{
			Status: schema.InterpretationStatusResolved,
			ReferenceCandidates: []schema.ReferenceCandidate{{
				Ref: "secret", TargetRecordID: "target-high", Resolved: true, Confidence: 0.9,
			}},
			RelationCandidates: []schema.RelationCandidate{{
				Predicate: "related_to", TargetRecordID: "target-high", Resolved: true, Confidence: 0.9,
			}},
		},
	}
	svc, store := newCaptureTestService(t, resolver)
	ctx := context.Background()
	target := schema.NewMemoryRecord("target-high", schema.MemoryTypeSemantic, schema.SensitivityHigh, &schema.SemanticPayload{
		Kind: "semantic", Subject: "secret", Predicate: "is", Object: "classified",
		Validity: schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	target.Scope = "project:alpha"
	if err := store.Create(ctx, target); err != nil {
		t.Fatalf("Create target: %v", err)
	}

	resp, err := svc.CaptureMemoryWithAccess(ctx, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "event",
		Content:     map[string]any{"ref": "evt-access-target", "text": "secret"},
		Scope:       "project:alpha",
		Sensitivity: schema.SensitivityLow,
	}, lowCaptureAccess())
	if err != nil {
		t.Fatalf("CaptureMemoryWithAccess: %v", err)
	}
	if len(resp.Edges) != 0 {
		t.Fatalf("Edges = %+v, want no edges to unauthorized target", resp.Edges)
	}
	got := resp.PrimaryRecord.Interpretation
	if got == nil || len(got.ReferenceCandidates) != 1 || got.ReferenceCandidates[0].Resolved || got.ReferenceCandidates[0].TargetRecordID != "" {
		t.Fatalf("ReferenceCandidates = %+v, want unresolved non-oracular candidate", got)
	}
	if len(got.RelationCandidates) != 1 || got.RelationCandidates[0].Resolved || got.RelationCandidates[0].TargetRecordID != "" {
		t.Fatalf("RelationCandidates = %+v, want unresolved non-oracular candidate", got)
	}
	storedTarget, err := store.Get(ctx, target.ID)
	if err != nil {
		t.Fatalf("Get target: %v", err)
	}
	if len(storedTarget.Relations) != 0 {
		t.Fatalf("target relations = %+v, want unauthorized target unchanged", storedTarget.Relations)
	}
}

func TestCaptureMemoryWithAccessDoesNotReinforceUnauthorizedExactFact(t *testing.T) {
	svc, store := newCaptureTestService(t, nil)
	ctx := context.Background()
	existing := schema.NewMemoryRecord("semantic-high", schema.MemoryTypeSemantic, schema.SensitivityHigh, &schema.SemanticPayload{
		Kind: "semantic", Subject: "Orchid", Predicate: "deploy_target_for", Object: "staging",
		Validity: schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	existing.Scope = "project:alpha"
	existing.Salience = 0.4
	if err := store.Create(ctx, existing); err != nil {
		t.Fatalf("Create existing semantic: %v", err)
	}

	resp, err := svc.CaptureMemoryWithAccess(ctx, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "observation",
		Content:     map[string]any{"subject": "Orchid", "predicate": "deploy_target_for", "object": "staging"},
		Scope:       "project:alpha",
		Sensitivity: schema.SensitivityLow,
	}, lowCaptureAccess())
	if err != nil {
		t.Fatalf("CaptureMemoryWithAccess: %v", err)
	}
	var createdSemantic *schema.MemoryRecord
	for _, rec := range resp.CreatedRecords {
		if rec.Type == schema.MemoryTypeSemantic {
			createdSemantic = rec
		}
	}
	if createdSemantic == nil || createdSemantic.ID == existing.ID {
		t.Fatalf("CreatedRecords = %+v, want a new accessible semantic record", resp.CreatedRecords)
	}
	for _, edge := range resp.Edges {
		if edge.SourceID == existing.ID || edge.TargetID == existing.ID {
			t.Fatalf("Edges = %+v, leaked unauthorized exact fact", resp.Edges)
		}
	}
	stored, err := store.Get(ctx, existing.ID)
	if err != nil {
		t.Fatalf("Get existing semantic: %v", err)
	}
	if stored.Salience != 0.4 || len(stored.Relations) != 0 || len(stored.AuditLog) != 1 {
		t.Fatalf("unauthorized exact fact mutated: %+v", stored)
	}
}

func TestCaptureMemoryWithAccessRequiresWriteAccessToMaterializeReadableTarget(t *testing.T) {
	resolver := &recordingResolver{resolved: &schema.Interpretation{
		Status: schema.InterpretationStatusResolved,
		ReferenceCandidates: []schema.ReferenceCandidate{{
			Ref: "read-only", TargetRecordID: "target-read-only", Resolved: true,
		}},
	}}
	svc, store := newCaptureTestService(t, resolver)
	ctx := context.Background()
	target := schema.NewMemoryRecord("target-read-only", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind: "semantic", Subject: "read-only", Predicate: "is", Object: "visible",
		Validity: schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	target.Scope = "project:alpha"
	if err := store.Create(ctx, target); err != nil {
		t.Fatalf("Create target: %v", err)
	}

	resp, err := svc.CaptureMemoryWithAccess(ctx, CaptureMemoryRequest{
		Source: "tester", SourceKind: "event", Content: map[string]any{"note": "read-only"},
		Scope: "project:alpha", Sensitivity: schema.SensitivityLow,
	}, CaptureAccess{
		CanRead:  func(*schema.MemoryRecord) bool { return true },
		CanWrite: func(rec *schema.MemoryRecord) bool { return rec.ID != target.ID },
	})
	if err != nil {
		t.Fatalf("CaptureMemoryWithAccess: %v", err)
	}
	ref := resp.PrimaryRecord.Interpretation.ReferenceCandidates[0]
	if ref.Resolved || ref.TargetRecordID != "" || len(resp.Edges) != 0 {
		t.Fatalf("reference = %+v, edges = %+v; want readable but non-writable target unresolved", ref, resp.Edges)
	}
}

func TestFetchCaptureCandidatesFailsClosedWithoutBoundedListStore(t *testing.T) {
	base := teststore.NewMemoryStore()
	t.Cleanup(func() { _ = base.Close() })
	svc := &Service{store: &unboundedOnlyCaptureStore{Store: base}}

	_, err := svc.fetchCaptureCandidates(context.Background(), CaptureMemoryRequest{}, nil)
	if err == nil || !strings.Contains(err.Error(), "bounded candidate lookup") {
		t.Fatalf("fetchCaptureCandidates error = %v, want fail-closed bounded lookup error", err)
	}
}

func TestFetchCaptureCandidatesSharesProjectionBudgetAndSanitizesHistory(t *testing.T) {
	base := teststore.NewMemoryStore()
	t.Cleanup(func() { _ = base.Close() })
	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	newCandidate := func(id, scope string) *schema.MemoryRecord {
		record := schema.NewMemoryRecord(id, schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
			Kind: "semantic", Subject: id, Predicate: "is", Object: "bounded",
			Validity: schema.Validity{Mode: schema.ValidityModeGlobal},
		})
		record.Scope = scope
		record.CreatedAt = now
		record.Relations = []schema.Relation{{Predicate: "related_to", TargetID: "hidden-history"}}
		record.AuditLog = []schema.AuditEntry{{Action: schema.AuditActionCreate, Actor: "history"}}
		record.Provenance.Sources = []schema.ProvenanceSource{{Kind: schema.ProvenanceKindEvent, Ref: "history"}}
		return record
	}
	scoped := newCandidate("scoped", "project:alpha")
	global := newCandidate("global", "")
	scopedProjection := *scoped
	scopedProjection.Relations = nil
	scopedProjection.AuditLog = nil
	scopedProjection.Provenance.Sources = nil
	firstCharge := storage.ProjectedRecordBytes(&scopedProjection, storage.MaxBoundedHydrationBytes)
	store := &scriptedBoundedCaptureStore{
		Store: base,
		results: []storage.BoundedListResult{
			// Deliberately under-report to verify the caller independently measures
			// the sanitized projection before carrying the shared remainder.
			{Records: []*schema.MemoryRecord{scoped}, ProjectedBytes: 1},
			{Records: []*schema.MemoryRecord{global}, ProjectedBytes: storage.ProjectedRecordOverheadBytes},
		},
	}
	svc := &Service{store: store}

	got, err := svc.fetchCaptureCandidates(context.Background(), CaptureMemoryRequest{Scope: "project:alpha"}, nil)
	if err != nil {
		t.Fatalf("fetchCaptureCandidates: %v", err)
	}
	if len(store.calls) != 2 {
		t.Fatalf("ListBounded calls = %d, want scoped and global", len(store.calls))
	}
	first, second := store.calls[0], store.calls[1]
	if first.Scope != "project:alpha" || first.Limit != captureCandidateSearchPool || !first.OmitRelations || !first.OmitHistory {
		t.Fatalf("scoped ListBounded options = %+v, want bounded history-free scoped projection", first)
	}
	if first.MaxHydratedBytes != storage.MaxBoundedHydrationBytes {
		t.Fatalf("scoped hydration budget = %d, want %d", first.MaxHydratedBytes, storage.MaxBoundedHydrationBytes)
	}
	if second.Scope != "" || len(second.Scopes) != 1 || second.Scopes[0] != "" || !second.IncludeUnscoped || !second.OmitRelations || !second.OmitHistory {
		t.Fatalf("global ListBounded options = %+v, want exact-unscoped history-free projection", second)
	}
	if want := storage.MaxBoundedHydrationBytes - firstCharge; second.MaxHydratedBytes != want {
		t.Fatalf("global hydration budget = %d, want shared remainder %d", second.MaxHydratedBytes, want)
	}
	if len(got) != 2 {
		t.Fatalf("candidates = %v, want both bounded projections", recordIDs(got))
	}
	for _, record := range got {
		if len(record.Relations) != 0 || len(record.AuditLog) != 0 || len(record.Provenance.Sources) != 0 {
			t.Fatalf("candidate %q retained history: relations=%d audit=%d provenance=%d", record.ID, len(record.Relations), len(record.AuditLog), len(record.Provenance.Sources))
		}
	}
	if len(scoped.Relations) == 0 || len(scoped.AuditLog) == 0 || len(scoped.Provenance.Sources) == 0 {
		t.Fatal("candidate sanitizer mutated the store-owned record")
	}
}

func TestCaptureCandidateSearchTermsBoundHugeStoreFields(t *testing.T) {
	const oversized = 10_000
	entity := schema.NewMemoryRecord("entity", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "bounded entity",
		PrimaryType:   schema.EntityTypeProject,
		Aliases:       make([]schema.EntityAlias, oversized),
	})
	entity.Tags = make([]string, oversized)
	for i := 0; i < oversized; i++ {
		entity.Tags[i] = fmt.Sprintf("tag-%05d", i)
		entity.Payload.(*schema.EntityPayload).Aliases[i] = schema.EntityAlias{Value: fmt.Sprintf("alias-%05d", i)}
	}
	entity.Tags[0] = strings.Repeat("A", 2<<20)
	episode := schema.NewMemoryRecord("episode", schema.MemoryTypeEpisodic, schema.SensitivityLow, &schema.EpisodicPayload{
		Kind:     "episodic",
		Timeline: make([]schema.TimelineEvent, oversized),
	})
	for i := range episode.Payload.(*schema.EpisodicPayload).Timeline {
		episode.Payload.(*schema.EpisodicPayload).Timeline[i] = schema.TimelineEvent{Ref: fmt.Sprintf("ref-%05d", i), Summary: "event"}
	}

	for _, record := range []*schema.MemoryRecord{entity, episode} {
		terms := recordSearchTerms(record)
		if len(terms) > 256 {
			t.Fatalf("recordSearchTerms(%q) = %d terms, want deterministic <= 256 field budget", record.ID, len(terms))
		}
		totalBytes := 0
		for _, term := range terms {
			totalBytes += len(term)
			if term != strings.ToLower(term) {
				t.Fatalf("recordSearchTerms(%q) retained an unnormalized term", record.ID)
			}
		}
		if totalBytes > 1<<20 {
			t.Fatalf("recordSearchTerms(%q) = %d bytes, want <= 1 MiB", record.ID, totalBytes)
		}
	}
	if recordContainsReference(episode, "ref-09999") {
		t.Fatal("recordContainsReference searched beyond the bounded candidate field prefix")
	}

	queries := candidateQueryTerms(CaptureMemoryRequest{Tags: []string{strings.Repeat("Q", 1<<20)}}, nil)
	queryBytes := 0
	for _, query := range queries {
		queryBytes += len(query)
		if query != strings.ToLower(query) {
			t.Fatal("candidateQueryTerms retained an unnormalized query")
		}
	}
	if queryBytes > 256<<10 {
		t.Fatalf("candidateQueryTerms = %d bytes, want <= 256 KiB", queryBytes)
	}
	remainingMatchBytes := int64(32)
	if recordSearchTermsMatchNormalized([]string{strings.Repeat("a", 1<<20)}, "z", &remainingMatchBytes) {
		t.Fatal("oversized normalized candidate unexpectedly matched")
	}
	if remainingMatchBytes != 0 {
		t.Fatalf("oversized comparison left %d match bytes, want fail-closed exhaustion", remainingMatchBytes)
	}
}

func TestCaptureResolutionIndexPrecomputesMaxShapeOnce(t *testing.T) {
	candidates := make([]*schema.MemoryRecord, 0, captureCandidateLimit)
	for candidateIdx := 0; candidateIdx < captureCandidateLimit; candidateIdx++ {
		if candidateIdx%2 == 0 {
			entity := schema.NewMemoryRecord(fmt.Sprintf("entity-%02d", candidateIdx), schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
				Kind:          "entity",
				CanonicalName: fmt.Sprintf("entity-%02d", candidateIdx),
				Aliases:       make([]schema.EntityAlias, MaxCaptureCandidateSearchFields-1),
			})
			entity.Scope = "project:alpha"
			for fieldIdx := range entity.Payload.(*schema.EntityPayload).Aliases {
				entity.Payload.(*schema.EntityPayload).Aliases[fieldIdx] = schema.EntityAlias{
					Value: fmt.Sprintf("entity-%02d-alias-%03d", candidateIdx, fieldIdx),
				}
			}
			candidates = append(candidates, entity)
			continue
		}

		episode := schema.NewMemoryRecord(fmt.Sprintf("episode-%02d", candidateIdx), schema.MemoryTypeEpisodic, schema.SensitivityLow, &schema.EpisodicPayload{
			Kind:     "episodic",
			Timeline: make([]schema.TimelineEvent, MaxCaptureCandidateSearchFields),
		})
		episode.Scope = "project:other"
		for fieldIdx := range episode.Payload.(*schema.EpisodicPayload).Timeline {
			episode.Payload.(*schema.EpisodicPayload).Timeline[fieldIdx] = schema.TimelineEvent{
				Ref: fmt.Sprintf("shared-ref-%03d", fieldIdx),
			}
		}
		candidates = append(candidates, episode)
	}

	index := newCaptureResolutionIndex(candidates)
	indexedFields := index.candidateFieldsNormalized
	indexedBytes := index.candidateBytesNormalized
	if indexedFields != MaxCaptureResolutionCandidateFields {
		t.Fatalf("candidate normalizations = %d, want full bounded shape %d", indexedFields, MaxCaptureResolutionCandidateFields)
	}
	if indexedBytes > MaxCaptureResolutionCandidateBytes {
		t.Fatalf("candidate normalization bytes = %d, want <= %d", indexedBytes, MaxCaptureResolutionCandidateBytes)
	}

	for queryIdx := 0; queryIdx < MaxCaptureReferenceCandidates; queryIdx++ {
		_ = index.findReference("shared-ref-255", "project:alpha")
	}
	for queryIdx := 0; queryIdx < MaxCaptureMentions; queryIdx++ {
		aliases := make([]string, MaxCaptureAliasesPerMention)
		for aliasIdx := range aliases {
			aliases[aliasIdx] = fmt.Sprintf("missing-%02d-alias-%02d", queryIdx, aliasIdx)
		}
		_ = index.findMatchingEntity(&schema.Mention{
			Surface: fmt.Sprintf("missing-%02d", queryIdx),
			Aliases: aliases,
		}, "project:alpha")
	}

	if index.candidateFieldsNormalized != indexedFields || index.candidateBytesNormalized != indexedBytes {
		t.Fatalf("candidate normalization repeated during resolution: fields %d->%d bytes %d->%d",
			indexedFields, index.candidateFieldsNormalized, indexedBytes, index.candidateBytesNormalized)
	}
	if index.queryFieldsNormalized > MaxCaptureResolutionQueryFields {
		t.Fatalf("query normalizations = %d, want <= %d", index.queryFieldsNormalized, MaxCaptureResolutionQueryFields)
	}
	if index.queryBytesNormalized > MaxCaptureResolutionQueryBytes {
		t.Fatalf("query normalization bytes = %d, want <= %d", index.queryBytesNormalized, MaxCaptureResolutionQueryBytes)
	}
	if index.matchOperations != MaxCaptureResolutionMatchOperations || index.remainingMatchOperations != 0 {
		t.Fatalf("match operations = %d with %d remaining, want deterministic exhaustion at %d",
			index.matchOperations, index.remainingMatchOperations, MaxCaptureResolutionMatchOperations)
	}
	if index.matchBytes > MaxCaptureResolutionMatchBytes {
		t.Fatalf("match bytes = %d, want <= %d", index.matchBytes, MaxCaptureResolutionMatchBytes)
	}
}

func TestCaptureResolutionIndexExhaustionSkipsQueriesAndCandidateScans(t *testing.T) {
	entity := schema.NewMemoryRecord("entity", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Project Orchid",
		Aliases:       []schema.EntityAlias{{Value: "Orchid"}},
	})
	episode := schema.NewMemoryRecord("episode", schema.MemoryTypeEpisodic, schema.SensitivityLow, &schema.EpisodicPayload{
		Kind:     "episodic",
		Timeline: []schema.TimelineEvent{{Ref: "evt-42"}},
	})
	index := newCaptureResolutionIndex([]*schema.MemoryRecord{entity, episode})
	index.remainingMatchOperations = 0
	index.remainingMatchBytes = 0

	queryFields := index.queryFieldsNormalized
	queryBytes := index.queryBytesNormalized
	matchOperations := index.matchOperations
	matchBytes := index.matchBytes
	if got := index.findMatchingEntity(&schema.Mention{Surface: "Orchid"}, ""); got != nil {
		t.Fatalf("entity match after exhaustion = %q, want nil", got.ID)
	}
	if got := index.findReference("evt-42", ""); got != nil {
		t.Fatalf("reference match after exhaustion = %q, want nil", got.ID)
	}
	if index.queryFieldsNormalized != queryFields || index.queryBytesNormalized != queryBytes {
		t.Fatalf("exhausted budget still normalized queries: fields %d->%d bytes %d->%d",
			queryFields, index.queryFieldsNormalized, queryBytes, index.queryBytesNormalized)
	}
	if index.matchOperations != matchOperations || index.matchBytes != matchBytes {
		t.Fatalf("exhausted budget still scanned candidates: operations %d->%d bytes %d->%d",
			matchOperations, index.matchOperations, matchBytes, index.matchBytes)
	}
}
