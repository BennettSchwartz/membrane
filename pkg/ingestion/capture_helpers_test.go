package ingestion

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
	sqlitestore "github.com/BennettSchwartz/membrane/pkg/storage/sqlite"
)

type directFailRelationStore struct {
	storage.Store
}

func (s *directFailRelationStore) AddRelation(context.Context, string, schema.Relation) error {
	return errors.New("forced relation write failure")
}

type relationFailAtStore struct {
	storage.Store
	failAt int
	calls  int
}

func (s *relationFailAtStore) AddRelation(ctx context.Context, sourceID string, rel schema.Relation) error {
	s.calls++
	if s.calls == s.failAt {
		return errors.New("forced relation write failure")
	}
	return s.Store.AddRelation(ctx, sourceID, rel)
}

type createFailStore struct {
	storage.Store
	err error
}

func (s *createFailStore) Create(context.Context, *schema.MemoryRecord) error {
	return s.err
}

type updateFailStore struct {
	storage.Store
	err error
}

func (s *updateFailStore) Update(context.Context, *schema.MemoryRecord) error {
	return s.err
}

type updateFailAtStore struct {
	storage.Store
	failAt int
	calls  int
	err    error
}

func (s *updateFailAtStore) Update(ctx context.Context, rec *schema.MemoryRecord) error {
	s.calls++
	if s.calls == s.failAt {
		return s.err
	}
	return s.Store.Update(ctx, rec)
}

type listCaptureStore struct {
	storage.Store
	calls []storage.ListOptions
	errAt int
	sets  [][]*schema.MemoryRecord
}

func (s *listCaptureStore) List(_ context.Context, opts storage.ListOptions) ([]*schema.MemoryRecord, error) {
	s.calls = append(s.calls, opts)
	if s.errAt > 0 && len(s.calls) == s.errAt {
		return nil, errors.New("list failed")
	}
	if len(s.sets) == 0 {
		return nil, nil
	}
	idx := len(s.calls) - 1
	if idx >= len(s.sets) {
		idx = len(s.sets) - 1
	}
	return s.sets[idx], nil
}

func TestCaptureHelperValueConversions(t *testing.T) {
	if got := maxFloat(2, 1); got != 2 {
		t.Fatalf("maxFloat high-first = %v, want 2", got)
	}
	if got := maxFloat(1, 2); got != 2 {
		t.Fatalf("maxFloat high-second = %v, want 2", got)
	}

	for _, tc := range []struct {
		value float64
		want  float64
	}{
		{value: -0.1, want: 0},
		{value: 0.4, want: 0.4},
		{value: 1.2, want: 1},
	} {
		if got := clamp01(tc.value); got != tc.want {
			t.Fatalf("clamp01(%v) = %v, want %v", tc.value, got, tc.want)
		}
	}

	if got := summarizeContent("plain text"); got != "plain text" {
		t.Fatalf("summarizeContent string = %q, want plain text", got)
	}
	if got := summarizeContent(map[string]any{"k": "v"}); got != `{"k":"v"}` {
		t.Fatalf("summarizeContent map = %q, want JSON object", got)
	}
	if got := summarizeContent(make(chan int)); got != "" {
		t.Fatalf("summarizeContent unsupported = %q, want empty", got)
	}

	obj := asObject(map[string]any{"a": 1})
	if obj["a"] != 1 {
		t.Fatalf("asObject map[string]any = %#v, want value", obj)
	}
	obj = asObject(map[string]string{"a": "1"})
	if !reflect.DeepEqual(obj, map[string]any{"a": "1"}) {
		t.Fatalf("asObject map[string]string = %#v, want converted map", obj)
	}
	obj = asObject("not an object")
	if len(obj) != 0 {
		t.Fatalf("asObject default = %#v, want empty map", obj)
	}
}

func TestEntityTypeFromMentionKind(t *testing.T) {
	for _, tc := range []struct {
		kind schema.EntityKind
		want string
	}{
		{kind: schema.EntityKindPerson, want: schema.EntityTypePerson},
		{kind: schema.EntityKindTool, want: schema.EntityTypeTool},
		{kind: schema.EntityKindProject, want: schema.EntityTypeProject},
		{kind: schema.EntityKindFile, want: schema.EntityTypeFile},
		{kind: schema.EntityKindConcept, want: schema.EntityTypeConcept},
		{kind: schema.EntityKindOther, want: schema.EntityTypeOther},
		{kind: "", want: schema.EntityTypeOther},
	} {
		if got := entityTypeFromMentionKind(tc.kind); got != tc.want {
			t.Fatalf("entityTypeFromMentionKind(%q) = %q, want %q", tc.kind, got, tc.want)
		}
	}
}

func TestCaptureFallbackHelpers(t *testing.T) {
	if got := firstNonEmpty(" ", "\t", "fallback"); got != "fallback" {
		t.Fatalf("firstNonEmpty fallback = %q, want fallback", got)
	}
	if got := firstNonEmpty(" ", "\t"); got != "" {
		t.Fatalf("firstNonEmpty blank = %q, want empty", got)
	}

	edges := []schema.GraphEdge{
		{SourceID: "rec-a", Predicate: "mentions_entity"},
		{SourceID: "rec-b", Predicate: "references_record"},
	}
	if got := edgePredicate(edges, "rec-b"); got != "references_record" {
		t.Fatalf("edgePredicate hit = %q, want references_record", got)
	}
	if got := edgePredicate(edges, "missing"); got != "" {
		t.Fatalf("edgePredicate miss = %q, want empty", got)
	}

	mentions := inferMentionsFromContent(map[string]any{"project": "", "tool_name": "rg"})
	if len(mentions) != 1 || mentions[0].Surface != "rg" {
		t.Fatalf("inferMentionsFromContent = %+v, want one non-empty tool mention", mentions)
	}
	refs := inferReferenceCandidates(map[string]any{"ref": ""}, map[string]any{"record_id": "rec-1"})
	if len(refs) != 1 || refs[0].Ref != "rec-1" {
		t.Fatalf("inferReferenceCandidates = %+v, want one non-empty reference", refs)
	}

	working := schema.NewMemoryRecord("working", schema.MemoryTypeWorking, schema.SensitivityLow, &schema.WorkingPayload{
		Kind:     "working",
		ThreadID: "thread",
		State:    schema.TaskStatePlanning,
	})
	attachCaptureContext(working, CaptureMemoryRequest{Content: "ignored"})
	if _, ok := working.Payload.(*schema.WorkingPayload); !ok {
		t.Fatalf("attachCaptureContext changed non-episodic payload: %+v", working.Payload)
	}
}

func TestMergeInterpretationsNilAndOverrideFields(t *testing.T) {
	override := &schema.Interpretation{
		Status:               schema.InterpretationStatusResolved,
		Summary:              "override",
		ProposedType:         schema.MemoryTypeSemantic,
		TopicalLabels:        []string{"override"},
		Mentions:             []schema.Mention{{Surface: "Orchid"}},
		RelationCandidates:   []schema.RelationCandidate{{Predicate: "depends_on"}},
		ReferenceCandidates:  []schema.ReferenceCandidate{{Ref: "evt-1"}},
		ExtractionConfidence: 0.8,
	}
	if got := mergeInterpretations(nil, override); got != override {
		t.Fatalf("merge nil base = %+v, want override", got)
	}
	base := &schema.Interpretation{
		Status:        schema.InterpretationStatusTentative,
		Summary:       "base",
		TopicalLabels: []string{"base"},
	}
	if got := mergeInterpretations(base, nil); got != base {
		t.Fatalf("merge nil override = %+v, want base", got)
	}
	got := mergeInterpretations(base, override)
	if got.Status != override.Status || got.Summary != override.Summary || got.ProposedType != override.ProposedType {
		t.Fatalf("merged scalar fields = %+v, want override fields", got)
	}
	if !reflect.DeepEqual(got.TopicalLabels, []string{"base", "override"}) || len(got.Mentions) != 1 ||
		len(got.RelationCandidates) != 1 || len(got.ReferenceCandidates) != 1 || got.ExtractionConfidence != 0.8 {
		t.Fatalf("merged interpretation = %+v, want merged labels and override collections", got)
	}
}

func TestEntityAliasMentionAndInterpretationStatusHelpers(t *testing.T) {
	aliases := entityAliases([]string{" Orchid ", "", "\t", "Borealis"})
	if !reflect.DeepEqual(aliases, []schema.EntityAlias{{Value: "Orchid"}, {Value: "Borealis"}}) {
		t.Fatalf("entityAliases = %+v, want trimmed non-empty aliases", aliases)
	}

	mentions := uniqueMentions([]schema.Mention{
		{Surface: " Orchid "},
		{Surface: "orchid"},
		{Surface: " "},
		{Surface: "Borealis"},
	})
	if len(mentions) != 2 || mentions[0].Surface != " Orchid " || mentions[1].Surface != "Borealis" {
		t.Fatalf("uniqueMentions = %+v, want first distinct non-empty surfaces", mentions)
	}

	statusTests := []struct {
		name           string
		interpretation *schema.Interpretation
		want           schema.InterpretationStatus
	}{
		{name: "nil", interpretation: nil, want: schema.InterpretationStatusTentative},
		{
			name: "unresolved mention",
			interpretation: &schema.Interpretation{
				Mentions: []schema.Mention{{Surface: "Orchid"}},
			},
			want: schema.InterpretationStatusTentative,
		},
		{
			name: "unresolved reference",
			interpretation: &schema.Interpretation{
				Mentions:            []schema.Mention{{Surface: "Orchid", CanonicalEntityID: "entity-orchid"}},
				ReferenceCandidates: []schema.ReferenceCandidate{{Ref: "obs-1"}},
			},
			want: schema.InterpretationStatusTentative,
		},
		{
			name: "unresolved relation",
			interpretation: &schema.Interpretation{
				Mentions:             []schema.Mention{{Surface: "Orchid", CanonicalEntityID: "entity-orchid"}},
				ReferenceCandidates:  []schema.ReferenceCandidate{{Ref: "obs-1", Resolved: true}},
				RelationCandidates:   []schema.RelationCandidate{{Predicate: "depends_on"}},
				ExtractionConfidence: 0.8,
			},
			want: schema.InterpretationStatusTentative,
		},
		{
			name: "resolved",
			interpretation: &schema.Interpretation{
				Mentions:            []schema.Mention{{Surface: "Orchid", CanonicalEntityID: "entity-orchid"}},
				ReferenceCandidates: []schema.ReferenceCandidate{{Ref: "obs-1", Resolved: true}},
				RelationCandidates:  []schema.RelationCandidate{{Predicate: "depends_on", Resolved: true}},
			},
			want: schema.InterpretationStatusResolved,
		},
	}
	for _, tc := range statusTests {
		t.Run(tc.name, func(t *testing.T) {
			if got := finalInterpretationStatus(tc.interpretation); got != tc.want {
				t.Fatalf("finalInterpretationStatus = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestProvenanceKindForCandidateDefault(t *testing.T) {
	for _, tc := range []struct {
		kind CandidateKind
		want schema.ProvenanceKind
	}{
		{kind: CandidateKindEvent, want: schema.ProvenanceKindEvent},
		{kind: CandidateKindToolOutput, want: schema.ProvenanceKindToolCall},
		{kind: CandidateKindObservation, want: schema.ProvenanceKindObservation},
		{kind: CandidateKindOutcome, want: schema.ProvenanceKindOutcome},
		{kind: CandidateKind("unknown"), want: schema.ProvenanceKindEvent},
	} {
		if got := provenanceKindForCandidate(tc.kind); got != tc.want {
			t.Fatalf("provenanceKindForCandidate(%q) = %q, want %q", tc.kind, got, tc.want)
		}
	}
}

func TestRecordSearchTermsCoversPayloadVariants(t *testing.T) {
	records := []*schema.MemoryRecord{
		schema.NewMemoryRecord("entity", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
			Kind:          "entity",
			CanonicalName: "Orchid",
			PrimaryType:   schema.EntityTypeProject,
			Types:         []string{schema.EntityTypeProject},
			Aliases:       []schema.EntityAlias{{Value: "orchid alias"}},
			Summary:       "deployment project",
		}),
		schema.NewMemoryRecord("semantic", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
			Kind:      "semantic",
			Subject:   "Orchid",
			Predicate: "depends_on",
			Object:    "Borealis",
			Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
		}),
		schema.NewMemoryRecord("episode", schema.MemoryTypeEpisodic, schema.SensitivityLow, &schema.EpisodicPayload{
			Kind:     "episodic",
			Timeline: []schema.TimelineEvent{{Ref: "evt-1", EventKind: "deploy", Summary: "deployed Orchid"}},
		}),
		schema.NewMemoryRecord("working", schema.MemoryTypeWorking, schema.SensitivityLow, &schema.WorkingPayload{
			Kind:           "working",
			ThreadID:       "thread-1",
			State:          schema.TaskStatePlanning,
			ContextSummary: "planning deploy",
			NextActions:    []string{"run tests"},
			OpenQuestions:  []string{"which cluster?"},
		}),
		schema.NewMemoryRecord("competence", schema.MemoryTypeCompetence, schema.SensitivityLow, &schema.CompetencePayload{
			Kind:          "competence",
			SkillName:     "debug deploy",
			RequiredTools: []string{"rg"},
		}),
		schema.NewMemoryRecord("plan", schema.MemoryTypePlanGraph, schema.SensitivityLow, &schema.PlanGraphPayload{
			Kind:   "plan_graph",
			PlanID: "plan-1",
			Intent: "deploy safely",
		}),
	}
	records[0].Scope = "project:alpha"
	records[0].Tags = []string{"deploy"}
	records[0].Interpretation = &schema.Interpretation{
		Summary:       "interpreted entity",
		TopicalLabels: []string{"infra"},
		Mentions:      []schema.Mention{{Surface: "Orchid", Aliases: []string{"orchid project"}}},
	}

	joined := strings.Join(recordSearchTerms(records[0]), "\n")
	for _, want := range []string{"entity", "project:alpha", "deploy", "interpreted entity", "infra", "Orchid", "orchid project", "orchid alias", "deployment project"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("entity recordSearchTerms = %q, want term %q", joined, want)
		}
	}

	for _, tc := range []struct {
		rec  *schema.MemoryRecord
		term string
	}{
		{rec: records[1], term: "depends_on"},
		{rec: records[2], term: "evt-1"},
		{rec: records[3], term: "which cluster?"},
		{rec: records[4], term: "debug deploy"},
		{rec: records[5], term: "deploy safely"},
	} {
		if !recordMatchesTerm(tc.rec, tc.term) {
			t.Fatalf("recordMatchesTerm(%s, %q) = false, want true", tc.rec.ID, tc.term)
		}
	}
}

func TestRecordMatchingReferenceAndSemanticExtractionEdgeCases(t *testing.T) {
	if recordMatchesTerm(nil, "orchid") {
		t.Fatalf("recordMatchesTerm nil record = true, want false")
	}
	if recordMatchesTerm(schema.NewMemoryRecord("rec", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "Orchid",
		Predicate: "is",
		Object:    "project",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	}), "   ") {
		t.Fatalf("recordMatchesTerm blank term = true, want false")
	}
	if recordMatchesTerm(schema.NewMemoryRecord("rec", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "Orchid",
		Predicate: "is",
		Object:    "project",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	}), "missing") {
		t.Fatalf("recordMatchesTerm missing term = true, want false")
	}

	episode := schema.NewMemoryRecord("episode", schema.MemoryTypeEpisodic, schema.SensitivityLow, &schema.EpisodicPayload{
		Kind:     "episodic",
		Timeline: []schema.TimelineEvent{{Ref: " evt-1 "}},
	})
	if !recordContainsReference(episode, "evt-1") {
		t.Fatalf("recordContainsReference did not match normalized event ref")
	}
	if recordContainsReference(episode, "evt-2") {
		t.Fatalf("recordContainsReference missing ref = true, want false")
	}
	if recordContainsReference(nil, "evt-1") || recordContainsReference(episode, " ") {
		t.Fatalf("recordContainsReference nil/blank should be false")
	}
	semantic := schema.NewMemoryRecord("semantic", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "Orchid",
		Predicate: "is",
		Object:    "project",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	if recordContainsReference(semantic, "evt-1") {
		t.Fatalf("recordContainsReference semantic record = true, want false")
	}

	subject, predicate, object, ok := extractSemanticFact(map[string]any{
		"fact": map[string]any{"subject": "Orchid", "predicate": "deploys_to", "object": "staging"},
	})
	if !ok || subject != "Orchid" || predicate != "deploys_to" || object != "staging" {
		t.Fatalf("extractSemanticFact nested = %q/%q/%#v/%v, want Orchid/deploys_to/staging/true", subject, predicate, object, ok)
	}
	if _, _, _, ok := extractSemanticFact(map[string]any{"subject": "Orchid", "predicate": "is"}); ok {
		t.Fatalf("extractSemanticFact missing object ok = true, want false")
	}
}

func TestCaptureCandidateScoreClampsFutureRecency(t *testing.T) {
	rec := schema.NewMemoryRecord("future", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "Orchid",
		Predicate: "is",
		Object:    "ready",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	rec.CreatedAt = time.Now().UTC().Add(24 * time.Hour)

	if got := captureCandidateScore(rec, CaptureMemoryRequest{}, nil); got != 1 {
		t.Fatalf("future record score = %v, want recency boost clamped to 1", got)
	}
}

func TestCaptureCandidateScoreIncludesScopeTagsTypesAndTerms(t *testing.T) {
	rec := schema.NewMemoryRecord("entity-scored", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Orchid",
		PrimaryType:   schema.EntityTypeProject,
		Aliases:       []schema.EntityAlias{{Value: "Project Orchid"}},
	})
	rec.Scope = "project:alpha"
	rec.Tags = []string{"deploy"}
	rec.CreatedAt = time.Now().UTC()

	score := captureCandidateScore(rec, CaptureMemoryRequest{
		Scope: "project:alpha",
		Tags:  []string{"DEPLOY"},
		Context: map[string]any{
			"active_entities": []any{"Project Orchid"},
		},
		Content: map[string]any{
			"subject": "Orchid",
		},
	}, &schema.Interpretation{
		ProposedType:        schema.MemoryTypeEntity,
		TopicalLabels:       []string{"rollout"},
		Mentions:            []schema.Mention{{Surface: "Orchid", Aliases: []string{"Project Orchid"}}},
		ReferenceCandidates: []schema.ReferenceCandidate{{Ref: "orchid-ref"}},
	})
	if score < 180 {
		t.Fatalf("captureCandidateScore = %v, want scope/tag/type/term bonuses", score)
	}
}

func TestFetchCaptureCandidatesDedupesOrdersAndAvoidsDuplicateGlobalList(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	matching := schema.NewMemoryRecord("matching", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Orchid",
		PrimaryType:   schema.EntityTypeProject,
	})
	matching.Scope = "project:alpha"
	matching.Tags = []string{"deploy"}
	matching.CreatedAt = now.Add(-2 * time.Hour)
	recent := schema.NewMemoryRecord("recent", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Unrelated",
		PrimaryType:   schema.EntityTypeProject,
	})
	recent.CreatedAt = now
	oldDuplicate := schema.NewMemoryRecord("duplicate", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "Orchid",
		Predicate: "runs_on",
		Object:    "staging",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	oldDuplicate.CreatedAt = now.Add(-24 * time.Hour)

	store := &listCaptureStore{sets: [][]*schema.MemoryRecord{
		{nil, oldDuplicate, matching},
		{recent, oldDuplicate},
	}}
	svc := &Service{store: store}
	got, err := svc.fetchCaptureCandidates(ctx, CaptureMemoryRequest{
		Scope: "project:alpha",
		Tags:  []string{"deploy"},
	}, &schema.Interpretation{
		ProposedType:  schema.MemoryTypeEntity,
		TopicalLabels: []string{"orchid"},
	})
	if err != nil {
		t.Fatalf("fetchCaptureCandidates: %v", err)
	}
	if len(store.calls) != 2 || store.calls[0].Scope != "project:alpha" || store.calls[1].Scope != "" {
		t.Fatalf("List calls = %+v, want scoped then global lookup", store.calls)
	}
	if len(got) != 3 || got[0].ID != "matching" || got[1].ID != "recent" || got[2].ID != "duplicate" {
		t.Fatalf("candidates = %v, want scored unique candidates", recordIDs(got))
	}

	store = &listCaptureStore{sets: [][]*schema.MemoryRecord{{recent, matching}}}
	svc = &Service{store: store}
	got, err = svc.fetchCaptureCandidates(ctx, CaptureMemoryRequest{}, nil)
	if err != nil {
		t.Fatalf("fetchCaptureCandidates unscoped: %v", err)
	}
	if len(store.calls) != 1 || store.calls[0].Scope != "" {
		t.Fatalf("unscoped List calls = %+v, want one global list", store.calls)
	}
	if len(got) != 2 {
		t.Fatalf("unscoped candidates = %v, want both records", recordIDs(got))
	}

	many := make([]*schema.MemoryRecord, 0, captureCandidateLimit+2)
	for i := 0; i < captureCandidateLimit+2; i++ {
		rec := newHelperSemanticRecord(fmt.Sprintf("candidate-%02d", i), "same")
		rec.CreatedAt = now.Add(time.Duration(i) * time.Minute)
		many = append(many, rec)
	}
	store = &listCaptureStore{sets: [][]*schema.MemoryRecord{many}}
	svc = &Service{store: store}
	got, err = svc.fetchCaptureCandidates(ctx, CaptureMemoryRequest{}, nil)
	if err != nil {
		t.Fatalf("fetchCaptureCandidates limited: %v", err)
	}
	if len(got) != captureCandidateLimit {
		t.Fatalf("limited candidates len = %d, want %d", len(got), captureCandidateLimit)
	}
	if got[0].ID != "candidate-21" {
		t.Fatalf("limited candidates first = %s, want newest tie-break candidate-21", got[0].ID)
	}
}

func TestFetchCaptureCandidatesPropagatesListErrors(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		scope string
		errAt int
	}{
		{name: "scoped list", scope: "project:alpha", errAt: 1},
		{name: "global list", scope: "project:alpha", errAt: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &listCaptureStore{
				errAt: tc.errAt,
				sets:  [][]*schema.MemoryRecord{{}, {}},
			}
			svc := &Service{store: store}
			_, err := svc.fetchCaptureCandidates(ctx, CaptureMemoryRequest{Scope: tc.scope}, nil)
			if err == nil || !strings.Contains(err.Error(), "list failed") {
				t.Fatalf("fetchCaptureCandidates error = %v, want list failed", err)
			}
		})
	}
}

func recordIDs(records []*schema.MemoryRecord) []string {
	ids := make([]string, 0, len(records))
	for _, rec := range records {
		if rec == nil {
			ids = append(ids, "<nil>")
			continue
		}
		ids = append(ids, rec.ID)
	}
	return ids
}

func TestInverseRelationPredicatePairs(t *testing.T) {
	for _, tc := range []struct {
		predicate string
		want      string
	}{
		{predicate: "mentions_entity", want: "mentioned_in"},
		{predicate: "mentioned_in", want: "mentions_entity"},
		{predicate: "references_record", want: "referenced_by"},
		{predicate: "referenced_by", want: "references_record"},
		{predicate: "derived_semantic", want: "derived_from"},
		{predicate: "derived_from", want: "derived_semantic"},
		{predicate: "subject_entity", want: "fact_subject_of"},
		{predicate: "fact_subject_of", want: "subject_entity"},
		{predicate: "object_entity", want: "fact_object_of"},
		{predicate: "fact_object_of", want: "object_entity"},
		{predicate: "depends_on", want: "dependency_of"},
		{predicate: "dependency_of", want: "depends_on"},
		{predicate: "uses", want: "used_by"},
		{predicate: "used_by", want: "uses"},
		{predicate: "caused_by", want: "causes"},
		{predicate: "causes", want: "caused_by"},
		{predicate: "supports", want: "supported_by"},
		{predicate: "supported_by", want: "supports"},
		{predicate: "contradicts", want: "contradicted_by"},
		{predicate: "contradicted_by", want: "contradicts"},
		{predicate: "custom", want: "inverse_of_custom"},
	} {
		if got := inverseRelationPredicate(tc.predicate); got != tc.want {
			t.Fatalf("inverseRelationPredicate(%q) = %q, want %q", tc.predicate, got, tc.want)
		}
	}
}

func TestResolveMentionEntityFallbacks(t *testing.T) {
	svc, store := newCaptureTestService(t, nil)
	ctx := context.Background()
	stored := newObservationEntity("entity-stored", "Orchid", "project:alpha")
	if err := store.Create(ctx, stored); err != nil {
		t.Fatalf("Create stored entity: %v", err)
	}

	if rec := svc.resolveMentionEntity(ctx, nil, "project:alpha", nil); rec != nil {
		t.Fatalf("resolveMentionEntity nil = %+v, want nil", rec)
	}

	rec := svc.resolveMentionEntity(ctx, &schema.Mention{CanonicalEntityID: stored.ID}, "project:alpha", nil)
	if rec == nil || rec.ID != stored.ID {
		t.Fatalf("resolveMentionEntity canonical store = %+v, want %s", rec, stored.ID)
	}

	rec = svc.resolveMentionEntity(ctx, &schema.Mention{Surface: "Orchid"}, "project:alpha", nil)
	if rec == nil || rec.ID != stored.ID {
		t.Fatalf("resolveMentionEntity lookup = %+v, want %s", rec, stored.ID)
	}

	local := newObservationEntity("entity-local", "Borealis", "project:alpha")
	rec = svc.resolveMentionEntity(ctx, &schema.Mention{Surface: "borealis"}, "project:alpha", []*schema.MemoryRecord{local})
	if rec == nil || rec.ID != local.ID {
		t.Fatalf("resolveMentionEntity local fallback = %+v, want %s", rec, local.ID)
	}
}

func TestResolveMentionErrorsAndEntityDefaults(t *testing.T) {
	_, base := newCaptureTestService(t, nil)
	ctx := context.Background()
	ts := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	source := newHelperSemanticRecord("mention-source", "source")

	createErr := errors.New("create failed")
	createFailSvc := NewService(&createFailStore{Store: base, err: createErr}, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))
	if _, _, _, err := createFailSvc.resolveMention(ctx, source, &schema.Mention{Surface: "Orchid"}, "project:alpha", "tester", schema.SensitivityLow, ts, nil); !errors.Is(err, createErr) {
		t.Fatalf("resolveMention create error = %v, want %v", err, createErr)
	}

	if err := base.Create(ctx, source); err != nil {
		t.Fatalf("Create source: %v", err)
	}
	relationFailSvc := NewService(&relationFailAtStore{Store: base, failAt: 2}, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))
	if _, _, _, err := relationFailSvc.resolveMention(ctx, source, &schema.Mention{Surface: "Borealis"}, "project:alpha", "tester", schema.SensitivityLow, ts, nil); err == nil || !strings.Contains(err.Error(), "forced relation write failure") {
		t.Fatalf("resolveMention second relation error = %v, want forced relation write failure", err)
	}

	rec := buildEntityRecord(&schema.Mention{}, "project:alpha", "tester", "", ts)
	if rec.Sensitivity != schema.SensitivityLow {
		t.Fatalf("default entity sensitivity = %q, want low", rec.Sensitivity)
	}
	payload := rec.Payload.(*schema.EntityPayload)
	if payload.CanonicalName != "entity" || payload.Summary != "entity" {
		t.Fatalf("default entity payload = %+v, want fallback canonical name", payload)
	}
}

func TestFindMatchingEntityMatchesAliasesAndSkipsBlankTerms(t *testing.T) {
	rec := newObservationEntity("entity-orchid", "Orchid Canonical", "project:alpha")
	rec.Payload.(*schema.EntityPayload).Aliases = []schema.EntityAlias{{Value: "Project Orchid"}}

	got := findMatchingEntity(&schema.Mention{
		Surface: " ",
		Aliases: []string{"", "project orchid"},
	}, []*schema.MemoryRecord{
		newHelperSemanticRecord("semantic", "Project Orchid"),
		rec,
	})
	if got == nil || got.ID != rec.ID {
		t.Fatalf("findMatchingEntity alias = %+v, want %s", got, rec.ID)
	}

	if got := findMatchingEntity(&schema.Mention{Surface: "missing"}, []*schema.MemoryRecord{rec}); got != nil {
		t.Fatalf("findMatchingEntity missing = %+v, want nil", got)
	}
}

func TestResolveRelationCandidateTarget(t *testing.T) {
	svc, store := newCaptureTestService(t, nil)
	ctx := context.Background()
	local := newHelperSemanticRecord("local-target", "Go")
	stored := newHelperSemanticRecord("stored-target", "SQLite")
	entity := newObservationEntity("entity-target", "Orchid", "project")
	for _, rec := range []*schema.MemoryRecord{stored, entity} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	if target, ok := svc.resolveRelationCandidateTarget(ctx, nil, nil); ok || target != nil {
		t.Fatalf("nil relation target = (%+v, %v), want nil false", target, ok)
	}
	target, ok := svc.resolveRelationCandidateTarget(ctx, &schema.RelationCandidate{TargetRecordID: local.ID}, []*schema.MemoryRecord{local})
	if !ok || target.ID != local.ID {
		t.Fatalf("local record target = (%+v, %v), want %s", target, ok, local.ID)
	}
	target, ok = svc.resolveRelationCandidateTarget(ctx, &schema.RelationCandidate{TargetRecordID: stored.ID}, nil)
	if !ok || target.ID != stored.ID {
		t.Fatalf("stored record target = (%+v, %v), want %s", target, ok, stored.ID)
	}
	target, ok = svc.resolveRelationCandidateTarget(ctx, &schema.RelationCandidate{TargetEntityID: entity.ID}, nil)
	if !ok || target.ID != entity.ID {
		t.Fatalf("stored entity target = (%+v, %v), want %s", target, ok, entity.ID)
	}
	target, ok = svc.resolveRelationCandidateTarget(ctx, &schema.RelationCandidate{TargetRecordID: "missing"}, []*schema.MemoryRecord{local})
	if ok || target != nil {
		t.Fatalf("missing target = (%+v, %v), want nil false", target, ok)
	}
}

func TestMaterializeRelationCandidateEdgeCases(t *testing.T) {
	svc, store := newCaptureTestService(t, nil)
	ctx := context.Background()
	ts := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	source := newHelperSemanticRecord("relation-source", "Orchid")
	target := newHelperSemanticRecord("relation-target", "Borealis")
	entityTarget := newObservationEntity("entity-relation-target", "Borealis", "project:alpha")
	for _, rec := range []*schema.MemoryRecord{source, target, entityTarget} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	if edges, err := svc.materializeRelationCandidate(ctx, source, nil, ts, nil); err != nil || edges != nil {
		t.Fatalf("nil candidate = %+v, %v; want nil nil", edges, err)
	}

	unresolved := &schema.RelationCandidate{Predicate: "depends_on", TargetRecordID: "missing", Resolved: true}
	edges, err := svc.materializeRelationCandidate(ctx, source, unresolved, ts, nil)
	if err != nil || edges != nil || unresolved.Resolved || unresolved.TargetRecordID != "" {
		t.Fatalf("unresolved candidate = edges:%+v err:%v candidate:%+v; want cleared unresolved", edges, err, unresolved)
	}

	edges, err = svc.materializeRelationCandidate(ctx, source, &schema.RelationCandidate{
		Predicate:      "   ",
		TargetRecordID: target.ID,
	}, ts, []*schema.MemoryRecord{target})
	if err != nil || edges != nil {
		t.Fatalf("blank predicate = %+v, %v; want nil nil", edges, err)
	}

	entityRel := &schema.RelationCandidate{
		Predicate:      "uses",
		TargetEntityID: entityTarget.ID,
		Confidence:     2,
	}
	edges, err = svc.materializeRelationCandidate(ctx, source, entityRel, ts, []*schema.MemoryRecord{entityTarget})
	if err != nil {
		t.Fatalf("entity relation: %v", err)
	}
	if len(edges) != 2 || edges[0].Predicate != "uses" || edges[1].Predicate != "used_by" {
		t.Fatalf("entity relation edges = %+v, want uses/used_by pair", edges)
	}
	if entityRel.TargetRecordID != "" || entityRel.TargetEntityID != entityTarget.ID || entityRel.Confidence != 1 {
		t.Fatalf("entity relation candidate = %+v, want entity target and clamped confidence", entityRel)
	}
}

func TestMaterializeRelationCandidatePropagatesRelationWriteError(t *testing.T) {
	base, err := sqlitestore.Open(":memory:", "")
	if err != nil {
		t.Fatalf("Open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = base.Close() })

	svc := NewService(&directFailRelationStore{Store: base}, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))
	source := newHelperSemanticRecord("relation-source", "Orchid")
	target := newHelperSemanticRecord("relation-target", "Borealis")

	_, err = svc.materializeRelationCandidate(context.Background(), source, &schema.RelationCandidate{
		Predicate:      "depends_on",
		TargetRecordID: target.ID,
	}, time.Now().UTC(), []*schema.MemoryRecord{target})
	if err == nil || !strings.Contains(err.Error(), "forced relation write failure") {
		t.Fatalf("materializeRelationCandidate write error = %v, want forced relation error", err)
	}

	for _, rec := range []*schema.MemoryRecord{source, target} {
		if err := base.Create(context.Background(), rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}
	secondFailSvc := NewService(&relationFailAtStore{Store: base, failAt: 2}, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))
	_, err = secondFailSvc.materializeRelationCandidate(context.Background(), source, &schema.RelationCandidate{
		Predicate:      "depends_on",
		TargetRecordID: target.ID,
	}, time.Now().UTC(), []*schema.MemoryRecord{target})
	if err == nil || !strings.Contains(err.Error(), "forced relation write failure") {
		t.Fatalf("materializeRelationCandidate inverse write error = %v, want forced relation error", err)
	}
}

func TestMaybeCreateSemanticRecordGuardsAndRelationErrors(t *testing.T) {
	svc, store := newCaptureTestService(t, nil)
	ctx := context.Background()
	ts := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	source := newHelperSemanticRecord("semantic-source", "Orchid")
	source.Interpretation = &schema.Interpretation{
		ProposedType:         schema.MemoryTypeWorking,
		ExtractionConfidence: 0.1,
	}
	if rec, edges, err := svc.maybeCreateSemanticRecord(ctx, source, CaptureMemoryRequest{
		SourceKind: "event",
		Content:    map[string]any{"subject": "Orchid", "predicate": "runs_on", "object": "staging"},
	}, ts); err != nil || rec != nil || edges != nil {
		t.Fatalf("low-confidence non-observation semantic = rec:%+v edges:%+v err:%v, want nil nil nil", rec, edges, err)
	}

	createErr := errors.New("create semantic failed")
	createFailSvc := NewService(&createFailStore{Store: store, err: createErr}, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))
	if _, _, err := createFailSvc.maybeCreateSemanticRecord(ctx, source, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "observation",
		Sensitivity: schema.SensitivityLow,
		Content:     map[string]any{"subject": "Orchid", "predicate": "runs_on", "object": "staging"},
	}, ts); !errors.Is(err, createErr) {
		t.Fatalf("maybeCreateSemanticRecord create error = %v, want %v", err, createErr)
	}

	source.Interpretation = &schema.Interpretation{ProposedType: schema.MemoryTypeSemantic, ExtractionConfidence: 0.9}
	if err := store.Create(ctx, source); err != nil {
		t.Fatalf("Create source: %v", err)
	}
	firstEdgeFailSvc := NewService(&relationFailAtStore{Store: store, failAt: 1}, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))
	_, _, err := firstEdgeFailSvc.maybeCreateSemanticRecord(ctx, source, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "observation",
		Sensitivity: schema.SensitivityLow,
		Content:     map[string]any{"subject": "Orchid", "predicate": "runs_on", "object": "staging"},
	}, ts)
	if err == nil || !strings.Contains(err.Error(), "forced relation write failure") {
		t.Fatalf("maybeCreateSemanticRecord first relation error = %v, want forced relation error", err)
	}

	failSvc := NewService(&relationFailAtStore{Store: store, failAt: 2}, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))
	_, _, err = failSvc.maybeCreateSemanticRecord(ctx, source, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "observation",
		Sensitivity: schema.SensitivityLow,
		Content:     map[string]any{"subject": "Orchid", "predicate": "runs_on", "object": "staging"},
	}, ts)
	if err == nil || !strings.Contains(err.Error(), "forced relation write failure") {
		t.Fatalf("maybeCreateSemanticRecord relation error = %v, want forced relation error", err)
	}

	entity := newObservationEntity("entity-orchid-link", "Orchid", "")
	if err := store.Create(ctx, entity); err != nil {
		t.Fatalf("Create entity: %v", err)
	}
	source.Interpretation.Mentions = []schema.Mention{{Surface: "Orchid", CanonicalEntityID: entity.ID}}
	linkFailSvc := NewService(&relationFailAtStore{Store: store, failAt: 3}, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))
	_, _, err = linkFailSvc.maybeCreateSemanticRecord(ctx, source, CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "observation",
		Sensitivity: schema.SensitivityLow,
		Content:     map[string]any{"subject": "Orchid", "predicate": "runs_on", "object": "staging"},
	}, ts)
	if err == nil || !strings.Contains(err.Error(), "forced relation write failure") {
		t.Fatalf("maybeCreateSemanticRecord entity link error = %v, want forced relation error", err)
	}
}

func TestResolveReferenceCandidate(t *testing.T) {
	svc, store := newCaptureTestService(t, nil)
	ctx := context.Background()
	local := newHelperSemanticRecord("local-ref", "Go")
	localEntity := newObservationEntity("entity-local-ref", "Local Entity", "project")
	stored := newHelperSemanticRecord("stored-ref", "SQLite")
	entity := newObservationEntity("entity-ref", "Orchid", "project")
	episode := schema.NewMemoryRecord("episode-ref", schema.MemoryTypeEpisodic, schema.SensitivityLow, &schema.EpisodicPayload{
		Kind:     "episodic",
		Timeline: []schema.TimelineEvent{{Ref: "evt-42", EventKind: "observation", Summary: "Saw Orchid"}},
	})
	for _, rec := range []*schema.MemoryRecord{stored, entity} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	if target, ok := svc.resolveReferenceCandidate(ctx, nil, nil); ok || target != nil {
		t.Fatalf("nil reference target = (%+v, %v), want nil false", target, ok)
	}
	target, ok := svc.resolveReferenceCandidate(ctx, &schema.ReferenceCandidate{TargetRecordID: local.ID}, []*schema.MemoryRecord{local})
	if !ok || target.ID != local.ID {
		t.Fatalf("local record reference = (%+v, %v), want %s", target, ok, local.ID)
	}
	target, ok = svc.resolveReferenceCandidate(ctx, &schema.ReferenceCandidate{TargetEntityID: localEntity.ID}, []*schema.MemoryRecord{localEntity})
	if !ok || target.ID != localEntity.ID {
		t.Fatalf("local entity reference = (%+v, %v), want %s", target, ok, localEntity.ID)
	}
	target, ok = svc.resolveReferenceCandidate(ctx, &schema.ReferenceCandidate{TargetRecordID: stored.ID}, nil)
	if !ok || target.ID != stored.ID {
		t.Fatalf("stored record reference = (%+v, %v), want %s", target, ok, stored.ID)
	}
	target, ok = svc.resolveReferenceCandidate(ctx, &schema.ReferenceCandidate{TargetEntityID: entity.ID}, nil)
	if !ok || target.ID != entity.ID {
		t.Fatalf("stored entity reference = (%+v, %v), want %s", target, ok, entity.ID)
	}
	target, ok = svc.resolveReferenceCandidate(ctx, &schema.ReferenceCandidate{Ref: "evt-42"}, []*schema.MemoryRecord{episode})
	if !ok || target.ID != episode.ID {
		t.Fatalf("event ref reference = (%+v, %v), want %s", target, ok, episode.ID)
	}
	target, ok = svc.resolveReferenceCandidate(ctx, &schema.ReferenceCandidate{Ref: "missing"}, []*schema.MemoryRecord{local})
	if ok || target != nil {
		t.Fatalf("missing reference = (%+v, %v), want nil false", target, ok)
	}
	target, ok = svc.resolveReferenceCandidate(ctx, &schema.ReferenceCandidate{Ref: "missing"}, []*schema.MemoryRecord{nil})
	if ok || target != nil {
		t.Fatalf("nil candidate reference = (%+v, %v), want nil false", target, ok)
	}
}

func TestLinkRecordToCanonicalEntitiesDirectly(t *testing.T) {
	svc, store := newCaptureTestService(t, nil)
	ctx := context.Background()
	ts := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	plain := newHelperSemanticRecord("plain-semantic", "plain")
	if edges, err := svc.linkRecordToCanonicalEntities(ctx, plain, ts); err != nil || edges != nil {
		t.Fatalf("plain semantic links = %+v, %v; want nil nil", edges, err)
	}

	subject := newObservationEntity("entity-subject", "Subject", "project:alpha")
	object := newObservationEntity("entity-object", "Object", "project:alpha")
	wrongType := newHelperSemanticRecord("entity-wrong-type", "wrong")
	for _, rec := range []*schema.MemoryRecord{subject, object, wrongType} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	linked := schema.NewMemoryRecord("linked-semantic", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   subject.ID,
		Predicate: "depends_on",
		Object:    object.ID,
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	linked.CreatedAt = ts
	linked.UpdatedAt = ts
	if err := store.Create(ctx, linked); err != nil {
		t.Fatalf("Create linked semantic: %v", err)
	}

	edges, err := svc.linkRecordToCanonicalEntities(ctx, linked, ts)
	if err != nil {
		t.Fatalf("linkRecordToCanonicalEntities: %v", err)
	}
	if len(edges) != 4 {
		t.Fatalf("edges len = %d, want subject/object bidirectional links: %+v", len(edges), edges)
	}
	got, err := store.Get(ctx, linked.ID)
	if err != nil {
		t.Fatalf("Get linked semantic: %v", err)
	}
	if len(got.Relations) != 2 {
		t.Fatalf("linked relations = %+v, want two outbound entity links", got.Relations)
	}

	skipsMissingAndWrongType := schema.NewMemoryRecord("skip-semantic", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "entity-missing",
		Predicate: "mentions",
		Object:    wrongType.ID,
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	if edges, err := svc.linkRecordToCanonicalEntities(ctx, skipsMissingAndWrongType, ts); err != nil || len(edges) != 0 {
		t.Fatalf("missing/wrong type links = %+v, %v; want no edges", edges, err)
	}
}

func TestLinkRecordToCanonicalEntitiesPropagatesRelationErrors(t *testing.T) {
	_, base := newCaptureTestService(t, nil)
	ctx := context.Background()
	ts := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	subject := newObservationEntity("entity-link-error-subject", "Subject", "project:alpha")
	linked := schema.NewMemoryRecord("linked-error-semantic", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   subject.ID,
		Predicate: "depends_on",
		Object:    "plain",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	for _, rec := range []*schema.MemoryRecord{subject, linked} {
		if err := base.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	failFirst := NewService(&relationFailAtStore{Store: base, failAt: 1}, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))
	if _, err := failFirst.linkRecordToCanonicalEntities(ctx, linked, ts); err == nil || !strings.Contains(err.Error(), "forced relation write failure") {
		t.Fatalf("first relation error = %v, want forced relation error", err)
	}

	failSecond := NewService(&relationFailAtStore{Store: base, failAt: 2}, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))
	if _, err := failSecond.linkRecordToCanonicalEntities(ctx, linked, ts); err == nil || !strings.Contains(err.Error(), "forced relation write failure") {
		t.Fatalf("second relation error = %v, want forced relation error", err)
	}

	updateErr := errors.New("update linked semantic failed")
	updateFailSvc := NewService(&updateFailStore{Store: base, err: updateErr}, NewClassifier(), NewPolicyEngine(DefaultPolicyDefaults()))
	if _, err := updateFailSvc.linkRecordToCanonicalEntities(ctx, linked, ts); !errors.Is(err, updateErr) {
		t.Fatalf("update semantic entity links error = %v, want %v", err, updateErr)
	}
}

func TestSemanticInterpretationHelpers(t *testing.T) {
	interpretation := &schema.Interpretation{
		Summary:              "linked semantic",
		TopicalLabels:        []string{"deploy", "orchid"},
		ExtractionConfidence: 0.82,
		Mentions: []schema.Mention{
			{Surface: "Orchid", Aliases: []string{"orchid project"}, CanonicalEntityID: "entity-orchid"},
		},
	}

	if got := canonicalizeSemanticSide("Orchid", interpretation); got != "entity-orchid" {
		t.Fatalf("canonicalizeSemanticSide surface = %q, want entity-orchid", got)
	}
	if got := canonicalizeSemanticSide("orchid project", interpretation); got != "entity-orchid" {
		t.Fatalf("canonicalizeSemanticSide alias = %q, want entity-orchid", got)
	}
	if got := canonicalizeSemanticSide("Borealis", interpretation); got != "Borealis" {
		t.Fatalf("canonicalizeSemanticSide miss = %q, want original", got)
	}
	if got := canonicalizeSemanticSide("Orchid", nil); got != "Orchid" {
		t.Fatalf("canonicalizeSemanticSide nil interpretation = %q, want original", got)
	}
	if got := canonicalizeSemanticObject("orchid project", interpretation); got != "entity-orchid" {
		t.Fatalf("canonicalizeSemanticObject string = %q, want entity-orchid", got)
	}
	obj := map[string]any{"name": "Orchid"}
	if got := canonicalizeSemanticObject(obj, interpretation); !reflect.DeepEqual(got, obj) {
		t.Fatalf("canonicalizeSemanticObject object = %#v, want original object", got)
	}
	if labels := interpretationLabels(interpretation); !reflect.DeepEqual(labels, []string{"deploy", "orchid"}) {
		t.Fatalf("interpretationLabels = %v, want labels", labels)
	}
	if labels := interpretationLabels(nil); labels != nil {
		t.Fatalf("interpretationLabels nil = %v, want nil", labels)
	}
	if got := interpretationConfidence(interpretation, 0.7); got != 0.82 {
		t.Fatalf("interpretationConfidence = %.2f, want 0.82", got)
	}
	if got := interpretationConfidence(&schema.Interpretation{}, 0.7); got != 0.7 {
		t.Fatalf("interpretationConfidence fallback = %.2f, want 0.7", got)
	}
}

func TestSemanticEntityLinksAndDerivedInterpretation(t *testing.T) {
	rec := schema.NewMemoryRecord("semantic-linked", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "entity-subject",
		Predicate: "depends_on",
		Object:    "550e8400-e29b-41d4-a716-446655440000",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})

	links := semanticEntityLinks(rec)
	if len(links) != 2 {
		t.Fatalf("semanticEntityLinks len = %d, want 2", len(links))
	}
	if ids := semanticEntityIDs(rec); !reflect.DeepEqual(ids, []string{"entity-subject", "550e8400-e29b-41d4-a716-446655440000"}) {
		t.Fatalf("semanticEntityIDs = %v, want subject and object entity IDs", ids)
	}

	source := &schema.Interpretation{
		Summary:              "source summary",
		TopicalLabels:        []string{"label"},
		ExtractionConfidence: 0.9,
	}
	derived := deriveSemanticInterpretation(source, rec)
	if derived == nil {
		t.Fatal("deriveSemanticInterpretation = nil, want interpretation")
	}
	if derived.ExtractionConfidence != 0.9 || derived.Summary != source.Summary || len(derived.Mentions) != 2 {
		t.Fatalf("derived interpretation = %+v, want copied source metadata and entity mentions", derived)
	}
	source.TopicalLabels[0] = "changed"
	if derived.TopicalLabels[0] == "changed" {
		t.Fatalf("derived TopicalLabels share backing array with source")
	}

	plain := schema.NewMemoryRecord("semantic-plain", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "subject",
		Predicate: "is",
		Object:    "object",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	if links := semanticEntityLinks(plain); len(links) != 0 {
		t.Fatalf("plain semantic links = %+v, want none", links)
	}
	if links := semanticEntityLinks(nil); links != nil {
		t.Fatalf("nil semantic links = %+v, want nil", links)
	}
	working := schema.NewMemoryRecord("working", schema.MemoryTypeWorking, schema.SensitivityLow, &schema.WorkingPayload{Kind: "working", ThreadID: "thread", State: schema.TaskStatePlanning})
	if links := semanticEntityLinks(working); links != nil {
		t.Fatalf("non-semantic links = %+v, want nil", links)
	}
	if derived := deriveSemanticInterpretation(nil, plain); derived != nil {
		t.Fatalf("deriveSemanticInterpretation empty = %+v, want nil", derived)
	}
	if derived := deriveSemanticInterpretation(nil, nil); derived != nil {
		t.Fatalf("deriveSemanticInterpretation nil record = %+v, want nil", derived)
	}
}

func TestInferEntityKindAndProposedType(t *testing.T) {
	for _, tc := range []struct {
		key  string
		want schema.EntityKind
	}{
		{key: "tool_name", want: schema.EntityKindTool},
		{key: "project", want: schema.EntityKindProject},
		{key: "file_path", want: schema.EntityKindFile},
		{key: "user", want: schema.EntityKindPerson},
		{key: "subject", want: schema.EntityKindPerson},
		{key: "topic", want: schema.EntityKindConcept},
	} {
		if got := inferEntityKind(tc.key); got != tc.want {
			t.Fatalf("inferEntityKind(%q) = %q, want %q", tc.key, got, tc.want)
		}
	}

	for _, tc := range []struct {
		sourceKind string
		want       schema.MemoryType
	}{
		{sourceKind: "working_state", want: schema.MemoryTypeWorking},
		{sourceKind: "observation", want: schema.MemoryTypeSemantic},
		{sourceKind: "event", want: schema.MemoryTypeEpisodic},
	} {
		if got := inferProposedType(tc.sourceKind); got != tc.want {
			t.Fatalf("inferProposedType(%q) = %q, want %q", tc.sourceKind, got, tc.want)
		}
	}
}

func newHelperSemanticRecord(id, subject string) *schema.MemoryRecord {
	return schema.NewMemoryRecord(id, schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   subject,
		Predicate: "is",
		Object:    "tested",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
}
