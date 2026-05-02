package consolidation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

type entityLookupStore struct {
	*fakeExtractionStore
	matches map[string][]*schema.MemoryRecord
	err     error
}

func (s *entityLookupStore) FindEntitiesByTerm(_ context.Context, term, _ string, limit int) ([]*schema.MemoryRecord, error) {
	if s.err != nil {
		return nil, s.err
	}
	records := s.matches[schema.NormalizeEntityTerm(term)]
	if limit > 0 && len(records) > limit {
		records = records[:limit]
	}
	return records, nil
}

func (s *entityLookupStore) FindEntityByIdentifier(context.Context, string, string, string) (*schema.MemoryRecord, error) {
	return nil, storage.ErrNotFound
}

func TestCanonicalizeSemanticRecordEntities(t *testing.T) {
	ctx := context.Background()
	ts := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	subject := newEntityRecord("entity-orchid", "Orchid")
	object := newEntityRecord("entity-borealis", "Borealis")
	store := &entityLookupStore{
		fakeExtractionStore: newFakeExtractionStore(),
		matches: map[string][]*schema.MemoryRecord{
			"orchid":   {subject},
			"borealis": {object},
		},
	}
	rec := schema.NewMemoryRecord("semantic-1", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "Orchid",
		Predicate: "depends_on",
		Object:    "Borealis",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	rec.Scope = "project"
	rec.CreatedAt = ts

	edges := canonicalizeSemanticRecordEntities(ctx, store, rec)
	payload := rec.Payload.(*schema.SemanticPayload)
	if payload.Subject != subject.ID || payload.Object != object.ID {
		t.Fatalf("payload = %+v, want canonical entity IDs", payload)
	}
	if len(rec.Relations) != 2 {
		t.Fatalf("relations len = %d, want 2", len(rec.Relations))
	}
	if len(edges) != 4 {
		t.Fatalf("edges len = %d, want 4", len(edges))
	}
	if edges[0].CreatedAt != ts {
		t.Fatalf("edge CreatedAt = %s, want %s", edges[0].CreatedAt, ts)
	}
}

func TestCanonicalizeSemanticRecordEntitiesSkipsUnsupportedInputs(t *testing.T) {
	ctx := context.Background()
	store := &entityLookupStore{fakeExtractionStore: newFakeExtractionStore()}
	working := schema.NewMemoryRecord("working-1", schema.MemoryTypeWorking, schema.SensitivityLow, &schema.WorkingPayload{
		Kind:     "working",
		ThreadID: "thread",
		State:    schema.TaskStatePlanning,
	})
	if edges := canonicalizeSemanticRecordEntities(ctx, store, working); edges != nil {
		t.Fatalf("working edges = %#v, want nil", edges)
	}

	semantic := schema.NewMemoryRecord("semantic-1", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "Orchid",
		Predicate: "is",
		Object:    "project",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	if edges := canonicalizeSemanticRecordEntities(ctx, newFakeExtractionStore(), semantic); edges != nil {
		t.Fatalf("non-lookup store edges = %#v, want nil", edges)
	}
}

func TestFindEntityByTermRejectsInvalidMatches(t *testing.T) {
	ctx := context.Background()
	entity := newEntityRecord("entity-orchid", "Orchid")
	semantic := schema.NewMemoryRecord("semantic-1", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "Orchid",
		Predicate: "is",
		Object:    "project",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})

	for _, tc := range []struct {
		name  string
		store *entityLookupStore
		want  *schema.MemoryRecord
	}{
		{name: "match", store: &entityLookupStore{fakeExtractionStore: newFakeExtractionStore(), matches: map[string][]*schema.MemoryRecord{"orchid": {entity}}}, want: entity},
		{name: "error", store: &entityLookupStore{fakeExtractionStore: newFakeExtractionStore(), err: errors.New("lookup failed")}},
		{name: "empty", store: &entityLookupStore{fakeExtractionStore: newFakeExtractionStore(), matches: map[string][]*schema.MemoryRecord{"orchid": {}}}},
		{name: "nil", store: &entityLookupStore{fakeExtractionStore: newFakeExtractionStore(), matches: map[string][]*schema.MemoryRecord{"orchid": {nil}}}},
		{name: "non-entity", store: &entityLookupStore{fakeExtractionStore: newFakeExtractionStore(), matches: map[string][]*schema.MemoryRecord{"orchid": {semantic}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := findEntityByTerm(ctx, tc.store, "Orchid", "project")
			if got != tc.want {
				t.Fatalf("findEntityByTerm = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestLinkRecordToEntityTermsDedupesMatches(t *testing.T) {
	ctx := context.Background()
	ts := time.Date(2026, 5, 1, 11, 0, 0, 0, time.UTC)
	entity := newEntityRecord("entity-orchid", "Orchid")
	store := &entityLookupStore{
		fakeExtractionStore: newFakeExtractionStore(),
		matches: map[string][]*schema.MemoryRecord{
			"orchid":  {entity},
			"project": {entity},
		},
	}
	rec := schema.NewMemoryRecord("competence-1", schema.MemoryTypeCompetence, schema.SensitivityLow, &schema.CompetencePayload{
		Kind:      "competence",
		SkillName: "debug",
		Triggers:  []schema.Trigger{{Signal: "panic"}},
		Recipe:    []schema.RecipeStep{{Step: "read logs"}},
	})
	rec.Scope = "project"

	edges := linkRecordToEntityTerms(ctx, store, rec, []string{"Orchid", "project", "missing"}, "uses", "used_by", ts)
	if len(edges) != 2 {
		t.Fatalf("edges len = %d, want 2", len(edges))
	}
	if len(rec.Relations) != 1 {
		t.Fatalf("relations len = %d, want 1", len(rec.Relations))
	}
	if rec.Relations[0].TargetID != entity.ID || rec.Relations[0].Predicate != "uses" {
		t.Fatalf("relation = %+v, want uses relation to entity", rec.Relations[0])
	}

	if edges := linkRecordToEntityTerms(ctx, newFakeExtractionStore(), rec, []string{"Orchid"}, "uses", "used_by", ts); edges != nil {
		t.Fatalf("non-lookup store edges = %#v, want nil", edges)
	}
	if edges := linkRecordToEntityTerms(ctx, store, nil, []string{"Orchid"}, "uses", "used_by", ts); edges != nil {
		t.Fatalf("nil record edges = %#v, want nil", edges)
	}
}

func TestFormatEpisodicContentVariants(t *testing.T) {
	if got := formatEpisodicContent(schema.NewMemoryRecord("semantic-1", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "Go",
		Predicate: "is",
		Object:    "typed",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})); got != "" {
		t.Fatalf("format non-episodic = %q, want empty", got)
	}

	rec := schema.NewMemoryRecord("episode-1", schema.MemoryTypeEpisodic, schema.SensitivityLow, &schema.EpisodicPayload{
		Kind: "episodic",
		Timeline: []schema.TimelineEvent{
			{EventKind: "blank", Summary: " "},
			{EventKind: "user_input", Summary: "Remember Orchid"},
		},
		ToolGraph: []schema.ToolNode{
			{Tool: "noop"},
			{Tool: "search", Result: "found result"},
		},
		Outcome: schema.OutcomeStatusPartial,
	})
	got := formatEpisodicContent(rec)
	want := "user_input: Remember Orchid\ntool noop\ntool search -> found result\noutcome: partial"
	if got != want {
		t.Fatalf("format episodic = %q, want %q", got, want)
	}
}

func TestSensitivityRank(t *testing.T) {
	for _, tc := range []struct {
		sensitivity schema.Sensitivity
		want        int
	}{
		{sensitivity: schema.SensitivityPublic, want: 0},
		{sensitivity: schema.SensitivityLow, want: 1},
		{sensitivity: schema.SensitivityMedium, want: 2},
		{sensitivity: schema.SensitivityHigh, want: 3},
		{sensitivity: schema.SensitivityHyper, want: 4},
		{sensitivity: "", want: -1},
	} {
		if got := sensitivityRank(tc.sensitivity); got != tc.want {
			t.Fatalf("sensitivityRank(%q) = %d, want %d", tc.sensitivity, got, tc.want)
		}
	}
}

func newEntityRecord(id, name string) *schema.MemoryRecord {
	rec := schema.NewMemoryRecord(id, schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: name,
		PrimaryType:   schema.EntityTypeProject,
		Aliases:       []schema.EntityAlias{{Value: name}},
	})
	rec.Scope = "project"
	return rec
}
