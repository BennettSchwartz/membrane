package consolidation

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/BennettSchwartz/membrane/internal/teststore"
	"github.com/BennettSchwartz/membrane/pkg/schema"
)

func TestPlanGraphConsolidatorCreatesPlanWithEntityLinksAndSkipsDuplicates(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)

	entity := newEntityRecord("entity-deploy", "deploy")
	if err := store.Create(ctx, entity); err != nil {
		t.Fatalf("Create entity: %v", err)
	}
	source := episodicToolRecord("plan-source-linked", []schema.ToolNode{
		{ID: "n1", Tool: "checkout"},
		{ID: "n2", Tool: "test", DependsOn: []string{"n1"}},
		{ID: "n3", Tool: "deploy", DependsOn: []string{"n2"}},
	})
	source.Scope = "project"
	if err := store.Create(ctx, source); err != nil {
		t.Fatalf("Create source: %v", err)
	}

	created, err := NewPlanGraphConsolidator(store).Consolidate(ctx)
	if err != nil {
		t.Fatalf("Consolidate: %v", err)
	}
	if created != 1 {
		t.Fatalf("created = %d, want 1", created)
	}

	plans, err := store.ListByType(ctx, schema.MemoryTypePlanGraph)
	if err != nil {
		t.Fatalf("ListByType plan graph: %v", err)
	}
	if len(plans) != 1 {
		t.Fatalf("plans len = %d, want 1", len(plans))
	}
	plan := plans[0]
	payload := plan.Payload.(*schema.PlanGraphPayload)
	if payload.Intent != "tool_sequence" || len(payload.Nodes) != 3 || len(payload.Edges) != 2 {
		t.Fatalf("plan payload = %+v, want converted tool graph", payload)
	}
	rels, err := store.GetRelations(ctx, plan.ID)
	if err != nil {
		t.Fatalf("GetRelations plan: %v", err)
	}
	if !hasRelation(rels, "derived_from", source.ID) || !hasRelation(rels, "uses", entity.ID) {
		t.Fatalf("plan relations = %+v, want derived_from source and uses entity", rels)
	}
	entityRels, err := store.GetRelations(ctx, entity.ID)
	if err != nil {
		t.Fatalf("GetRelations entity: %v", err)
	}
	if !hasRelation(entityRels, "used_by", plan.ID) {
		t.Fatalf("entity relations = %+v, want used_by plan", entityRels)
	}

	created, err = NewPlanGraphConsolidator(store).Consolidate(ctx)
	if err != nil {
		t.Fatalf("Consolidate duplicate pass: %v", err)
	}
	if created != 0 {
		t.Fatalf("duplicate pass created = %d, want 0", created)
	}
}

func TestPlanGraphConsolidatorSkipsUnsupportedEpisodes(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)

	short := episodicToolRecord("short-plan-source", []schema.ToolNode{
		{ID: "n1", Tool: "checkout"},
		{ID: "n2", Tool: "test"},
	})
	working := schema.NewMemoryRecord("not-episodic", schema.MemoryTypeWorking, schema.SensitivityLow, &schema.WorkingPayload{
		Kind:     "working",
		ThreadID: "thread",
		State:    schema.TaskStatePlanning,
	})
	badPayload := schema.NewMemoryRecord("bad-payload", schema.MemoryTypeEpisodic, schema.SensitivityLow, &schema.EpisodicPayload{
		Kind: "episodic",
	})
	badPayload.Payload = &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "x",
		Predicate: "is",
		Object:    "y",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	}
	for _, rec := range []*schema.MemoryRecord{short, working} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}
	if err := store.Create(ctx, badPayload); err == nil {
		t.Fatalf("Create bad payload error = nil, want validation failure")
	}

	created, err := NewPlanGraphConsolidator(store).Consolidate(ctx)
	if err != nil {
		t.Fatalf("Consolidate: %v", err)
	}
	if created != 0 {
		t.Fatalf("created = %d, want 0 for unsupported episodes", created)
	}
}

func TestPlanGraphConsolidatorNormalizesDerivedFromPredicateWhenSkippingDuplicates(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)

	source := episodicToolRecord("plan-source-legacy-predicate", []schema.ToolNode{
		{ID: "n1", Tool: "checkout"},
		{ID: "n2", Tool: "test", DependsOn: []string{"n1"}},
		{ID: "n3", Tool: "deploy", DependsOn: []string{"n2"}},
	})
	if err := store.Create(ctx, source); err != nil {
		t.Fatalf("Create source: %v", err)
	}

	plan := schema.NewMemoryRecord("existing-plan-legacy-predicate", schema.MemoryTypePlanGraph, schema.SensitivityLow, &schema.PlanGraphPayload{
		Kind:    "plan_graph",
		PlanID:  "legacy-plan",
		Version: "1",
		Intent:  "tool_sequence",
	})
	if err := store.Create(ctx, plan); err != nil {
		t.Fatalf("Create plan: %v", err)
	}

	legacyStore := &legacyPlanRelationStore{
		MemoryStore: store,
		planID:      plan.ID,
		sourceID:    source.ID,
	}
	created, err := NewPlanGraphConsolidator(legacyStore).Consolidate(ctx)
	if err != nil {
		t.Fatalf("Consolidate: %v", err)
	}
	if created != 0 {
		t.Fatalf("created = %d, want 0 for legacy derived_from predicate", created)
	}
	plans, err := store.ListByType(ctx, schema.MemoryTypePlanGraph)
	if err != nil {
		t.Fatalf("ListByType plan graph: %v", err)
	}
	if len(plans) != 1 {
		t.Fatalf("plans len = %d, want only existing legacy plan", len(plans))
	}
}

func TestPlanGraphConsolidatorFailsClosedWhenExistingPlanRelationsAreUnavailable(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)

	source := episodicToolRecord("plan-source-relation-error", []schema.ToolNode{
		{ID: "n1", Tool: "checkout"},
		{ID: "n2", Tool: "test", DependsOn: []string{"n1"}},
		{ID: "n3", Tool: "deploy", DependsOn: []string{"n2"}},
	})
	if err := store.Create(ctx, source); err != nil {
		t.Fatalf("Create source: %v", err)
	}
	plan := schema.NewMemoryRecord("existing-plan-relation-error", schema.MemoryTypePlanGraph, schema.SensitivityLow, &schema.PlanGraphPayload{
		Kind:    "plan_graph",
		PlanID:  "existing-plan",
		Version: "1",
		Intent:  "tool_sequence",
	})
	if err := store.Create(ctx, plan); err != nil {
		t.Fatalf("Create plan: %v", err)
	}

	relationErr := errors.New("relations unavailable")
	created, err := NewPlanGraphConsolidator(&failingPlanRelationStore{
		MemoryStore: store,
		planID:      plan.ID,
		err:         relationErr,
	}).Consolidate(ctx)
	if err == nil || !strings.Contains(err.Error(), "load plan graph relations") || !errors.Is(err, relationErr) {
		t.Fatalf("Consolidate error = %v, want wrapped relation lookup error", err)
	}
	if created != 0 {
		t.Fatalf("created = %d, want 0 when duplicate guard cannot inspect existing plan relations", created)
	}
	plans, err := store.ListByType(ctx, schema.MemoryTypePlanGraph)
	if err != nil {
		t.Fatalf("ListByType plan graph: %v", err)
	}
	if len(plans) != 1 {
		t.Fatalf("plans len = %d, want no duplicate plan while relation lookup is unavailable", len(plans))
	}
}

func TestPlanEntityTermsIncludesKnownStringParams(t *testing.T) {
	got := planEntityTerms([]schema.PlanNode{
		{
			Op: "deploy",
			Params: map[string]any{
				"tool":       "kubectl",
				"command":    "apply",
				"repo":       "orchid",
				"repository": "orchid/service",
				"file":       "deployment.yaml",
				"service":    "api",
				"package":    "pkg/api",
				"ignored":    "not indexed",
			},
		},
		{
			Op: "",
			Params: map[string]any{
				"tool":    "",
				"command": 123,
			},
		},
	})
	want := []string{"deploy", "kubectl", "apply", "orchid", "orchid/service", "deployment.yaml", "api", "pkg/api"}
	if len(got) != len(want) {
		t.Fatalf("planEntityTerms len = %d, want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("planEntityTerms[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

type failingPlanRelationStore struct {
	*teststore.MemoryStore
	planID string
	err    error
}

func (s *failingPlanRelationStore) GetRelations(ctx context.Context, id string) ([]schema.Relation, error) {
	if id == s.planID {
		return nil, s.err
	}
	return s.MemoryStore.GetRelations(ctx, id)
}

type legacyPlanRelationStore struct {
	*teststore.MemoryStore
	planID   string
	sourceID string
}

func (s *legacyPlanRelationStore) GetRelations(ctx context.Context, id string) ([]schema.Relation, error) {
	if id == s.planID {
		return []schema.Relation{{
			Predicate: "Derived From",
			TargetID:  s.sourceID,
			Weight:    1.0,
		}}, nil
	}
	return s.MemoryStore.GetRelations(ctx, id)
}

func hasRelation(rels []schema.Relation, predicate, targetID string) bool {
	for _, rel := range rels {
		if rel.Predicate == predicate && rel.TargetID == targetID {
			return true
		}
	}
	return false
}
