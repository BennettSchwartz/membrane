package consolidation

import (
	"context"
	"testing"

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

func hasRelation(rels []schema.Relation, predicate, targetID string) bool {
	for _, rel := range rels {
		if rel.Predicate == predicate && rel.TargetID == targetID {
			return true
		}
	}
	return false
}
