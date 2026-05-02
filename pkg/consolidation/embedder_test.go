package consolidation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
)

type recordingConsolidationEmbedder struct {
	ids []string
	err error
}

func (e *recordingConsolidationEmbedder) EmbedRecord(_ context.Context, rec *schema.MemoryRecord) error {
	e.ids = append(e.ids, rec.ID)
	return e.err
}

func episodicToolRecord(id string, tools []schema.ToolNode) *schema.MemoryRecord {
	now := time.Date(2026, 5, 1, 11, 30, 0, 0, time.UTC)
	rec := schema.NewMemoryRecord(id, schema.MemoryTypeEpisodic, schema.SensitivityLow, &schema.EpisodicPayload{
		Kind: "episodic",
		Timeline: []schema.TimelineEvent{{
			T:         now,
			EventKind: "tool_sequence",
			Ref:       id,
			Summary:   "ran a reusable tool sequence",
		}},
		ToolGraph: tools,
		Outcome:   schema.OutcomeStatusSuccess,
	})
	rec.CreatedAt = now
	rec.UpdatedAt = now
	rec.Lifecycle.LastReinforcedAt = now
	return rec
}

func TestCompetenceConsolidatorWithEmbedderEmbedsNewCompetence(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)
	embedder := &recordingConsolidationEmbedder{err: errors.New("embedding unavailable")}

	tools := []schema.ToolNode{{ID: "t1", Tool: "go-test"}}
	for _, rec := range []*schema.MemoryRecord{
		episodicToolRecord("episode-a", tools),
		episodicToolRecord("episode-b", tools),
	} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	created, reinforced, err := NewCompetenceConsolidatorWithEmbedder(store, embedder).Consolidate(ctx)
	if err != nil {
		t.Fatalf("Consolidate: %v", err)
	}
	if created != 1 || reinforced != 0 {
		t.Fatalf("created=%d reinforced=%d, want 1/0", created, reinforced)
	}

	competences, err := store.ListByType(ctx, schema.MemoryTypeCompetence)
	if err != nil {
		t.Fatalf("ListByType competence: %v", err)
	}
	if len(competences) != 1 {
		t.Fatalf("competences len = %d, want 1", len(competences))
	}
	if len(embedder.ids) != 1 || embedder.ids[0] != competences[0].ID {
		t.Fatalf("embedded IDs = %v, want [%s]", embedder.ids, competences[0].ID)
	}
}

func TestPlanGraphConsolidatorWithEmbedderEmbedsNewPlan(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)
	embedder := &recordingConsolidationEmbedder{err: errors.New("embedding unavailable")}

	rec := episodicToolRecord("plan-source", []schema.ToolNode{
		{ID: "n1", Tool: "checkout"},
		{ID: "n2", Tool: "test", DependsOn: []string{"n1"}},
		{ID: "n3", Tool: "deploy", DependsOn: []string{"n2"}},
	})
	if err := store.Create(ctx, rec); err != nil {
		t.Fatalf("Create source: %v", err)
	}

	created, err := NewPlanGraphConsolidatorWithEmbedder(store, embedder).Consolidate(ctx)
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
	if len(embedder.ids) != 1 || embedder.ids[0] != plans[0].ID {
		t.Fatalf("embedded IDs = %v, want [%s]", embedder.ids, plans[0].ID)
	}
}

func TestNewServiceWithEmbedderWiresDurableConsolidators(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)
	embedder := &recordingConsolidationEmbedder{}

	tools := []schema.ToolNode{
		{ID: "n1", Tool: "checkout"},
		{ID: "n2", Tool: "test", DependsOn: []string{"n1"}},
		{ID: "n3", Tool: "deploy", DependsOn: []string{"n2"}},
	}
	for _, rec := range []*schema.MemoryRecord{
		episodicToolRecord("run-all-a", tools),
		episodicToolRecord("run-all-b", tools),
	} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	result, err := NewServiceWithEmbedder(store, embedder).RunAll(ctx)
	if err != nil {
		t.Fatalf("RunAll: %v", err)
	}
	if result.CompetenceExtracted != 1 || result.PlanGraphsExtracted != 2 {
		t.Fatalf("result = %+v, want one competence and two plan graphs", result)
	}
	if len(embedder.ids) != 3 {
		t.Fatalf("embedded IDs = %v, want three durable records", embedder.ids)
	}
}

func TestNewServiceWithExtractorRunsExtractorPipeline(t *testing.T) {
	ctx := context.Background()
	store := newFakeExtractionStore()
	llm := &fakeLLMClient{}
	reinforcer := &fakeReinforcer{}

	result, err := NewServiceWithExtractor(store, nil, reinforcer, llm, store).RunAll(ctx)
	if err != nil {
		t.Fatalf("RunAll: %v", err)
	}
	if result.SemanticTriplesExtracted != 0 || result.ExtractionSkipped != 0 {
		t.Fatalf("result = %+v, want no claimed extraction work", result)
	}
	if store.cleanCalls != 1 {
		t.Fatalf("cleanCalls = %d, want extractor cleanup to run once", store.cleanCalls)
	}
}
