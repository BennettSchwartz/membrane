package consolidation

import (
	"context"
	"testing"

	"github.com/BennettSchwartz/membrane/pkg/schema"
)

func unreinforcedSemantic(source *schema.MemoryRecord, subject string, sensitivity schema.Sensitivity) *schema.MemoryRecord {
	rec := newExtractedSemanticRecord(source, Triple{Subject: subject, Predicate: "observed_in", Object: "done"}, source.CreatedAt)
	rec.ID = "existing"
	rec.Sensitivity = sensitivity
	rec.Salience = 0.2
	rec.Lifecycle.Decay.ReinforcementGain = 0.1
	rec.Provenance.Sources = nil
	rec.Payload.(*schema.SemanticPayload).Evidence = nil
	return rec
}

func TestExtractorRetainsConsumedSourceSnapshot(t *testing.T) {
	ctx := context.Background()
	for _, change := range []string{"downgrade", "scope"} {
		t.Run(change, func(t *testing.T) {
			store := newConsolidationTestStore(t)
			snapshot := semanticSourceEpisode("source", "deploy", "done", schema.OutcomeStatusSuccess)
			snapshot.Sensitivity = schema.SensitivityHigh
			existing := unreinforcedSemantic(snapshot, "deploy", schema.SensitivityLow)
			for _, rec := range []*schema.MemoryRecord{snapshot, existing} {
				if err := store.Create(ctx, rec); err != nil {
					t.Fatal(err)
				}
			}
			current, _ := store.Get(ctx, snapshot.ID)
			current.Sensitivity = schema.SensitivityLow
			if change == "scope" {
				current.Scope = "other"
			}
			if err := store.Update(ctx, current); err != nil {
				t.Fatal(err)
			}
			queue := newFakeExtractionStore()
			queue.existingSemantic["deploy\x00observed_in\x00done"] = existing
			custom := &fakeSourceReinforcer{}
			extractor := NewSemanticExtractor(store, queue, custom, &fakeLLMClient{})
			inserted, err := extractor.upsertTriple(ctx, snapshot, Triple{Subject: "deploy", Predicate: "observed_in", Object: "done"})
			if inserted || (change == "scope" && err == nil) || (change == "downgrade" && err != nil) {
				t.Fatalf("upsert = %v, %v", inserted, err)
			}
			after, _ := store.Get(ctx, existing.ID)
			if len(custom.sourceCalls) != 0 {
				t.Fatal("ID-only custom reinforcer received consumed snapshot")
			}
			if change == "downgrade" {
				if after.Sensitivity != schema.SensitivityHigh || !semanticFactHasSource(after, snapshot.ID) {
					t.Fatal("consumed high snapshot was downgraded")
				}
			} else if after.Sensitivity != existing.Sensitivity || semanticFactHasSource(after, snapshot.ID) || after.Salience != existing.Salience {
				t.Fatal("moved source influenced old-scope destination")
			}
		})
	}
}

func TestExtractorRechecksCurrentDestinationFact(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)
	source := semanticSourceEpisode("source", "deploy", "done", schema.OutcomeStatusSuccess)
	existing := unreinforcedSemantic(source, "deploy", source.Sensitivity)
	for _, rec := range []*schema.MemoryRecord{source, existing} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}
	current, _ := store.Get(ctx, existing.ID)
	current.Payload.(*schema.SemanticPayload).Object = "changed"
	if err := store.Update(ctx, current); err != nil {
		t.Fatal(err)
	}
	queue := newFakeExtractionStore()
	queue.existingSemantic["deploy\x00observed_in\x00done"] = existing
	_, err := NewSemanticExtractor(store, queue, &fakeReinforcer{}, &fakeLLMClient{}).upsertTriple(ctx, source, Triple{Subject: "deploy", Predicate: "observed_in", Object: "done"})
	if err == nil {
		t.Fatal("stale match reinforced a different current fact")
	}
	after, _ := store.Get(ctx, existing.ID)
	if after.Salience != existing.Salience || semanticFactHasSource(after, source.ID) || after.Payload.(*schema.SemanticPayload).Object != "changed" {
		t.Fatal("stale match changed destination state")
	}
}

func TestExtractorRechecksCanonicalPolicyAtPersistence(t *testing.T) {
	ctx := context.Background()
	base := newConsolidationTestStore(t)
	source := semanticSourceEpisode("source", "deploy", "done", schema.OutcomeStatusSuccess)
	source.Sensitivity = schema.SensitivityLow
	entity := newEntityRecord("entity", "deploy")
	existing := unreinforcedSemantic(source, entity.ID, schema.SensitivityLow)
	for _, rec := range []*schema.MemoryRecord{source, entity, existing} {
		if err := base.Create(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}
	store := &policyConsolidationStore{MemoryStore: base}
	store.beforeBegin = func() {
		store.beforeBegin = func() {
			current, _ := base.Get(ctx, entity.ID)
			current.Sensitivity = schema.SensitivityHyper
			if err := base.Update(ctx, current); err != nil {
				t.Fatal(err)
			}
		}
	}
	queue := newFakeExtractionStore()
	queue.existingSemantic["entity\x00observed_in\x00done"] = existing
	_, err := NewSemanticExtractor(store, queue, &fakeReinforcer{}, &fakeLLMClient{}).upsertTriple(ctx, source, Triple{Subject: "deploy", Predicate: "observed_in", Object: "done"})
	if err == nil {
		t.Fatal("stale entity authorization reached reinforcement")
	}
	after, _ := base.Get(ctx, existing.ID)
	if after.Salience != existing.Salience || semanticFactHasSource(after, source.ID) {
		t.Fatal("changed entity policy allowed influence")
	}
}

func TestLowerSourcesDoNotInfluenceExistingHighFacts(t *testing.T) {
	ctx := context.Background()
	for _, path := range []string{"semantic", "llm"} {
		for _, change := range []string{"low", "snapshot-low", "current-low"} {
			t.Run(path+"/"+change, func(t *testing.T) {
				base := newConsolidationTestStore(t)
				source := semanticSourceEpisode("source", "deploy", "done", schema.OutcomeStatusSuccess)
				source.Sensitivity = schema.SensitivityLow
				if change == "current-low" {
					source.Sensitivity = schema.SensitivityHigh
				}
				existing := unreinforcedSemantic(source, "deploy", schema.SensitivityHigh)
				for _, rec := range []*schema.MemoryRecord{source, existing} {
					if err := base.Create(ctx, rec); err != nil {
						t.Fatal(err)
					}
				}
				store := &policyConsolidationStore{MemoryStore: base}
				if change != "low" {
					store.beforeBegin = func() {
						current, _ := base.Get(ctx, source.ID)
						current.Sensitivity = schema.SensitivityLow
						if change == "snapshot-low" {
							current.Sensitivity = schema.SensitivityHigh
						}
						if err := base.Update(ctx, current); err != nil {
							t.Fatal(err)
						}
					}
				}
				if path == "semantic" {
					created, reinforced, err := NewSemanticConsolidator(store).Consolidate(ctx)
					if err != nil || created != 0 || reinforced != 0 {
						t.Fatalf("consolidate = %d/%d, %v", created, reinforced, err)
					}
				} else {
					queue := newFakeExtractionStore()
					queue.existingSemantic["deploy\x00observed_in\x00done"] = existing
					inserted, err := NewSemanticExtractor(store, queue, &fakeReinforcer{}, &fakeLLMClient{}).upsertTriple(ctx, source, Triple{Subject: "deploy", Predicate: "observed_in", Object: "done"})
					if err != nil || inserted {
						t.Fatalf("upsert = %v, %v", inserted, err)
					}
				}
				after, _ := base.Get(ctx, existing.ID)
				if after.Sensitivity != existing.Sensitivity || after.Salience != existing.Salience || len(after.Provenance.Sources) != 0 || len(after.AuditLog) != len(existing.AuditLog) {
					t.Fatal("lower source influenced high destination")
				}
			})
		}
	}
}

func TestCompetenceAdmitsHigherSourcesAndSkipsLowerContributions(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)
	tools := []schema.ToolNode{{ID: "n", Tool: "test"}}
	for _, sensitivity := range []schema.Sensitivity{schema.SensitivityLow, schema.SensitivityHyper} {
		source := episodicToolRecord(string(sensitivity), tools)
		source.Sensitivity = sensitivity
		if err := store.Create(ctx, source); err != nil {
			t.Fatal(err)
		}
	}
	existing := schema.NewMemoryRecord("existing", schema.MemoryTypeCompetence, schema.SensitivityHigh, &schema.CompetencePayload{Kind: "competence", SkillName: "skill:test", Recipe: []schema.RecipeStep{{Step: "test", Tool: "test"}}, Triggers: []schema.Trigger{{Signal: "test"}}, Performance: &schema.PerformanceStats{SuccessCount: 7, SuccessRate: 1}})
	existing.Salience = 0.2
	if err := store.Create(ctx, existing); err != nil {
		t.Fatal(err)
	}
	created, reinforced, err := NewCompetenceConsolidator(store).Consolidate(ctx)
	if err != nil || created != 0 || reinforced != 1 {
		t.Fatalf("consolidate = %d/%d, %v", created, reinforced, err)
	}
	after, _ := store.Get(ctx, existing.ID)
	if after.Sensitivity != schema.SensitivityHyper || after.Payload.(*schema.CompetencePayload).Performance.SuccessCount != 8 || competenceHasSource(after, "low") || !competenceHasSource(after, "hyper") {
		t.Fatal("mixed group lost authorized source or admitted lower evidence")
	}
	priorSalience, priorAudits := after.Salience, len(after.AuditLog)
	if _, reinforced, err := NewCompetenceConsolidator(store).Consolidate(ctx); err != nil || reinforced != 0 {
		t.Fatalf("repeat = %d, %v", reinforced, err)
	}
	after, _ = store.Get(ctx, existing.ID)
	if after.Payload.(*schema.CompetencePayload).Performance.SuccessCount != 8 || after.Salience != priorSalience || len(after.AuditLog) != priorAudits {
		t.Fatal("known source or lower source gained repeat influence")
	}
}

func TestLowerSemanticContributionDoesNotAbortUnrelatedExtraction(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)
	low := semanticSourceEpisode("source", "deploy", "done", schema.OutcomeStatusSuccess)
	low.Sensitivity = schema.SensitivityLow
	existing := unreinforcedSemantic(low, "deploy", schema.SensitivityHigh)
	unrelated := semanticSourceEpisode("other", "test", "another fact", schema.OutcomeStatusSuccess)
	unrelated.Sensitivity = schema.SensitivityLow
	for _, rec := range []*schema.MemoryRecord{low, existing, unrelated} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}
	created, reinforced, err := NewSemanticConsolidator(store).Consolidate(ctx)
	if err != nil || created != 1 || reinforced != 0 {
		t.Fatalf("independent extraction = %d/%d, %v", created, reinforced, err)
	}
}
