package consolidation

import (
	"context"
	"errors"
	"testing"

	"github.com/BennettSchwartz/membrane/internal/teststore"
	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

type policyConsolidationStore struct {
	*teststore.MemoryStore
	beforeBegin func()
	unsupported bool
	metadata    func(string, storage.RecordAuthorizationMetadata) (storage.RecordAuthorizationMetadata, bool)
}

func (s *policyConsolidationStore) Begin(ctx context.Context) (storage.Transaction, error) {
	if s.beforeBegin != nil {
		fn := s.beforeBegin
		s.beforeBegin = nil
		fn()
	}
	tx, err := s.MemoryStore.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if s.unsupported {
		return &struct{ storage.Transaction }{tx}, nil
	}
	return &policyConsolidationTx{Transaction: tx, metadata: s.metadata}, nil
}

type policyConsolidationTx struct {
	storage.Transaction
	metadata func(string, storage.RecordAuthorizationMetadata) (storage.RecordAuthorizationMetadata, bool)
}

func (tx *policyConsolidationTx) GetAuthorizationMetadata(ctx context.Context, ids []string) ([]storage.RecordAuthorizationMetadata, error) {
	rows, err := tx.Transaction.(storage.AuthorizationMetadataStore).GetAuthorizationMetadata(ctx, ids)
	if err != nil || tx.metadata == nil {
		return rows, err
	}
	var result []storage.RecordAuthorizationMetadata
	for _, row := range rows {
		if replacement, keep := tx.metadata(row.ID, row); keep {
			result = append(result, replacement)
		}
	}
	return result, nil
}

func TestSemanticReinforcementPromotesKnownSourceAndPrunesInverse(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)
	source := semanticSourceEpisode("policy-source", "deploy", "done", schema.OutcomeStatusSuccess)
	source.Sensitivity = schema.SensitivityLow
	entity := newEntityRecord("policy-entity", "deploy")
	for _, rec := range []*schema.MemoryRecord{source, entity} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := NewSemanticConsolidator(store).Consolidate(ctx); err != nil {
		t.Fatal(err)
	}
	semantics, _ := store.ListByType(ctx, schema.MemoryTypeSemantic)
	if len(semantics) != 1 {
		t.Fatalf("semantics = %d", len(semantics))
	}
	before := semantics[0]
	source.Sensitivity = schema.SensitivityHyper
	if err := store.Update(ctx, source); err != nil {
		t.Fatal(err)
	}
	created, reinforced, err := NewSemanticConsolidator(store).Consolidate(ctx)
	if err != nil || created != 0 || reinforced != 1 {
		t.Fatalf("consolidate = %d/%d, %v", created, reinforced, err)
	}
	after, _ := store.Get(ctx, before.ID)
	if after.Sensitivity != schema.SensitivityHyper || after.Salience != before.Salience || len(after.Payload.(*schema.SemanticPayload).Evidence) != 1 {
		t.Fatalf("known source repair = %+v", after)
	}
	if !hasRelation(after.Relations, schema.GraphPredicateSubjectEntity, entity.ID) {
		t.Fatal("safe forward relation removed")
	}
	inverse, _ := store.GetRelations(ctx, entity.ID)
	if hasRelation(inverse, schema.GraphPredicateFactSubjectOf, after.ID) {
		t.Fatal("lower entity exposes promoted semantic ID")
	}
}

func TestConsolidationUsesCurrentSourceAndDestinationPolicy(t *testing.T) {
	ctx := context.Background()
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "new", true: "reinforce"}[existing], func(t *testing.T) {
			base := newConsolidationTestStore(t)
			source := semanticSourceEpisode("source", "deploy", "done", schema.OutcomeStatusSuccess)
			source.Sensitivity = schema.SensitivityLow
			if err := base.Create(ctx, source); err != nil {
				t.Fatal(err)
			}
			var destinationID string
			if existing {
				if _, _, err := NewSemanticConsolidator(base).Consolidate(ctx); err != nil {
					t.Fatal(err)
				}
				rows, _ := base.ListByType(ctx, schema.MemoryTypeSemantic)
				destinationID = rows[0].ID
			}
			store := &policyConsolidationStore{MemoryStore: base, beforeBegin: func() {
				current, _ := base.Get(ctx, source.ID)
				current.Sensitivity = schema.SensitivityHigh
				if err := base.Update(ctx, current); err != nil {
					t.Fatal(err)
				}
				if existing {
					current, _ := base.Get(ctx, destinationID)
					current.Tags = append(current.Tags, "concurrent-tag")
					current.Sensitivity = schema.SensitivityHyper
					if err := base.Update(ctx, current); err != nil {
						t.Fatal(err)
					}
				}
			}}
			if _, _, err := NewSemanticConsolidator(store).Consolidate(ctx); err != nil {
				t.Fatal(err)
			}
			rows, _ := base.ListByType(ctx, schema.MemoryTypeSemantic)
			want := schema.SensitivityHigh
			if existing {
				want = schema.SensitivityHyper
			}
			if len(rows) != 1 || rows[0].Sensitivity != want {
				t.Fatalf("derived policy = %+v", rows)
			}
			if existing && rows[0].Tags[len(rows[0].Tags)-1] != "concurrent-tag" {
				t.Fatal("stale destination overwrote current tags")
			}
		})
	}
}

func TestCompetenceReinforcementPromotesHighEvidenceAndRepairsKnownSources(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)
	for _, id := range []string{"a", "b"} {
		rec := episodicToolRecord(id, []schema.ToolNode{{ID: "n", Tool: "test"}})
		if err := store.Create(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := NewCompetenceConsolidator(store).Consolidate(ctx); err != nil {
		t.Fatal(err)
	}
	rows, _ := store.ListByType(ctx, schema.MemoryTypeCompetence)
	id := rows[0].ID
	rec := episodicToolRecord("c", []schema.ToolNode{{ID: "n", Tool: "test"}})
	rec.Sensitivity = schema.SensitivityHigh
	if err := store.Create(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if _, _, err := NewCompetenceConsolidator(store).Consolidate(ctx); err != nil {
		t.Fatal(err)
	}
	after, _ := store.Get(ctx, id)
	if after.Sensitivity != schema.SensitivityHigh || after.Payload.(*schema.CompetencePayload).Performance.SuccessCount != 3 {
		t.Fatalf("high reinforcement = %+v", after)
	}
	rec.Sensitivity = schema.SensitivityHyper
	if err := store.Update(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if _, _, err := NewCompetenceConsolidator(store).Consolidate(ctx); err != nil {
		t.Fatal(err)
	}
	after, _ = store.Get(ctx, id)
	if after.Sensitivity != schema.SensitivityHyper || after.Payload.(*schema.CompetencePayload).Performance.SuccessCount != 3 {
		t.Fatalf("known source repair = %+v", after)
	}
}

func TestCompetenceDoesNotCombineIncompatibleScopes(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)
	for _, scope := range []string{"project:a", "project:b"} {
		rec := episodicToolRecord(scope, []schema.ToolNode{{ID: "n", Tool: "test"}})
		rec.Scope = scope
		if err := store.Create(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}
	created, reinforced, err := NewCompetenceConsolidator(store).Consolidate(ctx)
	if err != nil || created != 0 || reinforced != 0 {
		t.Fatalf("cross-scope group = %d/%d, %v", created, reinforced, err)
	}
}

func TestBackgroundEntityPolicyAcrossConsolidationPaths(t *testing.T) {
	ctx := context.Background()
	for _, path := range []string{"semantic", "competence", "plan", "llm"} {
		for _, policy := range []string{"same", "global", "lower", "hidden", "changed", "missing"} {
			t.Run(path+"/"+policy, func(t *testing.T) {
				base := newConsolidationTestStore(t)
				entity := newEntityRecord("entity", "deploy")
				entity.Sensitivity = schema.SensitivityMedium
				if policy == "global" {
					entity.Scope = ""
				}
				if policy == "lower" {
					entity.Sensitivity = schema.SensitivityLow
				}
				if policy == "hidden" {
					entity.Sensitivity = schema.SensitivityHyper
				}
				if err := base.Create(ctx, entity); err != nil {
					t.Fatal(err)
				}
				source := semanticSourceEpisode("source", "deploy", "done", schema.OutcomeStatusSuccess)
				source.Payload.(*schema.EpisodicPayload).ToolGraph = []schema.ToolNode{{ID: "a", Tool: "deploy"}, {ID: "b", Tool: "test"}, {ID: "c", Tool: "ship"}}
				if err := base.Create(ctx, source); err != nil {
					t.Fatal(err)
				}
				store := &policyConsolidationStore{MemoryStore: base}
				if policy == "changed" || policy == "missing" {
					store.metadata = func(id string, row storage.RecordAuthorizationMetadata) (storage.RecordAuthorizationMetadata, bool) {
						if id == entity.ID {
							if policy == "missing" {
								return row, false
							}
							row.Sensitivity = schema.SensitivityHyper
						}
						return row, true
					}
				}
				var typ schema.MemoryType
				switch path {
				case "semantic":
					typ = schema.MemoryTypeSemantic
					if _, _, err := NewSemanticConsolidator(store).Consolidate(ctx); err != nil {
						t.Fatal(err)
					}
				case "competence":
					typ = schema.MemoryTypeCompetence
					other := *source
					other.ID = "source-two"
					if err := base.Create(ctx, &other); err != nil {
						t.Fatal(err)
					}
					if _, _, err := NewCompetenceConsolidator(store).Consolidate(ctx); err != nil {
						t.Fatal(err)
					}
				case "plan":
					typ = schema.MemoryTypePlanGraph
					if _, err := NewPlanGraphConsolidator(store).Consolidate(ctx); err != nil {
						t.Fatal(err)
					}
				case "llm":
					typ = schema.MemoryTypeSemantic
					extractor := NewSemanticExtractor(store, newFakeExtractionStore(), &fakeReinforcer{}, &fakeLLMClient{})
					if _, err := extractor.upsertTriple(ctx, source, Triple{Subject: "deploy", Predicate: "observed_in", Object: "done"}); err != nil {
						t.Fatal(err)
					}
				}
				rows, _ := base.ListByType(ctx, typ)
				if len(rows) != 1 {
					t.Fatalf("derived records = %d", len(rows))
				}
				derived := rows[0]
				allowed := policy == "same" || policy == "global" || policy == "lower"
				linked := false
				for _, rel := range derived.Relations {
					if rel.TargetID == entity.ID {
						linked = true
					}
				}
				if linked != allowed {
					t.Fatalf("forward link = %v, want %v", linked, allowed)
				}
				if payload, ok := derived.Payload.(*schema.SemanticPayload); ok && (payload.Subject == entity.ID) != allowed {
					t.Fatalf("subject canonicalization = %q", payload.Subject)
				}
				inverse, _ := base.GetRelations(ctx, entity.ID)
				hasInverse := false
				for _, rel := range inverse {
					if rel.TargetID == derived.ID {
						hasInverse = true
					}
				}
				if hasInverse != (policy == "same") {
					t.Fatalf("inverse link = %v for %s", hasInverse, policy)
				}
			})
		}
	}
}

func TestConsolidationRejectsUnsupportedTransactionPolicyAndScopeChange(t *testing.T) {
	ctx := context.Background()
	for _, unsupported := range []bool{false, true} {
		t.Run(map[bool]string{false: "scope change", true: "unsupported"}[unsupported], func(t *testing.T) {
			base := newConsolidationTestStore(t)
			source := semanticSourceEpisode("source", "deploy", "done", schema.OutcomeStatusSuccess)
			if err := base.Create(ctx, source); err != nil {
				t.Fatal(err)
			}
			store := &policyConsolidationStore{MemoryStore: base, unsupported: unsupported}
			if !unsupported {
				store.beforeBegin = func() {
					current, _ := base.Get(ctx, source.ID)
					current.Scope = "other"
					if err := base.Update(ctx, current); err != nil {
						t.Fatal(err)
					}
				}
			}
			_, _, err := NewSemanticConsolidator(store).Consolidate(ctx)
			if err == nil {
				t.Fatal("policy failure allowed consolidation")
			}
			if unsupported && !errors.Is(err, storage.ErrAuthorizationMetadataUnsupported) {
				t.Fatalf("error = %v", err)
			}
			rows, _ := base.ListByType(ctx, schema.MemoryTypeSemantic)
			if len(rows) != 0 {
				t.Fatal("policy failure persisted derived data")
			}
		})
	}
}

func TestSemanticReinforcementPromotesNewHighSource(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)
	source := semanticSourceEpisode("low-source", "deploy", "done", schema.OutcomeStatusSuccess)
	source.Sensitivity = schema.SensitivityLow
	if err := store.Create(ctx, source); err != nil {
		t.Fatal(err)
	}
	if _, _, err := NewSemanticConsolidator(store).Consolidate(ctx); err != nil {
		t.Fatal(err)
	}
	high := *source
	high.ID = "high-source"
	high.Sensitivity = schema.SensitivityHigh
	if err := store.Create(ctx, &high); err != nil {
		t.Fatal(err)
	}
	created, reinforced, err := NewSemanticConsolidator(store).Consolidate(ctx)
	if err != nil || created != 0 || reinforced != 1 {
		t.Fatalf("consolidate = %d/%d, %v", created, reinforced, err)
	}
	rows, _ := store.ListByType(ctx, schema.MemoryTypeSemantic)
	if len(rows) != 1 || rows[0].Sensitivity != schema.SensitivityHigh || len(rows[0].Payload.(*schema.SemanticPayload).Evidence) != 2 {
		t.Fatalf("reinforced facts = %+v", rows)
	}
}

func TestSemanticDedupDoesNotUseHiddenEntityIDs(t *testing.T) {
	ctx := context.Background()
	for _, llm := range []bool{false, true} {
		t.Run(map[bool]string{false: "deterministic", true: "llm"}[llm], func(t *testing.T) {
			store := newConsolidationTestStore(t)
			entity := newEntityRecord("hidden-entity", "deploy")
			entity.Sensitivity = schema.SensitivityHyper
			source := semanticSourceEpisode("source", "deploy", "done", schema.OutcomeStatusSuccess)
			source.Sensitivity = schema.SensitivityLow
			existing := newExtractedSemanticRecord(source, Triple{Subject: entity.ID, Predicate: "observed_in", Object: "done"}, source.CreatedAt)
			existing.ID = "existing"
			existing.Sensitivity = schema.SensitivityHyper
			for _, rec := range []*schema.MemoryRecord{source, entity, existing} {
				if err := store.Create(ctx, rec); err != nil {
					t.Fatal(err)
				}
			}
			if llm {
				queue := newFakeExtractionStore()
				queue.existingSemantic[entity.ID+"\x00observed_in\x00done"] = existing
				extractor := NewSemanticExtractor(store, queue, &fakeSourceReinforcer{}, &fakeLLMClient{})
				inserted, err := extractor.upsertTriple(ctx, source, Triple{Subject: "deploy", Predicate: "observed_in", Object: "done"})
				if err != nil || !inserted {
					t.Fatalf("upsert = %v, %v", inserted, err)
				}
			} else {
				created, reinforced, err := NewSemanticConsolidator(store).Consolidate(ctx)
				if err != nil || created != 1 || reinforced != 0 {
					t.Fatalf("consolidate = %d/%d, %v", created, reinforced, err)
				}
			}
			rows, _ := store.ListByType(ctx, schema.MemoryTypeSemantic)
			if len(rows) != 2 {
				t.Fatalf("facts = %d, want separate visible fact", len(rows))
			}
			for _, rec := range rows {
				if rec.ID != existing.ID && (rec.Payload.(*schema.SemanticPayload).Subject != "deploy" || rec.Sensitivity != schema.SensitivityLow) {
					t.Fatalf("hidden entity affected visible fact: %+v", rec)
				}
			}
		})
	}
}

func TestLegacyExtractorPromotesPolicyWithoutCallingUncheckedReinforcer(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)
	source := newTestEpisodic("source", "deploy", "done")
	source.Sensitivity = schema.SensitivityHyper
	existing := newExtractedSemanticRecord(source, Triple{Subject: "deploy", Predicate: "observed_in", Object: "done"}, source.CreatedAt)
	existing.Sensitivity = schema.SensitivityLow
	existing.Payload.(*schema.SemanticPayload).Evidence = nil
	existing.Provenance.Sources = nil
	for _, rec := range []*schema.MemoryRecord{source, existing} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}
	queue := newFakeExtractionStore()
	queue.existingSemantic["deploy\x00observed_in\x00done"] = existing
	legacy := &fakeReinforcer{err: errors.New("legacy reinforcer must not be called")}
	extractor := NewSemanticExtractor(store, queue, legacy, &fakeLLMClient{})
	inserted, err := extractor.upsertTriple(ctx, source, Triple{Subject: "deploy", Predicate: "observed_in", Object: "done"})
	if err != nil || inserted {
		t.Fatalf("upsert = %v, %v", inserted, err)
	}
	after, _ := store.Get(ctx, existing.ID)
	if after.Sensitivity != schema.SensitivityHyper || !semanticFactHasSource(after, source.ID) || len(legacy.calls) != 0 {
		t.Fatalf("fallback policy = %+v; calls = %v", after, legacy.calls)
	}
}

func TestPlanKnownSourceClassificationRepair(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)
	source := episodicToolRecord("source", []schema.ToolNode{{ID: "a", Tool: "test"}, {ID: "b", Tool: "test"}, {ID: "c", Tool: "test"}})
	if err := store.Create(ctx, source); err != nil {
		t.Fatal(err)
	}
	if created, err := NewPlanGraphConsolidator(store).Consolidate(ctx); err != nil || created != 1 {
		t.Fatalf("initial = %d, %v", created, err)
	}
	source.Sensitivity = schema.SensitivityHyper
	if err := store.Update(ctx, source); err != nil {
		t.Fatal(err)
	}
	if created, err := NewPlanGraphConsolidator(store).Consolidate(ctx); err != nil || created != 0 {
		t.Fatalf("repair = %d, %v", created, err)
	}
	rows, _ := store.ListByType(ctx, schema.MemoryTypePlanGraph)
	if len(rows) != 1 || rows[0].Sensitivity != schema.SensitivityHyper || rows[0].Payload.(*schema.PlanGraphPayload).Metrics.ExecutionCount != 1 {
		t.Fatalf("repaired plan = %+v", rows)
	}
}
