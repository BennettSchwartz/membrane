package consolidation

import (
	"context"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
)

func semanticSourceEpisode(id, eventKind, summary string, outcome schema.OutcomeStatus) *schema.MemoryRecord {
	now := time.Date(2026, 5, 1, 15, 0, 0, 0, time.UTC)
	rec := schema.NewMemoryRecord(id, schema.MemoryTypeEpisodic, schema.SensitivityMedium, &schema.EpisodicPayload{
		Kind: "episodic",
		Timeline: []schema.TimelineEvent{{
			T:         now,
			EventKind: eventKind,
			Ref:       id,
			Summary:   summary,
		}},
		Outcome: outcome,
	})
	rec.CreatedAt = now
	rec.UpdatedAt = now
	rec.Lifecycle.LastReinforcedAt = now
	rec.Confidence = 0.7
	rec.Salience = 0.8
	rec.Scope = "project"
	rec.Tags = []string{"deploy", "Consolidated"}
	return rec
}

func TestSemanticConsolidatorCreatesRecordWithEntityLinks(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)

	entity := newEntityRecord("entity-deploy", "deploy")
	entity.Sensitivity = schema.SensitivityMedium
	if err := store.Create(ctx, entity); err != nil {
		t.Fatalf("Create entity: %v", err)
	}
	source := semanticSourceEpisode("episode-semantic-create", "deploy", "deployment completed", schema.OutcomeStatusSuccess)
	if err := store.Create(ctx, source); err != nil {
		t.Fatalf("Create source episode: %v", err)
	}

	created, reinforced, err := NewSemanticConsolidator(store).Consolidate(ctx)
	if err != nil {
		t.Fatalf("Consolidate: %v", err)
	}
	if created != 1 || reinforced != 0 {
		t.Fatalf("created/reinforced = %d/%d, want 1/0", created, reinforced)
	}

	semantics, err := store.ListByType(ctx, schema.MemoryTypeSemantic)
	if err != nil {
		t.Fatalf("ListByType semantic: %v", err)
	}
	if len(semantics) != 1 {
		t.Fatalf("semantic count = %d, want 1", len(semantics))
	}
	semantic := semantics[0]
	payload := semantic.Payload.(*schema.SemanticPayload)
	if payload.Subject != entity.ID || payload.Predicate != "observed_in" || payload.Object != "deployment completed" {
		t.Fatalf("semantic payload = %+v, want subject entity and observed summary", payload)
	}
	if semantic.Sensitivity != source.Sensitivity || semantic.Confidence != source.Confidence || semantic.Scope != source.Scope {
		t.Fatalf("semantic metadata = sensitivity:%s confidence:%v scope:%q; want source metadata", semantic.Sensitivity, semantic.Confidence, semantic.Scope)
	}
	if len(semantic.Tags) != 2 || semantic.Tags[0] != "consolidated" || semantic.Tags[1] != "deploy" {
		t.Fatalf("semantic tags = %#v, want consolidated plus source tag without duplicate", semantic.Tags)
	}
	rels, err := store.GetRelations(ctx, semantic.ID)
	if err != nil {
		t.Fatalf("GetRelations semantic: %v", err)
	}
	if !hasRelation(rels, "derived_from", source.ID) || !hasRelation(rels, "subject_entity", entity.ID) {
		t.Fatalf("semantic relations = %+v, want derived_from source and subject_entity", rels)
	}
	entityRels, err := store.GetRelations(ctx, entity.ID)
	if err != nil {
		t.Fatalf("GetRelations entity: %v", err)
	}
	if !hasRelation(entityRels, "fact_subject_of", semantic.ID) {
		t.Fatalf("entity relations = %+v, want fact_subject_of semantic", entityRels)
	}
}

func TestSemanticConsolidatorCanonicalizesThroughGlobalEntityFallback(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)

	globalEntity := newEntityRecord("entity-global-deploy", "deploy")
	globalEntity.Scope = ""
	if err := store.Create(ctx, globalEntity); err != nil {
		t.Fatalf("Create global entity: %v", err)
	}
	source := semanticSourceEpisode("episode-semantic-global-entity", "deploy", "deployment completed", schema.OutcomeStatusSuccess)
	source.Scope = "project:alpha"
	if err := store.Create(ctx, source); err != nil {
		t.Fatalf("Create source episode: %v", err)
	}

	created, reinforced, err := NewSemanticConsolidator(store).Consolidate(ctx)
	if err != nil {
		t.Fatalf("Consolidate: %v", err)
	}
	if created != 1 || reinforced != 0 {
		t.Fatalf("created/reinforced = %d/%d, want 1/0", created, reinforced)
	}

	semantics, err := store.ListByType(ctx, schema.MemoryTypeSemantic)
	if err != nil {
		t.Fatalf("ListByType semantic: %v", err)
	}
	if len(semantics) != 1 {
		t.Fatalf("semantic count = %d, want 1", len(semantics))
	}
	payload := semantics[0].Payload.(*schema.SemanticPayload)
	if payload.Subject != globalEntity.ID {
		t.Fatalf("semantic subject = %q, want global entity %s", payload.Subject, globalEntity.ID)
	}
	rels, err := store.GetRelations(ctx, semantics[0].ID)
	if err != nil {
		t.Fatalf("GetRelations semantic: %v", err)
	}
	if !hasRelation(rels, schema.GraphPredicateSubjectEntity, globalEntity.ID) {
		t.Fatalf("semantic relations = %+v, want global subject_entity relation", rels)
	}
	entityRels, err := store.GetRelations(ctx, globalEntity.ID)
	if err != nil {
		t.Fatalf("GetRelations global entity: %v", err)
	}
	if hasRelation(entityRels, schema.GraphPredicateFactSubjectOf, semantics[0].ID) {
		t.Fatalf("global entity relations = %+v, must not expose scoped semantic ID", entityRels)
	}
}

func TestSemanticConsolidatorReinforcesExistingAndDedupesWithinRun(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)

	existing := schema.NewMemoryRecord("semantic-existing", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "deploy",
		Predicate: "observed_in",
		Object:    "deployment completed",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	existing.Scope = "project"
	existing.Salience = 0.95
	if err := store.Create(ctx, existing); err != nil {
		t.Fatalf("Create existing semantic: %v", err)
	}
	for _, source := range []*schema.MemoryRecord{
		semanticSourceEpisode("episode-semantic-existing", "deploy", "deployment completed", schema.OutcomeStatusSuccess),
		semanticSourceEpisode("episode-semantic-new-a", "build", "build completed", schema.OutcomeStatusSuccess),
		semanticSourceEpisode("episode-semantic-new-b", "build", "build completed", schema.OutcomeStatusSuccess),
	} {
		if err := store.Create(ctx, source); err != nil {
			t.Fatalf("Create %s: %v", source.ID, err)
		}
	}

	created, reinforced, err := NewSemanticConsolidator(store).Consolidate(ctx)
	if err != nil {
		t.Fatalf("Consolidate: %v", err)
	}
	if created != 1 || reinforced != 2 {
		t.Fatalf("created/reinforced = %d/%d, want 1/2", created, reinforced)
	}

	got, err := store.Get(ctx, existing.ID)
	if err != nil {
		t.Fatalf("Get existing semantic: %v", err)
	}
	if got.Salience != 1 {
		t.Fatalf("existing salience = %v, want capped at 1", got.Salience)
	}
	if !auditLogContains(got.AuditLog, schema.AuditActionReinforce, "consolidation/semantic") {
		t.Fatalf("existing audit log = %+v, want semantic reinforcement entry", got.AuditLog)
	}
	payload := got.Payload.(*schema.SemanticPayload)
	if len(payload.Evidence) != 1 || payload.Evidence[0].SourceID != "episode-semantic-existing" {
		t.Fatalf("existing evidence = %+v, want source episode recorded during reinforcement", payload.Evidence)
	}
	rels, err := store.GetRelations(ctx, existing.ID)
	if err != nil {
		t.Fatalf("GetRelations existing semantic: %v", err)
	}
	if !hasRelation(rels, schema.GraphPredicateDerivedFrom, "episode-semantic-existing") {
		t.Fatalf("existing semantic relations = %+v, want derived_from reinforced source", rels)
	}

	created, reinforced, err = NewSemanticConsolidator(store).Consolidate(ctx)
	if err != nil {
		t.Fatalf("second Consolidate: %v", err)
	}
	if created != 0 || reinforced != 0 {
		t.Fatalf("second created/reinforced = %d/%d, want 0/0 after sources are recorded", created, reinforced)
	}
}

func TestSemanticConsolidatorReinforcesEntityCanonicalizedFactAcrossRuns(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)

	entity := newEntityRecord("entity-deploy-canonical", "deploy")
	if err := store.Create(ctx, entity); err != nil {
		t.Fatalf("Create entity: %v", err)
	}
	first := semanticSourceEpisode("episode-semantic-canonical-first", "deploy", "deployment completed", schema.OutcomeStatusSuccess)
	if err := store.Create(ctx, first); err != nil {
		t.Fatalf("Create first source episode: %v", err)
	}
	consolidator := NewSemanticConsolidator(store)
	created, reinforced, err := consolidator.Consolidate(ctx)
	if err != nil {
		t.Fatalf("first Consolidate: %v", err)
	}
	if created != 1 || reinforced != 0 {
		t.Fatalf("first created/reinforced = %d/%d, want 1/0", created, reinforced)
	}

	second := semanticSourceEpisode("episode-semantic-canonical-second", "deploy", "deployment completed", schema.OutcomeStatusSuccess)
	if err := store.Create(ctx, second); err != nil {
		t.Fatalf("Create second source episode: %v", err)
	}
	created, reinforced, err = consolidator.Consolidate(ctx)
	if err != nil {
		t.Fatalf("second Consolidate: %v", err)
	}
	if created != 0 || reinforced != 1 {
		t.Fatalf("second created/reinforced = %d/%d, want 0/1 for only the new source episode", created, reinforced)
	}
	created, reinforced, err = consolidator.Consolidate(ctx)
	if err != nil {
		t.Fatalf("third Consolidate: %v", err)
	}
	if created != 0 || reinforced != 0 {
		t.Fatalf("third created/reinforced = %d/%d, want 0/0 after both source episodes are recorded", created, reinforced)
	}

	semantics, err := store.ListByType(ctx, schema.MemoryTypeSemantic)
	if err != nil {
		t.Fatalf("ListByType semantic: %v", err)
	}
	if len(semantics) != 1 {
		t.Fatalf("semantic count = %d, want one canonical fact", len(semantics))
	}
	payload := semantics[0].Payload.(*schema.SemanticPayload)
	if payload.Subject != entity.ID {
		t.Fatalf("semantic subject = %q, want canonical entity ID %q", payload.Subject, entity.ID)
	}
	if len(payload.Evidence) != 2 || payload.Evidence[0].SourceID != first.ID || payload.Evidence[1].SourceID != second.ID {
		t.Fatalf("semantic evidence = %+v, want first and second source episodes", payload.Evidence)
	}
}

func TestSemanticConsolidatorDoesNotMergeDifferentObjects(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)

	existing := schema.NewMemoryRecord("semantic-existing-object", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "deploy",
		Predicate: "observed_in",
		Object:    "old deployment note",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	if err := store.Create(ctx, existing); err != nil {
		t.Fatalf("Create existing semantic: %v", err)
	}
	source := semanticSourceEpisode("episode-semantic-different-object", "deploy", "deployment completed", schema.OutcomeStatusSuccess)
	if err := store.Create(ctx, source); err != nil {
		t.Fatalf("Create source episode: %v", err)
	}

	created, reinforced, err := NewSemanticConsolidator(store).Consolidate(ctx)
	if err != nil {
		t.Fatalf("Consolidate: %v", err)
	}
	if created != 1 || reinforced != 0 {
		t.Fatalf("created/reinforced = %d/%d, want 1/0 for different object", created, reinforced)
	}

	semantics, err := store.ListByType(ctx, schema.MemoryTypeSemantic)
	if err != nil {
		t.Fatalf("ListByType semantic: %v", err)
	}
	if len(semantics) != 2 {
		t.Fatalf("semantic count = %d, want existing plus new fact", len(semantics))
	}
}

func TestSemanticConsolidatorDoesNotMergeDifferentScopes(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)

	existing := schema.NewMemoryRecord("semantic-alpha", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "deploy",
		Predicate: "observed_in",
		Object:    "deployment completed",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	existing.Scope = "project:alpha"
	if err := store.Create(ctx, existing); err != nil {
		t.Fatalf("Create existing semantic: %v", err)
	}
	source := semanticSourceEpisode("episode-semantic-beta", "deploy", "deployment completed", schema.OutcomeStatusSuccess)
	source.Scope = "project:beta"
	if err := store.Create(ctx, source); err != nil {
		t.Fatalf("Create beta source episode: %v", err)
	}

	created, reinforced, err := NewSemanticConsolidator(store).Consolidate(ctx)
	if err != nil {
		t.Fatalf("Consolidate: %v", err)
	}
	if created != 1 || reinforced != 0 {
		t.Fatalf("created/reinforced = %d/%d, want 1/0 across different scopes", created, reinforced)
	}

	semantics, err := store.ListByType(ctx, schema.MemoryTypeSemantic)
	if err != nil {
		t.Fatalf("ListByType semantic: %v", err)
	}
	if len(semantics) != 2 {
		t.Fatalf("semantic count = %d, want alpha and beta facts", len(semantics))
	}
}

func TestSemanticConsolidatorDoesNotReinforceGlobalFactFromScopedEpisode(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)

	global := schema.NewMemoryRecord("semantic-global", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "deploy",
		Predicate: "observed_in",
		Object:    "deployment completed",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	global.Scope = ""
	global.Salience = 0.4
	if err := store.Create(ctx, global); err != nil {
		t.Fatalf("Create global semantic: %v", err)
	}
	source := semanticSourceEpisode("episode-semantic-scoped-global", "deploy", "deployment completed", schema.OutcomeStatusSuccess)
	source.Scope = "project:scoped"
	if err := store.Create(ctx, source); err != nil {
		t.Fatalf("Create scoped source episode: %v", err)
	}

	created, reinforced, err := NewSemanticConsolidator(store).Consolidate(ctx)
	if err != nil {
		t.Fatalf("Consolidate: %v", err)
	}
	if created != 1 || reinforced != 0 {
		t.Fatalf("created/reinforced = %d/%d, want scoped fact created and global fact untouched", created, reinforced)
	}

	gotGlobal, err := store.Get(ctx, global.ID)
	if err != nil {
		t.Fatalf("Get global semantic: %v", err)
	}
	if gotGlobal.Salience != 0.4 || semanticFactHasSource(gotGlobal, source.ID) {
		t.Fatalf("global semantic salience/source = %v/%v, want unchanged and no scoped evidence", gotGlobal.Salience, semanticFactHasSource(gotGlobal, source.ID))
	}

	semantics, err := store.ListByType(ctx, schema.MemoryTypeSemantic)
	if err != nil {
		t.Fatalf("ListByType semantic: %v", err)
	}
	if len(semantics) != 2 {
		t.Fatalf("semantic count = %d, want global plus scoped fact", len(semantics))
	}
	foundScoped := false
	for _, semantic := range semantics {
		if semantic.Scope == source.Scope && semanticFactHasSource(semantic, source.ID) {
			foundScoped = true
			break
		}
	}
	if !foundScoped {
		t.Fatalf("semantics = %+v, want scoped semantic fact derived from source", semantics)
	}
}

func TestSemanticConsolidatorSkipsIneligibleEpisodes(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)

	for _, source := range []*schema.MemoryRecord{
		semanticSourceEpisode("episode-semantic-failed", "deploy", "failed deploy", schema.OutcomeStatusFailure),
		semanticSourceEpisode("episode-semantic-empty", "deploy", "", schema.OutcomeStatusSuccess),
	} {
		if err := store.Create(ctx, source); err != nil {
			t.Fatalf("Create %s: %v", source.ID, err)
		}
	}

	created, reinforced, err := NewSemanticConsolidator(store).Consolidate(ctx)
	if err != nil {
		t.Fatalf("Consolidate: %v", err)
	}
	if created != 0 || reinforced != 0 {
		t.Fatalf("created/reinforced = %d/%d, want 0/0 for ineligible episodes", created, reinforced)
	}
}

func TestCanonicalizeSemanticRecordEntitiesUsesCurrentTimeFallback(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)
	entity := newEntityRecord("entity-zero-created", "deploy")
	if err := store.Create(ctx, entity); err != nil {
		t.Fatalf("Create entity: %v", err)
	}
	rec := schema.NewMemoryRecord("semantic-zero-created", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "deploy",
		Predicate: "observed_in",
		Object:    "deployment completed",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	rec.CreatedAt = time.Time{}
	rec.Scope = "project"

	edges := canonicalizeSemanticRecordEntities(ctx, store, rec)
	if len(edges) != 2 {
		t.Fatalf("edges = %+v, want subject entity edge pair", edges)
	}
	if edges[0].CreatedAt.IsZero() || rec.Relations[0].CreatedAt.IsZero() {
		t.Fatalf("edge timestamps = %+v relations=%+v, want current-time fallback", edges, rec.Relations)
	}
}

func TestDeriveTagsSkipsExistingConsolidatedTag(t *testing.T) {
	tags := deriveTags(&schema.MemoryRecord{Tags: []string{"alpha", "CONSOLIDATED", "beta"}})
	want := []string{"consolidated", "alpha", "beta"}
	if len(tags) != len(want) {
		t.Fatalf("deriveTags = %#v, want %#v", tags, want)
	}
	for i := range want {
		if tags[i] != want[i] {
			t.Fatalf("deriveTags[%d] = %q, want %q", i, tags[i], want[i])
		}
	}
}

func TestSemanticFactKeyIncludesObject(t *testing.T) {
	first := semanticFactKey(" deploy ", " observed_in ", "first")
	second := semanticFactKey("deploy", "observed_in", "second")
	if first == second {
		t.Fatalf("semanticFactKey = %v for both objects, want object-sensitive key", first)
	}
	if got := semanticFactKey(" deploy ", " observed_in ", " first "); got != first {
		t.Fatalf("semanticFactKey trim = %v, want %v", got, first)
	}
}
