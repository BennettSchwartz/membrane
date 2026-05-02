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

func TestSemanticConsolidatorReinforcesExistingAndDedupesWithinRun(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)

	existing := schema.NewMemoryRecord("semantic-existing", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "deploy",
		Predicate: "observed_in",
		Object:    "old deployment note",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	existing.Salience = 0.95
	if err := store.Create(ctx, existing); err != nil {
		t.Fatalf("Create existing semantic: %v", err)
	}
	for _, source := range []*schema.MemoryRecord{
		semanticSourceEpisode("episode-semantic-existing", "deploy", "deployment completed", schema.OutcomeStatusSuccess),
		semanticSourceEpisode("episode-semantic-new-a", "build", "build completed", schema.OutcomeStatusSuccess),
		semanticSourceEpisode("episode-semantic-new-b", "build", "build completed again", schema.OutcomeStatusSuccess),
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
