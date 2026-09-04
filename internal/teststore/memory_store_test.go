package teststore

import (
	"context"
	"errors"
	"testing"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

func TestContainsScopeMatchesProductionWildcardAndUnscopedSemantics(t *testing.T) {
	if !containsScope([]string{"*"}, "project:alpha", false) {
		t.Fatal("wildcard scope did not match a scoped record")
	}
	if !containsScope([]string{""}, "", true) {
		t.Fatal("blank-only scope policy did not include an unscoped record")
	}
	if containsScope([]string{""}, "project:alpha", true) {
		t.Fatal("blank-only scope policy matched a scoped record")
	}
}

func TestMemoryStoreFindEntitiesByTermRanksLikePostgres(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	broad := newTestEntity("entity-broad", "Debug Project Orchid Rollout", "project")
	exact := newTestEntity("entity-exact", "Project Orchid", "project")
	for _, rec := range []*schema.MemoryRecord{broad, exact} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	records, err := store.FindEntitiesByTerm(ctx, "project orchid", "project", 5)
	if err != nil {
		t.Fatalf("FindEntitiesByTerm: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("FindEntitiesByTerm len = %d, want 2", len(records))
	}
	if records[0].ID != exact.ID || records[1].ID != broad.ID {
		t.Fatalf("FindEntitiesByTerm order = [%s, %s], want exact before descriptor", records[0].ID, records[1].ID)
	}
}

func TestMemoryStoreFindEntitiesByTermPrefersSpecificDescriptorPhrase(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	generic := newTestEntity("entity-project", "Project", "project")
	specific := newTestEntity("entity-project-orchid", "Orchid", "project")
	specific.Payload.(*schema.EntityPayload).Aliases = []schema.EntityAlias{{Value: "Project Orchid"}}
	for _, rec := range []*schema.MemoryRecord{generic, specific} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	records, err := store.FindEntitiesByTerm(ctx, "debug project orchid rollout failure", "project", 2)
	if err != nil {
		t.Fatalf("FindEntitiesByTerm: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("FindEntitiesByTerm len = %d, want 2", len(records))
	}
	if records[0].ID != specific.ID {
		t.Fatalf("FindEntitiesByTerm first = %s, want more specific descriptor match %s", records[0].ID, specific.ID)
	}
}

func TestMemoryStoreFindEntitiesByTermCollapsesWhitespace(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	rec := newTestEntity("entity-spaced-orchid", "Project   Orchid", "project")
	if err := store.Create(ctx, rec); err != nil {
		t.Fatalf("Create entity: %v", err)
	}

	records, err := store.FindEntitiesByTerm(ctx, "debug project orchid rollout", "project", 5)
	if err != nil {
		t.Fatalf("FindEntitiesByTerm: %v", err)
	}
	if len(records) != 1 || records[0].ID != rec.ID {
		t.Fatalf("FindEntitiesByTerm = %+v, want whitespace-normalized entity %s", records, rec.ID)
	}
}

func TestMemoryStoreFindEntitiesByTermMatchesBareIdentifierValue(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	rec := newTestEntity("entity-identifier", "Project Orchid", "project")
	rec.Payload.(*schema.EntityPayload).Identifiers = []schema.EntityIdentifier{{Namespace: "github", Value: "BennettSchwartz/orchid"}}
	if err := store.Create(ctx, rec); err != nil {
		t.Fatalf("Create entity: %v", err)
	}

	records, err := store.FindEntitiesByTerm(ctx, "debug BennettSchwartz/orchid rollout", "project", 5)
	if err != nil {
		t.Fatalf("FindEntitiesByTerm: %v", err)
	}
	if len(records) != 1 || records[0].ID != rec.ID {
		t.Fatalf("FindEntitiesByTerm = %+v, want bare identifier match %s", records, rec.ID)
	}
}

func TestMemoryStoreFindEntitiesByTermUsesBoundedMatching(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	rec := newTestEntity("entity-postgres", "Postgres", "project")
	if err := store.Create(ctx, rec); err != nil {
		t.Fatalf("Create entity: %v", err)
	}

	records, err := store.FindEntitiesByTerm(ctx, "go", "project", 5)
	if err != nil {
		t.Fatalf("FindEntitiesByTerm: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("FindEntitiesByTerm = %+v, want no substring match inside Postgres", records)
	}
}

func TestMemoryStoreFindSemanticExactMatchesStructuredObjectKey(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	rec := schema.NewMemoryRecord("semantic-structured", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "Runtime",
		Predicate: "Uses",
		Object: map[string]any{
			"lang": "go",
			"db":   "postgres",
		},
		Validity: schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	if err := store.Create(ctx, rec); err != nil {
		t.Fatalf("Create semantic: %v", err)
	}

	got, err := store.FindSemanticExact(ctx, "Runtime", "uses", `{"db":"postgres","lang":"go"}`)
	if err != nil {
		t.Fatalf("FindSemanticExact: %v", err)
	}
	if got == nil || got.ID != rec.ID {
		t.Fatalf("FindSemanticExact = %+v, want %s", got, rec.ID)
	}
	got, err = store.FindSemanticExact(ctx, "Runtime", "Uses", `{"db":"postgres","lang":"go"}`)
	if err != nil {
		t.Fatalf("FindSemanticExact normalized predicate: %v", err)
	}
	if got == nil || got.ID != rec.ID {
		t.Fatalf("FindSemanticExact normalized predicate = %+v, want %s", got, rec.ID)
	}
}

func TestMemoryStoreFindSemanticExactInScope(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	alpha := schema.NewMemoryRecord("semantic-alpha", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "entity-orchid",
		Predicate: "deploy_target_for",
		Object:    "staging",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	alpha.Scope = "project:alpha"
	beta := schema.NewMemoryRecord("semantic-beta", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "entity-orchid",
		Predicate: "deploy_target_for",
		Object:    "staging",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	beta.Scope = "project:beta"
	if err := store.Create(ctx, alpha); err != nil {
		t.Fatalf("Create alpha: %v", err)
	}
	if err := store.Create(ctx, beta); err != nil {
		t.Fatalf("Create beta: %v", err)
	}

	got, err := store.FindSemanticExactInScope(ctx, "entity-orchid", "deploy_target_for", "staging", "project:beta")
	if err != nil {
		t.Fatalf("FindSemanticExactInScope: %v", err)
	}
	if got == nil || got.ID != beta.ID {
		t.Fatalf("FindSemanticExactInScope = %+v, want %s", got, beta.ID)
	}
	miss, err := store.FindSemanticExactInScope(ctx, "entity-orchid", "deploy_target_for", "staging", "project:gamma")
	if err != nil {
		t.Fatalf("FindSemanticExactInScope miss: %v", err)
	}
	if miss != nil {
		t.Fatalf("FindSemanticExactInScope miss = %+v, want nil", miss)
	}
}

func TestMemoryStoreFindEntityByIdentifierNormalizesNamespace(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	rec := newTestEntity("entity-github", "Project Orchid", "project")
	rec.Payload.(*schema.EntityPayload).Identifiers = []schema.EntityIdentifier{{
		Namespace: "GitHub",
		Value:     "BennettSchwartz/orchid",
	}}
	if err := store.Create(ctx, rec); err != nil {
		t.Fatalf("Create entity: %v", err)
	}

	got, err := store.FindEntityByIdentifier(ctx, " github ", "BennettSchwartz/orchid", "project")
	if err != nil {
		t.Fatalf("FindEntityByIdentifier: %v", err)
	}
	if got.ID != rec.ID {
		t.Fatalf("FindEntityByIdentifier ID = %q, want %q", got.ID, rec.ID)
	}
}

func TestMemoryStoreEntityLookupAllScopesIncludesScopedRecords(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	global := newTestEntity("entity-global-orchid", "Project Orchid", "")
	scoped := newTestEntity("entity-scoped-orchid", "Project Orchid", "project:alpha")
	scoped.Payload.(*schema.EntityPayload).Identifiers = []schema.EntityIdentifier{{
		Namespace: "GitHub",
		Value:     "BennettSchwartz/orchid",
	}}
	for _, rec := range []*schema.MemoryRecord{global, scoped} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	globalOnly, err := store.FindEntitiesByTerm(ctx, "project orchid", "", 5)
	if err != nil {
		t.Fatalf("FindEntitiesByTerm global scope: %v", err)
	}
	if len(globalOnly) != 1 || globalOnly[0].ID != global.ID {
		t.Fatalf("FindEntitiesByTerm global scope = %+v, want only %s", globalOnly, global.ID)
	}

	allScopes, err := store.FindEntitiesByTermAllScopes(ctx, "project orchid", 5)
	if err != nil {
		t.Fatalf("FindEntitiesByTermAllScopes: %v", err)
	}
	if len(allScopes) != 2 || allScopes[0].ID != global.ID || allScopes[1].ID != scoped.ID {
		t.Fatalf("FindEntitiesByTermAllScopes = %+v, want global and scoped records", allScopes)
	}

	if _, err := store.FindEntityByIdentifier(ctx, "github", "BennettSchwartz/orchid", ""); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("FindEntityByIdentifier global scope error = %v, want ErrNotFound", err)
	}
	got, err := store.FindEntityByIdentifierAllScopes(ctx, " github ", " BennettSchwartz/orchid ")
	if err != nil {
		t.Fatalf("FindEntityByIdentifierAllScopes: %v", err)
	}
	if got.ID != scoped.ID {
		t.Fatalf("FindEntityByIdentifierAllScopes ID = %q, want %q", got.ID, scoped.ID)
	}
}

func TestMemoryStoreAddRelationRejectsMissingTargetLikePostgres(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	source := newTestEntity("entity-source", "Source", "project")
	if err := store.Create(ctx, source); err != nil {
		t.Fatalf("Create source: %v", err)
	}

	err := store.AddRelation(ctx, source.ID, schema.Relation{Predicate: "mentions", TargetID: "missing-target"})
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("AddRelation missing target error = %v, want ErrNotFound", err)
	}
}

func TestMemoryStoreAddRelationNormalizesPredicate(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	source := newTestEntity("entity-source", "Source", "project")
	target := newTestEntity("entity-target", "Target", "project")
	for _, rec := range []*schema.MemoryRecord{source, target} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	if err := store.AddRelation(ctx, source.ID, schema.Relation{Predicate: "Depends-On", TargetID: target.ID}); err != nil {
		t.Fatalf("AddRelation: %v", err)
	}
	rels, err := store.GetRelations(ctx, source.ID)
	if err != nil {
		t.Fatalf("GetRelations: %v", err)
	}
	if len(rels) != 1 || rels[0].Predicate != schema.GraphPredicateDependsOn {
		t.Fatalf("relations = %+v, want normalized depends_on predicate", rels)
	}
}

func TestMemoryStoreAddRelationValidatesEdgeShape(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	source := newTestEntity("entity-source", "Source", "project")
	target := newTestEntity("entity-target", "Target", "project")
	for _, rec := range []*schema.MemoryRecord{source, target} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	tests := []struct {
		name     string
		sourceID string
		rel      schema.Relation
		want     string
	}{
		{name: "empty source", sourceID: " ", rel: schema.Relation{Predicate: "supports", TargetID: target.ID, Weight: 0.5}, want: "source_id"},
		{name: "empty predicate", sourceID: source.ID, rel: schema.Relation{TargetID: target.ID, Weight: 0.5}, want: "relation.predicate"},
		{name: "invalid weight", sourceID: source.ID, rel: schema.Relation{Predicate: "supports", TargetID: target.ID, Weight: 2}, want: "relation.weight"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := store.AddRelation(ctx, tc.sourceID, tc.rel)
			verr, ok := err.(*schema.ValidationError)
			if !ok {
				t.Fatalf("AddRelation error = %T/%v, want ValidationError", err, err)
			}
			if verr.Field != tc.want {
				t.Fatalf("AddRelation field = %q, want %q", verr.Field, tc.want)
			}
		})
	}

	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback()
	err = tx.AddRelation(ctx, source.ID, schema.Relation{Predicate: "supports", TargetID: target.ID, Weight: -1})
	verr, ok := err.(*schema.ValidationError)
	if !ok || verr.Field != "relation.weight" {
		t.Fatalf("tx.AddRelation error = %T/%v, want relation.weight ValidationError", err, err)
	}
}

func TestMemoryStoreGetIncomingRelations(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	target := newTestEntity("entity-target", "Target", "project")
	first := newTestEntity("entity-first-source", "First", "project")
	second := newTestEntity("entity-second-source", "Second", "project")
	for _, rec := range []*schema.MemoryRecord{target, second, first} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	if err := store.AddRelation(ctx, second.ID, schema.Relation{Predicate: "Mentions-Entity", TargetID: target.ID, Weight: 0.4}); err != nil {
		t.Fatalf("AddRelation second->target: %v", err)
	}
	if err := store.AddRelation(ctx, first.ID, schema.Relation{Predicate: "Subject Entity", TargetID: target.ID, Weight: 0.9}); err != nil {
		t.Fatalf("AddRelation first->target: %v", err)
	}

	edges, err := store.GetIncomingRelations(ctx, target.ID)
	if err != nil {
		t.Fatalf("GetIncomingRelations: %v", err)
	}
	if len(edges) != 2 {
		t.Fatalf("incoming edges = %+v, want two", edges)
	}
	if edges[0].SourceID != first.ID || edges[0].Predicate != schema.GraphPredicateSubjectEntity || edges[0].TargetID != target.ID {
		t.Fatalf("first incoming edge = %+v, want normalized first->target", edges[0])
	}
	if edges[1].SourceID != second.ID || edges[1].Predicate != schema.GraphPredicateMentionsEntity || edges[1].TargetID != target.ID {
		t.Fatalf("second incoming edge = %+v, want normalized second->target", edges[1])
	}
	if _, err := store.GetIncomingRelations(ctx, "missing"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("GetIncomingRelations missing error = %v, want ErrNotFound", err)
	}
}

func TestMemoryStoreCreateAndUpdateNormalizeRelationPredicates(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	target := newTestEntity("entity-target", "Target", "project")
	source := newTestEntity("entity-source", "Source", "project")
	source.Relations = []schema.Relation{{Predicate: "Depends-On", TargetID: target.ID}}
	if err := store.Create(ctx, target); err != nil {
		t.Fatalf("Create target: %v", err)
	}
	if err := store.Create(ctx, source); err != nil {
		t.Fatalf("Create source: %v", err)
	}

	got, err := store.Get(ctx, source.ID)
	if err != nil {
		t.Fatalf("Get source: %v", err)
	}
	if len(got.Relations) != 1 || got.Relations[0].Predicate != schema.GraphPredicateDependsOn {
		t.Fatalf("created relations = %+v, want normalized depends_on predicate", got.Relations)
	}

	got.Relations = []schema.Relation{{Predicate: "Mentions", TargetID: target.ID}}
	if err := store.Update(ctx, got); err != nil {
		t.Fatalf("Update source: %v", err)
	}
	got, err = store.Get(ctx, source.ID)
	if err != nil {
		t.Fatalf("Get updated source: %v", err)
	}
	if len(got.Relations) != 1 || got.Relations[0].Predicate != "mentions" {
		t.Fatalf("updated relations = %+v, want normalized mentions predicate", got.Relations)
	}
}

func TestMemoryStoreListBoundedFiltersExactIDBeforeProjection(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	for _, rec := range []*schema.MemoryRecord{
		newTestEntity("wanted", "Wanted", "project:alpha"),
		newTestEntity("other", "Other", "project:alpha"),
	} {
		rec.Relations = []schema.Relation{{Predicate: "related_to", TargetID: "wanted"}}
		rec.AuditLog = []schema.AuditEntry{{Action: schema.AuditActionCreate, Actor: "fixture"}}
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %q: %v", rec.ID, err)
		}
	}

	result, err := store.ListBounded(ctx, storage.ListOptions{
		ID: "wanted", Limit: 1, OmitRelations: true, OmitHistory: true,
		MaxHydratedBytes: storage.MaxBoundedHydrationBytes,
	})
	if err != nil {
		t.Fatalf("ListBounded: %v", err)
	}
	if len(result.Records) != 1 || result.Records[0].ID != "wanted" {
		t.Fatalf("records = %+v, want exact ID", result.Records)
	}
	if len(result.Records[0].Relations) != 0 || len(result.Records[0].AuditLog) != 0 {
		t.Fatalf("record = %+v, want relation/history projection", result.Records[0])
	}
}

func TestMemoryStoreAuthorizationMetadataIsCappedAndFieldOnly(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	rec := newTestEntity("policy-record", "Policy", "project:alpha")
	rec.Sensitivity = schema.SensitivityMedium
	rec.Tags = []string{"must-not-be-copied"}
	if err := store.Create(ctx, rec); err != nil {
		t.Fatalf("Create: %v", err)
	}

	metadata, err := store.GetAuthorizationMetadata(ctx, []string{"missing", rec.ID, rec.ID})
	if err != nil {
		t.Fatalf("GetAuthorizationMetadata: %v", err)
	}
	if len(metadata) != 1 || metadata[0] != (storage.RecordAuthorizationMetadata{
		ID: rec.ID, Scope: rec.Scope, Sensitivity: rec.Sensitivity,
	}) {
		t.Fatalf("metadata = %+v, want one exact field-only row", metadata)
	}

	tooMany := make([]string, storage.MaxAuthorizationMetadataIDs+1)
	if _, err := store.GetAuthorizationMetadata(ctx, tooMany); !errors.Is(err, storage.ErrAuthorizationMetadataLimit) {
		t.Fatalf("over-limit error = %v, want ErrAuthorizationMetadataLimit", err)
	}
}

func newTestEntity(id, name, scope string) *schema.MemoryRecord {
	rec := schema.NewMemoryRecord(id, schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: name,
		PrimaryType:   schema.EntityTypeProject,
		Types:         []string{schema.EntityTypeProject},
		Aliases:       []schema.EntityAlias{{Value: name}},
		Summary:       name,
	})
	rec.Scope = scope
	return rec
}
