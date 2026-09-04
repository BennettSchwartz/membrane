package consolidation

import (
	"context"
	"strings"
	"testing"

	"github.com/BennettSchwartz/membrane/pkg/schema"
)

func TestDeriveMaxSensitivityUsesPublicWhenAllSourcesArePublic(t *testing.T) {
	records := []*schema.MemoryRecord{
		{Sensitivity: schema.SensitivityPublic},
		{Sensitivity: schema.SensitivityPublic},
	}

	if got := deriveMaxSensitivity(records); got != schema.SensitivityPublic {
		t.Fatalf("deriveMaxSensitivity(public sources) = %q, want public", got)
	}
	if got := deriveMaxSensitivity([]*schema.MemoryRecord{{Sensitivity: schema.SensitivityLow}, {Sensitivity: schema.SensitivityHigh}}); got != schema.SensitivityHigh {
		t.Fatalf("deriveMaxSensitivity mixed = %q, want high", got)
	}
	if got := deriveMaxSensitivity(nil); got != schema.SensitivityLow {
		t.Fatalf("deriveMaxSensitivity empty = %q, want low fallback", got)
	}
	if got := deriveMaxSensitivity([]*schema.MemoryRecord{{Sensitivity: "invalid"}}); got != schema.SensitivityLow {
		t.Fatalf("deriveMaxSensitivity invalid = %q, want low fallback", got)
	}
}

func TestDeriveConservativeScopeVariants(t *testing.T) {
	if scope, policy := deriveConservativeScope(nil); scope != "" || policy != "preserved(unscoped)" {
		t.Fatalf("empty scope/policy = %q/%q, want unscoped preservation", scope, policy)
	}
	if scope, policy := deriveConservativeScope([]*schema.MemoryRecord{{Scope: "project:alpha"}, {Scope: "project:alpha"}}); scope != "project:alpha" || policy != "preserved(project:alpha)" {
		t.Fatalf("same scope/policy = %q/%q, want project preservation", scope, policy)
	}
	if scope, policy := deriveConservativeScope([]*schema.MemoryRecord{{Scope: ""}, {Scope: ""}}); scope != "" || policy != "preserved(unscoped)" {
		t.Fatalf("unscoped scope/policy = %q/%q, want unscoped preservation", scope, policy)
	}
	scope, policy := deriveConservativeScope([]*schema.MemoryRecord{{Scope: "project:beta"}, {Scope: ""}, {Scope: "project:alpha"}})
	if scope != mixedScopeFallback {
		t.Fatalf("mixed scope = %q, want %q", scope, mixedScopeFallback)
	}
	for _, want := range []string{"project:alpha", "project:beta", "unscoped"} {
		if !strings.Contains(policy, want) {
			t.Fatalf("mixed scope policy = %q, want mention %q", policy, want)
		}
	}
}

func TestCompetenceConsolidatorPreservesPublicSourceSensitivity(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)
	tools := []schema.ToolNode{{ID: "t1", Tool: "search"}}
	for _, rec := range []*schema.MemoryRecord{
		episodicToolRecord("public-a", tools),
		episodicToolRecord("public-b", tools),
	} {
		rec.Sensitivity = schema.SensitivityPublic
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	created, reinforced, err := NewCompetenceConsolidator(store).Consolidate(ctx)
	if err != nil {
		t.Fatalf("Consolidate: %v", err)
	}
	if created != 1 || reinforced != 0 {
		t.Fatalf("created/reinforced = %d/%d, want 1/0", created, reinforced)
	}
	competences, err := store.ListByType(ctx, schema.MemoryTypeCompetence)
	if err != nil {
		t.Fatalf("ListByType competence: %v", err)
	}
	if len(competences) != 1 || competences[0].Sensitivity != schema.SensitivityPublic {
		t.Fatalf("competences = %+v, want one public competence", competences)
	}
}

func TestCompetenceConsolidatorSkipsBlankToolPatterns(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)
	for _, rec := range []*schema.MemoryRecord{
		episodicToolRecord("blank-tool-a", []schema.ToolNode{{ID: "t1", Tool: " "}}),
		episodicToolRecord("blank-tool-b", []schema.ToolNode{{ID: "t2", Tool: ""}}),
	} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	created, reinforced, err := NewCompetenceConsolidator(store).Consolidate(ctx)
	if err != nil {
		t.Fatalf("Consolidate: %v", err)
	}
	if created != 0 || reinforced != 0 {
		t.Fatalf("created/reinforced = %d/%d, want 0/0 for blank tool patterns", created, reinforced)
	}
}

func TestCompetenceConsolidatorReinforcesExistingSkill(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)
	tools := []schema.ToolNode{{ID: "t1", Tool: " test "}}
	for _, rec := range []*schema.MemoryRecord{
		episodicToolRecord("reinforce-source-a", tools),
		episodicToolRecord("reinforce-source-b", tools),
	} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}
	existing := schema.NewMemoryRecord("existing-skill", schema.MemoryTypeCompetence, schema.SensitivityLow, &schema.CompetencePayload{
		Kind:      "competence",
		SkillName: "skill:test",
		Triggers:  []schema.Trigger{{Signal: "test"}},
		Recipe:    []schema.RecipeStep{{Step: "run tests", Tool: "test"}},
		Performance: &schema.PerformanceStats{
			SuccessCount: 4,
			SuccessRate:  1.0,
		},
	})
	existing.Salience = 0.95
	if err := store.Create(ctx, existing); err != nil {
		t.Fatalf("Create existing competence: %v", err)
	}

	created, reinforced, err := NewCompetenceConsolidator(store).Consolidate(ctx)
	if err != nil {
		t.Fatalf("Consolidate: %v", err)
	}
	if created != 0 || reinforced != 1 {
		t.Fatalf("created/reinforced = %d/%d, want 0/1", created, reinforced)
	}
	got, err := store.Get(ctx, existing.ID)
	if err != nil {
		t.Fatalf("Get existing competence: %v", err)
	}
	if got.Salience != 1 {
		t.Fatalf("reinforced salience = %v, want cap at 1", got.Salience)
	}
	if !auditLogContains(got.AuditLog, schema.AuditActionReinforce, "consolidation/competence") {
		t.Fatalf("AuditLog = %+v, want competence reinforce audit entry", got.AuditLog)
	}
	payload := got.Payload.(*schema.CompetencePayload)
	if payload.Performance == nil || payload.Performance.SuccessCount != 6 || payload.Performance.LastUsedAt == nil {
		t.Fatalf("Performance = %+v, want two new source successes recorded", payload.Performance)
	}
	if len(got.Provenance.Sources) != 2 || got.Provenance.Sources[0].Ref != "reinforce-source-a" || got.Provenance.Sources[1].Ref != "reinforce-source-b" {
		t.Fatalf("Provenance sources = %+v, want both source episodes", got.Provenance.Sources)
	}
	rels, err := store.GetRelations(ctx, existing.ID)
	if err != nil {
		t.Fatalf("GetRelations existing competence: %v", err)
	}
	if !hasRelation(rels, schema.GraphPredicateDerivedFrom, "reinforce-source-a") || !hasRelation(rels, schema.GraphPredicateDerivedFrom, "reinforce-source-b") {
		t.Fatalf("relations = %+v, want derived_from links to both source episodes", rels)
	}

	created, reinforced, err = NewCompetenceConsolidator(store).Consolidate(ctx)
	if err != nil {
		t.Fatalf("second Consolidate: %v", err)
	}
	if created != 0 || reinforced != 0 {
		t.Fatalf("second created/reinforced = %d/%d, want 0/0 after sources are recorded", created, reinforced)
	}
}

func TestCompetenceConsolidatorDoesNotReinforceDifferentScopeSkill(t *testing.T) {
	ctx := context.Background()
	store := newConsolidationTestStore(t)
	tools := []schema.ToolNode{{ID: "t1", Tool: "test"}}
	for _, rec := range []*schema.MemoryRecord{
		episodicToolRecord("beta-source-a", tools),
		episodicToolRecord("beta-source-b", tools),
	} {
		rec.Scope = "project:beta"
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}
	existing := schema.NewMemoryRecord("existing-alpha-skill", schema.MemoryTypeCompetence, schema.SensitivityLow, &schema.CompetencePayload{
		Kind:      "competence",
		SkillName: "skill:test",
		Triggers:  []schema.Trigger{{Signal: "test"}},
		Recipe:    []schema.RecipeStep{{Step: "run tests", Tool: "test"}},
	})
	existing.Scope = "project:alpha"
	existing.Salience = 0.25
	if err := store.Create(ctx, existing); err != nil {
		t.Fatalf("Create existing competence: %v", err)
	}

	created, reinforced, err := NewCompetenceConsolidator(store).Consolidate(ctx)
	if err != nil {
		t.Fatalf("Consolidate: %v", err)
	}
	if created != 1 || reinforced != 0 {
		t.Fatalf("created/reinforced = %d/%d, want beta skill created and alpha skill untouched", created, reinforced)
	}
	gotExisting, err := store.Get(ctx, existing.ID)
	if err != nil {
		t.Fatalf("Get existing competence: %v", err)
	}
	if gotExisting.Salience != 0.25 {
		t.Fatalf("existing salience = %v, want unchanged 0.25", gotExisting.Salience)
	}
	competences, err := store.ListByType(ctx, schema.MemoryTypeCompetence)
	if err != nil {
		t.Fatalf("ListByType competence: %v", err)
	}
	var betaFound bool
	for _, rec := range competences {
		if rec.ID != existing.ID && rec.Scope == "project:beta" {
			betaFound = true
		}
	}
	if !betaFound {
		t.Fatalf("competences = %+v, want new project:beta competence", competences)
	}
}

func TestExtractToolNamesNormalizesAndSkipsBlankTools(t *testing.T) {
	got := extractToolNames([]schema.ToolNode{
		{Tool: " rg "},
		{Tool: ""},
		{Tool: "  "},
		{Tool: "go test"},
	})
	want := []string{"rg", "go test"}
	if len(got) != len(want) {
		t.Fatalf("extractToolNames = %#v, want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("extractToolNames[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestInferIntentSkipsBlankTimelineEvents(t *testing.T) {
	if got := inferIntent(&schema.EpisodicPayload{Timeline: []schema.TimelineEvent{
		{EventKind: ""},
		{EventKind: "deploy"},
	}}); got != "deploy" {
		t.Fatalf("inferIntent = %q, want deploy", got)
	}
	if got := inferIntent(&schema.EpisodicPayload{}); got != "unknown" {
		t.Fatalf("inferIntent empty = %q, want unknown", got)
	}
}
