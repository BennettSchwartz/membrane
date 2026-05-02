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
