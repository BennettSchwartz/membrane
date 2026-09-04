package postgres

import (
	"context"
	"math"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/internal/teststore"
	"github.com/BennettSchwartz/membrane/pkg/metrics"
	"github.com/BennettSchwartz/membrane/pkg/retrieval"
	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

func newTestStore(t *testing.T) *PostgresStore {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN not set")
	}
	store, err := Open(dsn, EmbeddingConfig{Dimensions: 3, Model: "test-model"})
	if err != nil {
		t.Fatalf("open test store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if _, err := store.db.Exec(`
		TRUNCATE TABLE
			episodic_extraction_log,
			trigger_embeddings,
			competence_stats,
			audit_log,
			relations,
			provenance_sources,
			tags,
			payloads,
			decay_profiles,
			memory_records
		RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate test tables: %v", err)
	}

	return store
}

func newEpisodicRecord(id string) *schema.MemoryRecord {
	now := time.Date(2025, 1, 16, 12, 0, 0, 0, time.UTC)
	return &schema.MemoryRecord{
		ID:          id,
		Type:        schema.MemoryTypeEpisodic,
		Sensitivity: schema.SensitivityLow,
		Confidence:  0.7,
		Salience:    0.6,
		Scope:       "project",
		Tags:        []string{"test", "episodic"},
		CreatedAt:   now,
		UpdatedAt:   now,
		Lifecycle: schema.Lifecycle{
			Decay: schema.DecayProfile{
				Curve:             schema.DecayCurveExponential,
				HalfLifeSeconds:   86400,
				MinSalience:       0.1,
				ReinforcementGain: 0.2,
			},
			LastReinforcedAt: now,
			DeletionPolicy:   schema.DeletionPolicyAutoPrune,
		},
		Provenance: schema.Provenance{
			Sources: []schema.ProvenanceSource{
				{
					Kind:      schema.ProvenanceKindEvent,
					Ref:       "evt-001",
					CreatedBy: "test-agent",
					Timestamp: now,
				},
			},
		},
		Payload: &schema.EpisodicPayload{
			Kind: "episodic",
			Timeline: []schema.TimelineEvent{
				{
					T:         now,
					EventKind: "user_input",
					Ref:       "evt-001",
					Summary:   "User prefers concise answers",
				},
			},
			Outcome: schema.OutcomeStatusSuccess,
		},
		AuditLog: []schema.AuditEntry{
			{
				Action:    schema.AuditActionCreate,
				Actor:     "test",
				Timestamp: now,
				Rationale: "initial creation",
			},
		},
	}
}

func newSemanticRecord(id string) *schema.MemoryRecord {
	now := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)
	return &schema.MemoryRecord{
		ID:          id,
		Type:        schema.MemoryTypeSemantic,
		Sensitivity: schema.SensitivityLow,
		Confidence:  0.9,
		Salience:    0.8,
		Scope:       "project",
		Tags:        []string{"test", "semantic"},
		CreatedAt:   now,
		UpdatedAt:   now,
		Lifecycle: schema.Lifecycle{
			Decay: schema.DecayProfile{
				Curve:             schema.DecayCurveExponential,
				HalfLifeSeconds:   86400,
				MinSalience:       0.1,
				MaxAgeSeconds:     604800,
				ReinforcementGain: 0.2,
			},
			LastReinforcedAt: now,
			DeletionPolicy:   schema.DeletionPolicyAutoPrune,
		},
		Provenance: schema.Provenance{
			Sources: []schema.ProvenanceSource{
				{
					Kind:      schema.ProvenanceKindObservation,
					Ref:       "obs-001",
					CreatedBy: "test-agent",
					Timestamp: now,
				},
			},
		},
		Payload: &schema.SemanticPayload{
			Kind:      "semantic",
			Subject:   "Go",
			Predicate: "is_language",
			Object:    "programming",
			Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
		},
		AuditLog: []schema.AuditEntry{
			{
				Action:    schema.AuditActionCreate,
				Actor:     "test",
				Timestamp: now,
				Rationale: "initial creation",
			},
		},
	}
}

func TestAggregateMetricsMatchesPolicyFilteredInMemoryReference(t *testing.T) {
	store := newTestStore(t)
	reference := teststore.NewMemoryStore()
	t.Cleanup(func() { _ = reference.Close() })
	ctx := context.Background()
	now := time.Now().UTC()

	allowed := newSemanticRecord("metrics-allowed")
	allowed.Type = schema.MemoryTypeCompetence
	allowed.Sensitivity = schema.SensitivityLow
	allowed.Scope = "project:alpha"
	allowed.Confidence = 0.8
	allowed.Salience = 0.1
	allowed.CreatedAt = now.Add(-2 * time.Hour)
	allowed.UpdatedAt = allowed.CreatedAt
	allowed.Lifecycle.Pinned = true
	allowed.Payload = &schema.CompetencePayload{
		Kind:      "competence",
		SkillName: "debug",
		Triggers:  []schema.Trigger{{Signal: "panic"}},
		Recipe:    []schema.RecipeStep{{Step: "inspect logs"}},
		Performance: &schema.PerformanceStats{
			SuccessRate: 0.75,
		},
	}
	allowed.AuditLog = []schema.AuditEntry{
		{Action: schema.AuditActionReinforce, Actor: "test", Timestamp: now.Add(-time.Hour)},
		{Action: schema.AuditActionRevise, Actor: "test", Timestamp: now.Add(-30 * time.Minute)},
	}

	unscoped := newSemanticRecord("metrics-unscoped")
	unscoped.Type = schema.MemoryTypePlanGraph
	unscoped.Sensitivity = schema.SensitivityPublic
	unscoped.Scope = ""
	unscoped.Confidence = 0.6
	unscoped.Salience = 0.7
	unscoped.CreatedAt = now.Add(-48 * time.Hour)
	unscoped.UpdatedAt = unscoped.CreatedAt
	unscoped.Payload = &schema.PlanGraphPayload{
		Kind:    "plan_graph",
		PlanID:  "metrics-plan",
		Version: "1",
		Nodes:   []schema.PlanNode{{ID: "n1", Op: "inspect"}},
		Edges:   []schema.PlanEdge{},
		Metrics: &schema.PlanMetrics{ExecutionCount: 4},
	}
	unscoped.AuditLog = []schema.AuditEntry{
		{Action: schema.AuditActionFork, Actor: "test", Timestamp: now.Add(-24 * time.Hour)},
	}

	wrongScope := newSemanticRecord("metrics-wrong-scope")
	wrongScope.Scope = "project:beta"
	wrongScope.Sensitivity = schema.SensitivityLow
	wrongScope.Confidence = 0.5
	wrongScope.Salience = 0.3
	wrongScope.CreatedAt = now.Add(-3 * time.Hour)
	wrongScope.UpdatedAt = wrongScope.CreatedAt
	wrongScope.AuditLog = []schema.AuditEntry{
		{Action: schema.AuditActionReinforce, Actor: "test", Timestamp: now.Add(-2 * time.Hour)},
	}

	tooSensitive := newEpisodicRecord("metrics-too-sensitive")
	tooSensitive.Scope = "project:alpha"
	tooSensitive.Sensitivity = schema.SensitivityHigh
	tooSensitive.Confidence = 0.4
	tooSensitive.Salience = 0.9
	tooSensitive.CreatedAt = now.Add(-4 * time.Hour)
	tooSensitive.UpdatedAt = tooSensitive.CreatedAt
	tooSensitive.AuditLog = []schema.AuditEntry{
		{Action: schema.AuditActionMerge, Actor: "test", Timestamp: now.Add(-3 * time.Hour)},
	}

	records := []*schema.MemoryRecord{allowed, unscoped, wrongScope, tooSensitive}
	for _, record := range records {
		if err := store.Create(ctx, record); err != nil {
			t.Fatalf("Postgres Create(%s): %v", record.ID, err)
		}
		if err := reference.Create(ctx, record); err != nil {
			t.Fatalf("reference Create(%s): %v", record.ID, err)
		}
	}
	for _, id := range []string{allowed.ID, wrongScope.ID, tooSensitive.ID} {
		if err := store.StoreTriggerEmbedding(ctx, id, []float32{0.1, 0.2, 0.3}, "test-model"); err != nil {
			t.Fatalf("StoreTriggerEmbedding(%s): %v", id, err)
		}
	}

	tests := []struct {
		name            string
		filter          storage.MetricsFilter
		trust           *retrieval.TrustContext
		embeddedRecords int
	}{
		{
			name: "allowed scope plus unscoped",
			filter: storage.MetricsFilter{
				MaxSensitivity:  schema.SensitivityLow,
				Scopes:          []string{"project:alpha"},
				IncludeUnscoped: true,
			},
			trust:           retrieval.NewTrustContext(schema.SensitivityLow, true, "test", []string{"project:alpha"}),
			embeddedRecords: 1,
		},
		{
			name: "wildcard scope",
			filter: storage.MetricsFilter{
				MaxSensitivity:  schema.SensitivityHigh,
				Scopes:          []string{"*"},
				IncludeUnscoped: true,
			},
			trust:           retrieval.NewTrustContext(schema.SensitivityHigh, true, "test", []string{"*"}),
			embeddedRecords: 3,
		},
		{
			name: "blank scope is unscoped only",
			filter: storage.MetricsFilter{
				MaxSensitivity:  schema.SensitivityLow,
				Scopes:          []string{""},
				IncludeUnscoped: true,
			},
			trust:           retrieval.NewTrustContext(schema.SensitivityLow, true, "test", []string{""}),
			embeddedRecords: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := store.AggregateMetrics(ctx, tt.filter)
			if err != nil {
				t.Fatalf("AggregateMetrics: %v", err)
			}
			want, err := metrics.NewCollector(reference).CollectForTrust(ctx, tt.trust)
			if err != nil {
				t.Fatalf("reference CollectForTrust: %v", err)
			}
			assertAggregateMatchesSnapshot(t, got, want, tt.embeddedRecords, "test-model")
		})
	}
}

func assertAggregateMatchesSnapshot(t *testing.T, got storage.MetricsAggregate, want *metrics.Snapshot, embedded int, model string) {
	t.Helper()
	wantTypes := make(map[string]int)
	for memoryType, count := range want.RecordsByType {
		if count > 0 {
			wantTypes[memoryType] = count
		}
	}
	if got.TotalRecords != want.TotalRecords || !reflect.DeepEqual(got.RecordsByType, wantTypes) ||
		got.ActiveRecords != want.ActiveRecords || got.PinnedRecords != want.PinnedRecords ||
		got.TotalAuditEntries != want.TotalAuditEntries ||
		!reflect.DeepEqual(got.SalienceDistribution, want.SalienceDistribution) {
		t.Fatalf("aggregate counts = %+v, want snapshot %+v", got, want)
	}
	if got.EmbeddingModel != model || got.EmbeddedRecords != embedded {
		t.Fatalf("embedding aggregate = model:%q embedded:%d, want model:%q embedded:%d", got.EmbeddingModel, got.EmbeddedRecords, model, embedded)
	}
	wantCoverage := 0.0
	if want.TotalRecords > 0 {
		wantCoverage = float64(embedded) / float64(want.TotalRecords)
	}
	floatFields := []struct {
		name string
		got  float64
		want float64
	}{
		{"avg salience", got.AvgSalience, want.AvgSalience},
		{"avg confidence", got.AvgConfidence, want.AvgConfidence},
		{"embedding coverage", got.EmbeddingCoverage, wantCoverage},
		{"memory growth", got.MemoryGrowthRate, want.MemoryGrowthRate},
		{"retrieval usefulness", got.RetrievalUsefulness, want.RetrievalUsefulness},
		{"competence success", got.CompetenceSuccessRate, want.CompetenceSuccessRate},
		{"plan reuse", got.PlanReuseFrequency, want.PlanReuseFrequency},
		{"revision rate", got.RevisionRate, want.RevisionRate},
	}
	for _, field := range floatFields {
		// PostgreSQL stores confidence and salience as REAL; tolerate the
		// expected float32 round-trip while still catching semantic drift.
		if math.Abs(field.got-field.want) > 1e-6 {
			t.Fatalf("%s = %v, want %v", field.name, field.got, field.want)
		}
	}
}

func TestCreateAndGet(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	rec := newSemanticRecord("create-001")
	if err := store.Create(ctx, rec); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := store.Get(ctx, rec.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ID != rec.ID {
		t.Fatalf("got ID %q, want %q", got.ID, rec.ID)
	}
	if got.Scope != rec.Scope {
		t.Fatalf("got scope %q, want %q", got.Scope, rec.Scope)
	}
	if len(got.Tags) != len(rec.Tags) {
		t.Fatalf("got %d tags, want %d", len(got.Tags), len(rec.Tags))
	}
}

func TestTriggerEmbeddings(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	rec := newSemanticRecord("embed-001")
	if err := store.Create(ctx, rec); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := store.StoreTriggerEmbedding(ctx, rec.ID, []float32{0.1, 0.2, 0.3}, "test-model"); err != nil {
		t.Fatalf("StoreTriggerEmbedding: %v", err)
	}
	otherModel := newSemanticRecord("embed-other-model")
	if err := store.Create(ctx, otherModel); err != nil {
		t.Fatalf("Create other model record: %v", err)
	}
	if err := store.StoreTriggerEmbedding(ctx, otherModel.ID, []float32{0.1, 0.2, 0.31}, "legacy-model"); err != nil {
		t.Fatalf("StoreTriggerEmbedding other model: %v", err)
	}

	got, err := store.GetTriggerEmbedding(ctx, rec.ID)
	if err != nil {
		t.Fatalf("GetTriggerEmbedding: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got embedding len %d, want 3", len(got))
	}
	oldModelEmbedding, err := store.GetTriggerEmbedding(ctx, otherModel.ID)
	if err != nil {
		t.Fatalf("GetTriggerEmbedding old model: %v", err)
	}
	if oldModelEmbedding != nil {
		t.Fatalf("GetTriggerEmbedding old model = %#v, want nil for configured model", oldModelEmbedding)
	}

	ids, err := store.SearchByEmbedding(ctx, []float32{0.1, 0.2, 0.3}, 5)
	if err != nil {
		t.Fatalf("SearchByEmbedding: %v", err)
	}
	if len(ids) == 0 || ids[0] != rec.ID {
		t.Fatalf("expected %q first in search results, got %v", rec.ID, ids)
	}
	for _, id := range ids {
		if id == otherModel.ID {
			t.Fatalf("SearchByEmbedding returned other-model embedding %q in %v", otherModel.ID, ids)
		}
	}

	candidateIDs, err := store.SearchByEmbeddingCandidates(ctx, []float32{0.1, 0.2, 0.3}, []string{otherModel.ID, rec.ID}, 10)
	if err != nil {
		t.Fatalf("SearchByEmbeddingCandidates: %v", err)
	}
	if len(candidateIDs) != 1 || candidateIDs[0] != rec.ID {
		t.Fatalf("SearchByEmbeddingCandidates = %v, want current-model candidate %q only", candidateIDs, rec.ID)
	}
}

func TestExtractionLog(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	ep := newEpisodicRecord("episode-001")
	if err := store.Create(ctx, ep); err != nil {
		t.Fatalf("Create episodic: %v", err)
	}

	claimed, err := store.ClaimUnextractedEpisodics(ctx, 10)
	if err != nil {
		t.Fatalf("ClaimUnextractedEpisodics: %v", err)
	}
	if len(claimed) != 1 || claimed[0] != ep.ID {
		t.Fatalf("claimed = %v, want [%s]", claimed, ep.ID)
	}

	claimedAgain, err := store.ClaimUnextractedEpisodics(ctx, 10)
	if err != nil {
		t.Fatalf("ClaimUnextractedEpisodics second call: %v", err)
	}
	if len(claimedAgain) != 0 {
		t.Fatalf("claimedAgain = %v, want []", claimedAgain)
	}

	if err := store.MarkEpisodicExtracted(ctx, ep.ID, 3); err != nil {
		t.Fatalf("MarkEpisodicExtracted: %v", err)
	}

	claimedAfterMark, err := store.ClaimUnextractedEpisodics(ctx, 10)
	if err != nil {
		t.Fatalf("ClaimUnextractedEpisodics after mark: %v", err)
	}
	if len(claimedAfterMark) != 0 {
		t.Fatalf("claimedAfterMark = %v, want []", claimedAfterMark)
	}

	stale := newEpisodicRecord("episode-stale")
	stale.CreatedAt = stale.CreatedAt.Add(time.Hour)
	stale.UpdatedAt = stale.CreatedAt
	stale.Lifecycle.LastReinforcedAt = stale.CreatedAt
	if err := store.Create(ctx, stale); err != nil {
		t.Fatalf("Create stale episodic: %v", err)
	}
	if _, err := store.db.ExecContext(ctx,
		`INSERT INTO episodic_extraction_log (record_id, extracted_at, triple_count)
		 VALUES ($1, NOW() - INTERVAL '2 hours', -1)`,
		stale.ID,
	); err != nil {
		t.Fatalf("insert stale claim: %v", err)
	}
	if err := store.CleanStaleExtractionClaims(ctx, time.Hour); err != nil {
		t.Fatalf("CleanStaleExtractionClaims: %v", err)
	}

	reclaimed, err := store.ClaimUnextractedEpisodics(ctx, 10)
	if err != nil {
		t.Fatalf("ClaimUnextractedEpisodics after cleanup: %v", err)
	}
	if len(reclaimed) != 1 || reclaimed[0] != stale.ID {
		t.Fatalf("reclaimed = %v, want [%s]", reclaimed, stale.ID)
	}

	semantic := newSemanticRecord("semantic-001")
	if err := store.Create(ctx, semantic); err != nil {
		t.Fatalf("Create semantic: %v", err)
	}

	match, err := store.FindSemanticExact(ctx, "Go", "is_language", "programming")
	if err != nil {
		t.Fatalf("FindSemanticExact exact match: %v", err)
	}
	if match == nil || match.ID != semantic.ID {
		t.Fatalf("FindSemanticExact exact match = %#v, want %q", match, semantic.ID)
	}

	miss, err := store.FindSemanticExact(ctx, "Go", "is_language", "systems")
	if err != nil {
		t.Fatalf("FindSemanticExact mismatch: %v", err)
	}
	if miss != nil {
		t.Fatalf("FindSemanticExact mismatch = %#v, want nil", miss)
	}

	structured := newSemanticRecord("semantic-structured")
	structured.Payload.(*schema.SemanticPayload).Subject = "Runtime"
	structured.Payload.(*schema.SemanticPayload).Predicate = "Uses"
	structured.Payload.(*schema.SemanticPayload).Object = map[string]any{
		"db":   "postgres",
		"lang": "go",
	}
	if err := store.Create(ctx, structured); err != nil {
		t.Fatalf("Create structured semantic: %v", err)
	}
	structuredMatch, err := store.FindSemanticExact(ctx, "Runtime", "uses", `{"db":"postgres","lang":"go"}`)
	if err != nil {
		t.Fatalf("FindSemanticExact structured object: %v", err)
	}
	if structuredMatch == nil || structuredMatch.ID != structured.ID {
		t.Fatalf("FindSemanticExact structured object = %#v, want %q", structuredMatch, structured.ID)
	}
	structuredMatch, err = store.FindSemanticExact(ctx, "Runtime", "Uses", `{"db":"postgres","lang":"go"}`)
	if err != nil {
		t.Fatalf("FindSemanticExact normalized predicate: %v", err)
	}
	if structuredMatch == nil || structuredMatch.ID != structured.ID {
		t.Fatalf("FindSemanticExact normalized predicate = %#v, want %q", structuredMatch, structured.ID)
	}
}

func TestEntityLookupIndexesTermsTypesAndIdentifiers(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	entity := schema.NewMemoryRecord("entity-lookup", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Orchid",
		PrimaryType:   schema.EntityTypeProject,
		Types:         []string{schema.EntityTypeProject, schema.EntityTypeRepository},
		Aliases:       []schema.EntityAlias{{Value: "Project Orchid", Kind: "surface"}},
		Identifiers:   []schema.EntityIdentifier{{Namespace: "github", Value: "BennettSchwartz/orchid"}},
		Summary:       "Orchid repository",
	})
	entity.Scope = "project:alpha"
	if err := store.Create(ctx, entity); err != nil {
		t.Fatalf("Create entity: %v", err)
	}

	byAlias, err := store.FindEntitiesByTerm(ctx, "project orchid", "project:alpha", 5)
	if err != nil {
		t.Fatalf("FindEntitiesByTerm alias: %v", err)
	}
	if len(byAlias) != 1 || byAlias[0].ID != entity.ID {
		t.Fatalf("FindEntitiesByTerm alias = %+v, want %s", byAlias, entity.ID)
	}
	byDescriptor, err := store.FindEntitiesByTerm(ctx, "debug project orchid rollout failure", "project:alpha", 5)
	if err != nil {
		t.Fatalf("FindEntitiesByTerm descriptor: %v", err)
	}
	if len(byDescriptor) != 1 || byDescriptor[0].ID != entity.ID {
		t.Fatalf("FindEntitiesByTerm descriptor = %+v, want %s", byDescriptor, entity.ID)
	}
	byIdentifier, err := store.FindEntityByIdentifier(ctx, "github", "BennettSchwartz/orchid", "project:alpha")
	if err != nil {
		t.Fatalf("FindEntityByIdentifier: %v", err)
	}
	if byIdentifier.ID != entity.ID {
		t.Fatalf("FindEntityByIdentifier ID = %q, want %q", byIdentifier.ID, entity.ID)
	}
	byBareIdentifier, err := store.FindEntitiesByTerm(ctx, "debug BennettSchwartz/orchid rollout", "project:alpha", 5)
	if err != nil {
		t.Fatalf("FindEntitiesByTerm bare identifier: %v", err)
	}
	if len(byBareIdentifier) != 1 || byBareIdentifier[0].ID != entity.ID {
		t.Fatalf("FindEntitiesByTerm bare identifier = %+v, want %s", byBareIdentifier, entity.ID)
	}

	payload := entity.Payload.(*schema.EntityPayload)
	payload.CanonicalName = "Lotus"
	payload.Aliases = []schema.EntityAlias{{Value: "Project Lotus"}}
	payload.Identifiers = []schema.EntityIdentifier{{Namespace: "github", Value: "BennettSchwartz/lotus"}}
	if err := store.Update(ctx, entity); err != nil {
		t.Fatalf("Update entity: %v", err)
	}

	oldAlias, err := store.FindEntitiesByTerm(ctx, "project orchid", "project:alpha", 5)
	if err != nil {
		t.Fatalf("FindEntitiesByTerm old alias: %v", err)
	}
	if len(oldAlias) != 0 {
		t.Fatalf("Old alias lookup = %+v, want none after reindex", oldAlias)
	}
	newIdentifier, err := store.FindEntityByIdentifier(ctx, "github", "BennettSchwartz/lotus", "project:alpha")
	if err != nil {
		t.Fatalf("FindEntityByIdentifier new: %v", err)
	}
	if newIdentifier.ID != entity.ID {
		t.Fatalf("New identifier ID = %q, want %q", newIdentifier.ID, entity.ID)
	}
}
