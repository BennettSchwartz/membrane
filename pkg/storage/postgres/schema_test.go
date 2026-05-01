package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
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

	got, err := store.GetTriggerEmbedding(ctx, rec.ID)
	if err != nil {
		t.Fatalf("GetTriggerEmbedding: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got embedding len %d, want 3", len(got))
	}

	ids, err := store.SearchByEmbedding(ctx, []float32{0.1, 0.2, 0.3}, 5)
	if err != nil {
		t.Fatalf("SearchByEmbedding: %v", err)
	}
	if len(ids) == 0 || ids[0] != rec.ID {
		t.Fatalf("expected %q first in search results, got %v", rec.ID, ids)
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
	byIdentifier, err := store.FindEntityByIdentifier(ctx, "github", "BennettSchwartz/orchid", "project:alpha")
	if err != nil {
		t.Fatalf("FindEntityByIdentifier: %v", err)
	}
	if byIdentifier.ID != entity.ID {
		t.Fatalf("FindEntityByIdentifier ID = %q, want %q", byIdentifier.ID, entity.ID)
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
