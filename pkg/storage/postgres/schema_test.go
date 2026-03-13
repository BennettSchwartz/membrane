package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/GustyCube/membrane/pkg/schema"
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
