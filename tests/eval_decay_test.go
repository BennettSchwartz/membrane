package tests_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/GustyCube/membrane/pkg/decay"
	"github.com/GustyCube/membrane/pkg/ingestion"
	"github.com/GustyCube/membrane/pkg/retrieval"
	"github.com/GustyCube/membrane/pkg/schema"
	"github.com/GustyCube/membrane/pkg/storage"
)

func TestEvalDecayAndReinforce(t *testing.T) {
	ctx := context.Background()
	m := newTestMembrane(t)
	trust := fullTrust()

	rec, err := m.IngestEvent(ctx, ingestion.IngestEventRequest{
		Source:    "eval",
		EventKind: "tool_call",
		Ref:       "decay-1",
		Summary:   "Ran tests",
		Tags:      []string{"eval"},
	})
	if err != nil {
		t.Fatalf("IngestEvent: %v", err)
	}
	original := rec.Salience

	if err := m.Penalize(ctx, rec.ID, 0.4, "eval", "simulate decay"); err != nil {
		t.Fatalf("Penalize: %v", err)
	}
	decayed, err := m.RetrieveByID(ctx, rec.ID, trust)
	if err != nil {
		t.Fatalf("RetrieveByID decayed: %v", err)
	}
	if decayed.Salience >= original {
		t.Fatalf("expected salience to decrease: original=%.2f decayed=%.2f", original, decayed.Salience)
	}

	before := decayed.Lifecycle.LastReinforcedAt
	if err := m.Reinforce(ctx, rec.ID, "eval", "simulate success"); err != nil {
		t.Fatalf("Reinforce: %v", err)
	}
	after, err := m.RetrieveByID(ctx, rec.ID, trust)
	if err != nil {
		t.Fatalf("RetrieveByID reinforced: %v", err)
	}
	if after.Salience < decayed.Salience {
		t.Fatalf("expected salience to not decrease after reinforce: before=%.2f after=%.2f", decayed.Salience, after.Salience)
	}
	if after.Lifecycle.LastReinforcedAt.Before(before) {
		t.Fatalf("expected LastReinforcedAt to advance")
	}

	resp, err := m.Retrieve(ctx, &retrieval.RetrieveRequest{
		Trust:       trust,
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeEpisodic},
		MinSalience: decayed.Salience + 0.01,
	})
	if err != nil {
		t.Fatalf("Retrieve filtered: %v", err)
	}
	if containsRecord(resp.Records, rec.ID) {
		t.Fatalf("expected record to be filtered by min_salience")
	}
}

func TestEvalDecayPrune(t *testing.T) {
	ctx := context.Background()
	store := newEvalStore(t)
	svc := decay.NewService(store)

	rec := schema.NewMemoryRecord(uuid.NewString(), schema.MemoryTypeSemantic, schema.SensitivityLow,
		&schema.SemanticPayload{
			Kind:      "semantic",
			Subject:   "service",
			Predicate: "status",
			Object:    "deprecated",
			Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
		},
	)
	rec.Salience = 0.0
	rec.Lifecycle.Decay.MinSalience = 0.0
	rec.Lifecycle.DeletionPolicy = schema.DeletionPolicyAutoPrune

	if err := store.Create(ctx, rec); err != nil {
		t.Fatalf("store.Create: %v", err)
	}

	pruned, err := svc.Prune(ctx)
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if pruned != 1 {
		t.Fatalf("expected 1 pruned record, got %d", pruned)
	}

	_, err = store.Get(ctx, rec.ID)
	if err == nil || !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after prune, got %v", err)
	}
}
