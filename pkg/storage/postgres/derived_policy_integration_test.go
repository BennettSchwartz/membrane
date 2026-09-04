package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/BennettSchwartz/membrane/pkg/consolidation"
	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

func TestBackgroundEntityPolicyWithSinglePostgresConnection(t *testing.T) {
	for _, tc := range []struct {
		name        string
		entityScope string
		entityLabel schema.Sensitivity
		wantLink    bool
		wantInverse bool
	}{
		{"hidden", "project", schema.SensitivityHigh, false, false},
		{"same_policy", "project", schema.SensitivityLow, true, true},
		{"public_global", "", schema.SensitivityPublic, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestStore(t)
			s.db.SetMaxOpenConns(1)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			source := newEpisodicRecord("entity-policy-source")
			source.Payload.(*schema.EpisodicPayload).Timeline[0].EventKind = "orchid"
			entity := schema.NewMemoryRecord("protected-orchid-id", schema.MemoryTypeEntity, tc.entityLabel, &schema.EntityPayload{Kind: "entity", CanonicalName: "orchid", PrimaryType: "project"})
			entity.Scope = tc.entityScope
			for _, rec := range []*schema.MemoryRecord{source, entity} {
				if err := s.Create(ctx, rec); err != nil {
					t.Fatal(err)
				}
			}
			created, _, err := consolidation.NewSemanticConsolidator(s).Consolidate(ctx)
			if err != nil || created != 1 {
				t.Fatalf("consolidation created %d, error %v", created, err)
			}
			facts, err := s.ListByType(ctx, schema.MemoryTypeSemantic)
			if err != nil || len(facts) != 1 {
				t.Fatalf("facts = %v, error %v", facts, err)
			}
			fact := facts[0]
			linked := fact.Payload.(*schema.SemanticPayload).Subject == entity.ID
			if linked != tc.wantLink {
				t.Fatalf("canonical entity link = %t, want %t", linked, tc.wantLink)
			}
			storedEntity, err := s.Get(ctx, entity.ID)
			if err != nil {
				t.Fatal(err)
			}
			inverse := false
			for _, relation := range storedEntity.Relations {
				inverse = inverse || relation.TargetID == fact.ID
			}
			if inverse != tc.wantInverse {
				t.Fatalf("entity inverse relation = %t, want %t", inverse, tc.wantInverse)
			}
		})
	}
}

func TestDerivedPolicyLocksAndRollsBackWithPostgres(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	source := newEpisodicRecord("policy-source")
	source.Sensitivity = schema.SensitivityHigh
	target := newSemanticRecord("policy-target")
	target.Scope, target.Sensitivity = source.Scope, schema.SensitivityLow
	for _, rec := range []*schema.MemoryRecord{source, target} {
		if err := s.Create(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}
	tx, err := s.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	current, err := storage.GetDerivedDestination(ctx, tx, target.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := storage.ApplyDerivedSourcePolicy(ctx, tx, current, []*schema.MemoryRecord{source}); err != nil {
		t.Fatal(err)
	}
	if current.Sensitivity != schema.SensitivityHigh {
		t.Fatal("source label was not inherited")
	}
	// A second database transaction must be unable to relabel either row while
	// the derivation still relies on their authorization metadata.
	for _, id := range []string{source.ID, target.ID} {
		other, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, err = other.ExecContext(ctx, "SELECT id FROM memory_records WHERE id=$1 FOR UPDATE NOWAIT", id)
		_ = other.Rollback()
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
			t.Fatalf("row %s is not policy-locked: %v", id, err)
		}
	}
	if err := tx.Update(ctx, current); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	stored, err := s.Get(ctx, target.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Sensitivity != schema.SensitivityLow {
		t.Fatal("rolled-back policy change persisted")
	}
}

func TestSemanticConsolidationPreservesSourcePolicyWithPostgres(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	source := newEpisodicRecord("protected-episode")
	source.Sensitivity = schema.SensitivityHigh
	event := source.Payload.(*schema.EpisodicPayload).Timeline[0]
	target := newSemanticRecord("existing-low-fact")
	target.Scope, target.Sensitivity = source.Scope, schema.SensitivityLow
	payload := target.Payload.(*schema.SemanticPayload)
	payload.Subject, payload.Predicate, payload.Object = event.EventKind, "observed_in", event.Summary
	for _, rec := range []*schema.MemoryRecord{source, target} {
		if err := s.Create(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}
	_, reinforced, err := consolidation.NewSemanticConsolidator(s).Consolidate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if reinforced != 1 {
		t.Fatalf("reinforced = %d, want 1", reinforced)
	}
	stored, err := s.Get(ctx, target.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Sensitivity != schema.SensitivityHigh {
		t.Fatalf("sensitivity = %s, want high", stored.Sensitivity)
	}
	if len(stored.AuditLog) != len(target.AuditLog)+1 || stored.AuditLog[len(stored.AuditLog)-1].Actor != "consolidation/semantic" {
		t.Fatal("source promotion audit was not persisted")
	}
	found := false
	for _, evidence := range stored.Payload.(*schema.SemanticPayload).Evidence {
		found = found || evidence.SourceID == source.ID
	}
	if !found {
		t.Fatal("legitimate source evidence was not retained")
	}
	visible, err := s.ListBounded(ctx, storage.ListOptions{ID: target.ID, Scopes: []string{source.Scope}, MaxSensitivity: schema.SensitivityLow, Limit: 1, MaxHydratedBytes: storage.MaxBoundedHydrationBytes})
	if err != nil {
		t.Fatal(err)
	}
	if len(visible.Records) != 0 {
		t.Fatal("protected evidence remains visible to low readers")
	}
	// A later classification increase on known evidence repairs the destination
	// without adding evidence again, and must persist its own audit entry.
	source.Sensitivity = schema.SensitivityHyper
	if err := s.Update(ctx, source); err != nil {
		t.Fatal(err)
	}
	if _, _, err := consolidation.NewSemanticConsolidator(s).Consolidate(ctx); err != nil {
		t.Fatal(err)
	}
	repaired, err := s.Get(ctx, target.ID)
	if err != nil {
		t.Fatal(err)
	}
	if repaired.Sensitivity != schema.SensitivityHyper || len(repaired.AuditLog) != len(stored.AuditLog)+1 || len(repaired.Payload.(*schema.SemanticPayload).Evidence) != len(stored.Payload.(*schema.SemanticPayload).Evidence) {
		t.Fatal("known-source repair did not preserve classification, evidence, and audit")
	}
	lower := newEpisodicRecord("lower-source")
	if err := s.Create(ctx, lower); err != nil {
		t.Fatal(err)
	}
	if _, _, err := consolidation.NewSemanticConsolidator(s).Consolidate(ctx); err != nil {
		t.Fatal(err)
	}
	unchanged, err := s.Get(ctx, target.ID)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.Salience != repaired.Salience || len(unchanged.AuditLog) != len(repaired.AuditLog) || len(unchanged.Payload.(*schema.SemanticPayload).Evidence) != len(repaired.Payload.(*schema.SemanticPayload).Evidence) {
		t.Fatal("lower-sensitivity evidence influenced the protected fact")
	}
}
