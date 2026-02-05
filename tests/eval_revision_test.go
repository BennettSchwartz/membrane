package tests_test

import (
	"context"
	"testing"

	"github.com/GustyCube/membrane/pkg/ingestion"
	"github.com/GustyCube/membrane/pkg/retrieval"
	"github.com/GustyCube/membrane/pkg/schema"
)

func TestEvalRevisionLifecycle(t *testing.T) {
	ctx := context.Background()
	m := newTestMembrane(t)
	trust := fullTrust()

	base, err := m.IngestObservation(ctx, ingestion.IngestObservationRequest{
		Source:    "eval",
		Subject:   "project",
		Predicate: "uses_database",
		Object:    "SQLite",
		Tags:      []string{"eval"},
	})
	if err != nil {
		t.Fatalf("IngestObservation base: %v", err)
	}

	newRec := schema.NewMemoryRecord("", schema.MemoryTypeSemantic, schema.SensitivityLow,
		&schema.SemanticPayload{
			Kind:      "semantic",
			Subject:   "project",
			Predicate: "uses_database",
			Object:    "PostgreSQL",
			Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
			Evidence: []schema.ProvenanceRef{
				{
					SourceType: "eval",
					SourceID:   "supersede",
				},
			},
			RevisionPolicy: "replace",
		},
	)

	superseded, err := m.Supersede(ctx, base.ID, newRec, "eval", "updated database")
	if err != nil {
		t.Fatalf("Supersede: %v", err)
	}

	old, err := m.RetrieveByID(ctx, base.ID, trust)
	if err != nil {
		t.Fatalf("RetrieveByID old: %v", err)
	}
	if old.Salience != 0 {
		t.Fatalf("expected old record salience 0, got %.2f", old.Salience)
	}
	if sp, ok := old.Payload.(*schema.SemanticPayload); ok {
		if sp.Revision == nil || sp.Revision.Status != schema.RevisionStatusRetracted {
			t.Fatalf("expected old record retracted status, got %#v", sp.Revision)
		}
	}

	updated, err := m.RetrieveByID(ctx, superseded.ID, trust)
	if err != nil {
		t.Fatalf("RetrieveByID new: %v", err)
	}
	if sp, ok := updated.Payload.(*schema.SemanticPayload); ok {
		if sp.Revision == nil || sp.Revision.Status != schema.RevisionStatusActive {
			t.Fatalf("expected new record active status, got %#v", sp.Revision)
		}
	}

	toRetract, err := m.IngestObservation(ctx, ingestion.IngestObservationRequest{
		Source:    "eval",
		Subject:   "service",
		Predicate: "uses_queue",
		Object:    "RabbitMQ",
		Tags:      []string{"eval"},
	})
	if err != nil {
		t.Fatalf("IngestObservation retract: %v", err)
	}

	if err := m.Retract(ctx, toRetract.ID, "eval", "no longer accurate"); err != nil {
		t.Fatalf("Retract: %v", err)
	}

	retracted, err := m.RetrieveByID(ctx, toRetract.ID, trust)
	if err != nil {
		t.Fatalf("RetrieveByID retracted: %v", err)
	}
	if retracted.Salience != 0 {
		t.Fatalf("expected retracted salience 0, got %.2f", retracted.Salience)
	}

	resp, err := m.Retrieve(ctx, &retrieval.RetrieveRequest{
		Trust:       trust,
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		MinSalience: 0.01,
	})
	if err != nil {
		t.Fatalf("Retrieve filtered: %v", err)
	}
	if containsRecord(resp.Records, toRetract.ID) {
		t.Fatalf("expected retracted record to be filtered by min_salience")
	}

	// Fork creates a conditional variant without retracting the original.
	source, err := m.IngestObservation(ctx, ingestion.IngestObservationRequest{
		Source:    "eval",
		Subject:   "service",
		Predicate: "uses_cache",
		Object:    "Redis",
		Tags:      []string{"eval"},
	})
	if err != nil {
		t.Fatalf("IngestObservation fork source: %v", err)
	}

	forked := schema.NewMemoryRecord("", schema.MemoryTypeSemantic, schema.SensitivityLow,
		&schema.SemanticPayload{
			Kind:      "semantic",
			Subject:   "service",
			Predicate: "uses_cache",
			Object:    "Memcached",
			Validity: schema.Validity{
				Mode:       schema.ValidityModeConditional,
				Conditions: map[string]any{"env": "dev"},
			},
			Evidence: []schema.ProvenanceRef{
				{SourceType: "eval", SourceID: "fork"},
			},
			RevisionPolicy: "fork",
		},
	)

	forkedRec, err := m.Fork(ctx, source.ID, forked, "eval", "dev environment differs")
	if err != nil {
		t.Fatalf("Fork: %v", err)
	}

	sourceRec, err := m.RetrieveByID(ctx, source.ID, trust)
	if err != nil {
		t.Fatalf("RetrieveByID source: %v", err)
	}
	if sourceRec.Salience == 0 {
		t.Fatalf("expected source record to remain active after fork")
	}

	forkedStored, err := m.RetrieveByID(ctx, forkedRec.ID, trust)
	if err != nil {
		t.Fatalf("RetrieveByID forked: %v", err)
	}
	if !hasRelation(forkedStored.Relations, "derived_from", source.ID) {
		t.Fatalf("expected forked record derived_from source")
	}

	// Contest adds a relation and audit entry linking conflicting evidence.
	if err := m.Contest(ctx, source.ID, forkedRec.ID, "eval", "conflicting cache choices"); err != nil {
		t.Fatalf("Contest: %v", err)
	}

	contested, err := m.RetrieveByID(ctx, source.ID, trust)
	if err != nil {
		t.Fatalf("RetrieveByID contested: %v", err)
	}
	if !hasRelation(contested.Relations, "contested_by", forkedRec.ID) {
		t.Fatalf("expected contested_by relation to forked record")
	}
}

func containsRecord(records []*schema.MemoryRecord, id string) bool {
	for _, rec := range records {
		if rec.ID == id {
			return true
		}
	}
	return false
}
