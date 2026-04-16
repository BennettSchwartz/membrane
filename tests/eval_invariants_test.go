package tests_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/BennettSchwartz/membrane/pkg/ingestion"
	"github.com/BennettSchwartz/membrane/pkg/retrieval"
	"github.com/BennettSchwartz/membrane/pkg/revision"
	"github.com/BennettSchwartz/membrane/pkg/schema"
)

func TestEvalCaptureAndOutcomeValidation(t *testing.T) {
	ctx := context.Background()
	m := newTestMembrane(t)

	event, err := captureEventRecord(ctx, m, ingestion.IngestEventRequest{
		Source:    "eval",
		EventKind: "event",
		Ref:       "ref-1",
		Summary:   "summary",
	})
	if err != nil {
		t.Fatalf("capture event: %v", err)
	}
	if event.Type != schema.MemoryTypeEpisodic {
		t.Fatalf("expected episodic event, got %s", event.Type)
	}

	// Outcome must target an episodic record.
	semantic, err := captureObservationRecord(ctx, m, ingestion.IngestObservationRequest{
		Source:    "eval",
		Subject:   "service",
		Predicate: "uses",
		Object:    "go",
	})
	if err != nil {
		t.Fatalf("capture observation: %v", err)
	}
	_, err = recordOutcome(ctx, m, ingestion.IngestOutcomeRequest{
		Source:         "eval",
		TargetRecordID: semantic.ID,
		OutcomeStatus:  schema.OutcomeStatusSuccess,
	})
	if err == nil || !strings.Contains(err.Error(), "not episodic") {
		t.Fatalf("expected non-episodic outcome error, got %v", err)
	}
}

func TestEvalRetrievalRequiresTrust(t *testing.T) {
	ctx := context.Background()
	m := newTestMembrane(t)

	_, err := retrieveRecords(ctx, m, &retrieval.RetrieveRequest{})
	if !errors.Is(err, retrieval.ErrNilTrust) {
		t.Fatalf("expected ErrNilTrust for Retrieve, got %v", err)
	}

	_, err = m.RetrieveByID(ctx, "missing", nil)
	if !errors.Is(err, retrieval.ErrNilTrust) {
		t.Fatalf("expected ErrNilTrust for RetrieveByID, got %v", err)
	}
}

func TestEvalRevisionInvariants(t *testing.T) {
	ctx := context.Background()
	m := newTestMembrane(t)

	event, err := captureEventRecord(ctx, m, ingestion.IngestEventRequest{
		Source:    "eval",
		EventKind: "build",
		Ref:       "evt-1",
		Summary:   "ran build",
	})
	if err != nil {
		t.Fatalf("IngestEvent: %v", err)
	}

	rec := schema.NewMemoryRecord("", schema.MemoryTypeSemantic, schema.SensitivityLow,
		&schema.SemanticPayload{
			Kind:      "semantic",
			Subject:   "service",
			Predicate: "uses",
			Object:    "postgres",
			Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
			Evidence: []schema.ProvenanceRef{
				{SourceType: "eval", SourceID: "evidence"},
			},
		},
	)

	if _, err := m.Supersede(ctx, event.ID, rec, "eval", "noop"); err == nil || !errors.Is(err, revision.ErrEpisodicImmutable) {
		t.Fatalf("expected episodic immutability on supersede, got %v", err)
	}
	if _, err := m.Fork(ctx, event.ID, rec, "eval", "noop"); err == nil || !errors.Is(err, revision.ErrEpisodicImmutable) {
		t.Fatalf("expected episodic immutability on fork, got %v", err)
	}
	if _, err := m.Merge(ctx, []string{event.ID}, rec, "eval", "noop"); err == nil || !errors.Is(err, revision.ErrEpisodicImmutable) {
		t.Fatalf("expected episodic immutability on merge, got %v", err)
	}

	// Evidence required for semantic revisions.
	semantic, err := captureObservationRecord(ctx, m, ingestion.IngestObservationRequest{
		Source:    "eval",
		Subject:   "service",
		Predicate: "uses",
		Object:    "sqlite",
	})
	if err != nil {
		t.Fatalf("IngestObservation: %v", err)
	}
	noEvidence := schema.NewMemoryRecord("", schema.MemoryTypeSemantic, schema.SensitivityLow,
		&schema.SemanticPayload{
			Kind:      "semantic",
			Subject:   "service",
			Predicate: "uses",
			Object:    "postgres",
			Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
		},
	)
	if _, err := m.Supersede(ctx, semantic.ID, noEvidence, "eval", "missing evidence"); err == nil || !strings.Contains(err.Error(), "requires evidence") {
		t.Fatalf("expected evidence error, got %v", err)
	}

	if _, err := m.Merge(ctx, nil, rec, "eval", "no ids"); err == nil || !strings.Contains(err.Error(), "no source record IDs") {
		t.Fatalf("expected merge no-ids error, got %v", err)
	}
}

func TestEvalTrustAllowsRedacted(t *testing.T) {
	rec := schema.NewMemoryRecord("id", schema.MemoryTypeSemantic, schema.SensitivityMedium,
		&schema.SemanticPayload{Kind: "semantic", Subject: "x", Predicate: "y", Object: "z", Validity: schema.Validity{Mode: schema.ValidityModeGlobal}},
	)
	trust := retrieval.NewTrustContext(schema.SensitivityLow, true, "eval", nil)
	if !trust.AllowsRedacted(rec) {
		t.Fatalf("expected AllowsRedacted to be true for one-level higher sensitivity")
	}
}
