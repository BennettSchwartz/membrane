package tests_test

import (
	"context"
	"errors"
	"testing"

	"github.com/BennettSchwartz/membrane/pkg/ingestion"
	"github.com/BennettSchwartz/membrane/pkg/retrieval"
	"github.com/BennettSchwartz/membrane/pkg/schema"
)

func TestEvalTrustGating(t *testing.T) {
	ctx := context.Background()
	m := newTestMembrane(t)

	lowAlpha, err := captureObservationRecord(ctx, m, ingestion.IngestObservationRequest{
		Source:      "eval",
		Subject:     "project alpha",
		Predicate:   "uses",
		Object:      "PostgreSQL",
		Scope:       "project:alpha",
		Sensitivity: schema.SensitivityLow,
		Tags:        []string{"eval"},
	})
	if err != nil {
		t.Fatalf("IngestObservation low: %v", err)
	}

	highAlpha, err := captureObservationRecord(ctx, m, ingestion.IngestObservationRequest{
		Source:      "eval",
		Subject:     "project alpha",
		Predicate:   "secret",
		Object:      "rotation policy",
		Scope:       "project:alpha",
		Sensitivity: schema.SensitivityHigh,
		Tags:        []string{"eval"},
	})
	if err != nil {
		t.Fatalf("IngestObservation high: %v", err)
	}

	lowBeta, err := captureObservationRecord(ctx, m, ingestion.IngestObservationRequest{
		Source:      "eval",
		Subject:     "project beta",
		Predicate:   "uses",
		Object:      "Redis",
		Scope:       "project:beta",
		Sensitivity: schema.SensitivityLow,
		Tags:        []string{"eval"},
	})
	if err != nil {
		t.Fatalf("IngestObservation beta: %v", err)
	}

	trustLow := retrieval.NewTrustContext(schema.SensitivityLow, true, "eval", []string{"project:alpha"})
	respLow, err := retrieveRecords(ctx, m, &retrieval.RetrieveRequest{
		Trust:       trustLow,
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
	})
	if err != nil {
		t.Fatalf("Retrieve low trust: %v", err)
	}
	if !containsRecord(respLow.Records, lowAlpha.ID) {
		t.Fatalf("expected low alpha record")
	}
	if containsRecord(respLow.Records, highAlpha.ID) {
		t.Fatalf("did not expect high alpha record with low trust")
	}
	if containsRecord(respLow.Records, lowBeta.ID) {
		t.Fatalf("did not expect beta record with alpha scope")
	}

	_, err = m.RetrieveByID(ctx, highAlpha.ID, trustLow)
	if err == nil || !errors.Is(err, retrieval.ErrAccessDenied) {
		t.Fatalf("expected access denied for high sensitivity record, got %v", err)
	}

	trustHigh := retrieval.NewTrustContext(schema.SensitivityHigh, true, "eval", []string{"project:alpha"})
	respHigh, err := retrieveRecords(ctx, m, &retrieval.RetrieveRequest{
		Trust:       trustHigh,
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
	})
	if err != nil {
		t.Fatalf("Retrieve high trust: %v", err)
	}
	if !containsRecord(respHigh.Records, lowAlpha.ID) || !containsRecord(respHigh.Records, highAlpha.ID) {
		t.Fatalf("expected both alpha records with high trust")
	}
	if containsRecord(respHigh.Records, lowBeta.ID) {
		t.Fatalf("did not expect beta record with alpha scope")
	}
}
