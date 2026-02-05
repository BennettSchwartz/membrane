package tests_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/GustyCube/membrane/pkg/ingestion"
	"github.com/GustyCube/membrane/pkg/retrieval"
	"github.com/GustyCube/membrane/pkg/revision"
	"github.com/GustyCube/membrane/pkg/schema"
)

func TestEvalIngestionValidation(t *testing.T) {
	ctx := context.Background()
	m := newTestMembrane(t)

	cases := []struct {
		name    string
		errText string
		call    func() error
	}{
		{
			name:    "event-missing-kind",
			errText: "event kind is required",
			call: func() error {
				_, err := m.IngestEvent(ctx, ingestion.IngestEventRequest{
					Source:    "eval",
					EventKind: "",
					Ref:       "ref-1",
					Summary:   "summary",
				})
				return err
			},
		},
		{
			name:    "event-missing-ref",
			errText: "event ref is required",
			call: func() error {
				_, err := m.IngestEvent(ctx, ingestion.IngestEventRequest{
					Source:    "eval",
					EventKind: "event",
					Ref:       "",
					Summary:   "summary",
				})
				return err
			},
		},
		{
			name:    "tool-missing-name",
			errText: "tool name is required",
			call: func() error {
				_, err := m.IngestToolOutput(ctx, ingestion.IngestToolOutputRequest{
					Source:   "eval",
					ToolName: "",
					Args:     map[string]any{"cmd": "go test"},
					Result:   "ok",
				})
				return err
			},
		},
		{
			name:    "observation-missing-subject",
			errText: "subject is required",
			call: func() error {
				_, err := m.IngestObservation(ctx, ingestion.IngestObservationRequest{
					Source:    "eval",
					Subject:   "",
					Predicate: "uses",
					Object:    "go",
				})
				return err
			},
		},
		{
			name:    "observation-missing-predicate",
			errText: "predicate is required",
			call: func() error {
				_, err := m.IngestObservation(ctx, ingestion.IngestObservationRequest{
					Source:    "eval",
					Subject:   "service",
					Predicate: "",
					Object:    "go",
				})
				return err
			},
		},
		{
			name:    "working-missing-thread",
			errText: "thread ID is required",
			call: func() error {
				_, err := m.IngestWorkingState(ctx, ingestion.IngestWorkingStateRequest{
					Source:   "eval",
					ThreadID: "",
					State:    schema.TaskStateExecuting,
				})
				return err
			},
		},
		{
			name:    "working-missing-state",
			errText: "task state is required",
			call: func() error {
				_, err := m.IngestWorkingState(ctx, ingestion.IngestWorkingStateRequest{
					Source:   "eval",
					ThreadID: "thread-1",
					State:    "",
				})
				return err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			if err == nil || !strings.Contains(err.Error(), tc.errText) {
				t.Fatalf("expected error containing %q, got %v", tc.errText, err)
			}
		})
	}

	// Outcome must target an episodic record.
	semantic, err := m.IngestObservation(ctx, ingestion.IngestObservationRequest{
		Source:    "eval",
		Subject:   "service",
		Predicate: "uses",
		Object:    "go",
	})
	if err != nil {
		t.Fatalf("IngestObservation: %v", err)
	}
	_, err = m.IngestOutcome(ctx, ingestion.IngestOutcomeRequest{
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

	_, err := m.Retrieve(ctx, &retrieval.RetrieveRequest{})
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

	event, err := m.IngestEvent(ctx, ingestion.IngestEventRequest{
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
	semantic, err := m.IngestObservation(ctx, ingestion.IngestObservationRequest{
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
