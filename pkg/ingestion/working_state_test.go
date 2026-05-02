package ingestion

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
)

func TestIngestWorkingStateCreatesProvenancedWorkingRecord(t *testing.T) {
	svc, store := newCaptureTestService(t, nil)
	ctx := context.Background()
	ts := time.Date(2026, 5, 1, 14, 0, 0, 0, time.UTC)

	rec, err := svc.IngestWorkingState(ctx, IngestWorkingStateRequest{
		Source:         "planner",
		ThreadID:       "thread-1",
		State:          schema.TaskStatePlanning,
		NextActions:    []string{"inspect tests"},
		OpenQuestions:  []string{"which package is weakest?"},
		ContextSummary: "coverage pass",
		Timestamp:      ts,
		Tags:           []string{"planning"},
		Scope:          "project:alpha",
		Sensitivity:    schema.SensitivityLow,
	})
	if err != nil {
		t.Fatalf("IngestWorkingState: %v", err)
	}
	if rec.Type != schema.MemoryTypeWorking {
		t.Fatalf("Type = %q, want working", rec.Type)
	}
	payload, ok := rec.Payload.(*schema.WorkingPayload)
	if !ok || payload.ThreadID != "thread-1" || payload.State != schema.TaskStatePlanning {
		t.Fatalf("Payload = %#v, want thread-1 planning working payload", rec.Payload)
	}
	if len(rec.Provenance.Sources) != 1 || rec.Provenance.Sources[0].Ref != "thread-1" || rec.Provenance.Sources[0].CreatedBy != "planner" {
		t.Fatalf("Provenance = %+v, want source ref to thread ID", rec.Provenance.Sources)
	}
	if _, err := store.Get(ctx, rec.ID); err != nil {
		t.Fatalf("Get stored working record: %v", err)
	}
}

func TestIngestWorkingStateValidationErrors(t *testing.T) {
	svc, _ := newCaptureTestService(t, nil)
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		req  IngestWorkingStateRequest
		want string
	}{
		{name: "missing thread", req: IngestWorkingStateRequest{Source: "planner", State: schema.TaskStatePlanning}, want: "thread ID"},
		{name: "missing state", req: IngestWorkingStateRequest{Source: "planner", ThreadID: "thread-1"}, want: "task state"},
		{name: "invalid sensitivity", req: IngestWorkingStateRequest{Source: "planner", ThreadID: "thread-1", State: schema.TaskStatePlanning, Sensitivity: schema.Sensitivity("secret")}, want: "invalid sensitivity"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.IngestWorkingState(ctx, tc.req)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("IngestWorkingState error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestProvenanceRefForCandidateVariants(t *testing.T) {
	for _, tc := range []struct {
		name      string
		candidate *MemoryCandidate
		want      string
	}{
		{name: "nil", candidate: nil, want: ""},
		{name: "explicit event ref", candidate: &MemoryCandidate{Kind: CandidateKindEvent, EventRef: "evt-1", Source: "source"}, want: "evt-1"},
		{name: "observation", candidate: &MemoryCandidate{Kind: CandidateKindObservation, Source: "observer"}, want: "observer"},
		{name: "working state", candidate: &MemoryCandidate{Kind: CandidateKindWorkingState, ThreadID: "thread-1"}, want: "thread-1"},
		{name: "outcome", candidate: &MemoryCandidate{Kind: CandidateKindOutcome, TargetRecordID: "rec-1"}, want: "rec-1"},
		{name: "tool fallback", candidate: &MemoryCandidate{Kind: CandidateKindToolOutput, ToolName: "rg"}, want: "rg"},
		{name: "default source", candidate: &MemoryCandidate{Kind: CandidateKind("custom"), Source: "source"}, want: "source"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := provenanceRefForCandidate(tc.candidate); got != tc.want {
				t.Fatalf("provenanceRefForCandidate = %q, want %q", got, tc.want)
			}
		})
	}
}
