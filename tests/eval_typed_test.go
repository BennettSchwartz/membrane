package tests_test

import (
	"context"
	"testing"

	"github.com/GustyCube/membrane/pkg/ingestion"
	"github.com/GustyCube/membrane/pkg/retrieval"
	"github.com/GustyCube/membrane/pkg/schema"
)

func TestEvalTypedMemory(t *testing.T) {
	ctx := context.Background()
	m := newTestMembrane(t)

	event, err := captureEventRecord(ctx, m, ingestion.IngestEventRequest{
		Source:    "eval",
		EventKind: "user_input",
		Ref:       "evt-1",
		Summary:   "User asked to initialize a project",
		Tags:      []string{"eval"},
	})
	if err != nil {
		t.Fatalf("IngestEvent: %v", err)
	}

	tool, err := captureToolOutputRecord(ctx, m, ingestion.IngestToolOutputRequest{
		Source:   "eval",
		ToolName: "bash",
		Args:     map[string]any{"cmd": "go build"},
		Result:   "exit code 0",
		Tags:     []string{"eval"},
	})
	if err != nil {
		t.Fatalf("IngestToolOutput: %v", err)
	}

	obs, err := captureObservationRecord(ctx, m, ingestion.IngestObservationRequest{
		Source:    "eval",
		Subject:   "user",
		Predicate: "prefers_language",
		Object:    "Go",
		Tags:      []string{"eval"},
	})
	if err != nil {
		t.Fatalf("IngestObservation: %v", err)
	}

	working, err := captureWorkingStateRecord(ctx, m, ingestion.IngestWorkingStateRequest{
		Source:      "eval",
		ThreadID:    "thread-1",
		State:       schema.TaskStateExecuting,
		NextActions: []string{"run tests"},
	})
	if err != nil {
		t.Fatalf("IngestWorkingState: %v", err)
	}

	trust := fullTrust()

	episodicResp, err := retrieveRecords(ctx, m, &retrieval.RetrieveRequest{
		Trust:       trust,
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeEpisodic},
	})
	if err != nil {
		t.Fatalf("Retrieve episodic: %v", err)
	}
	requireRecord(t, episodicResp.Records, event.ID)
	requireRecord(t, episodicResp.Records, tool.ID)

	semanticResp, err := retrieveRecords(ctx, m, &retrieval.RetrieveRequest{
		Trust:       trust,
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
	})
	if err != nil {
		t.Fatalf("Retrieve semantic: %v", err)
	}
	requireRecord(t, semanticResp.Records, obs.ID)

	workingResp, err := retrieveRecords(ctx, m, &retrieval.RetrieveRequest{
		Trust:       trust,
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeWorking},
	})
	if err != nil {
		t.Fatalf("Retrieve working: %v", err)
	}
	requireRecord(t, workingResp.Records, working.ID)
}

func requireRecord(t *testing.T, records []*schema.MemoryRecord, id string) {
	t.Helper()
	for _, rec := range records {
		if rec.ID == id {
			return
		}
	}
	t.Fatalf("expected record %s in results", id)
}
