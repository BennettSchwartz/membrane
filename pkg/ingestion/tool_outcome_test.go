package ingestion

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
)

func TestIngestToolOutputCreatesEpisodicToolGraph(t *testing.T) {
	svc, store := newCaptureTestService(t, nil)
	ctx := context.Background()
	ts := time.Date(2026, 4, 30, 10, 15, 0, 0, time.UTC)
	args := map[string]any{"pattern": "panic", "path": "pkg"}
	result := map[string]any{"matches": []any{"pkg/a.go", "pkg/b.go"}}

	rec, err := svc.IngestToolOutput(ctx, IngestToolOutputRequest{
		Source:      "tester",
		ToolName:    "rg",
		Args:        args,
		Result:      result,
		DependsOn:   []string{"tool-parent"},
		Timestamp:   ts,
		Tags:        []string{"search"},
		Scope:       "project:alpha",
		Sensitivity: schema.SensitivityMedium,
	})
	if err != nil {
		t.Fatalf("IngestToolOutput: %v", err)
	}
	if rec.Type != schema.MemoryTypeEpisodic {
		t.Fatalf("Type = %q, want episodic", rec.Type)
	}
	if rec.Sensitivity != schema.SensitivityMedium {
		t.Fatalf("Sensitivity = %q, want medium", rec.Sensitivity)
	}
	if rec.Scope != "project:alpha" || !reflect.DeepEqual(rec.Tags, []string{"search"}) {
		t.Fatalf("Scope/Tags = %q/%v, want project:alpha/[search]", rec.Scope, rec.Tags)
	}
	if rec.Confidence != 0.9 {
		t.Fatalf("Confidence = %v, want 0.9", rec.Confidence)
	}

	payload, ok := rec.Payload.(*schema.EpisodicPayload)
	if !ok {
		t.Fatalf("Payload = %T, want *schema.EpisodicPayload", rec.Payload)
	}
	if len(payload.Timeline) != 1 || len(payload.ToolGraph) != 1 {
		t.Fatalf("Timeline/ToolGraph len = %d/%d, want 1/1", len(payload.Timeline), len(payload.ToolGraph))
	}
	node := payload.ToolGraph[0]
	if node.ID == "" {
		t.Fatalf("Tool node ID is empty")
	}
	if payload.Timeline[0].EventKind != "tool_call" || payload.Timeline[0].Ref != node.ID || payload.Timeline[0].T != ts {
		t.Fatalf("Timeline event = %+v, want tool_call ref to node at timestamp", payload.Timeline[0])
	}
	if node.Tool != "rg" || !reflect.DeepEqual(node.Args, args) || !reflect.DeepEqual(node.Result, result) {
		t.Fatalf("Tool node = %+v, want tool args/result", node)
	}
	if !reflect.DeepEqual(node.DependsOn, []string{"tool-parent"}) || node.Timestamp != ts {
		t.Fatalf("Tool node dependency/timestamp = %v/%v, want parent/%v", node.DependsOn, node.Timestamp, ts)
	}
	if len(rec.Provenance.Sources) != 1 || rec.Provenance.Sources[0].Kind != schema.ProvenanceKindToolCall || rec.Provenance.Sources[0].Ref != node.ID || rec.Provenance.Sources[0].CreatedBy != "tester" {
		t.Fatalf("Provenance = %+v, want tool_call source ref to tool node", rec.Provenance.Sources)
	}

	stored, err := store.Get(ctx, rec.ID)
	if err != nil {
		t.Fatalf("Get stored tool output: %v", err)
	}
	storedPayload, ok := stored.Payload.(*schema.EpisodicPayload)
	if !ok || len(storedPayload.ToolGraph) != 1 || storedPayload.ToolGraph[0].ID != node.ID {
		t.Fatalf("Stored payload = %#v, want tool graph node %q", stored.Payload, node.ID)
	}
}

func TestIngestToolOutputValidationErrors(t *testing.T) {
	svc, _ := newCaptureTestService(t, nil)
	ctx := context.Background()

	tests := []struct {
		name string
		req  IngestToolOutputRequest
		want string
	}{
		{
			name: "missing source",
			req:  IngestToolOutputRequest{ToolName: "rg"},
			want: "candidate source is required",
		},
		{
			name: "missing tool name",
			req:  IngestToolOutputRequest{Source: "tester"},
			want: "tool name is required",
		},
		{
			name: "invalid sensitivity",
			req:  IngestToolOutputRequest{Source: "tester", ToolName: "rg", Sensitivity: schema.Sensitivity("secret")},
			want: "invalid sensitivity",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.IngestToolOutput(ctx, tc.req)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("IngestToolOutput error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestIngestOutcomeUpdatesEpisodicRecord(t *testing.T) {
	svc, store := newCaptureTestService(t, nil)
	ctx := context.Background()
	eventTS := time.Date(2026, 4, 30, 11, 0, 0, 0, time.UTC)
	outcomeTS := eventTS.Add(10 * time.Minute)

	event, err := svc.IngestEvent(ctx, IngestEventRequest{
		Source:    "tester",
		EventKind: "deploy",
		Ref:       "deploy-1",
		Summary:   "Deploy finished",
		Timestamp: eventTS,
	})
	if err != nil {
		t.Fatalf("IngestEvent: %v", err)
	}

	updated, err := svc.IngestOutcome(ctx, IngestOutcomeRequest{
		Source:         "ci",
		TargetRecordID: event.ID,
		OutcomeStatus:  schema.OutcomeStatusSuccess,
		Timestamp:      outcomeTS,
	})
	if err != nil {
		t.Fatalf("IngestOutcome: %v", err)
	}

	payload, ok := updated.Payload.(*schema.EpisodicPayload)
	if !ok {
		t.Fatalf("Payload = %T, want *schema.EpisodicPayload", updated.Payload)
	}
	if payload.Outcome != schema.OutcomeStatusSuccess {
		t.Fatalf("Outcome = %q, want success", payload.Outcome)
	}
	if !updated.UpdatedAt.Equal(outcomeTS) {
		t.Fatalf("UpdatedAt = %v, want %v", updated.UpdatedAt, outcomeTS)
	}
	if len(updated.Provenance.Sources) != 2 {
		t.Fatalf("Provenance sources len = %d, want 2", len(updated.Provenance.Sources))
	}
	source := updated.Provenance.Sources[1]
	if source.Kind != schema.ProvenanceKindOutcome || source.CreatedBy != "ci" || source.Ref != "outcome:"+event.ID+":success" || !source.Timestamp.Equal(outcomeTS) {
		t.Fatalf("Outcome provenance = %+v, want outcome source for %q", source, event.ID)
	}
	if len(updated.AuditLog) != 2 {
		t.Fatalf("AuditLog len = %d, want 2", len(updated.AuditLog))
	}
	audit := updated.AuditLog[1]
	if audit.Action != schema.AuditActionRevise || audit.Actor != "ci" || audit.Rationale != "Outcome recorded: success" || !audit.Timestamp.Equal(outcomeTS) {
		t.Fatalf("Outcome audit = %+v, want revise audit entry", audit)
	}

	stored, err := store.Get(ctx, event.ID)
	if err != nil {
		t.Fatalf("Get updated event: %v", err)
	}
	storedPayload := stored.Payload.(*schema.EpisodicPayload)
	if storedPayload.Outcome != schema.OutcomeStatusSuccess {
		t.Fatalf("Stored outcome = %q, want success", storedPayload.Outcome)
	}
}

func TestIngestOutcomeRejectsInvalidAndNonEpisodicTargets(t *testing.T) {
	svc, _ := newCaptureTestService(t, nil)
	ctx := context.Background()
	ts := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)

	event, err := svc.IngestEvent(ctx, IngestEventRequest{
		Source:    "tester",
		EventKind: "build",
		Ref:       "build-1",
		Summary:   "Build completed",
		Timestamp: ts,
	})
	if err != nil {
		t.Fatalf("IngestEvent: %v", err)
	}

	tests := []struct {
		name string
		req  IngestOutcomeRequest
		want string
	}{
		{
			name: "missing source",
			req:  IngestOutcomeRequest{TargetRecordID: event.ID, OutcomeStatus: schema.OutcomeStatusSuccess, Timestamp: ts},
			want: "candidate source is required",
		},
		{
			name: "invalid outcome",
			req:  IngestOutcomeRequest{Source: "tester", TargetRecordID: event.ID, OutcomeStatus: schema.OutcomeStatus("unknown"), Timestamp: ts},
			want: "invalid outcome status",
		},
		{
			name: "missing target",
			req:  IngestOutcomeRequest{Source: "tester", TargetRecordID: "missing", OutcomeStatus: schema.OutcomeStatusFailure, Timestamp: ts},
			want: "get target record for outcome",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.IngestOutcome(ctx, tc.req)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("IngestOutcome error = %v, want containing %q", err, tc.want)
			}
		})
	}

	semantic, err := svc.IngestObservation(ctx, IngestObservationRequest{
		Source:    "observer",
		Subject:   "Orchid",
		Predicate: "status",
		Object:    "ready",
		Timestamp: ts,
	})
	if err != nil {
		t.Fatalf("IngestObservation: %v", err)
	}
	_, err = svc.IngestOutcome(ctx, IngestOutcomeRequest{
		Source:         "tester",
		TargetRecordID: semantic.ID,
		OutcomeStatus:  schema.OutcomeStatusPartial,
		Timestamp:      ts,
	})
	if err == nil || !strings.Contains(err.Error(), "is not episodic") {
		t.Fatalf("IngestOutcome semantic target error = %v, want not episodic", err)
	}
}

func TestCaptureValueHelpersNormalizeListsAndBooleans(t *testing.T) {
	if got := stringList([]string{" Alpha ", "alpha", "Beta", ""}); !reflect.DeepEqual(got, []string{"Alpha", "Beta"}) {
		t.Fatalf("stringList([]string) = %#v, want Alpha/Beta", got)
	}
	if got := stringList([]any{"one", 2, " ", "ONE"}); !reflect.DeepEqual(got, []string{"one", "2"}) {
		t.Fatalf("stringList([]any) = %#v, want one/2", got)
	}
	if got := stringList("one"); got != nil {
		t.Fatalf("stringList(string) = %#v, want nil", got)
	}

	constraints := constraintList([]any{
		map[string]any{"type": "scope", "key": "project", "value": "alpha", "required": true},
		"ignored",
	})
	if len(constraints) != 1 {
		t.Fatalf("constraintList len = %d, want 1", len(constraints))
	}
	if constraints[0].Type != "scope" || constraints[0].Key != "project" || constraints[0].Value != "alpha" || !constraints[0].Required {
		t.Fatalf("constraintList[0] = %+v, want normalized constraint", constraints[0])
	}
	if got := constraintList([]schema.Constraint{{Type: "scope"}}); len(got) != 1 || got[0].Type != "scope" {
		t.Fatalf("constraintList([]schema.Constraint) = %#v, want typed constraints preserved", got)
	}
	if got := constraintList([]map[string]any{{"type": "env", "key": "region", "value": "us-east", "required": true}}); len(got) != 1 || got[0].Key != "region" || !got[0].Required {
		t.Fatalf("constraintList([]map[string]any) = %#v, want normalized typed maps", got)
	}
	if !boolValue(true) || boolValue("true") {
		t.Fatalf("boolValue did not preserve only bool true")
	}
}
