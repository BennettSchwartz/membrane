package schema

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestMemoryRecordValidateRejectsInvalidSensitivity(t *testing.T) {
	rec := NewMemoryRecord("id-1", MemoryTypeSemantic, Sensitivity("invalid"), &SemanticPayload{Kind: "semantic"})

	err := rec.Validate()
	if err == nil {
		t.Fatalf("expected validation error for invalid sensitivity")
	}

	verr, ok := err.(*ValidationError)
	if !ok {
		t.Fatalf("expected ValidationError, got %T", err)
	}
	if verr.Field != "sensitivity" {
		t.Fatalf("expected sensitivity field error, got %q", verr.Field)
	}
}

func TestMemoryRecordValidateRejectsInvalidTypeAndPayloadMismatch(t *testing.T) {
	var nilRecord *MemoryRecord
	if err := nilRecord.Validate(); err == nil {
		t.Fatalf("Validate nil record error = nil")
	}
	if err := NewMemoryRecord("id-1", MemoryType("unknown"), SensitivityLow, &SemanticPayload{Kind: "semantic"}).Validate(); err == nil {
		t.Fatalf("Validate invalid memory type error = nil")
	}
	if err := NewMemoryRecord("id-2", MemoryTypeSemantic, SensitivityLow, &WorkingPayload{Kind: "working", ThreadID: "thread", State: TaskStatePlanning}).Validate(); err == nil {
		t.Fatalf("Validate mismatched payload error = nil")
	}
}

func TestIsValidSensitivity(t *testing.T) {
	valid := []Sensitivity{
		SensitivityPublic,
		SensitivityLow,
		SensitivityMedium,
		SensitivityHigh,
		SensitivityHyper,
	}
	for _, s := range valid {
		if !IsValidSensitivity(s) {
			t.Fatalf("expected %q to be valid", s)
		}
	}
	if IsValidSensitivity(Sensitivity("invalid")) {
		t.Fatalf("expected invalid sensitivity to be rejected")
	}
}

func TestIsValidMemoryType(t *testing.T) {
	valid := []MemoryType{
		MemoryTypeEpisodic,
		MemoryTypeWorking,
		MemoryTypeSemantic,
		MemoryTypeCompetence,
		MemoryTypePlanGraph,
		MemoryTypeEntity,
	}
	for _, mt := range valid {
		if !IsValidMemoryType(mt) {
			t.Fatalf("expected %q to be valid", mt)
		}
	}
	if IsValidMemoryType(MemoryType("invalid")) {
		t.Fatalf("expected invalid memory type to be rejected")
	}
}

func TestIsValidTaskState(t *testing.T) {
	valid := []TaskState{
		TaskStatePlanning,
		TaskStateExecuting,
		TaskStateBlocked,
		TaskStateWaiting,
		TaskStateDone,
	}
	for _, state := range valid {
		if !IsValidTaskState(state) {
			t.Fatalf("expected %q to be valid", state)
		}
	}
	if IsValidTaskState(TaskState("invalid")) {
		t.Fatalf("expected invalid task state to be rejected")
	}
}

func TestIsValidOutcomeStatus(t *testing.T) {
	valid := []OutcomeStatus{
		OutcomeStatusSuccess,
		OutcomeStatusFailure,
		OutcomeStatusPartial,
	}
	for _, state := range valid {
		if !IsValidOutcomeStatus(state) {
			t.Fatalf("expected %q to be valid", state)
		}
	}
	if IsValidOutcomeStatus(OutcomeStatus("invalid")) {
		t.Fatalf("expected invalid outcome status to be rejected")
	}
}

func TestMemoryRecordJSONRoundTripPreservesInterpretationAndEntityPayload(t *testing.T) {
	rec := NewMemoryRecord("entity-1", MemoryTypeEntity, SensitivityLow, &EntityPayload{
		Kind:          "entity",
		CanonicalName: "Orchid",
		PrimaryType:   EntityTypeProject,
		Types:         []string{EntityTypeProject},
		Aliases: []EntityAlias{
			{Value: "orchid"},
			{Value: "staging target"},
		},
		Summary: "Staging deploy target",
	})
	rec.Interpretation = &Interpretation{
		Status:               InterpretationStatusResolved,
		Summary:              "Canonicalized Orchid entity",
		ProposedType:         MemoryTypeEntity,
		TopicalLabels:        []string{"deploy", "staging"},
		ExtractionConfidence: 0.92,
		Mentions: []Mention{{
			Surface:           "Orchid",
			EntityKind:        EntityKindProject,
			CanonicalEntityID: "entity-1",
			Confidence:        0.92,
			Aliases:           []string{"orchid"},
		}},
	}

	data, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var got MemoryRecord
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	payload, ok := got.Payload.(*EntityPayload)
	if !ok {
		t.Fatalf("Payload type = %T, want *EntityPayload", got.Payload)
	}
	if payload.CanonicalName != "Orchid" {
		t.Fatalf("CanonicalName = %q, want Orchid", payload.CanonicalName)
	}
	if got.Interpretation == nil {
		t.Fatalf("Interpretation = nil, want populated interpretation")
	}
	if got.Interpretation.Status != InterpretationStatusResolved {
		t.Fatalf("Interpretation.Status = %q, want %q", got.Interpretation.Status, InterpretationStatusResolved)
	}
	if len(got.Interpretation.Mentions) != 1 || got.Interpretation.Mentions[0].CanonicalEntityID != "entity-1" {
		t.Fatalf("Interpretation mentions = %+v, want canonical entity link", got.Interpretation.Mentions)
	}
}

func TestMemoryRecordUnmarshalAndValidateBranches(t *testing.T) {
	var invalid MemoryRecord
	if err := invalid.UnmarshalJSON([]byte(`{`)); err == nil {
		t.Fatalf("direct UnmarshalJSON invalid JSON error = nil, want error")
	}
	if err := invalid.UnmarshalJSON([]byte(`{"id":"rec-1","type":"semantic","sensitivity":"low"}`)); err == nil {
		t.Fatalf("UnmarshalJSON missing payload error = nil, want payload error")
	}

	rec := NewMemoryRecord("valid", MemoryTypeSemantic, SensitivityLow, &SemanticPayload{
		Kind:      "semantic",
		Subject:   "Go",
		Predicate: "is",
		Object:    "typed",
		Validity:  Validity{Mode: ValidityModeGlobal},
	})
	if err := rec.Validate(); err != nil {
		t.Fatalf("Validate valid record: %v", err)
	}
}

type failingPayload struct{}

func (failingPayload) PayloadKind() string { return "failing" }
func (failingPayload) isPayload()          {}
func (failingPayload) MarshalJSON() ([]byte, error) {
	return nil, errors.New("forced payload marshal failure")
}

func TestMemoryRecordMarshalJSONReturnsPayloadError(t *testing.T) {
	rec := NewMemoryRecord("bad-payload", MemoryTypeSemantic, SensitivityLow, failingPayload{})
	if _, err := json.Marshal(rec); err == nil {
		t.Fatalf("Marshal MemoryRecord with failing payload error = nil")
	}
}
