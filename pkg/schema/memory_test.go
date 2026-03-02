package schema

import (
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
