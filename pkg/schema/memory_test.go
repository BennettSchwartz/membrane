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
