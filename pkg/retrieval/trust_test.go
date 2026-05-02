package retrieval

import (
	"testing"

	"github.com/BennettSchwartz/membrane/pkg/schema"
)

func TestTrustAllowsRejectsInvalidRecordSensitivity(t *testing.T) {
	trust := NewTrustContext(schema.SensitivityLow, true, "actor", nil)
	record := &schema.MemoryRecord{Sensitivity: schema.Sensitivity("invalid")}

	if trust.Allows(record) {
		t.Fatalf("expected invalid record sensitivity to be denied")
	}
	if trust.AllowsRedacted(record) {
		t.Fatalf("expected invalid record sensitivity to not allow redacted access")
	}
}

func TestTrustAllowsRejectsInvalidMaxSensitivity(t *testing.T) {
	trust := NewTrustContext(schema.Sensitivity("invalid"), true, "actor", nil)
	record := &schema.MemoryRecord{Sensitivity: schema.SensitivityLow}

	if trust.Allows(record) {
		t.Fatalf("expected invalid trust max sensitivity to deny access")
	}
	if trust.AllowsRedacted(record) {
		t.Fatalf("expected invalid trust max sensitivity to deny redacted access")
	}
}

func TestTrustAllowsRedactedHonorsScope(t *testing.T) {
	trust := NewTrustContext(schema.SensitivityLow, true, "actor", []string{"project:a"})

	inScope := &schema.MemoryRecord{Sensitivity: schema.SensitivityMedium, Scope: "project:a"}
	if !trust.AllowsRedacted(inScope) {
		t.Fatalf("expected one-level-higher in-scope record to allow redacted access")
	}

	outOfScope := &schema.MemoryRecord{Sensitivity: schema.SensitivityMedium, Scope: "project:b"}
	if trust.AllowsRedacted(outOfScope) {
		t.Fatalf("expected out-of-scope record to deny redacted access")
	}
}

func TestTrustAllowsScopeAndSensitivityBranches(t *testing.T) {
	trust := NewTrustContext(schema.SensitivityLow, true, "actor", []string{"project:a"})

	if trust.Allows(nil) {
		t.Fatalf("Allows(nil) = true, want false")
	}
	if trust.AllowsRedacted(nil) {
		t.Fatalf("AllowsRedacted(nil) = true, want false")
	}
	if !trust.Allows(&schema.MemoryRecord{Sensitivity: schema.SensitivityLow, Scope: ""}) {
		t.Fatalf("expected unscoped low-sensitivity record to be allowed")
	}
	if trust.Allows(&schema.MemoryRecord{Sensitivity: schema.SensitivityLow, Scope: "project:b"}) {
		t.Fatalf("expected out-of-scope low-sensitivity record to be denied")
	}
	if trust.Allows(&schema.MemoryRecord{Sensitivity: schema.SensitivityMedium, Scope: "project:a"}) {
		t.Fatalf("expected too-sensitive record to be denied for full access")
	}
	if trust.AllowsRedacted(&schema.MemoryRecord{Sensitivity: schema.SensitivityLow, Scope: "project:a"}) {
		t.Fatalf("expected fully allowed record to not require redacted access")
	}
	if trust.allowsScope(nil) {
		t.Fatalf("allowsScope(nil) = true, want false")
	}
}

func TestFilterBySensitivitySkipsInvalidValues(t *testing.T) {
	records := []*schema.MemoryRecord{
		{ID: "ok-low", Sensitivity: schema.SensitivityLow},
		{ID: "bad", Sensitivity: schema.Sensitivity("invalid")},
		{ID: "ok-high", Sensitivity: schema.SensitivityHigh},
	}

	filtered := FilterBySensitivity(records, schema.SensitivityMedium)
	if len(filtered) != 1 {
		t.Fatalf("expected 1 record, got %d", len(filtered))
	}
	if filtered[0].ID != "ok-low" {
		t.Fatalf("expected ok-low, got %s", filtered[0].ID)
	}
}
