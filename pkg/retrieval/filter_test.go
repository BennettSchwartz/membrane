package retrieval

import (
	"reflect"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
)

func TestFilterBySalienceSensitivityTrustAndSort(t *testing.T) {
	low := &schema.MemoryRecord{ID: "low", Sensitivity: schema.SensitivityLow, Salience: 0.2, Scope: "project:a"}
	medium := &schema.MemoryRecord{ID: "medium", Sensitivity: schema.SensitivityMedium, Salience: 0.8, Scope: "project:a"}
	high := &schema.MemoryRecord{ID: "high", Sensitivity: schema.SensitivityHigh, Salience: 0.5, Scope: "project:a"}
	outOfScope := &schema.MemoryRecord{ID: "out-of-scope", Sensitivity: schema.SensitivityLow, Salience: 1.0, Scope: "project:b"}
	records := []*schema.MemoryRecord{low, medium, high, outOfScope}

	if got := idsOf(FilterBySalience(records, 0.5)); !reflect.DeepEqual(got, []string{"medium", "high", "out-of-scope"}) {
		t.Fatalf("FilterBySalience IDs = %v, want medium/high/out-of-scope", got)
	}
	if got := FilterBySensitivity(records, schema.Sensitivity("invalid")); len(got) != 0 {
		t.Fatalf("FilterBySensitivity invalid max = %+v, want empty", got)
	}
	if got := idsOf(FilterBySensitivity(records, schema.SensitivityMedium)); !reflect.DeepEqual(got, []string{"low", "medium", "out-of-scope"}) {
		t.Fatalf("FilterBySensitivity IDs = %v, want low/medium/out-of-scope", got)
	}
	if got := FilterByTrust(records, nil); !reflect.DeepEqual(got, records) {
		t.Fatalf("FilterByTrust nil trust = %+v, want original records", got)
	}

	trust := NewTrustContext(schema.SensitivityLow, true, "tester", []string{"project:a"})
	filtered := FilterByTrust(records, trust)
	if got := idsOf(filtered); !reflect.DeepEqual(got, []string{"low", "medium"}) {
		t.Fatalf("FilterByTrust IDs = %v, want low and redacted medium", got)
	}
	if filtered[0] != low {
		t.Fatalf("allowed record was copied, want original pointer")
	}
	if filtered[1] == medium {
		t.Fatalf("redacted record reused original pointer")
	}
	if filtered[1].Payload != nil {
		t.Fatalf("redacted payload = %#v, want nil", filtered[1].Payload)
	}

	SortBySalience(filtered)
	if got := idsOf(filtered); !reflect.DeepEqual(got, []string{"medium", "low"}) {
		t.Fatalf("sorted IDs = %v, want medium then low", got)
	}
}

func TestRedactCopiesMetadataWithoutSharingSlices(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	record := &schema.MemoryRecord{
		ID:          "rec-1",
		Type:        schema.MemoryTypeSemantic,
		Sensitivity: schema.SensitivityMedium,
		Confidence:  0.8,
		Salience:    0.7,
		Scope:       "project",
		Tags:        []string{"tag-a", "tag-b"},
		CreatedAt:   now,
		UpdatedAt:   now,
		Provenance:  schema.Provenance{Sources: []schema.ProvenanceSource{{Ref: "obs-1"}}},
		Relations:   []schema.Relation{{Predicate: "related_to", TargetID: "other"}},
		Payload: &schema.SemanticPayload{
			Kind:      "semantic",
			Subject:   "Go",
			Predicate: "is",
			Object:    "typed",
			Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
		},
		Interpretation: &schema.Interpretation{Summary: "private"},
		AuditLog:       []schema.AuditEntry{{Action: schema.AuditActionCreate}},
	}

	redacted := Redact(record)
	if redacted == nil {
		t.Fatal("Redact = nil, want record")
	}
	if redacted.ID != record.ID || redacted.Type != record.Type || redacted.Scope != record.Scope {
		t.Fatalf("redacted metadata = %+v, want source metadata", redacted)
	}
	if redacted.Payload != nil || redacted.Interpretation != nil {
		t.Fatalf("redacted payload/interpretation = (%+v, %+v), want nil", redacted.Payload, redacted.Interpretation)
	}
	if len(redacted.Provenance.Sources) != 0 || len(redacted.Relations) != 0 || len(redacted.AuditLog) != 0 {
		t.Fatalf("redacted sensitive slices = provenance:%v relations:%v audit:%v, want empty", redacted.Provenance.Sources, redacted.Relations, redacted.AuditLog)
	}
	if !reflect.DeepEqual(redacted.Tags, record.Tags) {
		t.Fatalf("redacted tags = %v, want %v", redacted.Tags, record.Tags)
	}
	redacted.Tags[0] = "changed"
	if record.Tags[0] == "changed" {
		t.Fatalf("redacted tags share backing array with source")
	}
	if redacted.Lifecycle.Decay.Curve != schema.DecayCurveExponential ||
		redacted.Lifecycle.Decay.HalfLifeSeconds != 0 ||
		redacted.Lifecycle.DeletionPolicy != schema.DeletionPolicyAutoPrune {
		t.Fatalf("redacted lifecycle = %+v, want redacted defaults", redacted.Lifecycle)
	}

	if Redact(nil) != nil {
		t.Fatalf("Redact(nil) != nil")
	}
}
