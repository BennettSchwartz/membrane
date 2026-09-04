package ingestion

import (
	"context"
	"testing"

	"github.com/BennettSchwartz/membrane/pkg/schema"
)

func TestCaptureUsesConfiguredSensitivityForEveryCreatedRecord(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		configured, explicit, want schema.Sensitivity
	}{
		{"high default", schema.SensitivityHigh, "", schema.SensitivityHigh},
		{"hyper default", schema.SensitivityHyper, "", schema.SensitivityHyper},
		{"public default", schema.SensitivityPublic, "", schema.SensitivityPublic},
		{"explicit public", schema.SensitivityHigh, schema.SensitivityPublic, schema.SensitivityPublic},
		{"ordinary default", schema.SensitivityLow, "", schema.SensitivityLow},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc, store := newCaptureTestService(t, nil)
			defaults := DefaultPolicyDefaults()
			defaults.Sensitivity = tc.configured
			svc.policy = NewPolicyEngine(defaults)
			resp, err := svc.CaptureMemory(context.Background(), CaptureMemoryRequest{
				Source: "producer", SourceKind: "observation", Scope: "project",
				Content:     map[string]any{"subject": "Orchid", "predicate": "uses", "object": "PostgreSQL"},
				Sensitivity: tc.explicit,
			})
			if err != nil {
				t.Fatal(err)
			}
			if resp.PrimaryRecord.Sensitivity != tc.want {
				t.Fatalf("primary sensitivity = %s, want %s", resp.PrimaryRecord.Sensitivity, tc.want)
			}
			if len(resp.CreatedRecords) < 2 {
				t.Fatalf("expected derived entities/facts, got %d", len(resp.CreatedRecords))
			}
			for _, rec := range append(resp.CreatedRecords, resp.PrimaryRecord) {
				stored, err := store.Get(context.Background(), rec.ID)
				if err != nil {
					t.Fatal(err)
				}
				if stored.Sensitivity != tc.want {
					t.Errorf("%s %s sensitivity = %s, want %s", rec.Type, rec.ID, stored.Sensitivity, tc.want)
				}
			}
		})
	}
}

func TestCaptureRejectsInvalidConfiguredSensitivity(t *testing.T) {
	svc, store := newCaptureTestService(t, nil)
	defaults := DefaultPolicyDefaults()
	defaults.Sensitivity = "invalid"
	svc.policy = NewPolicyEngine(defaults)
	_, err := svc.CaptureMemory(context.Background(), CaptureMemoryRequest{Source: "producer", SourceKind: "event", Content: map[string]any{"text": "ordinary event"}})
	if err == nil {
		t.Fatal("expected invalid policy to fail")
	}
	for _, mt := range []schema.MemoryType{schema.MemoryTypeEpisodic, schema.MemoryTypeEntity, schema.MemoryTypeSemantic} {
		records, err := store.ListByType(context.Background(), mt)
		if err != nil {
			t.Fatal(err)
		}
		if len(records) != 0 {
			t.Errorf("failed capture persisted %d %s records", len(records), mt)
		}
	}
}
