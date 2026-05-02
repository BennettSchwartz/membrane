package ingestion

import (
	"strings"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
)

func TestPolicyValidateRejectsInvalidCandidates(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	valid := func() *MemoryCandidate {
		return &MemoryCandidate{
			Kind:      CandidateKindEvent,
			Source:    "tester",
			Timestamp: now,
			EventKind: "user_input",
			EventRef:  "evt-1",
		}
	}

	for _, tc := range []struct {
		name      string
		candidate *MemoryCandidate
		defaults  PolicyDefaults
		want      string
	}{
		{name: "missing kind", candidate: func() *MemoryCandidate { c := valid(); c.Kind = ""; return c }(), want: "kind"},
		{name: "missing source", candidate: func() *MemoryCandidate { c := valid(); c.Source = ""; return c }(), want: "source"},
		{name: "missing timestamp", candidate: func() *MemoryCandidate { c := valid(); c.Timestamp = time.Time{}; return c }(), want: "timestamp"},
		{name: "invalid sensitivity", candidate: func() *MemoryCandidate { c := valid(); c.Sensitivity = "private"; return c }(), want: "invalid sensitivity"},
		{name: "invalid default sensitivity", candidate: valid(), defaults: func() PolicyDefaults { d := DefaultPolicyDefaults(); d.Sensitivity = "private"; return d }(), want: "invalid default sensitivity"},
		{name: "event kind", candidate: func() *MemoryCandidate { c := valid(); c.EventKind = ""; return c }(), want: "event kind"},
		{name: "event ref", candidate: func() *MemoryCandidate { c := valid(); c.EventRef = ""; return c }(), want: "event ref"},
		{name: "tool name", candidate: &MemoryCandidate{Kind: CandidateKindToolOutput, Source: "tester", Timestamp: now}, want: "tool name"},
		{name: "observation subject", candidate: &MemoryCandidate{Kind: CandidateKindObservation, Source: "tester", Timestamp: now, Predicate: "is"}, want: "subject"},
		{name: "observation predicate", candidate: &MemoryCandidate{Kind: CandidateKindObservation, Source: "tester", Timestamp: now, Subject: "Go"}, want: "predicate"},
		{name: "outcome target", candidate: &MemoryCandidate{Kind: CandidateKindOutcome, Source: "tester", Timestamp: now, OutcomeStatus: schema.OutcomeStatusSuccess}, want: "target record ID"},
		{name: "outcome status", candidate: &MemoryCandidate{Kind: CandidateKindOutcome, Source: "tester", Timestamp: now, TargetRecordID: "rec-1"}, want: "outcome status"},
		{name: "working thread", candidate: &MemoryCandidate{Kind: CandidateKindWorkingState, Source: "tester", Timestamp: now, TaskState: schema.TaskStatePlanning}, want: "thread ID"},
		{name: "working state", candidate: &MemoryCandidate{Kind: CandidateKindWorkingState, Source: "tester", Timestamp: now, ThreadID: "thread-1"}, want: "task state"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defaults := tc.defaults
			if defaults.Sensitivity == "" {
				defaults = DefaultPolicyDefaults()
			}
			err := NewPolicyEngine(defaults).validate(tc.candidate)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validate error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestPolicyApplyAssignsExpectedDefaults(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	defaults := DefaultPolicyDefaults()
	defaults.DefaultInitialSalience = 0.42
	defaults.DefaultDeletionPolicy = schema.DeletionPolicyManualOnly
	engine := NewPolicyEngine(defaults)

	for _, tc := range []struct {
		name        string
		candidate   *MemoryCandidate
		memType     schema.MemoryType
		confidence  float64
		halfLife    int64
		sensitivity schema.Sensitivity
	}{
		{
			name:        "event",
			candidate:   &MemoryCandidate{Kind: CandidateKindEvent, Source: "tester", Timestamp: now, EventKind: "user_input", EventRef: "evt-1"},
			memType:     schema.MemoryTypeEpisodic,
			confidence:  0.8,
			halfLife:    defaults.EpisodicHalfLifeSeconds,
			sensitivity: defaults.Sensitivity,
		},
		{
			name:        "tool",
			candidate:   &MemoryCandidate{Kind: CandidateKindToolOutput, Source: "tester", Timestamp: now, ToolName: "go test", Sensitivity: schema.SensitivityHigh},
			memType:     schema.MemoryTypeEpisodic,
			confidence:  0.9,
			halfLife:    defaults.EpisodicHalfLifeSeconds,
			sensitivity: schema.SensitivityHigh,
		},
		{
			name:        "observation",
			candidate:   &MemoryCandidate{Kind: CandidateKindObservation, Source: "tester", Timestamp: now, Subject: "Go", Predicate: "is"},
			memType:     schema.MemoryTypeSemantic,
			confidence:  0.7,
			halfLife:    defaults.SemanticHalfLifeSeconds,
			sensitivity: defaults.Sensitivity,
		},
		{
			name:        "outcome",
			candidate:   &MemoryCandidate{Kind: CandidateKindOutcome, Source: "tester", Timestamp: now, TargetRecordID: "rec-1", OutcomeStatus: schema.OutcomeStatusSuccess},
			memType:     schema.MemoryTypeEpisodic,
			confidence:  0.85,
			halfLife:    defaults.EpisodicHalfLifeSeconds,
			sensitivity: defaults.Sensitivity,
		},
		{
			name:        "working",
			candidate:   &MemoryCandidate{Kind: CandidateKindWorkingState, Source: "tester", Timestamp: now, ThreadID: "thread-1", TaskState: schema.TaskStatePlanning},
			memType:     schema.MemoryTypeWorking,
			confidence:  1.0,
			halfLife:    defaults.WorkingHalfLifeSeconds,
			sensitivity: defaults.Sensitivity,
		},
		{
			name:        "unknown",
			candidate:   &MemoryCandidate{Kind: "unknown", Source: "tester", Timestamp: now},
			memType:     schema.MemoryTypeCompetence,
			confidence:  0.5,
			halfLife:    defaults.SemanticHalfLifeSeconds,
			sensitivity: defaults.Sensitivity,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := engine.Apply(tc.candidate, tc.memType)
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}
			if result.Sensitivity != tc.sensitivity {
				t.Fatalf("Sensitivity = %q, want %q", result.Sensitivity, tc.sensitivity)
			}
			if result.Confidence != tc.confidence {
				t.Fatalf("Confidence = %.2f, want %.2f", result.Confidence, tc.confidence)
			}
			if result.Salience != defaults.DefaultInitialSalience {
				t.Fatalf("Salience = %.2f, want %.2f", result.Salience, defaults.DefaultInitialSalience)
			}
			if result.DeletionPolicy != defaults.DefaultDeletionPolicy {
				t.Fatalf("DeletionPolicy = %q, want %q", result.DeletionPolicy, defaults.DefaultDeletionPolicy)
			}
			if result.Lifecycle.Decay.Curve != schema.DecayCurveExponential || result.Lifecycle.Decay.HalfLifeSeconds != tc.halfLife {
				t.Fatalf("Lifecycle.Decay = %+v, want exponential half-life %d", result.Lifecycle.Decay, tc.halfLife)
			}
		})
	}
}
