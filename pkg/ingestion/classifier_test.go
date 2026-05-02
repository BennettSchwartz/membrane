package ingestion

import (
	"testing"

	"github.com/BennettSchwartz/membrane/pkg/schema"
)

func TestClassifierClassify(t *testing.T) {
	classifier := NewClassifier()
	for _, tc := range []struct {
		name      string
		candidate *MemoryCandidate
		want      schema.MemoryType
	}{
		{name: "nil", candidate: nil, want: schema.MemoryTypeEpisodic},
		{name: "event", candidate: &MemoryCandidate{Kind: CandidateKindEvent}, want: schema.MemoryTypeEpisodic},
		{name: "tool output", candidate: &MemoryCandidate{Kind: CandidateKindToolOutput}, want: schema.MemoryTypeEpisodic},
		{name: "observation", candidate: &MemoryCandidate{Kind: CandidateKindObservation}, want: schema.MemoryTypeSemantic},
		{name: "outcome", candidate: &MemoryCandidate{Kind: CandidateKindOutcome}, want: schema.MemoryTypeEpisodic},
		{name: "working state", candidate: &MemoryCandidate{Kind: CandidateKindWorkingState}, want: schema.MemoryTypeWorking},
		{name: "unknown", candidate: &MemoryCandidate{Kind: "other"}, want: schema.MemoryTypeEpisodic},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifier.Classify(tc.candidate); got != tc.want {
				t.Fatalf("Classify = %q, want %q", got, tc.want)
			}
		})
	}
}
