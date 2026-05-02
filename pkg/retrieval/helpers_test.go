package retrieval

import (
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
)

func TestRelationBoostAndLexicalMatch(t *testing.T) {
	for _, tc := range []struct {
		predicate string
		fallback  float64
		want      float64
	}{
		{predicate: "mentions_entity", fallback: 10, want: 30},
		{predicate: "mentioned_in", fallback: 10, want: 30},
		{predicate: "subject_entity", fallback: 10, want: 35},
		{predicate: "fact_subject_of", fallback: 10, want: 35},
		{predicate: "object_entity", fallback: 10, want: 35},
		{predicate: "fact_object_of", fallback: 10, want: 35},
		{predicate: "supports", fallback: 10, want: 20},
		{predicate: "supported_by", fallback: 10, want: 20},
		{predicate: "depends_on", fallback: 10, want: 20},
		{predicate: "dependency_of", fallback: 10, want: 20},
		{predicate: "custom", fallback: 10, want: 10},
	} {
		if got := relationBoost(tc.predicate, tc.fallback); got != tc.want {
			t.Fatalf("relationBoost(%q, %.1f) = %.1f, want %.1f", tc.predicate, tc.fallback, got, tc.want)
		}
	}

	if !lexicalMatch("Orchid Deploy", "orchid") {
		t.Fatalf("lexicalMatch should match normalized token")
	}
	if !lexicalMatch("orchid", "orchid deploy") {
		t.Fatalf("lexicalMatch should match reverse containment")
	}
	if lexicalMatch("", "orchid") || lexicalMatch("orchid", "") || lexicalMatch("orchid", "borealis") {
		t.Fatalf("lexicalMatch accepted empty or unrelated values")
	}
	if got := normalizeSearchText("  Orchid  "); got != "orchid" {
		t.Fatalf("normalizeSearchText = %q, want orchid", got)
	}
	if !containsToken("orchid deploy", "deploy") {
		t.Fatalf("containsToken should use substring containment")
	}
}

func TestSelectorRecencyScores(t *testing.T) {
	selector := NewSelector(0.7)

	zero := &schema.MemoryRecord{}
	if got := selector.computeRecency(zero); got != 0.5 {
		t.Fatalf("zero recency = %.2f, want neutral 0.5", got)
	}

	future := &schema.MemoryRecord{Lifecycle: schema.Lifecycle{LastReinforcedAt: time.Now().Add(time.Hour)}}
	if got := selector.computeRecency(future); got != 1.0 {
		t.Fatalf("future recency = %.2f, want 1.0", got)
	}

	recent := &schema.MemoryRecord{Lifecycle: schema.Lifecycle{LastReinforcedAt: time.Now().Add(-time.Hour)}}
	recentScore := selector.computeRecency(recent)
	if recentScore <= 0.99 || recentScore > 1.0 {
		t.Fatalf("recent score = %.4f, want close to 1", recentScore)
	}

	old := &schema.MemoryRecord{Lifecycle: schema.Lifecycle{LastReinforcedAt: time.Now().Add(-60 * 24 * time.Hour)}}
	oldScore := selector.computeRecency(old)
	if oldScore < 0.24 || oldScore > 0.26 {
		t.Fatalf("old score = %.4f, want roughly 0.25 after two half-lives", oldScore)
	}
}
