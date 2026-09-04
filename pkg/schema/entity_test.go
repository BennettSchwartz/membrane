package schema

import (
	"strings"
	"testing"
)

func TestEntityTermMatchesQuery(t *testing.T) {
	tests := []struct {
		name        string
		indexedTerm string
		query       string
		want        bool
	}{
		{
			name:        "exact match",
			indexedTerm: "Project Orchid",
			query:       "project orchid",
			want:        true,
		},
		{
			name:        "indexed phrase inside descriptor",
			indexedTerm: "Project Orchid",
			query:       "debug project orchid rollout failure",
			want:        true,
		},
		{
			name:        "short query inside indexed phrase",
			indexedTerm: "Project Orchid",
			query:       "orchid",
			want:        true,
		},
		{
			name:        "bounded punctuation",
			indexedTerm: "auth-service",
			query:       "debug auth-service latency",
			want:        true,
		},
		{
			name:        "collapses repeated whitespace",
			indexedTerm: "Project   Orchid",
			query:       "debug project orchid rollout",
			want:        true,
		},
		{
			name:        "does not match inside larger token",
			indexedTerm: "go",
			query:       "postgres migration",
			want:        false,
		},
		{
			name:        "blank indexed term",
			indexedTerm: " ",
			query:       "orchid",
			want:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EntityTermMatchesQuery(tt.indexedTerm, tt.query); got != tt.want {
				t.Fatalf("EntityTermMatchesQuery(%q, %q) = %v, want %v", tt.indexedTerm, tt.query, got, tt.want)
			}
		})
	}
	if !NormalizedEntityTermMatchesQuery("project orchid", "debug project orchid rollout") {
		t.Fatal("NormalizedEntityTermMatchesQuery rejected a bounded phrase match")
	}
}

func TestEntityTermMatchRank(t *testing.T) {
	tests := []struct {
		name        string
		indexedTerm string
		query       string
		want        int
	}{
		{name: "exact", indexedTerm: "Project Orchid", query: "project orchid", want: 0},
		{name: "indexed in descriptor", indexedTerm: "Project Orchid", query: "debug project orchid rollout", want: 1},
		{name: "query in indexed", indexedTerm: "Project Orchid rollout", query: "project orchid", want: 2},
		{name: "unrelated", indexedTerm: "go", query: "mongo migration", want: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EntityTermMatchRank(tt.indexedTerm, tt.query); got != tt.want {
				t.Fatalf("EntityTermMatchRank(%q, %q) = %d, want %d", tt.indexedTerm, tt.query, got, tt.want)
			}
		})
	}
}

func TestEntityTermMatchSpecificity(t *testing.T) {
	query := "debug project orchid rollout"
	if got, generic := EntityTermMatchSpecificity("Project Orchid", query), EntityTermMatchSpecificity("Project", query); got <= generic {
		t.Fatalf("specific descriptor phrase score = %d, generic = %d; want specific higher", got, generic)
	}
	if got, broad := EntityTermMatchSpecificity("Project Orchid", "project orchid"), EntityTermMatchSpecificity("Debug Project Orchid Rollout", "project orchid"); got <= broad {
		t.Fatalf("exact-or-shorter phrase score = %d, broad = %d; want shorter contained term higher", got, broad)
	}
}

func TestParseEntityIdentifierTokens(t *testing.T) {
	got := ParseEntityIdentifierTokens(`check (GitHub:BennettSchwartz/orchid), repo_path:pkg/auth repo_path:pkg/auth https://example.test`)
	if len(got) != 2 {
		t.Fatalf("ParseEntityIdentifierTokens len = %d, want 2: %+v", len(got), got)
	}
	if got[0].Namespace != "github" || got[0].Value != "BennettSchwartz/orchid" {
		t.Fatalf("first identifier = %+v, want normalized github identifier", got[0])
	}
	if got[1].Namespace != "repo_path" || got[1].Value != "pkg/auth" {
		t.Fatalf("second identifier = %+v, want repo_path identifier", got[1])
	}
	if got := ParseEntityIdentifierTokens("mailto:user@example.test bad/ns:value no-value: 12:00"); len(got) != 0 {
		t.Fatalf("ParseEntityIdentifierTokens skipped identifiers = %+v, want none", got)
	}
}

func TestParseEntityIdentifierTokensBoundedCapsInputBytesAndResults(t *testing.T) {
	text := "github:one repo_path:two jira:three " + strings.Repeat("padding ", 20_000) + "gitlab:beyond-limit"
	got := ParseEntityIdentifierTokensBounded(text, len("github:one repo_path:two jira:three "), 2)
	if len(got) != 2 {
		t.Fatalf("bounded identifiers len = %d, want 2: %+v", len(got), got)
	}
	if got[0].Namespace != "github" || got[0].Value != "one" || got[1].Namespace != "repo_path" || got[1].Value != "two" {
		t.Fatalf("bounded identifiers = %+v, want github:one and repo_path:two", got)
	}

	cutMidToken := ParseEntityIdentifierTokensBounded("github:one repo_path:partial", len("github:one repo_path:par"), 10)
	if len(cutMidToken) != 1 || cutMidToken[0].Namespace != "github" {
		t.Fatalf("mid-token byte cap identifiers = %+v, want only complete github token", cutMidToken)
	}
	if got := ParseEntityIdentifierTokensBounded(text, 0, 10); got != nil {
		t.Fatalf("zero byte budget identifiers = %+v, want nil", got)
	}
	if got := ParseEntityIdentifierTokensBounded(text, len(text), 0); got != nil {
		t.Fatalf("zero result budget identifiers = %+v, want nil", got)
	}
}
