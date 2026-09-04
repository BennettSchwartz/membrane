package schema

import "testing"

func TestSemanticObjectKey(t *testing.T) {
	if got := SemanticObjectKey("  Postgres  "); got != "Postgres" {
		t.Fatalf("SemanticObjectKey string = %q, want Postgres", got)
	}
	if got := SemanticObjectKey(map[string]any{"lang": "go", "db": "postgres"}); got != `{"db":"postgres","lang":"go"}` {
		t.Fatalf("SemanticObjectKey map = %q, want stable JSON object", got)
	}
	if got := SemanticObjectKey([]any{"postgres", 15}); got != `["postgres",15]` {
		t.Fatalf("SemanticObjectKey array = %q, want stable JSON array", got)
	}
}

func TestNormalizeSemanticPredicate(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  string
	}{
		{value: "Depends On", want: "depends_on"},
		{value: "dependsOn", want: "depends_on"},
		{value: "HTTPServerUses", want: "http_server_uses"},
		{value: "!!!", want: ""},
	} {
		if got := NormalizeSemanticPredicate(tc.value); got != tc.want {
			t.Fatalf("NormalizeSemanticPredicate(%q) = %q, want %q", tc.value, got, tc.want)
		}
	}
}
