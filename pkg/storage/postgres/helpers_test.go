package postgres

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestBuildIDPlaceholders(t *testing.T) {
	got, args := buildIDPlaceholders([]string{"a", "b", "c"}, 2)
	if got != "$2,$3,$4" {
		t.Fatalf("placeholders = %q, want $2,$3,$4", got)
	}
	if !reflect.DeepEqual(args, []any{"a", "b", "c"}) {
		t.Fatalf("args = %#v", args)
	}
}

func TestBoundedEmbeddingCandidateIDsCapsSQLWork(t *testing.T) {
	values := make([]string, 10_001)
	for i := range values {
		values[i] = fmt.Sprintf("record-%05d", i)
	}
	bounded := boundedEmbeddingCandidateIDs(values)
	if len(bounded) != 10_000 {
		t.Fatalf("bounded candidate count = %d, want 10000", len(bounded))
	}
	if bounded[0] != values[0] || bounded[len(bounded)-1] != values[9_999] {
		t.Fatalf("bounded candidates did not preserve ranked input order")
	}
}

func TestCapBoundedLookupLimitBeforeAllocation(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	if got := capBoundedLookupLimit(maxInt); got != 10_000 {
		t.Fatalf("capBoundedLookupLimit(maxInt) = %d, want 10000", got)
	}
	if got := capBoundedLookupLimit(17); got != 17 {
		t.Fatalf("capBoundedLookupLimit(17) = %d, want 17", got)
	}
}

func TestBudgetedCandidateLimitUsesConservativeRecordOverhead(t *testing.T) {
	if got := budgetedCandidateLimit(10_000, 16<<20); got != 33 {
		t.Fatalf("budgetedCandidateLimit(10000, 16MiB) = %d, want 33", got)
	}
	if got := budgetedCandidateLimit(2, 16<<20); got != 2 {
		t.Fatalf("budgetedCandidateLimit(2, 16MiB) = %d, want 2", got)
	}
	if got := budgetedCandidateLimit(10_000, 1); got != 1 {
		t.Fatalf("budgetedCandidateLimit(10000, 1) = %d, want 1", got)
	}
}

func TestBudgetedRelationCandidateLimitCapsSizePreflight(t *testing.T) {
	if got := budgetedRelationCandidateLimit(10_000, 16<<20); got != 4097 {
		t.Fatalf("budgetedRelationCandidateLimit(10000, 16MiB) = %d, want 4097", got)
	}
	if got := budgetedRelationCandidateLimit(10, 16<<20); got != 10 {
		t.Fatalf("budgetedRelationCandidateLimit(10, 16MiB) = %d, want 10", got)
	}
}

func TestSchemaHardensTriggerEmbeddingSearchSurface(t *testing.T) {
	required := []string{
		"embedding vector({{EMBEDDING_DIMENSIONS}}) NOT NULL",
		"DELETE FROM trigger_embeddings WHERE embedding IS NULL",
		"ALTER TABLE trigger_embeddings ALTER COLUMN embedding SET NOT NULL",
		"CREATE INDEX IF NOT EXISTS idx_trigger_embeddings_model ON trigger_embeddings(model)",
	}
	for _, fragment := range required {
		if !strings.Contains(ddl, fragment) {
			t.Fatalf("schema DDL missing %q", fragment)
		}
	}
}

func TestSchemaIndexesBoundedRelationOrdering(t *testing.T) {
	required := []string{
		"CREATE INDEX IF NOT EXISTS idx_relations_source_rank ON relations(source_id, weight DESC NULLS LAST, created_at DESC, predicate, target_id)",
		"CREATE INDEX IF NOT EXISTS idx_relations_target_rank ON relations(target_id, weight DESC NULLS LAST, created_at DESC, predicate, source_id)",
	}
	for _, fragment := range required {
		if !strings.Contains(ddl, fragment) {
			t.Fatalf("schema DDL missing bounded relation index %q", fragment)
		}
	}
}

func TestVectorLiteralAndParseVectorLiteral(t *testing.T) {
	literal := vectorLiteral([]float32{0.25, -1.5, 3})
	if literal != "[0.25,-1.5,3]" {
		t.Fatalf("vector literal = %q", literal)
	}

	values, err := parseVectorLiteral(" [0.25, -1.5, 3] ")
	if err != nil {
		t.Fatalf("parseVectorLiteral: %v", err)
	}
	if !reflect.DeepEqual(values, []float32{0.25, -1.5, 3}) {
		t.Fatalf("values = %#v", values)
	}

	values, err = parseVectorLiteral("[]")
	if err != nil {
		t.Fatalf("parse empty vector: %v", err)
	}
	if len(values) != 0 {
		t.Fatalf("empty vector = %#v, want empty", values)
	}

	if _, err := parseVectorLiteral("[nope]"); err == nil {
		t.Fatalf("parse invalid vector error = nil, want error")
	}
}

func TestNullableHelpers(t *testing.T) {
	if got := nullableString(""); got != nil {
		t.Fatalf("nullableString empty = %#v, want nil", got)
	}
	if got := nullableString("value"); got != "value" {
		t.Fatalf("nullableString value = %#v, want value", got)
	}
	if got := nullableInt64(0); got != nil {
		t.Fatalf("nullableInt64 zero = %#v, want nil", got)
	}
	if got := nullableInt64(42); got != int64(42) {
		t.Fatalf("nullableInt64 value = %#v, want 42", got)
	}
}

func TestIsDuplicateError(t *testing.T) {
	if !isDuplicateError(&pgconn.PgError{Code: "23505"}) {
		t.Fatalf("expected pg duplicate error to be detected")
	}
	if !isDuplicateError(fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: "23505"})) {
		t.Fatalf("expected wrapped pg duplicate error to be detected")
	}
	if isDuplicateError(&pgconn.PgError{Code: "22000"}) {
		t.Fatalf("expected non-duplicate pg error to be ignored")
	}
	if isDuplicateError(fmt.Errorf("plain error")) {
		t.Fatalf("expected plain error to be ignored")
	}
}
