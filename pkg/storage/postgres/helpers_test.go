package postgres

import (
	"fmt"
	"reflect"
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
