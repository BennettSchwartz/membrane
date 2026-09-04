package testutil

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/BennettSchwartz/membrane/pkg/membrane"
)

// ResetPostgresDatabase bootstraps the Membrane schema and removes application
// data so integration tests can share a Postgres service without leaking state.
func ResetPostgresDatabase(t testing.TB, dsn string) {
	t.Helper()
	if dsn == "" {
		t.Fatalf("ResetPostgresDatabase: empty dsn")
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("ResetPostgresDatabase: open: %v", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(context.Background(), `
		DROP TABLE IF EXISTS trigger_embeddings;
		DROP TABLE IF EXISTS embedding_metadata;
	`); err != nil {
		t.Fatalf("ResetPostgresDatabase: reset embedding schema: %v", err)
	}

	cfg := membrane.DefaultConfig()
	cfg.PostgresDSN = dsn
	m, err := membrane.New(cfg)
	if err != nil {
		t.Fatalf("ResetPostgresDatabase: bootstrap schema: %v", err)
	}
	if err := m.Stop(); err != nil {
		t.Fatalf("ResetPostgresDatabase: stop bootstrap membrane: %v", err)
	}

	if _, err := db.ExecContext(context.Background(), `TRUNCATE
		episodic_extraction_log,
		trigger_embeddings,
		competence_stats,
		audit_log,
		relations,
		entity_identifiers,
		entity_types,
		entity_terms,
		provenance_sources,
		tags,
		interpretations,
		payloads,
		decay_profiles,
		memory_records
		RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("ResetPostgresDatabase: truncate: %v", err)
	}
}
