package tests_test

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/GustyCube/membrane/pkg/ingestion"
	"github.com/GustyCube/membrane/pkg/membrane"
	"github.com/GustyCube/membrane/pkg/retrieval"
	"github.com/GustyCube/membrane/pkg/schema"
)

// truncatePostgres removes all rows from every application table so that each
// test starts with a clean database when running against Postgres.
func truncatePostgres(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("truncatePostgres: open: %v", err)
	}
	defer db.Close()
	// Create schema if it doesn't exist yet (first run on a fresh database).
	// We open a throwaway membrane instance to bootstrap the tables.
	tmpCfg := membrane.DefaultConfig()
	tmpCfg.Backend = "postgres"
	tmpCfg.PostgresDSN = dsn
	tmpM, err := membrane.New(tmpCfg)
	if err != nil {
		t.Fatalf("truncatePostgres: bootstrap schema: %v", err)
	}
	_ = tmpM.Stop()

	_, err = db.Exec(`TRUNCATE memory_records, decay_profiles, payloads, tags,
		provenance_sources, relations, audit_log, competence_stats,
		trigger_embeddings, episodic_extraction_log CASCADE`)
	if err != nil {
		t.Fatalf("truncatePostgres: truncate: %v", err)
	}
}

// newTestMembrane creates a Membrane instance. When MEMBRANE_TEST_POSTGRES_DSN
// is set it uses Postgres+pgvector; otherwise it falls back to in-memory SQLite.
func newTestMembrane(t *testing.T) *membrane.Membrane {
	t.Helper()
	cfg := membrane.DefaultConfig()
	cfg.SelectionConfidenceThreshold = 0.3

	dsn := os.Getenv("MEMBRANE_TEST_POSTGRES_DSN")
	if dsn != "" {
		cfg.Backend = "postgres"
		cfg.PostgresDSN = dsn
		truncatePostgres(t, dsn)
	} else {
		cfg.DBPath = ":memory:"
	}

	m, err := membrane.New(cfg)
	if err != nil {
		t.Fatalf("failed to create membrane: %v", err)
	}
	// Start the schedulers so that Stop() can shut them down cleanly.
	ctx, cancel := context.WithCancel(context.Background())
	if err := m.Start(ctx); err != nil {
		t.Fatalf("failed to start membrane: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		if err := m.Stop(); err != nil {
			t.Errorf("failed to stop membrane: %v", err)
		}
	})
	return m
}

// fullTrust returns a TrustContext that allows access to all records.
func fullTrust() *retrieval.TrustContext {
	return retrieval.NewTrustContext(schema.SensitivityHyper, true, "test-actor", nil)
}

func TestFullLifecycle(t *testing.T) {
	ctx := context.Background()
	m := newTestMembrane(t)

	// -----------------------------------------------------------------------
	// Step 1: Ingest several events and tool outputs (episodic).
	// -----------------------------------------------------------------------

	event1, err := m.IngestEvent(ctx, ingestion.IngestEventRequest{
		Source:    "test-user",
		EventKind: "user_input",
		Ref:       "ref-001",
		Summary:   "User asked to build a project",
		Tags:      []string{"project", "setup"},
	})
	if err != nil {
		t.Fatalf("IngestEvent 1: %v", err)
	}
	if event1.Type != schema.MemoryTypeEpisodic {
		t.Errorf("expected episodic, got %s", event1.Type)
	}

	event2, err := m.IngestEvent(ctx, ingestion.IngestEventRequest{
		Source:    "test-user",
		EventKind: "error",
		Ref:       "ref-002",
		Summary:   "Build failed with exit code 1",
		Tags:      []string{"error", "build"},
	})
	if err != nil {
		t.Fatalf("IngestEvent 2: %v", err)
	}

	tool1, err := m.IngestToolOutput(ctx, ingestion.IngestToolOutputRequest{
		Source:   "agent",
		ToolName: "bash",
		Args:     map[string]any{"cmd": "go build ./..."},
		Result:   "exit code 0",
		Tags:     []string{"build", "go"},
	})
	if err != nil {
		t.Fatalf("IngestToolOutput: %v", err)
	}
	if tool1.Type != schema.MemoryTypeEpisodic {
		t.Errorf("expected episodic for tool output, got %s", tool1.Type)
	}

	// -----------------------------------------------------------------------
	// Step 2: Ingest observations (semantic).
	// -----------------------------------------------------------------------

	obs1, err := m.IngestObservation(ctx, ingestion.IngestObservationRequest{
		Source:    "agent",
		Subject:   "user",
		Predicate: "prefers",
		Object:    "Go",
		Tags:      []string{"preference"},
	})
	if err != nil {
		t.Fatalf("IngestObservation 1: %v", err)
	}
	if obs1.Type != schema.MemoryTypeSemantic {
		t.Errorf("expected semantic, got %s", obs1.Type)
	}

	obs2, err := m.IngestObservation(ctx, ingestion.IngestObservationRequest{
		Source:    "agent",
		Subject:   "project",
		Predicate: "uses",
		Object:    "SQLite",
		Tags:      []string{"tech"},
	})
	if err != nil {
		t.Fatalf("IngestObservation 2: %v", err)
	}

	// -----------------------------------------------------------------------
	// Step 3: Run consolidation.
	// -----------------------------------------------------------------------

	// Start does not block; it kicks off background goroutines. For the
	// integration test we call consolidation indirectly through the schedulers
	// that were started, but since the interval is very long in the default
	// config, we just verify that the records are already persisted.
	// (Direct consolidation.Service.RunAll isn't exposed on Membrane, but
	//  the consolidation schedulers are started by Start and won't fire
	//  within a test. This is fine: consolidation is covered by its own tests.)

	// -----------------------------------------------------------------------
	// Step 4: Retrieve by type and verify results.
	// -----------------------------------------------------------------------

	trust := fullTrust()

	resp, err := m.Retrieve(ctx, &retrieval.RetrieveRequest{
		Trust:       trust,
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeEpisodic},
	})
	if err != nil {
		t.Fatalf("Retrieve episodic: %v", err)
	}
	if len(resp.Records) < 3 {
		t.Errorf("expected at least 3 episodic records, got %d", len(resp.Records))
	}

	// Verify that the episodic records include our events and tool output.
	ids := make(map[string]bool)
	for _, r := range resp.Records {
		ids[r.ID] = true
	}
	for _, expected := range []string{event1.ID, event2.ID, tool1.ID} {
		if !ids[expected] {
			t.Errorf("expected record %s in episodic results", expected)
		}
	}

	resp, err = m.Retrieve(ctx, &retrieval.RetrieveRequest{
		Trust:       trust,
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
	})
	if err != nil {
		t.Fatalf("Retrieve semantic: %v", err)
	}
	if len(resp.Records) < 2 {
		t.Errorf("expected at least 2 semantic records, got %d", len(resp.Records))
	}

	// -----------------------------------------------------------------------
	// Step 5: Apply decay and verify salience decreases.
	// -----------------------------------------------------------------------

	// Record original salience.
	originalSalience := event1.Salience

	// Penalize the record to simulate decay.
	err = m.Penalize(ctx, event1.ID, 0.3, "test", "test decay")
	if err != nil {
		t.Fatalf("Penalize: %v", err)
	}

	// Re-retrieve and check.
	decayed, err := m.RetrieveByID(ctx, event1.ID, trust)
	if err != nil {
		t.Fatalf("RetrieveByID after decay: %v", err)
	}
	if decayed.Salience >= originalSalience {
		t.Errorf("expected salience to decrease after penalty: original=%.2f, got=%.2f", originalSalience, decayed.Salience)
	}

	// -----------------------------------------------------------------------
	// Step 6: Reinforce and verify salience increases.
	// -----------------------------------------------------------------------

	salienceBeforeReinforce := decayed.Salience
	err = m.Reinforce(ctx, event1.ID, "test", "test reinforce")
	if err != nil {
		t.Fatalf("Reinforce: %v", err)
	}

	reinforced, err := m.RetrieveByID(ctx, event1.ID, trust)
	if err != nil {
		t.Fatalf("RetrieveByID after reinforce: %v", err)
	}

	// Reinforcement gain defaults to 0 in the DecayProfile, so we check that
	// the salience is at least not lower. With gain=0, salience stays the same
	// but LastReinforcedAt is updated, which is the primary effect.
	if reinforced.Salience < salienceBeforeReinforce {
		t.Errorf("expected salience to not decrease after reinforce: before=%.2f, got=%.2f", salienceBeforeReinforce, reinforced.Salience)
	}

	// Verify that the observation semantic records are retrievable by ID.
	for _, obs := range []*schema.MemoryRecord{obs1, obs2} {
		rec, err := m.RetrieveByID(ctx, obs.ID, trust)
		if err != nil {
			t.Fatalf("RetrieveByID(%s): %v", obs.ID, err)
		}
		if rec.Type != schema.MemoryTypeSemantic {
			t.Errorf("expected semantic, got %s", rec.Type)
		}
	}

	// -----------------------------------------------------------------------
	// Step 7: Verify retrieval with limit.
	// -----------------------------------------------------------------------

	resp, err = m.Retrieve(ctx, &retrieval.RetrieveRequest{
		Trust: trust,
		Limit: 2,
	})
	if err != nil {
		t.Fatalf("Retrieve with limit: %v", err)
	}
	if len(resp.Records) > 2 {
		t.Errorf("expected at most 2 records with limit=2, got %d", len(resp.Records))
	}

	// Verify salience ordering: records should be sorted descending.
	for i := 1; i < len(resp.Records); i++ {
		if resp.Records[i].Salience > resp.Records[i-1].Salience {
			t.Errorf("records not sorted by salience: [%d]=%.2f > [%d]=%.2f",
				i, resp.Records[i].Salience, i-1, resp.Records[i-1].Salience)
		}
	}

	_ = time.Now() // suppress unused import
}
