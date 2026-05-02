package membrane

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/embedding"
	"github.com/BennettSchwartz/membrane/pkg/ingestion"
	"github.com/BennettSchwartz/membrane/pkg/retrieval"
	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
	"github.com/BennettSchwartz/membrane/pkg/storage/postgres"
)

func TestNewSQLiteStartAndStop(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DBPath = filepath.Join(t.TempDir(), "membrane.db")
	cfg.DecayInterval = time.Hour
	cfg.ConsolidationInterval = time.Hour

	m, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if m.store == nil || m.ingestion == nil || m.retrieval == nil || m.decay == nil || m.revision == nil || m.consolidation == nil || m.metrics == nil {
		t.Fatalf("New returned incomplete membrane: %+v", m)
	}
	if m.embedding != nil {
		t.Fatalf("embedding = %+v, want nil for sqlite config without embedding", m.embedding)
	}

	ctx, cancel := context.WithCancel(context.Background())
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	cancel()
	if err := m.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestNewNilConfigUsesDefaults(t *testing.T) {
	t.Chdir(t.TempDir())

	m, err := New(nil)
	if err != nil {
		t.Fatalf("New nil config: %v", err)
	}
	t.Cleanup(func() { _ = m.Stop() })
	if m.config == nil || m.config.Backend != "sqlite" || m.store == nil {
		t.Fatalf("New nil config returned incomplete membrane: %+v", m)
	}
}

func TestNewRejectsInvalidRuntimeConfig(t *testing.T) {
	t.Setenv("MEMBRANE_POSTGRES_DSN", "")

	tests := []struct {
		name string
		cfg  *Config
		want string
	}{
		{
			name: "invalid sensitivity",
			cfg: func() *Config {
				cfg := DefaultConfig()
				cfg.DefaultSensitivity = "secret"
				return cfg
			}(),
			want: "invalid default sensitivity",
		},
		{
			name: "unsupported backend",
			cfg: func() *Config {
				cfg := DefaultConfig()
				cfg.Backend = "memory"
				return cfg
			}(),
			want: "unsupported backend",
		},
		{
			name: "postgres missing dsn",
			cfg: func() *Config {
				cfg := DefaultConfig()
				cfg.Backend = "postgres"
				cfg.PostgresDSN = ""
				return cfg
			}(),
			want: "postgres_dsn is required",
		},
		{
			name: "sqlite open error",
			cfg: func() *Config {
				cfg := DefaultConfig()
				cfg.DBPath = t.TempDir()
				return cfg
			}(),
			want: "open sqlite store",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, err := New(tc.cfg)
			if m != nil {
				t.Cleanup(func() { _ = m.Stop() })
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("New error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestNewPostgresUsesEnvDSNAndWrapsOpenError(t *testing.T) {
	t.Setenv("MEMBRANE_POSTGRES_DSN", "postgres://%")
	cfg := DefaultConfig()
	cfg.Backend = "postgres"
	cfg.PostgresDSN = ""

	m, err := New(cfg)
	if m != nil {
		t.Cleanup(func() { _ = m.Stop() })
	}
	if err == nil || !strings.Contains(err.Error(), "open postgres store") {
		t.Fatalf("New postgres env DSN error = %v, want open postgres store error", err)
	}
	if cfg.PostgresDSN != "postgres://%" {
		t.Fatalf("PostgresDSN = %q, want env fallback value", cfg.PostgresDSN)
	}
}

func TestNewPostgresConfiguresEmbeddingAndLLMBranches(t *testing.T) {
	oldOpenPostgres := openPostgresStore
	defer func() { openPostgresStore = oldOpenPostgres }()

	var gotDSN string
	var gotEmbeddingConfig postgres.EmbeddingConfig
	openPostgresStore = func(dsn string, cfg postgres.EmbeddingConfig) (*postgres.PostgresStore, error) {
		gotDSN = dsn
		gotEmbeddingConfig = cfg
		return &postgres.PostgresStore{}, nil
	}

	t.Setenv("MEMBRANE_EMBEDDING_API_KEY", "embedding-env-key")
	t.Setenv("MEMBRANE_LLM_API_KEY", "llm-env-key")
	cfg := DefaultConfig()
	cfg.Backend = "postgres"
	cfg.PostgresDSN = "postgres://fake/db"
	cfg.EmbeddingEndpoint = "http://127.0.0.1:1/embeddings"
	cfg.EmbeddingModel = "text-embedding-test"
	cfg.EmbeddingDimensions = 7
	cfg.LLMEndpoint = "http://127.0.0.1:1/chat"
	cfg.LLMModel = "llm-test"

	m, err := New(cfg)
	if err != nil {
		t.Fatalf("New postgres with injected store: %v", err)
	}
	t.Cleanup(func() { _ = m.Stop() })

	if gotDSN != cfg.PostgresDSN || gotEmbeddingConfig.Dimensions != 7 || gotEmbeddingConfig.Model != "text-embedding-test" {
		t.Fatalf("postgres open args = %q/%+v, want configured DSN/model/dimensions", gotDSN, gotEmbeddingConfig)
	}
	if m.embedding == nil {
		t.Fatalf("embedding service = nil, want configured service for postgres embeddings")
	}
	if m.store == nil || m.retrieval == nil || m.revision == nil || m.consolidation == nil {
		t.Fatalf("New returned incomplete postgres membrane: %+v", m)
	}
}

func TestNewPostgresConfiguresEmbeddingWithoutLLM(t *testing.T) {
	oldOpenPostgres := openPostgresStore
	defer func() { openPostgresStore = oldOpenPostgres }()

	openPostgresStore = func(string, postgres.EmbeddingConfig) (*postgres.PostgresStore, error) {
		return &postgres.PostgresStore{}, nil
	}

	cfg := DefaultConfig()
	cfg.Backend = "postgres"
	cfg.PostgresDSN = "postgres://fake/db"
	cfg.EmbeddingEndpoint = "http://127.0.0.1:1/embeddings"
	cfg.EmbeddingModel = "text-embedding-test"
	cfg.LLMEndpoint = ""
	cfg.LLMModel = ""

	m, err := New(cfg)
	if err != nil {
		t.Fatalf("New postgres embedding only: %v", err)
	}
	t.Cleanup(func() { _ = m.Stop() })
	if m.embedding == nil || m.consolidation == nil || m.revision == nil || m.retrieval == nil {
		t.Fatalf("New returned incomplete embedding-only postgres membrane: %+v", m)
	}
}

func TestNewUsesInjectedSQLiteOpenAndEnvEncryptionKey(t *testing.T) {
	oldOpenSQLite := openSQLiteStore
	defer func() { openSQLiteStore = oldOpenSQLite }()

	var gotPath, gotKey string
	openSQLiteStore = func(path, key string) (storage.Store, error) {
		gotPath = path
		gotKey = key
		return nil, errors.New("forced sqlite open")
	}

	t.Setenv("MEMBRANE_ENCRYPTION_KEY", "env-sqlite-key")
	cfg := DefaultConfig()
	cfg.DBPath = "custom.db"
	_, err := New(cfg)
	if err == nil || !strings.Contains(err.Error(), "open sqlite store") {
		t.Fatalf("New injected sqlite error = %v, want wrapped open error", err)
	}
	if gotPath != "custom.db" || gotKey != "env-sqlite-key" {
		t.Fatalf("sqlite open args = %q/%q, want custom.db/env-sqlite-key", gotPath, gotKey)
	}
}

func TestNewSQLiteConfiguresIngestLLMFromEnv(t *testing.T) {
	t.Setenv("MEMBRANE_INGEST_LLM_API_KEY", "env-key")
	cfg := DefaultConfig()
	cfg.DBPath = filepath.Join(t.TempDir(), "membrane.db")
	cfg.IngestLLMEnabled = true
	cfg.IngestLLMEndpoint = "http://127.0.0.1:1/interpret"
	cfg.IngestLLMModel = "test-model"

	m, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = m.Stop() })
	if m.ingestion == nil {
		t.Fatalf("ingestion service = nil, want configured service")
	}
}

func TestRetrieveGraphAppliesConfigDefaultsAndMetricsDelegate(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DBPath = filepath.Join(t.TempDir(), "membrane.db")
	cfg.GraphDefaultRootLimit = 3
	cfg.GraphDefaultNodeLimit = 4
	cfg.GraphDefaultEdgeLimit = 5
	cfg.GraphDefaultMaxHops = 2

	m, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = m.Stop() })

	req := &retrieval.RetrieveGraphRequest{
		Trust: retrieval.NewTrustContext(schema.SensitivityLow, true, "tester", nil),
	}
	resp, err := m.RetrieveGraph(context.Background(), req)
	if err != nil {
		t.Fatalf("RetrieveGraph: %v", err)
	}
	if resp == nil {
		t.Fatal("RetrieveGraph response = nil")
	}
	if req.RootLimit != 3 || req.NodeLimit != 4 || req.EdgeLimit != 5 || req.MaxHops != 2 {
		t.Fatalf("request limits = root:%d node:%d edge:%d hops:%d, want config defaults", req.RootLimit, req.NodeLimit, req.EdgeLimit, req.MaxHops)
	}

	snapshot, err := m.GetMetrics(context.Background())
	if err != nil {
		t.Fatalf("GetMetrics: %v", err)
	}
	if snapshot.TotalRecords != 0 {
		t.Fatalf("TotalRecords = %d, want 0", snapshot.TotalRecords)
	}
}

func TestRetrieveGraphNilRequestStillRequiresTrust(t *testing.T) {
	m := newTestMembrane(t)

	_, err := m.RetrieveGraph(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "trust context is required") {
		t.Fatalf("RetrieveGraph nil request error = %v, want trust context error", err)
	}
}

func TestRetrieveGraphNegativeMaxHopsDisablesExpansion(t *testing.T) {
	ctx := context.Background()
	m := newTestMembrane(t)

	root := captureSemanticDelegateRecord(t, ctx, m, "orchid", "deploys_to", "staging")
	neighbor := captureSemanticDelegateRecord(t, ctx, m, "borealis", "supports", "orchid")
	if err := m.store.AddRelation(ctx, root.ID, schema.Relation{Predicate: "related_to", TargetID: neighbor.ID, Weight: 1}); err != nil {
		t.Fatalf("AddRelation: %v", err)
	}

	req := &retrieval.RetrieveGraphRequest{
		Trust:       retrieval.NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		RootLimit:   1,
		NodeLimit:   5,
		MaxHops:     -1,
	}
	resp, err := m.RetrieveGraph(ctx, req)
	if err != nil {
		t.Fatalf("RetrieveGraph: %v", err)
	}
	if req.MaxHops != 0 {
		t.Fatalf("MaxHops after call = %d, want 0", req.MaxHops)
	}
	if len(resp.Nodes) != 1 || len(resp.Edges) != 0 {
		t.Fatalf("graph nodes/edges = %d/%d, want one root and no expansion", len(resp.Nodes), len(resp.Edges))
	}
}

func TestStartBackfillsEmbeddingsWhenConfigured(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := newTestMembrane(t)

	rec := captureSemanticDelegateRecord(t, ctx, m, "orchid", "deploys_to", "staging")
	vectorStore := &fakeMembraneVectorStore{stored: make(chan string, 10)}
	m.embedding = embedding.NewService(fakeMembraneEmbeddingClient{}, m.store, vectorStore, "test-model")

	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer m.consolScheduler.Stop()
	defer m.decayScheduler.Stop()

	deadline := time.After(time.Second)
	for {
		select {
		case id := <-vectorStore.stored:
			if id == rec.ID {
				return
			}
		case <-deadline:
			t.Fatalf("embedding backfill did not store semantic record %q", rec.ID)
		}
	}
}

func TestMembraneTopLevelDelegates(t *testing.T) {
	ctx := context.Background()
	m := newTestMembrane(t)

	event, err := m.CaptureMemory(ctx, ingestion.CaptureMemoryRequest{
		Source:           "tester",
		SourceKind:       "event",
		Content:          map[string]any{"ref": "evt-1", "text": "deploy completed"},
		ReasonToRemember: "deployment outcome should be recorded",
		Summary:          "deploy completed",
		Scope:            "project:alpha",
		Sensitivity:      schema.SensitivityLow,
	})
	if err != nil {
		t.Fatalf("CaptureMemory event: %v", err)
	}
	if event.PrimaryRecord == nil || event.PrimaryRecord.ID == "" {
		t.Fatalf("CaptureMemory primary = %+v, want persisted record", event.PrimaryRecord)
	}

	outcome, err := m.RecordOutcome(ctx, ingestion.IngestOutcomeRequest{
		Source:         "tester",
		TargetRecordID: event.PrimaryRecord.ID,
		OutcomeStatus:  schema.OutcomeStatusSuccess,
	})
	if err != nil {
		t.Fatalf("RecordOutcome: %v", err)
	}
	if payload := outcome.Payload.(*schema.EpisodicPayload); payload.Outcome != schema.OutcomeStatusSuccess {
		t.Fatalf("outcome payload = %+v, want success", payload)
	}

	trust := retrieval.NewTrustContext(schema.SensitivityLow, true, "tester", []string{"project:alpha"})
	got, err := m.RetrieveByID(ctx, event.PrimaryRecord.ID, trust)
	if err != nil {
		t.Fatalf("RetrieveByID: %v", err)
	}
	if got.ID != event.PrimaryRecord.ID {
		t.Fatalf("RetrieveByID ID = %q, want %q", got.ID, event.PrimaryRecord.ID)
	}
	if err := m.Reinforce(ctx, event.PrimaryRecord.ID, "tester", "useful"); err != nil {
		t.Fatalf("Reinforce: %v", err)
	}
	if err := m.Penalize(ctx, event.PrimaryRecord.ID, 0.05, "tester", "less relevant"); err != nil {
		t.Fatalf("Penalize: %v", err)
	}

	old := captureSemanticDelegateRecord(t, ctx, m, "api", "rate_limit", "50 rps")
	replacement, err := m.Supersede(ctx, old.ID, semanticDelegateRecord("", "api", "rate_limit", "200 rps"), "tester", "rate limit changed")
	if err != nil {
		t.Fatalf("Supersede: %v", err)
	}
	if replacement.ID == "" {
		t.Fatal("Supersede replacement ID = empty")
	}

	source := captureSemanticDelegateRecord(t, ctx, m, "database", "primary", "postgres")
	forked, err := m.Fork(ctx, source.ID, semanticDelegateRecord("", "database", "primary", "sqlite"), "tester", "local variant")
	if err != nil {
		t.Fatalf("Fork: %v", err)
	}
	if forked.ID == "" {
		t.Fatal("Fork ID = empty")
	}

	first := captureSemanticDelegateRecord(t, ctx, m, "editor", "preferred", "vim")
	second := captureSemanticDelegateRecord(t, ctx, m, "editor", "preferred", "neovim")
	merged, err := m.Merge(ctx, []string{first.ID, second.ID}, semanticDelegateRecord("", "editor", "preferred", "neovim"), "tester", "merge preferences")
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if len(merged.Relations) != 2 {
		t.Fatalf("merged relations = %+v, want two source links", merged.Relations)
	}

	contested := captureSemanticDelegateRecord(t, ctx, m, "queue", "primary", "sqs")
	contesting := captureSemanticDelegateRecord(t, ctx, m, "queue", "primary", "kafka")
	if err := m.Contest(ctx, contested.ID, contesting.ID, "tester", "conflicting source"); err != nil {
		t.Fatalf("Contest: %v", err)
	}
	if err := m.Retract(ctx, forked.ID, "tester", "variant obsolete"); err != nil {
		t.Fatalf("Retract: %v", err)
	}
}

func newTestMembrane(t *testing.T) *Membrane {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DBPath = filepath.Join(t.TempDir(), "membrane.db")
	m, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = m.Stop() })
	return m
}

func captureSemanticDelegateRecord(t *testing.T, ctx context.Context, m *Membrane, subject, predicate string, object any) *schema.MemoryRecord {
	t.Helper()
	resp, err := m.CaptureMemory(ctx, ingestion.CaptureMemoryRequest{
		Source:      "tester",
		SourceKind:  "observation",
		Content:     map[string]any{"subject": subject, "predicate": predicate, "object": object},
		Scope:       "project:alpha",
		Sensitivity: schema.SensitivityLow,
	})
	if err != nil {
		t.Fatalf("CaptureMemory semantic: %v", err)
	}
	for _, rec := range resp.CreatedRecords {
		if rec != nil && rec.Type == schema.MemoryTypeSemantic {
			return rec
		}
	}
	t.Fatalf("created records = %+v, want semantic record", resp.CreatedRecords)
	return nil
}

func semanticDelegateRecord(id, subject, predicate string, object any) *schema.MemoryRecord {
	rec := schema.NewMemoryRecord(id, schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   subject,
		Predicate: predicate,
		Object:    object,
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
		Evidence: []schema.ProvenanceRef{{
			SourceType: "observation",
			SourceID:   "delegate-test",
		}},
	})
	rec.Scope = "project:alpha"
	return rec
}

type fakeMembraneEmbeddingClient struct{}

func (fakeMembraneEmbeddingClient) Embed(context.Context, string) ([]float32, error) {
	return []float32{1, 0}, nil
}

type fakeMembraneVectorStore struct {
	stored chan string
}

func (f *fakeMembraneVectorStore) GetTriggerEmbedding(context.Context, string) ([]float32, error) {
	return nil, nil
}

func (f *fakeMembraneVectorStore) StoreTriggerEmbedding(_ context.Context, recordID string, _ []float32, _ string) error {
	select {
	case f.stored <- recordID:
	default:
	}
	return nil
}
