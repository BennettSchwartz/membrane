package membrane

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/internal/teststore"
	"github.com/BennettSchwartz/membrane/pkg/consolidation"
	"github.com/BennettSchwartz/membrane/pkg/decay"
	"github.com/BennettSchwartz/membrane/pkg/embedding"
	"github.com/BennettSchwartz/membrane/pkg/ingestion"
	"github.com/BennettSchwartz/membrane/pkg/metrics"
	"github.com/BennettSchwartz/membrane/pkg/retrieval"
	"github.com/BennettSchwartz/membrane/pkg/revision"
	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage/postgres"
)

func TestNewRequiresPostgresDSNByDefault(t *testing.T) {
	cfg := DefaultConfig()
	cfg.PostgresDSN = ""
	t.Setenv("MEMBRANE_POSTGRES_DSN", "")

	if _, err := New(cfg); err == nil || !strings.Contains(err.Error(), "postgres_dsn is required") {
		t.Fatalf("New default config error = %v, want postgres_dsn is required", err)
	}
}

func TestNewNilConfigUsesPostgresDefaults(t *testing.T) {
	t.Setenv("MEMBRANE_POSTGRES_DSN", "")

	if _, err := New(nil); err == nil || !strings.Contains(err.Error(), "postgres_dsn is required") {
		t.Fatalf("New nil config error = %v, want postgres_dsn is required", err)
	}
}

func TestNewRejectsInvalidRuntimeConfig(t *testing.T) {
	oldOpenPostgres := openPostgresStore
	defer func() { openPostgresStore = oldOpenPostgres }()
	openPostgresStore = func(string, postgres.EmbeddingConfig) (*postgres.PostgresStore, error) {
		return nil, errors.New("unexpected postgres open")
	}

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
			name: "postgres missing dsn",
			cfg: func() *Config {
				cfg := DefaultConfig()
				cfg.PostgresDSN = ""
				return cfg
			}(),
			want: "postgres_dsn is required",
		},
		{
			name: "postgres whitespace dsn",
			cfg: func() *Config {
				cfg := DefaultConfig()
				cfg.PostgresDSN = " \t "
				return cfg
			}(),
			want: "postgres_dsn is required",
		},
		{
			name: "zero decay interval",
			cfg: func() *Config {
				cfg := DefaultConfig()
				cfg.PostgresDSN = "postgres://fake/db"
				cfg.DecayInterval = 0
				return cfg
			}(),
			want: "decay_interval must be positive",
		},
		{
			name: "negative consolidation interval",
			cfg: func() *Config {
				cfg := DefaultConfig()
				cfg.PostgresDSN = "postgres://fake/db"
				cfg.ConsolidationInterval = -time.Second
				return cfg
			}(),
			want: "consolidation_interval must be positive",
		},
		{
			name: "selection threshold outside unit interval",
			cfg: func() *Config {
				cfg := DefaultConfig()
				cfg.PostgresDSN = "postgres://fake/db"
				cfg.SelectionConfidenceThreshold = 1.5
				return cfg
			}(),
			want: "selection_confidence_threshold must be finite and between 0 and 1",
		},
		{
			name: "selection threshold nan",
			cfg: func() *Config {
				cfg := DefaultConfig()
				cfg.PostgresDSN = "postgres://fake/db"
				cfg.SelectionConfidenceThreshold = math.NaN()
				return cfg
			}(),
			want: "selection_confidence_threshold must be finite and between 0 and 1",
		},
		{
			name: "embedding dimensions not positive",
			cfg: func() *Config {
				cfg := DefaultConfig()
				cfg.PostgresDSN = "postgres://fake/db"
				cfg.EmbeddingDimensions = 0
				return cfg
			}(),
			want: "embedding_dimensions must be positive",
		},
		{
			name: "embedding endpoint without model",
			cfg: func() *Config {
				cfg := DefaultConfig()
				cfg.PostgresDSN = "postgres://fake/db"
				cfg.EmbeddingEndpoint = "http://127.0.0.1:1/embeddings"
				return cfg
			}(),
			want: "embedding_model is required when embedding_endpoint is set",
		},
		{
			name: "embedding model without endpoint",
			cfg: func() *Config {
				cfg := DefaultConfig()
				cfg.PostgresDSN = "postgres://fake/db"
				cfg.EmbeddingModel = "text-embedding-test"
				return cfg
			}(),
			want: "embedding_endpoint is required when embedding_model is set",
		},
		{
			name: "llm endpoint without model",
			cfg: func() *Config {
				cfg := DefaultConfig()
				cfg.PostgresDSN = "postgres://fake/db"
				cfg.LLMEndpoint = "http://127.0.0.1:1/chat"
				return cfg
			}(),
			want: "llm_model is required when llm_endpoint is set",
		},
		{
			name: "llm model without endpoint",
			cfg: func() *Config {
				cfg := DefaultConfig()
				cfg.PostgresDSN = "postgres://fake/db"
				cfg.LLMModel = "test-model"
				return cfg
			}(),
			want: "llm_endpoint is required when llm_model is set",
		},
		{
			name: "ingest llm enabled without endpoint",
			cfg: func() *Config {
				cfg := DefaultConfig()
				cfg.PostgresDSN = "postgres://fake/db"
				cfg.IngestLLMEnabled = true
				cfg.IngestLLMModel = "test-model"
				return cfg
			}(),
			want: "ingest_llm_endpoint is required when ingest_llm_enabled is true",
		},
		{
			name: "ingest llm enabled without model",
			cfg: func() *Config {
				cfg := DefaultConfig()
				cfg.PostgresDSN = "postgres://fake/db"
				cfg.IngestLLMEnabled = true
				cfg.IngestLLMEndpoint = "http://127.0.0.1:1/interpret"
				return cfg
			}(),
			want: "ingest_llm_model is required when ingest_llm_enabled is true",
		},
		{
			name: "negative graph default",
			cfg: func() *Config {
				cfg := DefaultConfig()
				cfg.PostgresDSN = "postgres://fake/db"
				cfg.GraphDefaultNodeLimit = -1
				return cfg
			}(),
			want: "graph_default_node_limit must be non-negative",
		},
		{
			name: "graph default exceeds hard service ceiling",
			cfg: func() *Config {
				cfg := DefaultConfig()
				cfg.PostgresDSN = "postgres://fake/db"
				cfg.GraphDefaultEdgeLimit = retrieval.MaxGraphLimit + 1
				return cfg
			}(),
			want: "graph_default_edge_limit must be at most 10000",
		},
		{
			name: "tls cert without key",
			cfg: func() *Config {
				cfg := DefaultConfig()
				cfg.PostgresDSN = "postgres://fake/db"
				cfg.TLSCertFile = "/tmp/membrane.crt"
				return cfg
			}(),
			want: "tls_cert_file and tls_key_file must be configured together",
		},
		{
			name: "tls key without cert",
			cfg: func() *Config {
				cfg := DefaultConfig()
				cfg.PostgresDSN = "postgres://fake/db"
				cfg.TLSKeyFile = "/tmp/membrane.key"
				return cfg
			}(),
			want: "tls_cert_file and tls_key_file must be configured together",
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

func TestNewTrimsPostgresDSNBeforeOpen(t *testing.T) {
	oldOpenPostgres := openPostgresStore
	defer func() { openPostgresStore = oldOpenPostgres }()

	var gotDSN string
	openPostgresStore = func(dsn string, _ postgres.EmbeddingConfig) (*postgres.PostgresStore, error) {
		gotDSN = dsn
		return nil, errors.New("open failed")
	}

	cfg := DefaultConfig()
	cfg.PostgresDSN = "  postgres://trimmed/db  "
	m, err := New(cfg)
	if err == nil || !strings.Contains(err.Error(), "open postgres store") {
		t.Fatalf("New trimmed DSN error = %v, want open postgres store", err)
	}
	if m != nil {
		t.Fatalf("New trimmed DSN membrane = %+v, want nil", m)
	}
	if gotDSN != "postgres://trimmed/db" || cfg.PostgresDSN != "postgres://trimmed/db" {
		t.Fatalf("trimmed DSN got open=%q cfg=%q", gotDSN, cfg.PostgresDSN)
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

func TestNewUsesRuntimeEndpointEnvFallbacksBeforeValidation(t *testing.T) {
	oldOpenPostgres := openPostgresStore
	defer func() { openPostgresStore = oldOpenPostgres }()

	var gotEmbeddingConfig postgres.EmbeddingConfig
	openPostgresStore = func(_ string, cfg postgres.EmbeddingConfig) (*postgres.PostgresStore, error) {
		gotEmbeddingConfig = cfg
		return &postgres.PostgresStore{}, nil
	}

	t.Setenv("MEMBRANE_EMBEDDING_ENDPOINT", " http://127.0.0.1:1/embeddings ")
	t.Setenv("MEMBRANE_EMBEDDING_MODEL", " text-embedding-env ")
	t.Setenv("MEMBRANE_EMBEDDING_DIMENSIONS", "768")
	t.Setenv("MEMBRANE_LLM_ENDPOINT", " http://127.0.0.1:1/chat ")
	t.Setenv("MEMBRANE_LLM_MODEL", " llm-env ")
	t.Setenv("MEMBRANE_INGEST_LLM_ENDPOINT", " http://127.0.0.1:1/interpret ")
	t.Setenv("MEMBRANE_INGEST_LLM_MODEL", " ingest-env ")

	cfg := DefaultConfig()
	cfg.PostgresDSN = "postgres://fake/db"
	cfg.IngestLLMEnabled = true
	m, err := New(cfg)
	if err != nil {
		t.Fatalf("New with endpoint env fallbacks: %v", err)
	}
	t.Cleanup(func() { _ = m.Stop() })

	if cfg.EmbeddingEndpoint != "http://127.0.0.1:1/embeddings" || cfg.EmbeddingModel != "text-embedding-env" {
		t.Fatalf("embedding env fallback = %q/%q", cfg.EmbeddingEndpoint, cfg.EmbeddingModel)
	}
	if gotEmbeddingConfig.Model != "text-embedding-env" {
		t.Fatalf("postgres embedding model = %q, want env model", gotEmbeddingConfig.Model)
	}
	if cfg.EmbeddingDimensions != 768 || gotEmbeddingConfig.Dimensions != 768 {
		t.Fatalf("embedding dimensions env fallback = cfg:%d postgres:%d, want 768", cfg.EmbeddingDimensions, gotEmbeddingConfig.Dimensions)
	}
	if cfg.LLMEndpoint != "http://127.0.0.1:1/chat" || cfg.LLMModel != "llm-env" {
		t.Fatalf("llm env fallback = %q/%q", cfg.LLMEndpoint, cfg.LLMModel)
	}
	if cfg.IngestLLMEndpoint != "http://127.0.0.1:1/interpret" || cfg.IngestLLMModel != "ingest-env" {
		t.Fatalf("ingest llm env fallback = %q/%q", cfg.IngestLLMEndpoint, cfg.IngestLLMModel)
	}
	if m.embedding == nil || m.ingestion == nil || m.consolidation == nil {
		t.Fatalf("New returned incomplete env-backed membrane: %+v", m)
	}
}

func TestNewValidatesPartialEndpointEnvBeforeOpeningPostgres(t *testing.T) {
	oldOpenPostgres := openPostgresStore
	defer func() { openPostgresStore = oldOpenPostgres }()

	opened := false
	openPostgresStore = func(string, postgres.EmbeddingConfig) (*postgres.PostgresStore, error) {
		opened = true
		return &postgres.PostgresStore{}, nil
	}

	t.Setenv("MEMBRANE_EMBEDDING_ENDPOINT", "http://127.0.0.1:1/embeddings")
	t.Setenv("MEMBRANE_EMBEDDING_MODEL", "")
	cfg := DefaultConfig()
	cfg.PostgresDSN = "postgres://fake/db"

	if _, err := New(cfg); err == nil || !strings.Contains(err.Error(), "embedding_model is required") {
		t.Fatalf("New partial embedding env error = %v, want embedding_model is required", err)
	}
	if opened {
		t.Fatal("postgres opened before partial embedding env validation failed")
	}
}

func TestNewValidatesEmbeddingDimensionsEnvBeforeOpeningPostgres(t *testing.T) {
	oldOpenPostgres := openPostgresStore
	defer func() { openPostgresStore = oldOpenPostgres }()

	opened := false
	openPostgresStore = func(string, postgres.EmbeddingConfig) (*postgres.PostgresStore, error) {
		opened = true
		return &postgres.PostgresStore{}, nil
	}

	t.Setenv("MEMBRANE_EMBEDDING_DIMENSIONS", "not-an-int")
	cfg := DefaultConfig()
	cfg.PostgresDSN = "postgres://fake/db"

	if _, err := New(cfg); err == nil || !strings.Contains(err.Error(), "invalid MEMBRANE_EMBEDDING_DIMENSIONS") {
		t.Fatalf("New invalid embedding dimensions env error = %v, want invalid MEMBRANE_EMBEDDING_DIMENSIONS", err)
	}
	if opened {
		t.Fatal("postgres opened before embedding dimensions env validation failed")
	}
}

func TestNewPostgresConfiguresIngestLLMFromEnv(t *testing.T) {
	oldOpenPostgres := openPostgresStore
	defer func() { openPostgresStore = oldOpenPostgres }()

	openPostgresStore = func(string, postgres.EmbeddingConfig) (*postgres.PostgresStore, error) {
		return &postgres.PostgresStore{}, nil
	}

	t.Setenv("MEMBRANE_INGEST_LLM_API_KEY", "env-key")
	cfg := DefaultConfig()
	cfg.PostgresDSN = "postgres://fake/db"
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
	m := newTestMembrane(t)
	m.config.GraphDefaultRootLimit = 3
	m.config.GraphDefaultNodeLimit = 4
	m.config.GraphDefaultEdgeLimit = 5
	m.config.GraphDefaultMaxHops = 2

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
	if req.RootLimit != 0 || req.NodeLimit != 0 || req.EdgeLimit != 0 || req.MaxHops != 0 {
		t.Fatalf("request was mutated to root:%d node:%d edge:%d hops:%d, want caller request unchanged", req.RootLimit, req.NodeLimit, req.EdgeLimit, req.MaxHops)
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
	if req.MaxHops != -1 {
		t.Fatalf("MaxHops after call = %d, want caller request unchanged at -1", req.MaxHops)
	}
	if len(resp.Nodes) != 1 || len(resp.Edges) != 0 {
		t.Fatalf("graph nodes/edges = %d/%d, want one root and no expansion", len(resp.Nodes), len(resp.Edges))
	}

	req.MaxHops = -2
	if _, err := m.RetrieveGraph(ctx, req); err == nil || !strings.Contains(err.Error(), "max_hops") {
		t.Fatalf("RetrieveGraph invalid max hops error = %v, want max_hops validation error", err)
	}
}

func TestRetrieveGraphRejectsNegativeLimitsWithoutMutatingRequest(t *testing.T) {
	ctx := context.Background()
	m := newTestMembrane(t)

	for _, tc := range []struct {
		name string
		req  retrieval.RetrieveGraphRequest
		want string
	}{
		{name: "root", req: retrieval.RetrieveGraphRequest{RootLimit: -1}, want: "root_limit"},
		{name: "node", req: retrieval.RetrieveGraphRequest{NodeLimit: -1}, want: "node_limit"},
		{name: "edge", req: retrieval.RetrieveGraphRequest{EdgeLimit: -1}, want: "edge_limit"},
	} {
		tc.req.Trust = retrieval.NewTrustContext(schema.SensitivityLow, true, "tester", nil)
		req := tc.req
		if _, err := m.RetrieveGraph(ctx, &req); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s negative limit error = %v, want %s validation error", tc.name, err, tc.want)
		}
		if req.RootLimit != tc.req.RootLimit || req.NodeLimit != tc.req.NodeLimit || req.EdgeLimit != tc.req.EdgeLimit || req.MaxHops != tc.req.MaxHops || req.Trust != tc.req.Trust {
			t.Fatalf("%s request mutated to %+v, want %+v", tc.name, req, tc.req)
		}
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

func TestStartRunsEmbeddingBackfillOnce(t *testing.T) {
	ctx := context.Background()
	m := newTestMembrane(t)
	captureSemanticDelegateRecord(t, ctx, m, "orchid", "deploys_to", "staging")

	release := make(chan struct{})
	vectorStore := &blockingMembraneVectorStore{
		started: make(chan string, 2),
		release: release,
	}
	m.embedding = embedding.NewService(fakeMembraneEmbeddingClient{}, m.store, vectorStore, "test-model")

	if err := m.Start(ctx); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	if err := m.Start(ctx); err != nil {
		t.Fatalf("second Start: %v", err)
	}

	select {
	case id := <-vectorStore.started:
		if id == "" {
			t.Fatal("backfilled ID = empty")
		}
	case <-time.After(time.Second):
		t.Fatal("embedding backfill did not start")
	}

	select {
	case id := <-vectorStore.started:
		t.Fatalf("second Start launched duplicate embedding backfill for %q", id)
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	if err := m.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestStopCancelsAndWaitsForEmbeddingBackfillBeforeClosingStore(t *testing.T) {
	ctx := context.Background()
	m := newTestMembrane(t)

	store := &trackedCloseStore{
		MemoryStore: teststore.NewMemoryStore(),
		closeCalled: make(chan struct{}),
	}
	rec := semanticDelegateRecord("backfill-stop-order", "orchid", "deploys_to", "staging")
	if err := store.Create(ctx, rec); err != nil {
		t.Fatalf("create record: %v", err)
	}
	m.store = store

	vectorStore := &closeOrderingVectorStore{
		entered:     make(chan struct{}),
		finished:    make(chan struct{}),
		closeCalled: store.closeCalled,
	}
	m.embedding = embedding.NewService(fakeMembraneEmbeddingClient{}, store, vectorStore, "test-model")

	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	select {
	case <-vectorStore.entered:
	case <-time.After(time.Second):
		t.Fatal("embedding backfill did not enter vector store")
	}

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- m.Stop()
	}()

	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("Stop: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop did not return after canceling embedding backfill")
	}
	select {
	case <-vectorStore.finished:
	default:
		t.Fatal("Stop returned before embedding backfill finished")
	}
	if vectorStore.closedBeforeReturn {
		t.Fatal("store closed before embedding backfill returned")
	}
	select {
	case <-store.closeCalled:
	default:
		t.Fatal("Stop returned without closing the store")
	}
}

func TestStartAfterStopReturnsError(t *testing.T) {
	m := newTestMembrane(t)
	if err := m.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := m.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "cannot start after stop") {
		t.Fatalf("Start after Stop error = %v, want cannot start after stop", err)
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
	forked, err := m.Fork(ctx, source.ID, semanticDelegateRecord("", "database", "primary", "standby-postgres"), "tester", "local variant")
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
	store := teststore.NewMemoryStore()

	classifier := ingestion.NewClassifier()
	policyDefaults := ingestion.DefaultPolicyDefaults()
	policyEngine := ingestion.NewPolicyEngine(policyDefaults)
	ingestionSvc := ingestion.NewService(store, classifier, policyEngine)
	selector := retrieval.NewSelector(cfg.SelectionConfidenceThreshold)
	retrievalSvc := retrieval.NewService(store, selector)
	decaySvc := decay.NewService(store)
	consolidationSvc := consolidation.NewService(store)

	m := &Membrane{
		config:          cfg,
		store:           store,
		ingestion:       ingestionSvc,
		retrieval:       retrievalSvc,
		decay:           decaySvc,
		revision:        revision.NewService(store),
		consolidation:   consolidationSvc,
		metrics:         metrics.NewCollector(store),
		decayScheduler:  decay.NewScheduler(decaySvc, cfg.DecayInterval),
		consolScheduler: consolidation.NewScheduler(consolidationSvc, cfg.ConsolidationInterval),
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

type blockingMembraneVectorStore struct {
	started chan string
	release <-chan struct{}
}

func (f *blockingMembraneVectorStore) GetTriggerEmbedding(context.Context, string) ([]float32, error) {
	return nil, nil
}

func (f *blockingMembraneVectorStore) StoreTriggerEmbedding(ctx context.Context, recordID string, _ []float32, _ string) error {
	select {
	case f.started <- recordID:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-f.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type trackedCloseStore struct {
	*teststore.MemoryStore
	closeCalled chan struct{}
	closeOnce   sync.Once
}

func (s *trackedCloseStore) Close() error {
	s.closeOnce.Do(func() {
		close(s.closeCalled)
	})
	return s.MemoryStore.Close()
}

type closeOrderingVectorStore struct {
	enterOnce sync.Once
	doneOnce  sync.Once

	entered     chan struct{}
	finished    chan struct{}
	closeCalled <-chan struct{}

	closedBeforeReturn bool
}

func (f *closeOrderingVectorStore) GetTriggerEmbedding(context.Context, string) ([]float32, error) {
	return nil, nil
}

func (f *closeOrderingVectorStore) StoreTriggerEmbedding(ctx context.Context, _ string, _ []float32, _ string) error {
	f.enterOnce.Do(func() {
		close(f.entered)
	})
	<-ctx.Done()
	select {
	case <-f.closeCalled:
		f.closedBeforeReturn = true
	default:
	}
	f.doneOnce.Do(func() {
		close(f.finished)
	})
	return ctx.Err()
}
