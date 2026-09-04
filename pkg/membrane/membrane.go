package membrane

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/BennettSchwartz/membrane/pkg/consolidation"
	"github.com/BennettSchwartz/membrane/pkg/decay"
	"github.com/BennettSchwartz/membrane/pkg/embedding"
	"github.com/BennettSchwartz/membrane/pkg/ingestion"
	"github.com/BennettSchwartz/membrane/pkg/metrics"
	"github.com/BennettSchwartz/membrane/pkg/retrieval"
	"github.com/BennettSchwartz/membrane/pkg/revision"
	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
	"github.com/BennettSchwartz/membrane/pkg/storage/postgres"
)

// Membrane wires all subsystems together and exposes the unified API surface.
type Membrane struct {
	config *Config
	store  storage.Store

	ingestion     *ingestion.Service
	retrieval     *retrieval.Service
	decay         *decay.Service
	revision      *revision.Service
	consolidation *consolidation.Service
	metrics       *metrics.Collector
	embedding     *embedding.Service

	decayScheduler  *decay.Scheduler
	consolScheduler *consolidation.Scheduler

	lifecycleMu      sync.Mutex
	started          bool
	stopped          bool
	backgroundCancel context.CancelFunc
	backgroundWG     sync.WaitGroup
}

var (
	openPostgresStore = postgres.Open
)

// New initialises all subsystems from the provided Config and returns a
// ready-to-start Membrane instance.
func New(cfg *Config) (*Membrane, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	if err := applyRuntimeEnv(cfg); err != nil {
		return nil, err
	}
	if err := validateRuntimeConfig(cfg); err != nil {
		return nil, err
	}

	var (
		store   storage.Store
		pgStore *postgres.PostgresStore
		err     error
	)
	if cfg.PostgresDSN == "" {
		return nil, fmt.Errorf("membrane: postgres_dsn is required")
	}
	pgStore, err = openPostgresStore(cfg.PostgresDSN, postgres.EmbeddingConfig{
		Dimensions: cfg.EmbeddingDimensions,
		Model:      cfg.EmbeddingModel,
	})
	if err != nil {
		return nil, fmt.Errorf("membrane: open postgres store: %w", err)
	}
	store = pgStore

	// Ingestion
	classifier := ingestion.NewClassifier()
	policyDefaults := ingestion.DefaultPolicyDefaults()
	policyDefaults.Sensitivity = schema.Sensitivity(cfg.DefaultSensitivity)
	policyEngine := ingestion.NewPolicyEngine(policyDefaults)
	var interpreter ingestion.Interpreter
	if cfg.IngestLLMEnabled && cfg.IngestLLMEndpoint != "" && cfg.IngestLLMModel != "" {
		apiKey := cfg.IngestLLMAPIKey
		if apiKey == "" {
			apiKey = os.Getenv("MEMBRANE_INGEST_LLM_API_KEY")
		}
		interpreter = ingestion.NewHTTPInterpreter(cfg.IngestLLMEndpoint, cfg.IngestLLMModel, apiKey)
	}
	var ingestionSvc *ingestion.Service
	if interpreter != nil {
		ingestionSvc = ingestion.NewServiceWithInterpreter(store, classifier, policyEngine, interpreter)
	} else {
		ingestionSvc = ingestion.NewService(store, classifier, policyEngine)
	}

	var embService *embedding.Service
	if pgStore != nil && cfg.EmbeddingEndpoint != "" && cfg.EmbeddingModel != "" {
		apiKey := cfg.EmbeddingAPIKey
		if apiKey == "" {
			apiKey = os.Getenv("MEMBRANE_EMBEDDING_API_KEY")
		}
		embClient := embedding.NewHTTPClient(
			cfg.EmbeddingEndpoint,
			cfg.EmbeddingModel,
			apiKey,
			cfg.EmbeddingDimensions,
		)
		embService = embedding.NewService(embClient, store, pgStore, cfg.EmbeddingModel)
	}

	var llmClient consolidation.LLMClient
	if pgStore != nil && cfg.LLMEndpoint != "" && cfg.LLMModel != "" {
		apiKey := cfg.LLMAPIKey
		if apiKey == "" {
			apiKey = os.Getenv("MEMBRANE_LLM_API_KEY")
		}
		llmClient = consolidation.NewHTTPLLMClient(cfg.LLMEndpoint, cfg.LLMModel, apiKey)
	}

	// Retrieval
	var selector *retrieval.Selector
	var retrievalSvc *retrieval.Service
	if embService != nil && pgStore != nil {
		selector = retrieval.NewSelectorWithEmbedding(cfg.SelectionConfidenceThreshold, embService)
		retrievalSvc = retrieval.NewServiceWithVectorRanker(store, selector, embService, pgStore)
	} else {
		selector = retrieval.NewSelector(cfg.SelectionConfidenceThreshold)
		retrievalSvc = retrieval.NewService(store, selector)
	}

	// Decay
	decaySvc := decay.NewService(store)
	decayScheduler := decay.NewScheduler(decaySvc, cfg.DecayInterval)

	// Revision
	var revisionSvc *revision.Service
	if embService != nil {
		revisionSvc = revision.NewServiceWithEmbedder(store, embService)
	} else {
		revisionSvc = revision.NewService(store)
	}

	// Consolidation
	var consolidationSvc *consolidation.Service
	if llmClient != nil {
		consolidationSvc = consolidation.NewServiceWithExtractor(store, embService, decaySvc, llmClient, pgStore)
	} else if embService != nil {
		consolidationSvc = consolidation.NewServiceWithEmbedder(store, embService)
	} else {
		consolidationSvc = consolidation.NewService(store)
	}
	consolScheduler := consolidation.NewScheduler(consolidationSvc, cfg.ConsolidationInterval)

	// Metrics
	metricsCollector := metrics.NewCollector(store)

	return &Membrane{
		config:          cfg,
		store:           store,
		ingestion:       ingestionSvc,
		retrieval:       retrievalSvc,
		decay:           decaySvc,
		revision:        revisionSvc,
		consolidation:   consolidationSvc,
		metrics:         metricsCollector,
		embedding:       embService,
		decayScheduler:  decayScheduler,
		consolScheduler: consolScheduler,
	}, nil
}

func applyRuntimeEnv(cfg *Config) error {
	applyStringEnv(&cfg.PostgresDSN, "MEMBRANE_POSTGRES_DSN")
	applyStringEnv(&cfg.EmbeddingEndpoint, "MEMBRANE_EMBEDDING_ENDPOINT")
	applyStringEnv(&cfg.EmbeddingModel, "MEMBRANE_EMBEDDING_MODEL")
	if err := applyDefaultIntEnv(&cfg.EmbeddingDimensions, defaultEmbeddingDimensions, "MEMBRANE_EMBEDDING_DIMENSIONS"); err != nil {
		return err
	}
	applyStringEnv(&cfg.LLMEndpoint, "MEMBRANE_LLM_ENDPOINT")
	applyStringEnv(&cfg.LLMModel, "MEMBRANE_LLM_MODEL")
	applyStringEnv(&cfg.IngestLLMEndpoint, "MEMBRANE_INGEST_LLM_ENDPOINT")
	applyStringEnv(&cfg.IngestLLMModel, "MEMBRANE_INGEST_LLM_MODEL")
	applyStringEnv(&cfg.ReadMaxSensitivity, "MEMBRANE_READ_MAX_SENSITIVITY")
	applyStringEnv(&cfg.WriteMaxSensitivity, "MEMBRANE_WRITE_MAX_SENSITIVITY")
	applyStringSliceEnv(&cfg.ReadScopes, "MEMBRANE_READ_SCOPES")
	applyStringSliceEnv(&cfg.WriteScopes, "MEMBRANE_WRITE_SCOPES")
	return nil
}

func applyStringEnv(target *string, envName string) {
	*target = strings.TrimSpace(*target)
	if *target == "" {
		*target = strings.TrimSpace(os.Getenv(envName))
	}
}

func applyDefaultIntEnv(target *int, defaultValue int, envName string) error {
	if *target != defaultValue {
		return nil
	}
	value := strings.TrimSpace(os.Getenv(envName))
	if value == "" {
		return nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Errorf("membrane: invalid %s: %w", envName, err)
	}
	*target = parsed
	return nil
}

func applyStringSliceEnv(target *[]string, envName string) {
	value := strings.TrimSpace(os.Getenv(envName))
	if value == "" {
		*target = normalizeConfigScopes(*target)
		return
	}
	*target = normalizeConfigScopes(strings.Split(value, ","))
}

func normalizeConfigScopes(scopes []string) []string {
	out := make([]string, 0, len(scopes))
	seen := make(map[string]struct{}, len(scopes))
	for _, scope := range scopes {
		scope = strings.TrimSpace(scope)
		if scope == "" {
			continue
		}
		if _, ok := seen[scope]; ok {
			continue
		}
		seen[scope] = struct{}{}
		out = append(out, scope)
	}
	return out
}

func validateRuntimeConfig(cfg *Config) error {
	if !schema.IsValidSensitivity(schema.Sensitivity(cfg.DefaultSensitivity)) {
		return fmt.Errorf("membrane: invalid default sensitivity %q", cfg.DefaultSensitivity)
	}
	if !schema.IsValidSensitivity(schema.Sensitivity(cfg.ReadMaxSensitivity)) {
		return fmt.Errorf("membrane: invalid read_max_sensitivity %q", cfg.ReadMaxSensitivity)
	}
	if !schema.IsValidSensitivity(schema.Sensitivity(cfg.WriteMaxSensitivity)) {
		return fmt.Errorf("membrane: invalid write_max_sensitivity %q", cfg.WriteMaxSensitivity)
	}
	if len(cfg.ReadScopes) == 0 {
		return fmt.Errorf("membrane: read_scopes must contain at least one scope")
	}
	if len(cfg.WriteScopes) == 0 {
		return fmt.Errorf("membrane: write_scopes must contain at least one scope")
	}
	if cfg.DecayInterval <= 0 {
		return fmt.Errorf("membrane: decay_interval must be positive")
	}
	if cfg.ConsolidationInterval <= 0 {
		return fmt.Errorf("membrane: consolidation_interval must be positive")
	}
	if math.IsNaN(cfg.SelectionConfidenceThreshold) || math.IsInf(cfg.SelectionConfidenceThreshold, 0) ||
		cfg.SelectionConfidenceThreshold < 0 || cfg.SelectionConfidenceThreshold > 1 {
		return fmt.Errorf("membrane: selection_confidence_threshold must be finite and between 0 and 1")
	}
	if cfg.EmbeddingDimensions <= 0 {
		return fmt.Errorf("membrane: embedding_dimensions must be positive")
	}
	if cfg.EmbeddingEndpoint != "" && cfg.EmbeddingModel == "" {
		return fmt.Errorf("membrane: embedding_model is required when embedding_endpoint is set")
	}
	if cfg.EmbeddingModel != "" && cfg.EmbeddingEndpoint == "" {
		return fmt.Errorf("membrane: embedding_endpoint is required when embedding_model is set")
	}
	if cfg.LLMEndpoint != "" && cfg.LLMModel == "" {
		return fmt.Errorf("membrane: llm_model is required when llm_endpoint is set")
	}
	if cfg.LLMModel != "" && cfg.LLMEndpoint == "" {
		return fmt.Errorf("membrane: llm_endpoint is required when llm_model is set")
	}
	if cfg.IngestLLMEnabled && cfg.IngestLLMEndpoint == "" {
		return fmt.Errorf("membrane: ingest_llm_endpoint is required when ingest_llm_enabled is true")
	}
	if cfg.IngestLLMEnabled && cfg.IngestLLMModel == "" {
		return fmt.Errorf("membrane: ingest_llm_model is required when ingest_llm_enabled is true")
	}
	if cfg.RateLimitPerSecond < 0 {
		return fmt.Errorf("membrane: rate_limit_per_second must be non-negative")
	}
	if (cfg.TLSCertFile == "") != (cfg.TLSKeyFile == "") {
		return fmt.Errorf("membrane: tls_cert_file and tls_key_file must be configured together")
	}
	if cfg.GraphDefaultRootLimit < 0 {
		return fmt.Errorf("membrane: graph_default_root_limit must be non-negative")
	}
	if cfg.GraphDefaultNodeLimit < 0 {
		return fmt.Errorf("membrane: graph_default_node_limit must be non-negative")
	}
	if cfg.GraphDefaultEdgeLimit < 0 {
		return fmt.Errorf("membrane: graph_default_edge_limit must be non-negative")
	}
	if cfg.GraphDefaultMaxHops < 0 {
		return fmt.Errorf("membrane: graph_default_max_hops must be non-negative")
	}
	for _, limit := range []struct {
		name  string
		value int
	}{
		{"graph_default_root_limit", cfg.GraphDefaultRootLimit},
		{"graph_default_node_limit", cfg.GraphDefaultNodeLimit},
		{"graph_default_edge_limit", cfg.GraphDefaultEdgeLimit},
		{"graph_default_max_hops", cfg.GraphDefaultMaxHops},
	} {
		if limit.value > retrieval.MaxGraphLimit {
			return fmt.Errorf("membrane: %s must be at most %d", limit.name, retrieval.MaxGraphLimit)
		}
	}
	return nil
}

// Start begins the background schedulers and one-shot embedding backfill.
// Start is idempotent; only the first call launches background work.
func (m *Membrane) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	m.lifecycleMu.Lock()
	if m.stopped {
		m.lifecycleMu.Unlock()
		return fmt.Errorf("membrane: cannot start after stop")
	}
	if m.started {
		m.lifecycleMu.Unlock()
		return nil
	}
	m.started = true
	backgroundCtx, cancel := context.WithCancel(ctx)
	m.backgroundCancel = cancel
	m.lifecycleMu.Unlock()

	m.decayScheduler.Start(backgroundCtx)
	m.consolScheduler.Start(backgroundCtx)
	if m.embedding != nil {
		m.backgroundWG.Add(1)
		go func() {
			defer m.backgroundWG.Done()
			count, err := m.embedding.BackfillMissing(backgroundCtx)
			if err != nil && backgroundCtx.Err() == nil {
				log.Printf("membrane: embedding backfill error: %v", err)
			} else if count > 0 {
				log.Printf("membrane: embedding backfill stored %d missing embeddings", count)
			}
		}()
	}
	return nil
}

// Stop gracefully shuts down schedulers, waits for background work, and closes the store.
// Stop is idempotent.
func (m *Membrane) Stop() error {
	m.lifecycleMu.Lock()
	if m.stopped {
		m.lifecycleMu.Unlock()
		return nil
	}
	m.stopped = true
	cancel := m.backgroundCancel
	m.lifecycleMu.Unlock()

	if cancel != nil {
		cancel()
	}
	m.decayScheduler.Stop()
	m.consolScheduler.Stop()
	m.backgroundWG.Wait()
	return m.store.Close()
}

// CaptureMemory creates a graph-aware source record and any linked entity records.
func (m *Membrane) CaptureMemory(ctx context.Context, req ingestion.CaptureMemoryRequest) (*ingestion.CaptureMemoryResponse, error) {
	return m.ingestion.CaptureMemory(ctx, req)
}

// CaptureMemoryWithAccess captures memory with per-call filtering and mutation
// authorization for any existing records consulted during resolution.
func (m *Membrane) CaptureMemoryWithAccess(ctx context.Context, req ingestion.CaptureMemoryRequest, access ingestion.CaptureAccess) (*ingestion.CaptureMemoryResponse, error) {
	return m.ingestion.CaptureMemoryWithAccess(ctx, req, access)
}

// RecordOutcome attaches an outcome to an existing episodic record.
func (m *Membrane) RecordOutcome(ctx context.Context, req ingestion.IngestOutcomeRequest) (*schema.MemoryRecord, error) {
	return m.ingestion.IngestOutcome(ctx, req)
}

// RetrieveByID fetches a single record by ID with trust context gating.
func (m *Membrane) RetrieveByID(ctx context.Context, id string, trust *retrieval.TrustContext) (*schema.MemoryRecord, error) {
	return m.retrieval.RetrieveByID(ctx, id, trust)
}

// RetrieveProjectedByID returns the byte-bounded, no-history/no-relations
// representation used by network transports. RetrieveByID remains complete.
func (m *Membrane) RetrieveProjectedByID(ctx context.Context, id string, trust *retrieval.TrustContext) (*retrieval.ProjectedRecordResponse, error) {
	return m.retrieval.RetrieveProjectedByID(ctx, id, trust)
}

// GetAuthorizationMetadata loads only exact ID/scope/sensitivity fields for a
// capped policy check without hydrating record content or append-only history.
func (m *Membrane) GetAuthorizationMetadata(ctx context.Context, ids []string) ([]storage.RecordAuthorizationMetadata, error) {
	lookup, ok := m.store.(storage.AuthorizationMetadataStore)
	if !ok {
		return nil, storage.ErrAuthorizationMetadataUnsupported
	}
	return lookup.GetAuthorizationMetadata(ctx, ids)
}

// RetrieveGraph returns a graph-expanded retrieval response rooted at ranked records.
func (m *Membrane) RetrieveGraph(ctx context.Context, req *retrieval.RetrieveGraphRequest) (*retrieval.RetrieveGraphResponse, error) {
	if req == nil {
		req = &retrieval.RetrieveGraphRequest{}
	}
	normalized := *req
	if normalized.RootLimit < 0 {
		return nil, fmt.Errorf("membrane: root_limit must be non-negative")
	}
	if normalized.NodeLimit < 0 {
		return nil, fmt.Errorf("membrane: node_limit must be non-negative")
	}
	if normalized.EdgeLimit < 0 {
		return nil, fmt.Errorf("membrane: edge_limit must be non-negative")
	}
	if normalized.RootLimit == 0 {
		normalized.RootLimit = m.config.GraphDefaultRootLimit
	}
	if normalized.NodeLimit == 0 {
		normalized.NodeLimit = m.config.GraphDefaultNodeLimit
	}
	if normalized.EdgeLimit == 0 {
		normalized.EdgeLimit = m.config.GraphDefaultEdgeLimit
	}
	if normalized.MaxHops == 0 {
		normalized.MaxHops = m.config.GraphDefaultMaxHops
	} else if normalized.MaxHops == -1 {
		normalized.MaxHops = 0
	} else if normalized.MaxHops < -1 {
		return nil, fmt.Errorf("membrane: max_hops must be -1 or non-negative")
	}
	return m.retrieval.RetrieveGraph(ctx, &normalized)
}

// ---------------------------------------------------------------------------
// Revision delegates
// ---------------------------------------------------------------------------

// Supersede atomically replaces an old record with a new one.
func (m *Membrane) Supersede(ctx context.Context, oldID string, newRec *schema.MemoryRecord, actor, rationale string) (*schema.MemoryRecord, error) {
	return m.revision.Supersede(ctx, oldID, newRec, actor, rationale)
}

// Fork creates a new record derived from an existing source record.
func (m *Membrane) Fork(ctx context.Context, sourceID string, forkedRec *schema.MemoryRecord, actor, rationale string) (*schema.MemoryRecord, error) {
	return m.revision.Fork(ctx, sourceID, forkedRec, actor, rationale)
}

// Retract marks a record as retracted without deleting it.
func (m *Membrane) Retract(ctx context.Context, id, actor, rationale string) error {
	return m.revision.Retract(ctx, id, actor, rationale)
}

// Merge atomically combines multiple source records into a single merged record.
func (m *Membrane) Merge(ctx context.Context, ids []string, mergedRec *schema.MemoryRecord, actor, rationale string) (*schema.MemoryRecord, error) {
	return m.revision.Merge(ctx, ids, mergedRec, actor, rationale)
}

// Contest marks a record as contested, indicating conflicting evidence exists.
func (m *Membrane) Contest(ctx context.Context, id, contestingRef, actor, rationale string) error {
	return m.revision.Contest(ctx, id, contestingRef, actor, rationale)
}

// ContestWithAccess materializes graph relations for a stored contesting
// reference only when canLink authorizes that record. Opaque, missing, and
// denied references still contest the selected record without exposing which
// case occurred.
func (m *Membrane) ContestWithAccess(ctx context.Context, id, contestingRef, actor, rationale string, canLink func(*schema.MemoryRecord) bool) error {
	return m.revision.ContestWithAccess(ctx, id, contestingRef, actor, rationale, canLink)
}

// ---------------------------------------------------------------------------
// Decay delegates
// ---------------------------------------------------------------------------

// Reinforce boosts a record's salience.
func (m *Membrane) Reinforce(ctx context.Context, id, actor, rationale string) error {
	return m.decay.Reinforce(ctx, id, actor, rationale)
}

// Penalize reduces a record's salience by the given amount.
func (m *Membrane) Penalize(ctx context.Context, id string, amount float64, actor, rationale string) error {
	return m.decay.Penalize(ctx, id, amount, actor, rationale)
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

// GetMetrics collects a point-in-time snapshot of substrate metrics.
func (m *Membrane) GetMetrics(ctx context.Context) (*metrics.Snapshot, error) {
	return m.metrics.Collect(ctx)
}

// GetMetricsForTrust collects metrics only over records directly visible to
// the supplied server-derived trust context.
func (m *Membrane) GetMetricsForTrust(ctx context.Context, trust *retrieval.TrustContext) (*metrics.Snapshot, error) {
	return m.metrics.CollectForTrust(ctx, trust)
}
