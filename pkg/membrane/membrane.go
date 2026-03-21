package membrane

import (
	"context"
	"fmt"
	"os"

	"github.com/GustyCube/membrane/pkg/consolidation"
	"github.com/GustyCube/membrane/pkg/decay"
	"github.com/GustyCube/membrane/pkg/embedding"
	"github.com/GustyCube/membrane/pkg/ingestion"
	"github.com/GustyCube/membrane/pkg/metrics"
	"github.com/GustyCube/membrane/pkg/retrieval"
	"github.com/GustyCube/membrane/pkg/revision"
	"github.com/GustyCube/membrane/pkg/schema"
	"github.com/GustyCube/membrane/pkg/storage"
	"github.com/GustyCube/membrane/pkg/storage/postgres"
	"github.com/GustyCube/membrane/pkg/storage/sqlite"
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
}

// New initialises all subsystems from the provided Config and returns a
// ready-to-start Membrane instance.
func New(cfg *Config) (*Membrane, error) {
	if !schema.IsValidSensitivity(schema.Sensitivity(cfg.DefaultSensitivity)) {
		return nil, fmt.Errorf("membrane: invalid default sensitivity %q", cfg.DefaultSensitivity)
	}

	var (
		store   storage.Store
		pgStore *postgres.PostgresStore
		err     error
	)
	switch cfg.Backend {
	case "", "sqlite":
		encKey := cfg.EncryptionKey
		if encKey == "" {
			encKey = os.Getenv("MEMBRANE_ENCRYPTION_KEY")
		}
		store, err = sqlite.Open(cfg.DBPath, encKey)
		if err != nil {
			return nil, fmt.Errorf("membrane: open sqlite store: %w", err)
		}
	case "postgres":
		if cfg.PostgresDSN == "" {
			cfg.PostgresDSN = os.Getenv("MEMBRANE_POSTGRES_DSN")
		}
		if cfg.PostgresDSN == "" {
			return nil, fmt.Errorf("membrane: postgres_dsn is required when backend=postgres")
		}
		pgStore, err = postgres.Open(cfg.PostgresDSN, postgres.EmbeddingConfig{
			Dimensions: cfg.EmbeddingDimensions,
			Model:      cfg.EmbeddingModel,
		})
		if err != nil {
			return nil, fmt.Errorf("membrane: open postgres store: %w", err)
		}
		store = pgStore
	default:
		return nil, fmt.Errorf("membrane: unsupported backend %q", cfg.Backend)
	}

	// Ingestion
	classifier := ingestion.NewClassifier()
	policyDefaults := ingestion.DefaultPolicyDefaults()
	policyDefaults.Sensitivity = schema.Sensitivity(cfg.DefaultSensitivity)
	policyEngine := ingestion.NewPolicyEngine(policyDefaults)
	ingestionSvc := ingestion.NewService(store, classifier, policyEngine)

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
	} else if embService != nil {
		selector = retrieval.NewSelectorWithEmbedding(cfg.SelectionConfidenceThreshold, embService)
		retrievalSvc = retrieval.NewServiceWithEmbedding(store, selector, embService)
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

// Start begins the background schedulers (decay, consolidation).
func (m *Membrane) Start(ctx context.Context) error {
	m.decayScheduler.Start(ctx)
	m.consolScheduler.Start(ctx)
	if m.embedding != nil {
		go func() {
			_, _ = m.embedding.BackfillMissing(ctx)
		}()
	}
	return nil
}

// Stop gracefully shuts down schedulers and closes the store.
func (m *Membrane) Stop() error {
	m.decayScheduler.Stop()
	m.consolScheduler.Stop()
	return m.store.Close()
}

// ---------------------------------------------------------------------------
// Ingestion delegates
// ---------------------------------------------------------------------------

// IngestEvent creates an episodic memory record from an event.
func (m *Membrane) IngestEvent(ctx context.Context, req ingestion.IngestEventRequest) (*schema.MemoryRecord, error) {
	return m.ingestion.IngestEvent(ctx, req)
}

// IngestToolOutput creates an episodic memory record from a tool invocation.
func (m *Membrane) IngestToolOutput(ctx context.Context, req ingestion.IngestToolOutputRequest) (*schema.MemoryRecord, error) {
	return m.ingestion.IngestToolOutput(ctx, req)
}

// IngestObservation creates a semantic memory record from an observation.
func (m *Membrane) IngestObservation(ctx context.Context, req ingestion.IngestObservationRequest) (*schema.MemoryRecord, error) {
	return m.ingestion.IngestObservation(ctx, req)
}

// IngestOutcome updates an existing episodic record with outcome data.
func (m *Membrane) IngestOutcome(ctx context.Context, req ingestion.IngestOutcomeRequest) (*schema.MemoryRecord, error) {
	return m.ingestion.IngestOutcome(ctx, req)
}

// IngestWorkingState creates a working memory record from a working state snapshot.
func (m *Membrane) IngestWorkingState(ctx context.Context, req ingestion.IngestWorkingStateRequest) (*schema.MemoryRecord, error) {
	return m.ingestion.IngestWorkingState(ctx, req)
}

// ---------------------------------------------------------------------------
// Retrieval delegates
// ---------------------------------------------------------------------------

// Retrieve performs layered retrieval as specified in RFC 15.8.
func (m *Membrane) Retrieve(ctx context.Context, req *retrieval.RetrieveRequest) (*retrieval.RetrieveResponse, error) {
	return m.retrieval.Retrieve(ctx, req)
}

// RetrieveByID fetches a single record by ID with trust context gating.
func (m *Membrane) RetrieveByID(ctx context.Context, id string, trust *retrieval.TrustContext) (*schema.MemoryRecord, error) {
	return m.retrieval.RetrieveByID(ctx, id, trust)
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
