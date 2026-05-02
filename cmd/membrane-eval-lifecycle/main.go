// membrane-eval-lifecycle tests Membrane's lifecycle features against pure RAG.
//
// It runs four scenarios where Membrane's structured memory (retraction, reinforcement,
// supersession, decay) should outperform naive vector similarity search:
//
//  1. Retraction:  Wrong facts retracted → Membrane filters them, RAG still returns them.
//  2. Reinforcement: Useful records boosted → Membrane ranks them higher via hybrid scoring.
//  3. Supersession: Outdated facts replaced → Membrane prefers the new version.
//  4. Decay: Stale records lose salience → Membrane deprioritizes them.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/google/uuid"

	"github.com/BennettSchwartz/membrane/pkg/embedding"
	"github.com/BennettSchwartz/membrane/pkg/ingestion"
	"github.com/BennettSchwartz/membrane/pkg/membrane"
	"github.com/BennettSchwartz/membrane/pkg/retrieval"
	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage/postgres"
)

type embeddingWriter interface {
	StoreTriggerEmbedding(context.Context, string, []float32, string) error
}

type embeddingSearcher interface {
	SearchByEmbedding(context.Context, []float32, int) ([]string, error)
}

type lifecycleVectorStore interface {
	embeddingWriter
	embeddingSearcher
}

type lifecycleCLIVectorStore interface {
	lifecycleVectorStore
	Close() error
}

type lifecycleMembrane interface {
	Start(context.Context) error
	Stop() error
	CaptureMemory(context.Context, ingestion.CaptureMemoryRequest) (*ingestion.CaptureMemoryResponse, error)
	RetrieveGraph(context.Context, *retrieval.RetrieveGraphRequest) (*retrieval.RetrieveGraphResponse, error)
	Supersede(context.Context, string, *schema.MemoryRecord, string, string) (*schema.MemoryRecord, error)
	Retract(context.Context, string, string, string) error
	Reinforce(context.Context, string, string, string) error
	Penalize(context.Context, string, float64, string, string) error
}

type lifecycleCLIDependencies struct {
	newMembrane        func(*membrane.Config) (lifecycleMembrane, error)
	openVectorStore    func(string, postgres.EmbeddingConfig) (lifecycleCLIVectorStore, error)
	newEmbeddingClient func(endpoint, model, apiKey string, dimensions int) embedding.Client
}

func (d lifecycleCLIDependencies) withDefaults() lifecycleCLIDependencies {
	if d.newMembrane == nil {
		d.newMembrane = func(cfg *membrane.Config) (lifecycleMembrane, error) {
			return membrane.New(cfg)
		}
	}
	if d.openVectorStore == nil {
		d.openVectorStore = func(dsn string, cfg postgres.EmbeddingConfig) (lifecycleCLIVectorStore, error) {
			return postgres.Open(dsn, cfg)
		}
	}
	if d.newEmbeddingClient == nil {
		d.newEmbeddingClient = func(endpoint, model, apiKey string, dimensions int) embedding.Client {
			return embedding.NewHTTPClient(endpoint, model, apiKey, dimensions)
		}
	}
	return d
}

type lifecycleEvalResult struct {
	ragWins int
	memWins int
	ties    int
}

func (r *lifecycleEvalResult) add(delta lifecycleEvalResult) {
	r.ragWins += delta.ragWins
	r.memWins += delta.memWins
	r.ties += delta.ties
}

func databaseScenarioOutcome(memHasWrong, ragHasWrong bool) (string, lifecycleEvalResult) {
	if !memHasWrong && ragHasWrong {
		return "  ✓ Membrane wins: filtered retracted record", lifecycleEvalResult{memWins: 1}
	}
	if memHasWrong == ragHasWrong {
		return "  — Tie", lifecycleEvalResult{ties: 1}
	}
	return "  ✗ RAG wins", lifecycleEvalResult{ragWins: 1}
}

func topResultScenarioOutcome(memFirst, ragFirst, preferredID, memWinMessage, tieMessage string) (string, lifecycleEvalResult) {
	if memFirst == preferredID && ragFirst != preferredID {
		return memWinMessage, lifecycleEvalResult{memWins: 1}
	}
	if memFirst == preferredID && ragFirst == preferredID {
		return tieMessage, lifecycleEvalResult{ties: 1}
	}
	return "  ✗ RAG wins", lifecycleEvalResult{ragWins: 1}
}

func supersessionScenarioOutcome(memReturnsNew, memReturnsOld, ragReturnsOld bool) (string, lifecycleEvalResult) {
	if memReturnsNew && !memReturnsOld && ragReturnsOld {
		return "  ✓ Membrane wins: superseded record filtered, new version returned", lifecycleEvalResult{memWins: 1}
	}
	if !memReturnsOld && !ragReturnsOld {
		return "  — Tie", lifecycleEvalResult{ties: 1}
	}
	return "  ~ Partial (both return new, RAG also returns old)", lifecycleEvalResult{ties: 1}
}

var (
	lifecycleStderr io.Writer = os.Stderr
	lifecycleExit             = os.Exit
)

func main() {
	if err := runLifecycleCLI(context.Background(), os.Args[1:], os.Getenv); err != nil {
		fmt.Fprintln(lifecycleStderr, err)
		lifecycleExit(1)
	}
}

func runLifecycleCLI(ctx context.Context, args []string, getenv func(string) string) error {
	return runLifecycleCLIWithDeps(ctx, args, getenv, lifecycleCLIDependencies{})
}

func runLifecycleCLIWithDeps(ctx context.Context, args []string, getenv func(string) string, deps lifecycleCLIDependencies) error {
	deps = deps.withDefaults()
	if getenv == nil {
		getenv = os.Getenv
	}
	fs := flag.NewFlagSet("membrane-eval-lifecycle", flag.ContinueOnError)
	var (
		postgresDSN       = fs.String("postgres-dsn", "", "PostgreSQL DSN (required)")
		embeddingEndpoint = fs.String("embedding-endpoint", "https://openrouter.ai/api/v1/embeddings", "Embedding API endpoint")
		embeddingModel    = fs.String("embedding-model", "openai/text-embedding-3-small", "Embedding model")
		embeddingAPIKey   = fs.String("embedding-api-key", "", "Embedding API key (or MEMBRANE_EMBEDDING_API_KEY)")
		embeddingDims     = fs.Int("embedding-dimensions", 1536, "Embedding dimensions")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}

	apiKey := *embeddingAPIKey
	if apiKey == "" {
		apiKey = getenv("MEMBRANE_EMBEDDING_API_KEY")
	}
	if *postgresDSN == "" || apiKey == "" {
		return fmt.Errorf("config: --postgres-dsn and embedding API key required")
	}

	cfg := membrane.DefaultConfig()
	cfg.Backend = "postgres"
	cfg.PostgresDSN = *postgresDSN
	cfg.EmbeddingEndpoint = *embeddingEndpoint
	cfg.EmbeddingModel = *embeddingModel
	cfg.EmbeddingAPIKey = apiKey
	cfg.EmbeddingDimensions = *embeddingDims
	cfg.SelectionConfidenceThreshold = 0.3

	m, err := deps.newMembrane(cfg)
	if err != nil {
		return fmt.Errorf("create membrane: %w", err)
	}
	if err := m.Start(ctx); err != nil {
		if stopErr := m.Stop(); stopErr != nil {
			return fmt.Errorf("start membrane: %w; cleanup: %v", err, stopErr)
		}
		return fmt.Errorf("start membrane: %w", err)
	}
	defer func() { _ = m.Stop() }()

	pgStore, err := deps.openVectorStore(*postgresDSN, postgres.EmbeddingConfig{
		Dimensions: *embeddingDims,
		Model:      *embeddingModel,
	})
	if err != nil {
		return fmt.Errorf("open pgstore: %w", err)
	}
	defer func() { _ = pgStore.Close() }()

	embClient := deps.newEmbeddingClient(*embeddingEndpoint, *embeddingModel, apiKey, *embeddingDims)

	if _, err := runLifecycleEval(ctx, m, embClient, pgStore, *embeddingModel); err != nil {
		return fmt.Errorf("run lifecycle eval: %w", err)
	}
	return nil
}

func runLifecycleEval(ctx context.Context, m lifecycleMembrane, embClient embedding.Client, pgStore lifecycleVectorStore, embeddingModel string) (lifecycleEvalResult, error) {
	var result lifecycleEvalResult

	// -----------------------------------------------------------------------
	// Scenario 1: RETRACTION — wrong facts should disappear
	// -----------------------------------------------------------------------
	fmt.Println("=== Scenario 1: Retraction ===")
	fmt.Println("  Setup: Two records about preferred database. Retract the wrong one.")

	wrongDB, err := captureObservationRecord(ctx, m, "eval", "team", "prefers_database", "MySQL", []string{"database", "preference"}, "project:alpha", schema.SensitivityLow)
	if err != nil {
		return lifecycleEvalResult{}, fmt.Errorf("capture wrong database record: %w", err)
	}
	correctDB, err := captureObservationRecord(ctx, m, "eval", "team", "prefers_database", "PostgreSQL", []string{"database", "preference"}, "project:alpha", schema.SensitivityLow)
	if err != nil {
		return lifecycleEvalResult{}, fmt.Errorf("capture correct database record: %w", err)
	}
	// Add some noise records
	noiseIDs, err := ingestNoise(ctx, m, []string{
		"team uses Redis for caching",
		"team uses Kafka for event streaming",
		"project alpha deploys to Cloudflare Workers",
	})
	if err != nil {
		return lifecycleEvalResult{}, err
	}

	if err := embedRecord(ctx, embClient, pgStore, wrongDB.ID, "team prefers MySQL as the primary database", embeddingModel); err != nil {
		return lifecycleEvalResult{}, err
	}
	if err := embedRecord(ctx, embClient, pgStore, correctDB.ID, "team prefers PostgreSQL as the primary database", embeddingModel); err != nil {
		return lifecycleEvalResult{}, err
	}
	for i, id := range noiseIDs {
		if err := embedRecord(ctx, embClient, pgStore, id, []string{
			"team uses Redis for caching",
			"team uses Kafka for event streaming",
			"project alpha deploys to Cloudflare Workers",
		}[i], embeddingModel); err != nil {
			return lifecycleEvalResult{}, err
		}
	}

	// Retract the wrong one
	if err := m.Retract(ctx, wrongDB.ID, "eval", "MySQL was incorrect"); err != nil {
		return lifecycleEvalResult{}, fmt.Errorf("retract wrong database record: %w", err)
	}

	query := "What database does the team prefer?"
	qVec, err := embClient.Embed(ctx, query)
	if err != nil {
		return lifecycleEvalResult{}, fmt.Errorf("embed query %q: %w", query, err)
	}

	// RAG: pure vector search (doesn't know about retraction)
	ragTop, err := ragSearch(ctx, pgStore, qVec, 3)
	if err != nil {
		return lifecycleEvalResult{}, err
	}
	ragHasWrong := contains(ragTop, wrongDB.ID)
	ragHasCorrect := contains(ragTop, correctDB.ID)

	// Membrane: respects retraction (salience=0, filtered with MinSalience)
	memResp, err := retrieveRootRecords(ctx, m, &retrieval.RetrieveRequest{
		TaskDescriptor: query, QueryEmbedding: qVec,
		Trust:       fullTrust(),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		MinSalience: 0.01, Limit: 3,
	})
	if err != nil {
		return lifecycleEvalResult{}, fmt.Errorf("retrieve database scenario records: %w", err)
	}
	memHasWrong := containsRec(memResp.Records, wrongDB.ID)
	memHasCorrect := containsRec(memResp.Records, correctDB.ID)

	fmt.Printf("  Query: %q\n", query)
	fmt.Printf("  RAG:      returns wrong (MySQL)=%v  returns correct (PostgreSQL)=%v\n", ragHasWrong, ragHasCorrect)
	fmt.Printf("  Membrane: returns wrong (MySQL)=%v  returns correct (PostgreSQL)=%v\n", memHasWrong, memHasCorrect)
	message, delta := databaseScenarioOutcome(memHasWrong, ragHasWrong)
	fmt.Println(message)
	result.add(delta)

	// -----------------------------------------------------------------------
	// Scenario 2: REINFORCEMENT — useful records rank higher
	// -----------------------------------------------------------------------
	fmt.Println("\n=== Scenario 2: Reinforcement ===")
	fmt.Println("  Setup: Two similar debugging procedures. Reinforce the one that worked.")

	proc1, err := captureObservationRecord(ctx, m, "eval", "debug-oom", "procedure", "restart pods and hope for the best", []string{"debugging", "oom"}, "", schema.SensitivityLow)
	if err != nil {
		return lifecycleEvalResult{}, fmt.Errorf("capture weak oom procedure: %w", err)
	}
	proc2, err := captureObservationRecord(ctx, m, "eval", "debug-oom", "procedure", "capture pprof heap profile then trace allocations", []string{"debugging", "oom"}, "", schema.SensitivityLow)
	if err != nil {
		return lifecycleEvalResult{}, fmt.Errorf("capture strong oom procedure: %w", err)
	}

	if err := embedRecord(ctx, embClient, pgStore, proc1.ID, "debugging OOM kill by restarting pods and hoping the problem goes away", embeddingModel); err != nil {
		return lifecycleEvalResult{}, err
	}
	if err := embedRecord(ctx, embClient, pgStore, proc2.ID, "debugging OOM kill by capturing pprof heap profile and tracing memory allocations to source", embeddingModel); err != nil {
		return lifecycleEvalResult{}, err
	}

	// Reinforce proc2 multiple times (it actually worked)
	for i := 0; i < 5; i++ {
		if err := m.Reinforce(ctx, proc2.ID, "eval", "this procedure resolved the OOM"); err != nil {
			return lifecycleEvalResult{}, fmt.Errorf("reinforce strong oom procedure: %w", err)
		}
	}
	// Penalize proc1 (it didn't help)
	if err := m.Penalize(ctx, proc1.ID, 0.5, "eval", "restarting didn't fix the root cause"); err != nil {
		return lifecycleEvalResult{}, fmt.Errorf("penalize weak oom procedure: %w", err)
	}

	query = "How do I debug an OOM kill?"
	qVec, err = embClient.Embed(ctx, query)
	if err != nil {
		return lifecycleEvalResult{}, fmt.Errorf("embed query %q: %w", query, err)
	}

	ragTop, err = ragSearch(ctx, pgStore, qVec, 2)
	if err != nil {
		return lifecycleEvalResult{}, err
	}
	ragFirst := ""
	if len(ragTop) > 0 {
		ragFirst = ragTop[0]
	}

	memResp, err = retrieveRootRecords(ctx, m, &retrieval.RetrieveRequest{
		TaskDescriptor: query, QueryEmbedding: qVec,
		Trust:       fullTrust(),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		Limit:       2,
	})
	if err != nil {
		return lifecycleEvalResult{}, fmt.Errorf("retrieve oom scenario records: %w", err)
	}
	memFirst := ""
	if len(memResp.Records) > 0 {
		memFirst = memResp.Records[0].ID
	}

	fmt.Printf("  Query: %q\n", query)
	fmt.Printf("  RAG top result:      %s (proc2=%v)\n", label(ragFirst, proc1.ID, proc2.ID), ragFirst == proc2.ID)
	fmt.Printf("  Membrane top result: %s (proc2=%v)\n", label(memFirst, proc1.ID, proc2.ID), memFirst == proc2.ID)
	message, delta = topResultScenarioOutcome(
		memFirst,
		ragFirst,
		proc2.ID,
		"  ✓ Membrane wins: reinforced record ranked first",
		"  — Tie (both got it right)",
	)
	fmt.Println(message)
	result.add(delta)

	// -----------------------------------------------------------------------
	// Scenario 3: SUPERSESSION — outdated facts replaced
	// -----------------------------------------------------------------------
	fmt.Println("\n=== Scenario 3: Supersession ===")
	fmt.Println("  Setup: Rate limit changed from 50 rps to 200 rps. Old record superseded.")

	oldLimit, err := captureObservationRecord(ctx, m, "eval", "api", "rate_limit", "50 requests per second", []string{"api", "rate-limit"}, "", schema.SensitivityLow)
	if err != nil {
		return lifecycleEvalResult{}, fmt.Errorf("capture old rate limit: %w", err)
	}
	if err := embedRecord(ctx, embClient, pgStore, oldLimit.ID, "API rate limit is 50 requests per second per client", embeddingModel); err != nil {
		return lifecycleEvalResult{}, err
	}

	newLimitRec := schema.NewMemoryRecord(uuid.New().String(), schema.MemoryTypeSemantic, schema.SensitivityLow,
		&schema.SemanticPayload{
			Kind: "semantic", Subject: "api", Predicate: "rate_limit", Object: "200 requests per second",
			Validity: schema.Validity{Mode: schema.ValidityModeGlobal},
			Evidence: []schema.ProvenanceRef{{SourceType: "observation", SourceID: "config-update-2026"}},
		})
	newLimitRec.Tags = []string{"api", "rate-limit"}
	newLimitRec.Provenance.Sources = []schema.ProvenanceSource{{
		Kind: schema.ProvenanceKindObservation, Ref: "config-update-2026",
	}}

	superseded, err := m.Supersede(ctx, oldLimit.ID, newLimitRec, "eval", "rate limit increased to 200 rps")
	if err != nil {
		return lifecycleEvalResult{}, fmt.Errorf("supersede old rate limit: %w", err)
	}
	if err := embedRecord(ctx, embClient, pgStore, superseded.ID, "API rate limit is 200 requests per second per client", embeddingModel); err != nil {
		return lifecycleEvalResult{}, err
	}

	query = "What is the current API rate limit?"
	qVec, err = embClient.Embed(ctx, query)
	if err != nil {
		return lifecycleEvalResult{}, fmt.Errorf("embed query %q: %w", query, err)
	}

	ragTop, err = ragSearch(ctx, pgStore, qVec, 2)
	if err != nil {
		return lifecycleEvalResult{}, err
	}
	ragReturnsOld := contains(ragTop, oldLimit.ID)
	ragReturnsNew := contains(ragTop, superseded.ID)

	memResp, err = retrieveRootRecords(ctx, m, &retrieval.RetrieveRequest{
		TaskDescriptor: query, QueryEmbedding: qVec,
		Trust:       fullTrust(),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		MinSalience: 0.01, Limit: 2,
	})
	if err != nil {
		return lifecycleEvalResult{}, fmt.Errorf("retrieve rate limit scenario records: %w", err)
	}
	memReturnsOld := containsRec(memResp.Records, oldLimit.ID)
	memReturnsNew := containsRec(memResp.Records, superseded.ID)

	fmt.Printf("  Query: %q\n", query)
	fmt.Printf("  RAG:      old (50rps)=%v  new (200rps)=%v\n", ragReturnsOld, ragReturnsNew)
	fmt.Printf("  Membrane: old (50rps)=%v  new (200rps)=%v\n", memReturnsOld, memReturnsNew)
	message, delta = supersessionScenarioOutcome(memReturnsNew, memReturnsOld, ragReturnsOld)
	fmt.Println(message)
	result.add(delta)

	// -----------------------------------------------------------------------
	// Scenario 4: DECAY — stale records deprioritized
	// -----------------------------------------------------------------------
	fmt.Println("\n=== Scenario 4: Decay ===")
	fmt.Println("  Setup: Two deployment records. One is fresh, one has decayed salience.")

	staleRec, err := captureObservationRecord(ctx, m, "eval", "deployment", "target", "Heroku (deprecated)", []string{"deploy"}, "", schema.SensitivityLow)
	if err != nil {
		return lifecycleEvalResult{}, fmt.Errorf("capture stale deployment target: %w", err)
	}
	freshRec, err := captureObservationRecord(ctx, m, "eval", "deployment", "target", "Kubernetes on GKE", []string{"deploy"}, "", schema.SensitivityLow)
	if err != nil {
		return lifecycleEvalResult{}, fmt.Errorf("capture fresh deployment target: %w", err)
	}

	if err := embedRecord(ctx, embClient, pgStore, staleRec.ID, "production deployment targets Heroku platform", embeddingModel); err != nil {
		return lifecycleEvalResult{}, err
	}
	if err := embedRecord(ctx, embClient, pgStore, freshRec.ID, "production deployment targets Kubernetes on GKE", embeddingModel); err != nil {
		return lifecycleEvalResult{}, err
	}

	// Simulate decay: penalize the stale record heavily
	if err := m.Penalize(ctx, staleRec.ID, 0.85, "eval", "outdated deployment target"); err != nil {
		return lifecycleEvalResult{}, fmt.Errorf("penalize stale deployment target: %w", err)
	}
	// Reinforce the fresh one
	if err := m.Reinforce(ctx, freshRec.ID, "eval", "current deployment target confirmed"); err != nil {
		return lifecycleEvalResult{}, fmt.Errorf("reinforce fresh deployment target: %w", err)
	}

	query = "Where do we deploy to production?"
	qVec, err = embClient.Embed(ctx, query)
	if err != nil {
		return lifecycleEvalResult{}, fmt.Errorf("embed query %q: %w", query, err)
	}

	ragTop, err = ragSearch(ctx, pgStore, qVec, 2)
	if err != nil {
		return lifecycleEvalResult{}, err
	}
	ragFirstDeploy := ""
	if len(ragTop) > 0 {
		ragFirstDeploy = ragTop[0]
	}

	memResp, err = retrieveRootRecords(ctx, m, &retrieval.RetrieveRequest{
		TaskDescriptor: query, QueryEmbedding: qVec,
		Trust:       fullTrust(),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		Limit:       2,
	})
	if err != nil {
		return lifecycleEvalResult{}, fmt.Errorf("retrieve deployment scenario records: %w", err)
	}
	memFirstDeploy := ""
	if len(memResp.Records) > 0 {
		memFirstDeploy = memResp.Records[0].ID
	}

	fmt.Printf("  Query: %q\n", query)
	fmt.Printf("  RAG top result:      %s\n", label(ragFirstDeploy, staleRec.ID, freshRec.ID))
	fmt.Printf("  Membrane top result: %s\n", label(memFirstDeploy, staleRec.ID, freshRec.ID))
	message, delta = topResultScenarioOutcome(
		memFirstDeploy,
		ragFirstDeploy,
		freshRec.ID,
		"  ✓ Membrane wins: decayed stale record, fresh record ranked first",
		"  — Tie (both prefer fresh)",
	)
	fmt.Println(message)
	result.add(delta)

	// -----------------------------------------------------------------------
	// Summary
	// -----------------------------------------------------------------------
	fmt.Println("\n==================== LIFECYCLE EVAL SUMMARY ====================")
	fmt.Printf("  Membrane wins: %d\n", result.memWins)
	fmt.Printf("  RAG wins:      %d\n", result.ragWins)
	fmt.Printf("  Ties:          %d\n", result.ties)
	total := result.memWins + result.ragWins + result.ties
	if total > 0 {
		fmt.Printf("  Membrane advantage: %.0f%%\n", float64(result.memWins)/float64(total)*100)
	}
	fmt.Println("================================================================")

	return result, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func ingestNoise(ctx context.Context, m lifecycleMembrane, texts []string) ([]string, error) {
	ids := make([]string, len(texts))
	for i, text := range texts {
		parts := strings.SplitN(text, " ", 3)
		subj, pred, obj := parts[0], "uses", text
		if len(parts) >= 3 {
			subj, pred, obj = parts[0], parts[1], strings.Join(parts[2:], " ")
		}
		rec, err := captureObservationRecord(ctx, m, "eval", subj, pred, obj, []string{"noise"}, "", schema.SensitivityLow)
		if err != nil {
			return nil, fmt.Errorf("ingest noise %q: %w", text, err)
		}
		ids[i] = rec.ID
	}
	return ids, nil
}

func embedRecord(ctx context.Context, client embedding.Client, store embeddingWriter, id, text, model string) error {
	vec, err := client.Embed(ctx, text)
	if err != nil {
		return fmt.Errorf("embed %s: %w", id, err)
	}
	if err := store.StoreTriggerEmbedding(ctx, id, vec, model); err != nil {
		return fmt.Errorf("store embedding %s: %w", id, err)
	}
	return nil
}

func ragSearch(ctx context.Context, store embeddingSearcher, qVec []float32, k int) ([]string, error) {
	if k <= 0 {
		return []string{}, nil
	}
	// Search broadly then take top-k. ivfflat needs enough data to work;
	// searching with a larger limit avoids empty results on small datasets.
	limit := k
	if limit < 100 {
		limit = 100
	}
	ids, err := store.SearchByEmbedding(ctx, qVec, limit)
	if err != nil {
		return nil, fmt.Errorf("rag search: %w", err)
	}
	if len(ids) > k {
		ids = ids[:k]
	}
	return ids, nil
}

func fullTrust() *retrieval.TrustContext {
	return retrieval.NewTrustContext(schema.SensitivityHyper, true, "eval", nil)
}

func contains(ids []string, target string) bool {
	for _, id := range ids {
		if id == target {
			return true
		}
	}
	return false
}

func containsRec(recs []*schema.MemoryRecord, target string) bool {
	for _, r := range recs {
		if r != nil && r.ID == target {
			return true
		}
	}
	return false
}

func label(id, aID, bID string) string {
	switch id {
	case aID:
		return "A (old/wrong/stale)"
	case bID:
		return "B (correct/new/fresh)"
	default:
		if len(id) > 8 {
			id = id[:8]
		}
		return "other: " + id
	}
}

func captureObservationRecord(ctx context.Context, m lifecycleMembrane, source, subject, predicate string, object any, tags []string, scope string, sensitivity schema.Sensitivity) (*schema.MemoryRecord, error) {
	resp, err := m.CaptureMemory(ctx, ingestion.CaptureMemoryRequest{
		Source:           source,
		SourceKind:       "observation",
		Content:          map[string]any{"subject": subject, "predicate": predicate, "object": object},
		ReasonToRemember: fmt.Sprintf("%s %s", subject, predicate),
		Summary:          fmt.Sprintf("%s %s", subject, predicate),
		Tags:             tags,
		Scope:            scope,
		Sensitivity:      sensitivity,
	})
	if err != nil {
		return nil, err
	}
	for _, rec := range resp.CreatedRecords {
		if rec != nil && rec.Type == schema.MemoryTypeSemantic {
			return rec, nil
		}
	}
	return nil, fmt.Errorf("capture observation did not produce semantic record")
}

func retrieveRootRecords(ctx context.Context, m lifecycleMembrane, req *retrieval.RetrieveRequest) (*retrieval.RetrieveResponse, error) {
	if req == nil {
		return nil, errors.New("retrieve request is required")
	}
	rootLimit := req.Limit
	if rootLimit <= 0 {
		rootLimit = 10000
	}
	graphResp, err := m.RetrieveGraph(ctx, &retrieval.RetrieveGraphRequest{
		TaskDescriptor: req.TaskDescriptor,
		QueryEmbedding: req.QueryEmbedding,
		Trust:          req.Trust,
		MemoryTypes:    req.MemoryTypes,
		MinSalience:    req.MinSalience,
		RootLimit:      rootLimit,
		NodeLimit:      rootLimit,
		EdgeLimit:      0,
		MaxHops:        -1,
	})
	if err != nil {
		return nil, err
	}
	records := make([]*schema.MemoryRecord, 0, len(graphResp.Nodes))
	for _, node := range graphResp.Nodes {
		if node.Root && node.Record != nil {
			records = append(records, node.Record)
		}
	}
	return &retrieval.RetrieveResponse{Records: records, Selection: graphResp.Selection}, nil
}
