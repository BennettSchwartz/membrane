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
	"flag"
	"fmt"
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

func main() {
	var (
		postgresDSN       = flag.String("postgres-dsn", "", "PostgreSQL DSN (required)")
		embeddingEndpoint = flag.String("embedding-endpoint", "https://openrouter.ai/api/v1/embeddings", "Embedding API endpoint")
		embeddingModel    = flag.String("embedding-model", "openai/text-embedding-3-small", "Embedding model")
		embeddingAPIKey   = flag.String("embedding-api-key", "", "Embedding API key (or MEMBRANE_EMBEDDING_API_KEY)")
		embeddingDims     = flag.Int("embedding-dimensions", 1536, "Embedding dimensions")
	)
	flag.Parse()

	apiKey := *embeddingAPIKey
	if apiKey == "" {
		apiKey = os.Getenv("MEMBRANE_EMBEDDING_API_KEY")
	}
	if *postgresDSN == "" || apiKey == "" {
		fatal("config", fmt.Errorf("--postgres-dsn and embedding API key required"))
	}

	ctx := context.Background()

	cfg := membrane.DefaultConfig()
	cfg.Backend = "postgres"
	cfg.PostgresDSN = *postgresDSN
	cfg.EmbeddingEndpoint = *embeddingEndpoint
	cfg.EmbeddingModel = *embeddingModel
	cfg.EmbeddingAPIKey = apiKey
	cfg.EmbeddingDimensions = *embeddingDims
	cfg.SelectionConfidenceThreshold = 0.3

	m, err := membrane.New(cfg)
	if err != nil {
		fatal("create membrane", err)
	}
	if err := m.Start(ctx); err != nil {
		fatal("start membrane", err)
	}
	defer func() { _ = m.Stop() }()

	pgStore, err := postgres.Open(*postgresDSN, postgres.EmbeddingConfig{
		Dimensions: *embeddingDims,
		Model:      *embeddingModel,
	})
	if err != nil {
		fatal("open pgstore", err)
	}
	defer pgStore.Close()

	embClient := embedding.NewHTTPClient(*embeddingEndpoint, *embeddingModel, apiKey, *embeddingDims)

	var ragWins, memWins, ties int

	// -----------------------------------------------------------------------
	// Scenario 1: RETRACTION — wrong facts should disappear
	// -----------------------------------------------------------------------
	fmt.Println("=== Scenario 1: Retraction ===")
	fmt.Println("  Setup: Two records about preferred database. Retract the wrong one.")

	wrongDB, _ := captureObservationRecord(ctx, m, "eval", "team", "prefers_database", "MySQL", []string{"database", "preference"}, "project:alpha", schema.SensitivityLow)
	correctDB, _ := captureObservationRecord(ctx, m, "eval", "team", "prefers_database", "PostgreSQL", []string{"database", "preference"}, "project:alpha", schema.SensitivityLow)
	// Add some noise records
	noiseIDs := ingestNoise(ctx, m, []string{
		"team uses Redis for caching",
		"team uses Kafka for event streaming",
		"project alpha deploys to Cloudflare Workers",
	})

	embedRecord(ctx, embClient, pgStore, wrongDB.ID, "team prefers MySQL as the primary database", *embeddingModel)
	embedRecord(ctx, embClient, pgStore, correctDB.ID, "team prefers PostgreSQL as the primary database", *embeddingModel)
	for i, id := range noiseIDs {
		embedRecord(ctx, embClient, pgStore, id, []string{
			"team uses Redis for caching",
			"team uses Kafka for event streaming",
			"project alpha deploys to Cloudflare Workers",
		}[i], *embeddingModel)
	}

	// Retract the wrong one
	if err := m.Retract(ctx, wrongDB.ID, "eval", "MySQL was incorrect"); err != nil {
		fatal("retract", err)
	}

	query := "What database does the team prefer?"
	qVec, _ := embClient.Embed(ctx, query)

	// RAG: pure vector search (doesn't know about retraction)
	ragTop := ragSearch(ctx, pgStore, qVec, 3)
	ragHasWrong := contains(ragTop, wrongDB.ID)
	ragHasCorrect := contains(ragTop, correctDB.ID)

	// Membrane: respects retraction (salience=0, filtered with MinSalience)
	memResp, _ := retrieveRootRecords(ctx, m, &retrieval.RetrieveRequest{
		TaskDescriptor: query, QueryEmbedding: qVec,
		Trust:       fullTrust(),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		MinSalience: 0.01, Limit: 3,
	})
	memHasWrong := containsRec(memResp.Records, wrongDB.ID)
	memHasCorrect := containsRec(memResp.Records, correctDB.ID)

	fmt.Printf("  Query: %q\n", query)
	fmt.Printf("  RAG:      returns wrong (MySQL)=%v  returns correct (PostgreSQL)=%v\n", ragHasWrong, ragHasCorrect)
	fmt.Printf("  Membrane: returns wrong (MySQL)=%v  returns correct (PostgreSQL)=%v\n", memHasWrong, memHasCorrect)
	if !memHasWrong && ragHasWrong {
		fmt.Println("  ✓ Membrane wins: filtered retracted record")
		memWins++
	} else if memHasWrong == ragHasWrong {
		fmt.Println("  — Tie")
		ties++
	} else {
		fmt.Println("  ✗ RAG wins")
		ragWins++
	}

	// -----------------------------------------------------------------------
	// Scenario 2: REINFORCEMENT — useful records rank higher
	// -----------------------------------------------------------------------
	fmt.Println("\n=== Scenario 2: Reinforcement ===")
	fmt.Println("  Setup: Two similar debugging procedures. Reinforce the one that worked.")

	proc1, _ := captureObservationRecord(ctx, m, "eval", "debug-oom", "procedure", "restart pods and hope for the best", []string{"debugging", "oom"}, "", schema.SensitivityLow)
	proc2, _ := captureObservationRecord(ctx, m, "eval", "debug-oom", "procedure", "capture pprof heap profile then trace allocations", []string{"debugging", "oom"}, "", schema.SensitivityLow)

	embedRecord(ctx, embClient, pgStore, proc1.ID, "debugging OOM kill by restarting pods and hoping the problem goes away", *embeddingModel)
	embedRecord(ctx, embClient, pgStore, proc2.ID, "debugging OOM kill by capturing pprof heap profile and tracing memory allocations to source", *embeddingModel)

	// Reinforce proc2 multiple times (it actually worked)
	for i := 0; i < 5; i++ {
		_ = m.Reinforce(ctx, proc2.ID, "eval", "this procedure resolved the OOM")
	}
	// Penalize proc1 (it didn't help)
	_ = m.Penalize(ctx, proc1.ID, 0.5, "eval", "restarting didn't fix the root cause")

	query = "How do I debug an OOM kill?"
	qVec, _ = embClient.Embed(ctx, query)

	ragTop = ragSearch(ctx, pgStore, qVec, 2)
	ragFirst := ""
	if len(ragTop) > 0 {
		ragFirst = ragTop[0]
	}

	memResp, _ = retrieveRootRecords(ctx, m, &retrieval.RetrieveRequest{
		TaskDescriptor: query, QueryEmbedding: qVec,
		Trust:       fullTrust(),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		Limit:       2,
	})
	memFirst := ""
	if len(memResp.Records) > 0 {
		memFirst = memResp.Records[0].ID
	}

	fmt.Printf("  Query: %q\n", query)
	fmt.Printf("  RAG top result:      %s (proc2=%v)\n", label(ragFirst, proc1.ID, proc2.ID), ragFirst == proc2.ID)
	fmt.Printf("  Membrane top result: %s (proc2=%v)\n", label(memFirst, proc1.ID, proc2.ID), memFirst == proc2.ID)
	if memFirst == proc2.ID && ragFirst != proc2.ID {
		fmt.Println("  ✓ Membrane wins: reinforced record ranked first")
		memWins++
	} else if memFirst == proc2.ID && ragFirst == proc2.ID {
		fmt.Println("  — Tie (both got it right)")
		ties++
	} else {
		fmt.Println("  ✗ RAG wins")
		ragWins++
	}

	// -----------------------------------------------------------------------
	// Scenario 3: SUPERSESSION — outdated facts replaced
	// -----------------------------------------------------------------------
	fmt.Println("\n=== Scenario 3: Supersession ===")
	fmt.Println("  Setup: Rate limit changed from 50 rps to 200 rps. Old record superseded.")

	oldLimit, _ := captureObservationRecord(ctx, m, "eval", "api", "rate_limit", "50 requests per second", []string{"api", "rate-limit"}, "", schema.SensitivityLow)
	embedRecord(ctx, embClient, pgStore, oldLimit.ID, "API rate limit is 50 requests per second per client", *embeddingModel)

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
		fatal("supersede", err)
	}
	embedRecord(ctx, embClient, pgStore, superseded.ID, "API rate limit is 200 requests per second per client", *embeddingModel)

	query = "What is the current API rate limit?"
	qVec, _ = embClient.Embed(ctx, query)

	ragTop = ragSearch(ctx, pgStore, qVec, 2)
	ragReturnsOld := contains(ragTop, oldLimit.ID)
	ragReturnsNew := contains(ragTop, superseded.ID)

	memResp, _ = retrieveRootRecords(ctx, m, &retrieval.RetrieveRequest{
		TaskDescriptor: query, QueryEmbedding: qVec,
		Trust:       fullTrust(),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		MinSalience: 0.01, Limit: 2,
	})
	memReturnsOld := containsRec(memResp.Records, oldLimit.ID)
	memReturnsNew := containsRec(memResp.Records, superseded.ID)

	fmt.Printf("  Query: %q\n", query)
	fmt.Printf("  RAG:      old (50rps)=%v  new (200rps)=%v\n", ragReturnsOld, ragReturnsNew)
	fmt.Printf("  Membrane: old (50rps)=%v  new (200rps)=%v\n", memReturnsOld, memReturnsNew)
	if memReturnsNew && !memReturnsOld && ragReturnsOld {
		fmt.Println("  ✓ Membrane wins: superseded record filtered, new version returned")
		memWins++
	} else if !memReturnsOld && !ragReturnsOld {
		fmt.Println("  — Tie")
		ties++
	} else {
		fmt.Println("  ~ Partial (both return new, RAG also returns old)")
		if memReturnsNew && !memReturnsOld {
			memWins++
		} else {
			ties++
		}
	}

	// -----------------------------------------------------------------------
	// Scenario 4: DECAY — stale records deprioritized
	// -----------------------------------------------------------------------
	fmt.Println("\n=== Scenario 4: Decay ===")
	fmt.Println("  Setup: Two deployment records. One is fresh, one has decayed salience.")

	staleRec, _ := captureObservationRecord(ctx, m, "eval", "deployment", "target", "Heroku (deprecated)", []string{"deploy"}, "", schema.SensitivityLow)
	freshRec, _ := captureObservationRecord(ctx, m, "eval", "deployment", "target", "Kubernetes on GKE", []string{"deploy"}, "", schema.SensitivityLow)

	embedRecord(ctx, embClient, pgStore, staleRec.ID, "production deployment targets Heroku platform", *embeddingModel)
	embedRecord(ctx, embClient, pgStore, freshRec.ID, "production deployment targets Kubernetes on GKE", *embeddingModel)

	// Simulate decay: penalize the stale record heavily
	_ = m.Penalize(ctx, staleRec.ID, 0.85, "eval", "outdated deployment target")
	// Reinforce the fresh one
	_ = m.Reinforce(ctx, freshRec.ID, "eval", "current deployment target confirmed")

	query = "Where do we deploy to production?"
	qVec, _ = embClient.Embed(ctx, query)

	ragTop = ragSearch(ctx, pgStore, qVec, 2)
	ragFirstDeploy := ""
	if len(ragTop) > 0 {
		ragFirstDeploy = ragTop[0]
	}

	memResp, _ = retrieveRootRecords(ctx, m, &retrieval.RetrieveRequest{
		TaskDescriptor: query, QueryEmbedding: qVec,
		Trust:       fullTrust(),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		Limit:       2,
	})
	memFirstDeploy := ""
	if len(memResp.Records) > 0 {
		memFirstDeploy = memResp.Records[0].ID
	}

	fmt.Printf("  Query: %q\n", query)
	fmt.Printf("  RAG top result:      %s\n", label(ragFirstDeploy, staleRec.ID, freshRec.ID))
	fmt.Printf("  Membrane top result: %s\n", label(memFirstDeploy, staleRec.ID, freshRec.ID))
	if memFirstDeploy == freshRec.ID && ragFirstDeploy == staleRec.ID {
		fmt.Println("  ✓ Membrane wins: decayed stale record, fresh record ranked first")
		memWins++
	} else if memFirstDeploy == freshRec.ID && ragFirstDeploy == freshRec.ID {
		fmt.Println("  — Tie (both prefer fresh)")
		ties++
	} else {
		fmt.Println("  ✗ RAG wins")
		ragWins++
	}

	// -----------------------------------------------------------------------
	// Summary
	// -----------------------------------------------------------------------
	fmt.Println("\n==================== LIFECYCLE EVAL SUMMARY ====================")
	fmt.Printf("  Membrane wins: %d\n", memWins)
	fmt.Printf("  RAG wins:      %d\n", ragWins)
	fmt.Printf("  Ties:          %d\n", ties)
	total := memWins + ragWins + ties
	if total > 0 {
		fmt.Printf("  Membrane advantage: %.0f%%\n", float64(memWins)/float64(total)*100)
	}
	fmt.Println("================================================================")
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func ingestNoise(ctx context.Context, m *membrane.Membrane, texts []string) []string {
	ids := make([]string, len(texts))
	for i, text := range texts {
		parts := strings.SplitN(text, " ", 3)
		subj, pred, obj := parts[0], "uses", text
		if len(parts) >= 3 {
			subj, pred, obj = parts[0], parts[1], strings.Join(parts[2:], " ")
		}
		rec, err := captureObservationRecord(ctx, m, "eval", subj, pred, obj, []string{"noise"}, "", schema.SensitivityLow)
		if err != nil {
			fatal("ingest noise", err)
		}
		ids[i] = rec.ID
	}
	return ids
}

func embedRecord(ctx context.Context, client embedding.Client, store *postgres.PostgresStore, id, text, model string) {
	vec, err := client.Embed(ctx, text)
	if err != nil {
		fatal("embed "+id, err)
	}
	if err := store.StoreTriggerEmbedding(ctx, id, vec, model); err != nil {
		fatal("store embedding "+id, err)
	}
}

func ragSearch(ctx context.Context, store *postgres.PostgresStore, qVec []float32, k int) []string {
	// Search broadly then take top-k. ivfflat needs enough data to work;
	// searching with a larger limit avoids empty results on small datasets.
	limit := k
	if limit < 100 {
		limit = 100
	}
	ids, err := store.SearchByEmbedding(ctx, qVec, limit)
	if err != nil {
		fatal("rag search", err)
	}
	if len(ids) > k {
		ids = ids[:k]
	}
	return ids
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
		if r.ID == target {
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
		return "other: " + id[:8]
	}
}

func captureObservationRecord(ctx context.Context, m *membrane.Membrane, source, subject, predicate string, object any, tags []string, scope string, sensitivity schema.Sensitivity) (*schema.MemoryRecord, error) {
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

func retrieveRootRecords(ctx context.Context, m *membrane.Membrane, req *retrieval.RetrieveRequest) (*retrieval.RetrieveResponse, error) {
	rootLimit := req.Limit
	if rootLimit <= 0 {
		rootLimit = 10000
	}
	graphResp, err := m.RetrieveGraph(ctx, &retrieval.RetrieveGraphRequest{
		TaskDescriptor: req.TaskDescriptor,
		Trust:          req.Trust,
		MemoryTypes:    req.MemoryTypes,
		MinSalience:    req.MinSalience,
		RootLimit:      rootLimit,
		NodeLimit:      rootLimit,
		EdgeLimit:      0,
		MaxHops:        0,
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

func fatal(action string, err error) {
	fmt.Fprintf(os.Stderr, "%s: %v\n", action, err)
	os.Exit(1)
}
