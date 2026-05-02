package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/BennettSchwartz/membrane/pkg/embedding"
	"github.com/BennettSchwartz/membrane/pkg/ingestion"
	"github.com/BennettSchwartz/membrane/pkg/membrane"
	"github.com/BennettSchwartz/membrane/pkg/retrieval"
	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage/postgres"
)

// ---------------------------------------------------------------------------
// Dataset types
// ---------------------------------------------------------------------------

type planNodeEntry struct {
	ID string `json:"id"`
	Op string `json:"op"`
}

type planEdgeEntry struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type recordEntry struct {
	Type        string   `json:"type"`
	MemoryType  string   `json:"memory_type"`
	Key         string   `json:"key"`
	Text        string   `json:"text"` // used for embedding
	Sensitivity string   `json:"sensitivity"`
	Scope       string   `json:"scope"`
	Tags        []string `json:"tags"`
	// Semantic
	Subject   string `json:"subject"`
	Predicate string `json:"predicate"`
	Object    string `json:"object"`
	// Episodic
	EventKind string `json:"event_kind"`
	Ref       string `json:"ref"`
	Summary   string `json:"summary"`
	// Working
	ThreadID    string   `json:"thread_id"`
	State       string   `json:"state"`
	NextActions []string `json:"next_actions"`
	// Competence
	SkillName string   `json:"skill_name"`
	Triggers  []string `json:"triggers"`
	Steps     []string `json:"steps"`
	// Plan graph
	Intent string          `json:"intent"`
	Nodes  []planNodeEntry `json:"nodes"`
	Edges  []planEdgeEntry `json:"edges"`
}

type trustEntry struct {
	MaxSensitivity string   `json:"max_sensitivity"`
	Authenticated  bool     `json:"authenticated"`
	ActorID        string   `json:"actor_id"`
	Scopes         []string `json:"scopes"`
}

type queryEntry struct {
	Type        string     `json:"type"`
	Key         string     `json:"key"`
	Text        string     `json:"text"`
	Expected    []string   `json:"expected"`
	K           int        `json:"k"`
	Trust       trustEntry `json:"trust"`
	MinSalience float64    `json:"min_salience"`
	MemoryTypes []string   `json:"memory_types"`
}

type dataset struct {
	records []recordEntry
	queries []queryEntry
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

type evalMetrics struct {
	recall    float64
	precision float64
	mrr       float64
	ndcg      float64
}

type recordCreator interface {
	Create(context.Context, *schema.MemoryRecord) error
}

type embeddingSearcher interface {
	SearchByEmbedding(context.Context, []float32, int) ([]string, error)
}

type evalVectorStore interface {
	recordCreator
	embeddingSearcher
	StoreTriggerEmbedding(context.Context, string, []float32, string) error
	Close() error
}

type evalMembrane interface {
	Start(context.Context) error
	Stop() error
	CaptureMemory(context.Context, ingestion.CaptureMemoryRequest) (*ingestion.CaptureMemoryResponse, error)
	RetrieveGraph(context.Context, *retrieval.RetrieveGraphRequest) (*retrieval.RetrieveGraphResponse, error)
}

type evalCLIDependencies struct {
	newMembrane        func(*membrane.Config) (evalMembrane, error)
	openVectorStore    func(string, postgres.EmbeddingConfig) (evalVectorStore, error)
	newEmbeddingClient func(endpoint, model, apiKey string, dimensions int) embedding.Client
}

func (d evalCLIDependencies) withDefaults() evalCLIDependencies {
	if d.newMembrane == nil {
		d.newMembrane = func(cfg *membrane.Config) (evalMembrane, error) {
			return membrane.New(cfg)
		}
	}
	if d.openVectorStore == nil {
		d.openVectorStore = func(dsn string, cfg postgres.EmbeddingConfig) (evalVectorStore, error) {
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

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

var (
	evalStderr io.Writer = os.Stderr
	evalExit             = os.Exit
)

func main() {
	if err := runEvalCLI(context.Background(), os.Args[1:], os.Getenv); err != nil {
		fmt.Fprintln(evalStderr, err)
		evalExit(1)
	}
}

func runEvalCLI(ctx context.Context, args []string, getenv func(string) string) error {
	return runEvalCLIWithDeps(ctx, args, getenv, evalCLIDependencies{})
}

func runEvalCLIWithDeps(ctx context.Context, args []string, getenv func(string) string, deps evalCLIDependencies) error {
	deps = deps.withDefaults()
	if getenv == nil {
		getenv = os.Getenv
	}
	fs := flag.NewFlagSet("membrane-eval", flag.ContinueOnError)
	var (
		datasetPath       = fs.String("dataset", "tests/data/recall_dataset.jsonl", "Path to JSONL dataset")
		postgresDSN       = fs.String("postgres-dsn", "", "PostgreSQL DSN (required)")
		embeddingEndpoint = fs.String("embedding-endpoint", "https://openrouter.ai/api/v1/embeddings", "Embedding API endpoint")
		embeddingModel    = fs.String("embedding-model", "openai/text-embedding-3-small", "Embedding model")
		embeddingAPIKey   = fs.String("embedding-api-key", "", "Embedding API key (or MEMBRANE_EMBEDDING_API_KEY)")
		embeddingDims     = fs.Int("embedding-dimensions", 1536, "Embedding dimensions")
		defaultK          = fs.Int("k", 5, "Default top-K")
		verbose           = fs.Bool("verbose", false, "Per-query results")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}

	apiKey := *embeddingAPIKey
	if apiKey == "" {
		apiKey = getenv("MEMBRANE_EMBEDDING_API_KEY")
	}
	if *postgresDSN == "" || apiKey == "" {
		return fmt.Errorf("config: %w", errors.New("--postgres-dsn and embedding API key are required"))
	}

	data, err := loadDataset(*datasetPath, *defaultK)
	if err != nil {
		return fmt.Errorf("load dataset: %w", err)
	}

	// --- Create Membrane instance (fully equipped) ---
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

	// --- Direct pgvector handle for RAG ---
	pgStore, err := deps.openVectorStore(*postgresDSN, postgres.EmbeddingConfig{
		Dimensions: *embeddingDims,
		Model:      *embeddingModel,
	})
	if err != nil {
		return fmt.Errorf("open pgstore: %w", err)
	}
	defer func() { _ = pgStore.Close() }()

	embClient := deps.newEmbeddingClient(*embeddingEndpoint, *embeddingModel, apiKey, *embeddingDims)

	// --- Ingest records ---
	fmt.Printf("Ingesting %d records...\n", len(data.records))
	idByKey := make(map[string]string, len(data.records))

	for _, rec := range data.records {
		id, err := ingestRecord(ctx, m, pgStore, rec)
		if err != nil {
			return fmt.Errorf("ingest %s: %w", rec.Key, err)
		}
		idByKey[rec.Key] = id
	}

	// --- Embed all records for RAG ---
	fmt.Printf("Embedding %d records via %s...\n", len(data.records), *embeddingModel)
	for _, rec := range data.records {
		id := idByKey[rec.Key]
		vec, err := embClient.Embed(ctx, rec.Text)
		if err != nil {
			return fmt.Errorf("embed %s: %w", rec.Key, err)
		}
		if err := pgStore.StoreTriggerEmbedding(ctx, id, vec, *embeddingModel); err != nil {
			return fmt.Errorf("store embedding %s: %w", rec.Key, err)
		}
	}
	fmt.Println("Embeddings stored.")

	// --- Pre-compute all query embeddings once to avoid rate-limit issues ---
	fmt.Printf("Embedding %d queries...\n", len(data.queries))
	queryEmbeddings := make(map[string][]float32, len(data.queries))
	for _, q := range data.queries {
		vec, err := embClient.Embed(ctx, q.Text)
		if err != nil {
			return fmt.Errorf("embed query %s: %w", q.Key, err)
		}
		queryEmbeddings[q.Key] = vec
	}

	// --- Run both evals ---
	fmt.Println("\n=== RAG (pure vector similarity + trust filter) ===")
	ragMetrics, err := runRAGEval(ctx, m, pgStore, data, idByKey, queryEmbeddings, *defaultK, *verbose, len(data.records))
	if err != nil {
		return fmt.Errorf("run rag eval: %w", err)
	}

	fmt.Println("\n=== Membrane (full pipeline: salience + trust + vector ranking) ===")
	memMetrics, err := runMembraneEval(ctx, m, data, idByKey, queryEmbeddings, *defaultK, *verbose)
	if err != nil {
		return fmt.Errorf("run membrane eval: %w", err)
	}

	// --- Summary ---
	fmt.Println("\n==================== COMPARISON ====================")
	fmt.Printf("%-20s %10s %10s %10s\n", "Metric", "RAG", "Membrane", "Delta")
	fmt.Printf("%-20s %10s %10s %10s\n", "------", "---", "--------", "-----")
	printRow("recall@k", ragMetrics.recall, memMetrics.recall)
	printRow("precision@k", ragMetrics.precision, memMetrics.precision)
	printRow("MRR@k", ragMetrics.mrr, memMetrics.mrr)
	printRow("NDCG@k", ragMetrics.ndcg, memMetrics.ndcg)
	fmt.Println("====================================================")
	return nil
}

func printRow(name string, rag, mem float64) {
	delta := mem - rag
	sign := "+"
	if delta < 0 {
		sign = ""
	}
	fmt.Printf("%-20s %10.3f %10.3f %9s%.3f\n", name, rag, mem, sign, delta)
}

// ---------------------------------------------------------------------------
// Ingestion — typed records into Membrane + raw store
// ---------------------------------------------------------------------------

func ingestRecord(ctx context.Context, m evalMembrane, store recordCreator, rec recordEntry) (string, error) {
	sens := parseSensitivity(rec.Sensitivity)

	switch rec.MemoryType {
	case "semantic":
		r, err := captureObservationRecord(ctx, m, "eval", rec.Subject, rec.Predicate, rec.Object, rec.Tags, rec.Scope, sens)
		if err != nil {
			return "", err
		}
		return r.ID, nil

	case "episodic":
		r, err := captureEventRecord(ctx, m, "eval", rec.EventKind, rec.Ref, rec.Summary, rec.Tags, rec.Scope, sens)
		if err != nil {
			return "", err
		}
		return r.ID, nil

	case "working":
		r, err := captureWorkingStateRecord(ctx, m, "eval", rec.ThreadID, schema.TaskState(rec.State), rec.NextActions, rec.Tags, rec.Scope, sens)
		if err != nil {
			return "", err
		}
		return r.ID, nil

	case "competence":
		triggers := make([]schema.Trigger, len(rec.Triggers))
		for i, t := range rec.Triggers {
			triggers[i] = schema.Trigger{Signal: t}
		}
		steps := make([]schema.RecipeStep, len(rec.Steps))
		for i, s := range rec.Steps {
			steps[i] = schema.RecipeStep{Step: s}
		}
		record := schema.NewMemoryRecord(uuid.New().String(), schema.MemoryTypeCompetence, sens, &schema.CompetencePayload{
			Kind:      "competence",
			SkillName: rec.SkillName,
			Triggers:  triggers,
			Recipe:    steps,
			Performance: &schema.PerformanceStats{
				SuccessCount: 8,
				FailureCount: 2,
			},
		})
		record.Scope = rec.Scope
		record.Tags = rec.Tags
		record.Confidence = 0.85
		if err := store.Create(ctx, record); err != nil {
			return "", err
		}
		return record.ID, nil

	case "plan_graph":
		nodes := make([]schema.PlanNode, len(rec.Nodes))
		for i, n := range rec.Nodes {
			nodes[i] = schema.PlanNode{ID: n.ID, Op: n.Op}
		}
		edges := make([]schema.PlanEdge, len(rec.Edges))
		for i, e := range rec.Edges {
			edges[i] = schema.PlanEdge{From: e.From, To: e.To, Kind: schema.EdgeKindControl}
		}
		record := schema.NewMemoryRecord(uuid.New().String(), schema.MemoryTypePlanGraph, sens, &schema.PlanGraphPayload{
			Kind:    "plan_graph",
			PlanID:  rec.Key,
			Version: "1.0",
			Intent:  rec.Intent,
			Nodes:   nodes,
			Edges:   edges,
			Metrics: &schema.PlanMetrics{ExecutionCount: 3, FailureRate: 0.1},
		})
		record.Scope = rec.Scope
		record.Tags = rec.Tags
		record.Confidence = 0.9
		if err := store.Create(ctx, record); err != nil {
			return "", err
		}
		return record.ID, nil

	default:
		return "", fmt.Errorf("unknown memory_type %q", rec.MemoryType)
	}
}

// ---------------------------------------------------------------------------
// RAG eval: embed query → pgvector search → trust filter → top-k
// ---------------------------------------------------------------------------

func runRAGEval(ctx context.Context, m evalMembrane, pgStore embeddingSearcher, data *dataset, idByKey map[string]string, queryEmbeddings map[string][]float32, defaultK int, verbose bool, totalRecords int) (evalMetrics, error) {
	if data == nil || len(data.queries) == 0 {
		fmt.Println("  Mean: recall=0.000 precision=0.000 MRR=0.000 NDCG=0.000")
		return evalMetrics{}, nil
	}

	var recallSum, precisionSum, mrrSum, ndcgSum float64

	for _, q := range data.queries {
		k := queryLimit(q.K, defaultK)

		// Trust-filtered set from Membrane (RAG still respects access control).
		memTypes := allMemoryTypes()
		if len(q.MemoryTypes) > 0 {
			memTypes = parseMemoryTypes(q.MemoryTypes)
		}
		trust := toTrustContext(q.Trust)
		resp, err := retrieveRootRecords(ctx, m, &retrieval.RetrieveRequest{
			Trust:       trust,
			MemoryTypes: memTypes,
			MinSalience: q.MinSalience,
			Limit:       0,
		})
		if err != nil {
			return evalMetrics{}, fmt.Errorf("rag retrieve %s: %w", q.Key, err)
		}
		allowed := make(map[string]struct{}, len(resp.Records))
		for _, rec := range resp.Records {
			allowed[rec.ID] = struct{}{}
		}

		// Use pre-computed query embedding → pgvector cosine search.
		qVec, ok := queryEmbeddings[q.Key]
		if !ok {
			return evalMetrics{}, fmt.Errorf("missing query embedding for %q", q.Key)
		}
		ranked, err := pgStore.SearchByEmbedding(ctx, qVec, totalRecords)
		if err != nil {
			return evalMetrics{}, fmt.Errorf("rag search %s: %w", q.Key, err)
		}

		// Intersect with trust-filtered set.
		topIDs := make([]string, 0, k)
		for _, id := range ranked {
			if len(topIDs) >= k {
				break
			}
			if _, ok := allowed[id]; ok {
				topIDs = append(topIDs, id)
			}
		}

		expectedIDs, err := resolveExpected(q, idByKey)
		if err != nil {
			return evalMetrics{}, err
		}
		r, p, mrr, ndcg := computeMetrics(topIDs, expectedIDs, k)
		recallSum += r
		precisionSum += p
		mrrSum += mrr
		ndcgSum += ndcg

		if verbose {
			fmt.Printf("  %s: recall@%d=%.2f prec=%.2f mrr=%.2f ndcg=%.2f\n", q.Key, k, r, p, mrr, ndcg)
		}
	}

	count := float64(len(data.queries))
	met := evalMetrics{recallSum / count, precisionSum / count, mrrSum / count, ndcgSum / count}
	fmt.Printf("  Mean: recall=%.3f precision=%.3f MRR=%.3f NDCG=%.3f\n", met.recall, met.precision, met.mrr, met.ndcg)
	return met, nil
}

// ---------------------------------------------------------------------------
// Membrane eval: full pipeline via Retrieve with TaskDescriptor
// ---------------------------------------------------------------------------

func runMembraneEval(ctx context.Context, m evalMembrane, data *dataset, idByKey map[string]string, queryEmbeddings map[string][]float32, defaultK int, verbose bool) (evalMetrics, error) {
	if data == nil || len(data.queries) == 0 {
		fmt.Println("  Mean: recall=0.000 precision=0.000 MRR=0.000 NDCG=0.000")
		return evalMetrics{}, nil
	}

	var recallSum, precisionSum, mrrSum, ndcgSum float64

	for _, q := range data.queries {
		k := queryLimit(q.K, defaultK)

		memTypes := allMemoryTypes()
		if len(q.MemoryTypes) > 0 {
			memTypes = parseMemoryTypes(q.MemoryTypes)
		}
		trust := toTrustContext(q.Trust)

		qVec, ok := queryEmbeddings[q.Key]
		if !ok {
			return evalMetrics{}, fmt.Errorf("missing query embedding for %q", q.Key)
		}
		resp, err := retrieveRootRecords(ctx, m, &retrieval.RetrieveRequest{
			TaskDescriptor: q.Text,
			QueryEmbedding: qVec,
			Trust:          trust,
			MemoryTypes:    memTypes,
			MinSalience:    q.MinSalience,
			Limit:          k,
		})
		if err != nil {
			return evalMetrics{}, fmt.Errorf("membrane retrieve %s: %w", q.Key, err)
		}

		topIDs := make([]string, 0, len(resp.Records))
		for _, rec := range resp.Records {
			topIDs = append(topIDs, rec.ID)
		}

		expectedIDs, err := resolveExpected(q, idByKey)
		if err != nil {
			return evalMetrics{}, err
		}
		r, p, mrr, ndcg := computeMetrics(topIDs, expectedIDs, k)
		recallSum += r
		precisionSum += p
		mrrSum += mrr
		ndcgSum += ndcg

		if verbose {
			fmt.Printf("  %s: recall@%d=%.2f prec=%.2f mrr=%.2f ndcg=%.2f\n", q.Key, k, r, p, mrr, ndcg)
		}
	}

	count := float64(len(data.queries))
	met := evalMetrics{recallSum / count, precisionSum / count, mrrSum / count, ndcgSum / count}
	fmt.Printf("  Mean: recall=%.3f precision=%.3f MRR=%.3f NDCG=%.3f\n", met.recall, met.precision, met.mrr, met.ndcg)
	return met, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func resolveExpected(q queryEntry, idByKey map[string]string) ([]string, error) {
	ids := make([]string, 0, len(q.Expected))
	for _, key := range q.Expected {
		id, ok := idByKey[key]
		if !ok {
			return nil, fmt.Errorf("unknown key %q in query %q", key, q.Key)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func queryLimit(queryK, defaultK int) int {
	if queryK > 0 {
		return queryK
	}
	if defaultK > 0 {
		return defaultK
	}
	return 0
}

func allMemoryTypes() []schema.MemoryType {
	return []schema.MemoryType{
		schema.MemoryTypeEpisodic,
		schema.MemoryTypeWorking,
		schema.MemoryTypeSemantic,
		schema.MemoryTypeCompetence,
		schema.MemoryTypePlanGraph,
	}
}

func parseMemoryTypes(raw []string) []schema.MemoryType {
	out := make([]schema.MemoryType, len(raw))
	for i, s := range raw {
		out[i] = schema.MemoryType(s)
	}
	return out
}

func captureEventRecord(ctx context.Context, m evalMembrane, source, eventKind, ref, summary string, tags []string, scope string, sensitivity schema.Sensitivity) (*schema.MemoryRecord, error) {
	resp, err := m.CaptureMemory(ctx, ingestion.CaptureMemoryRequest{
		Source:           source,
		SourceKind:       "event",
		Content:          map[string]any{"ref": ref, "text": summary},
		ReasonToRemember: summary,
		Summary:          summary,
		Tags:             tags,
		Scope:            scope,
		Sensitivity:      sensitivity,
	})
	if err != nil {
		return nil, err
	}
	return resp.PrimaryRecord, nil
}

func captureObservationRecord(ctx context.Context, m evalMembrane, source, subject, predicate string, object any, tags []string, scope string, sensitivity schema.Sensitivity) (*schema.MemoryRecord, error) {
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

func captureWorkingStateRecord(ctx context.Context, m evalMembrane, source, threadID string, state schema.TaskState, nextActions []string, tags []string, scope string, sensitivity schema.Sensitivity) (*schema.MemoryRecord, error) {
	resp, err := m.CaptureMemory(ctx, ingestion.CaptureMemoryRequest{
		Source:     source,
		SourceKind: "working_state",
		Content: map[string]any{
			"thread_id":    threadID,
			"state":        state,
			"next_actions": nextActions,
		},
		Tags:        tags,
		Scope:       scope,
		Sensitivity: sensitivity,
	})
	if err != nil {
		return nil, err
	}
	return resp.PrimaryRecord, nil
}

func retrieveRootRecords(ctx context.Context, m evalMembrane, req *retrieval.RetrieveRequest) (*retrieval.RetrieveResponse, error) {
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

func computeMetrics(got, expected []string, k int) (recall, precision, mrr, ndcg float64) {
	return recallAtK(got, expected, k), precisionAtK(got, expected, k), mrrAtK(got, expected, k), ndcgAtK(got, expected, k)
}

func loadDataset(path string, defaultK int) (*dataset, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data := &dataset{}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var base struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal([]byte(line), &base); err != nil {
			return nil, fmt.Errorf("parse line: %w", err)
		}
		switch base.Type {
		case "record":
			var rec recordEntry
			if err := json.Unmarshal([]byte(line), &rec); err != nil {
				return nil, fmt.Errorf("parse record: %w", err)
			}
			data.records = append(data.records, rec)
		case "query":
			var q queryEntry
			if err := json.Unmarshal([]byte(line), &q); err != nil {
				return nil, fmt.Errorf("parse query: %w", err)
			}
			if q.K == 0 {
				q.K = defaultK
			}
			data.queries = append(data.queries, q)
		default:
			return nil, fmt.Errorf("unknown entry type %q", base.Type)
		}
	}
	return data, scanner.Err()
}

func toTrustContext(trust trustEntry) *retrieval.TrustContext {
	actor := trust.ActorID
	if actor == "" {
		actor = "eval"
	}
	return retrieval.NewTrustContext(parseSensitivity(trust.MaxSensitivity), trust.Authenticated, actor, trust.Scopes)
}

func parseSensitivity(raw string) schema.Sensitivity {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "public":
		return schema.SensitivityPublic
	case "medium":
		return schema.SensitivityMedium
	case "high":
		return schema.SensitivityHigh
	case "hyper":
		return schema.SensitivityHyper
	default:
		return schema.SensitivityLow
	}
}

// Silence the import.
var _ = time.Now

func recallAtK(got, expected []string, k int) float64 {
	if len(expected) == 0 {
		return 1
	}
	relevant := toSet(expected)
	found := 0
	limit := min(k, len(got))
	for i := 0; i < limit; i++ {
		if _, ok := relevant[got[i]]; ok {
			found++
		}
	}
	return float64(found) / float64(len(expected))
}

func precisionAtK(got, expected []string, k int) float64 {
	if k <= 0 {
		return 0
	}
	relevant := toSet(expected)
	found := 0
	limit := min(k, len(got))
	if limit == 0 {
		return 0
	}
	for i := 0; i < limit; i++ {
		if _, ok := relevant[got[i]]; ok {
			found++
		}
	}
	return float64(found) / float64(k)
}

func mrrAtK(got, expected []string, k int) float64 {
	if k <= 0 {
		return 0
	}
	relevant := toSet(expected)
	limit := min(k, len(got))
	for i := 0; i < limit; i++ {
		if _, ok := relevant[got[i]]; ok {
			return 1.0 / float64(i+1)
		}
	}
	return 0
}

func ndcgAtK(got, expected []string, k int) float64 {
	if len(expected) == 0 {
		return 1
	}
	if k <= 0 {
		return 0
	}
	relevant := toSet(expected)
	limit := min(k, len(got))
	var dcg float64
	for i := 0; i < limit; i++ {
		if _, ok := relevant[got[i]]; ok {
			dcg += 1.0 / math.Log2(float64(i)+2.0)
		}
	}
	idealCount := min(len(expected), k)
	var idcg float64
	for i := 0; i < idealCount; i++ {
		idcg += 1.0 / math.Log2(float64(i)+2.0)
	}
	return dcg / idcg
}

func toSet(ss []string) map[string]struct{} {
	m := make(map[string]struct{}, len(ss))
	for _, s := range ss {
		m[s] = struct{}{}
	}
	return m
}
