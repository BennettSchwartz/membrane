package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/GustyCube/membrane/pkg/ingestion"
	"github.com/GustyCube/membrane/pkg/membrane"
	"github.com/GustyCube/membrane/pkg/retrieval"
	"github.com/GustyCube/membrane/pkg/schema"
)

type recordEntry struct {
	Type        string   `json:"type"`
	Key         string   `json:"key"`
	Text        string   `json:"text"`
	Subject     string   `json:"subject"`
	Predicate   string   `json:"predicate"`
	Object      string   `json:"object"`
	Scope       string   `json:"scope"`
	Sensitivity string   `json:"sensitivity"`
	Tags        []string `json:"tags"`
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
}

type dataset struct {
	records []recordEntry
	queries []queryEntry
}

type candidate struct {
	id    string
	score float64
}

func main() {
	var (
		datasetPath = flag.String("dataset", "tests/data/recall_dataset.jsonl", "Path to JSONL dataset")
		model       = flag.String("model", "all-MiniLM-L6-v2", "Sentence-transformers model name")
		defaultK    = flag.Int("k", 5, "Default K when query does not specify one")
		minRecall   = flag.Float64("min-recall", -1, "Fail if mean recall@k is below this threshold")
		minPrec     = flag.Float64("min-precision", -1, "Fail if mean precision@k is below this threshold")
		minMRR      = flag.Float64("min-mrr", -1, "Fail if mean MRR@k is below this threshold")
		minNDCG     = flag.Float64("min-ndcg", -1, "Fail if mean NDCG@k is below this threshold")
		verbose     = flag.Bool("verbose", false, "Print per-query results")
	)
	flag.Parse()

	data, err := loadDataset(*datasetPath, *defaultK)
	if err != nil {
		fatal("load dataset", err)
	}

	ctx := context.Background()
	tmpDir, err := os.MkdirTemp("", "membrane-eval-")
	if err != nil {
		fatal("create temp dir", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := membrane.DefaultConfig()
	cfg.DBPath = filepath.Join(tmpDir, "membrane-eval.db")
	m, err := membrane.New(cfg)
	if err != nil {
		fatal("create membrane", err)
	}
	if err := m.Start(ctx); err != nil {
		fatal("start membrane", err)
	}
	defer func() {
		if stopErr := m.Stop(); stopErr != nil {
			fmt.Fprintf(os.Stderr, "membrane stop error: %v\n", stopErr)
		}
	}()

	idByKey := make(map[string]string, len(data.records))
	recordTexts := make([]string, 0, len(data.records))
	recordIDs := make([]string, 0, len(data.records))

	for _, rec := range data.records {
		if rec.Key == "" {
			fatal("dataset", errors.New("record key is required"))
		}
		stored, err := m.IngestObservation(ctx, ingestion.IngestObservationRequest{
			Source:      "eval",
			Subject:     rec.Subject,
			Predicate:   rec.Predicate,
			Object:      rec.Object,
			Tags:        rec.Tags,
			Scope:       rec.Scope,
			Sensitivity: parseSensitivity(rec.Sensitivity),
		})
		if err != nil {
			fatal("ingest record", err)
		}
		idByKey[rec.Key] = stored.ID
		recordIDs = append(recordIDs, stored.ID)
		recordTexts = append(recordTexts, rec.Text)
	}

	recordEmbeds, err := embedTexts(ctx, *model, recordTexts)
	if err != nil {
		fatal("embed records", err)
	}
	if len(recordEmbeds) != len(recordTexts) {
		fatal("embed records", fmt.Errorf("expected %d embeddings, got %d", len(recordTexts), len(recordEmbeds)))
	}

	embeddingByID := make(map[string][]float64, len(recordEmbeds))
	for i, id := range recordIDs {
		embeddingByID[id] = normalizeVec(recordEmbeds[i])
	}

	queryTexts := make([]string, 0, len(data.queries))
	for _, q := range data.queries {
		queryTexts = append(queryTexts, q.Text)
	}
	queryEmbeds, err := embedTexts(ctx, *model, queryTexts)
	if err != nil {
		fatal("embed queries", err)
	}
	if len(queryEmbeds) != len(queryTexts) {
		fatal("embed queries", fmt.Errorf("expected %d embeddings, got %d", len(queryTexts), len(queryEmbeds)))
	}

	var (
		recallSum    float64
		precisionSum float64
		mrrSum       float64
		ndcgSum      float64
	)

	for idx, q := range data.queries {
		trust := toTrustContext(q.Trust)
		resp, err := m.Retrieve(ctx, &retrieval.RetrieveRequest{
			Trust:       trust,
			MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
			MinSalience: q.MinSalience,
			Limit:       0,
		})
		if err != nil {
			fatal("retrieve candidates", err)
		}

		allowed := make(map[string]struct{}, len(resp.Records))
		for _, rec := range resp.Records {
			allowed[rec.ID] = struct{}{}
		}

		candidates := make([]candidate, 0, len(allowed))
		qVec := normalizeVec(queryEmbeds[idx])
		for id, vec := range embeddingByID {
			if _, ok := allowed[id]; !ok {
				continue
			}
			candidates = append(candidates, candidate{id: id, score: dot(qVec, vec)})
		}

		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].score > candidates[j].score
		})

		k := q.K
		if k <= 0 {
			k = *defaultK
		}

		topCount := k
		if topCount > len(candidates) {
			topCount = len(candidates)
		}

		topIDs := make([]string, 0, topCount)
		for i := 0; i < topCount; i++ {
			topIDs = append(topIDs, candidates[i].id)
		}

		expectedIDs := make([]string, 0, len(q.Expected))
		for _, key := range q.Expected {
			id, ok := idByKey[key]
			if !ok {
				fatal("expected id", fmt.Errorf("unknown record key %q for query %q", key, q.Key))
			}
			expectedIDs = append(expectedIDs, id)
		}

		recall := recallAtK(topIDs, expectedIDs, k)
		precision := precisionAtK(topIDs, expectedIDs, k)
		mrr := mrrAtK(topIDs, expectedIDs, k)
		ndcg := ndcgAtK(topIDs, expectedIDs, k)

		recallSum += recall
		precisionSum += precision
		mrrSum += mrr
		ndcgSum += ndcg

		if *verbose {
			fmt.Printf("%s: recall@%d=%.2f precision@%d=%.2f mrr@%d=%.2f ndcg@%d=%.2f top=%v expected=%v\n",
				q.Key, k, recall, k, precision, k, mrr, k, ndcg, shorten(topIDs, 5), shorten(expectedIDs, 5))
		}
	}

	count := float64(len(data.queries))
	if count == 0 {
		fatal("dataset", errors.New("no queries found"))
	}

	meanRecall := recallSum / count
	meanPrecision := precisionSum / count
	meanMRR := mrrSum / count
	meanNDCG := ndcgSum / count

	fmt.Printf("Vector eval complete (model=%s)\n", *model)
	fmt.Printf("Records: %d, Queries: %d\n", len(data.records), len(data.queries))
	fmt.Printf("Mean recall@k: %.3f\n", meanRecall)
	fmt.Printf("Mean precision@k: %.3f\n", meanPrecision)
	fmt.Printf("Mean mrr@k: %.3f\n", meanMRR)
	fmt.Printf("Mean ndcg@k: %.3f\n", meanNDCG)

	thresholdFailures := make([]string, 0, 4)
	if *minRecall >= 0 && meanRecall < *minRecall {
		thresholdFailures = append(thresholdFailures, fmt.Sprintf("recall@k %.3f < %.3f", meanRecall, *minRecall))
	}
	if *minPrec >= 0 && meanPrecision < *minPrec {
		thresholdFailures = append(thresholdFailures, fmt.Sprintf("precision@k %.3f < %.3f", meanPrecision, *minPrec))
	}
	if *minMRR >= 0 && meanMRR < *minMRR {
		thresholdFailures = append(thresholdFailures, fmt.Sprintf("mrr@k %.3f < %.3f", meanMRR, *minMRR))
	}
	if *minNDCG >= 0 && meanNDCG < *minNDCG {
		thresholdFailures = append(thresholdFailures, fmt.Sprintf("ndcg@k %.3f < %.3f", meanNDCG, *minNDCG))
	}
	if len(thresholdFailures) > 0 {
		fatal("thresholds", fmt.Errorf("failed: %s", strings.Join(thresholdFailures, "; ")))
	}
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
			if rec.Text == "" {
				return nil, fmt.Errorf("record %q missing text", rec.Key)
			}
			data.records = append(data.records, rec)
		case "query":
			var q queryEntry
			if err := json.Unmarshal([]byte(line), &q); err != nil {
				return nil, fmt.Errorf("parse query: %w", err)
			}
			if q.Text == "" {
				return nil, fmt.Errorf("query %q missing text", q.Key)
			}
			if q.K == 0 {
				q.K = defaultK
			}
			data.queries = append(data.queries, q)
		default:
			return nil, fmt.Errorf("unknown entry type %q", base.Type)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return data, nil
}

func embedTexts(ctx context.Context, model string, texts []string) ([][]float64, error) {
	if len(texts) == 0 {
		return nil, nil
	}

	payload := map[string]any{
		"model": model,
		"texts": texts,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	cmd := exec.CommandContext(ctx, "python3", "tools/eval/embed.py")
	cmd.Stdin = bytes.NewReader(data)
	var out bytes.Buffer
	var errBuf bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errBuf

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("embed failed: %w: %s", err, strings.TrimSpace(errBuf.String()))
	}

	var resp struct {
		Embeddings [][]float64 `json:"embeddings"`
	}
	if err := json.Unmarshal(out.Bytes(), &resp); err != nil {
		return nil, fmt.Errorf("parse embed response: %w", err)
	}
	return resp.Embeddings, nil
}

func toTrustContext(trust trustEntry) *retrieval.TrustContext {
	actor := trust.ActorID
	if actor == "" {
		actor = "eval"
	}
	max := parseSensitivity(trust.MaxSensitivity)
	scopes := trust.Scopes
	if len(scopes) == 0 {
		scopes = nil
	}
	return retrieval.NewTrustContext(max, trust.Authenticated, actor, scopes)
}

func parseSensitivity(raw string) schema.Sensitivity {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "public":
		return schema.SensitivityPublic
	case "low", "":
		return schema.SensitivityLow
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

func normalizeVec(vec []float64) []float64 {
	var sum float64
	for _, v := range vec {
		sum += v * v
	}
	if sum == 0 {
		return vec
	}
	inv := 1.0 / math.Sqrt(sum)
	out := make([]float64, len(vec))
	for i, v := range vec {
		out[i] = v * inv
	}
	return out
}

func dot(a, b []float64) float64 {
	if len(a) != len(b) {
		return 0
	}
	var sum float64
	for i := range a {
		sum += a[i] * b[i]
	}
	return sum
}

func recallAtK(got, expected []string, k int) float64 {
	if len(expected) == 0 {
		return 1
	}
	relevant := make(map[string]struct{}, len(expected))
	for _, id := range expected {
		relevant[id] = struct{}{}
	}

	found := 0
	limit := k
	if limit > len(got) {
		limit = len(got)
	}
	for i := 0; i < limit; i++ {
		if _, ok := relevant[got[i]]; ok {
			found++
		}
	}
	return float64(found) / float64(len(expected))
}

func precisionAtK(got, expected []string, k int) float64 {
	if k == 0 {
		if len(expected) == 0 {
			return 1
		}
		return 0
	}
	relevant := make(map[string]struct{}, len(expected))
	for _, id := range expected {
		relevant[id] = struct{}{}
	}

	found := 0
	limit := k
	if limit > len(got) {
		limit = len(got)
	}
	for i := 0; i < limit; i++ {
		if _, ok := relevant[got[i]]; ok {
			found++
		}
	}
	return float64(found) / float64(limit)
}

func mrrAtK(got, expected []string, k int) float64 {
	relevant := make(map[string]struct{}, len(expected))
	for _, id := range expected {
		relevant[id] = struct{}{}
	}

	limit := k
	if limit > len(got) {
		limit = len(got)
	}
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
	relevant := make(map[string]struct{}, len(expected))
	for _, id := range expected {
		relevant[id] = struct{}{}
	}

	limit := k
	if limit > len(got) {
		limit = len(got)
	}

	var dcg float64
	for i := 0; i < limit; i++ {
		if _, ok := relevant[got[i]]; ok {
			dcg += 1.0 / math.Log2(float64(i)+2.0)
		}
	}

	idealCount := len(expected)
	if idealCount > k {
		idealCount = k
	}
	var idcg float64
	for i := 0; i < idealCount; i++ {
		idcg += 1.0 / math.Log2(float64(i)+2.0)
	}
	if idcg == 0 {
		return 1
	}
	return dcg / idcg
}

func shorten(ids []string, max int) []string {
	if len(ids) <= max {
		return ids
	}
	out := make([]string, 0, max+1)
	out = append(out, ids[:max]...)
	out = append(out, "...")
	return out
}

func fatal(action string, err error) {
	fmt.Fprintf(os.Stderr, "%s: %v\n", action, err)
	os.Exit(1)
}
