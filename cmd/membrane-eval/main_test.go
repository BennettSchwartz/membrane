package main

import (
	"bytes"
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/BennettSchwartz/membrane/pkg/embedding"
	"github.com/BennettSchwartz/membrane/pkg/ingestion"
	"github.com/BennettSchwartz/membrane/pkg/membrane"
	"github.com/BennettSchwartz/membrane/pkg/retrieval"
	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage/postgres"
)

type fakeRecordCreator struct {
	records []*schema.MemoryRecord
	err     error
}

func (f *fakeRecordCreator) Create(_ context.Context, rec *schema.MemoryRecord) error {
	if f.err != nil {
		return f.err
	}
	f.records = append(f.records, rec)
	return nil
}

type fakeEmbeddingSearcher struct {
	ids       []string
	err       error
	query     []float32
	limit     int
	calls     int
	lastLimit int
}

func (f *fakeEmbeddingSearcher) SearchByEmbedding(_ context.Context, query []float32, limit int) ([]string, error) {
	f.calls++
	f.query = append([]float32(nil), query...)
	f.lastLimit = limit
	if f.err != nil {
		return nil, f.err
	}
	return append([]string(nil), f.ids...), nil
}

type fakeEvalVectorStore struct {
	created       []*schema.MemoryRecord
	storedIDs     []string
	storedVectors [][]float32
	storedModels  []string
	storeErr      error
	searchErr     error
	closed        bool
}

func (f *fakeEvalVectorStore) Create(_ context.Context, rec *schema.MemoryRecord) error {
	f.created = append(f.created, rec)
	return nil
}

func (f *fakeEvalVectorStore) StoreTriggerEmbedding(_ context.Context, id string, vec []float32, model string) error {
	if f.storeErr != nil {
		return f.storeErr
	}
	f.storedIDs = append(f.storedIDs, id)
	f.storedVectors = append(f.storedVectors, append([]float32(nil), vec...))
	f.storedModels = append(f.storedModels, model)
	return nil
}

func (f *fakeEvalVectorStore) SearchByEmbedding(_ context.Context, _ []float32, _ int) ([]string, error) {
	if f.searchErr != nil {
		return nil, f.searchErr
	}
	return append([]string(nil), f.storedIDs...), nil
}

func (f *fakeEvalVectorStore) Close() error {
	f.closed = true
	return nil
}

type stoppingEvalVectorStore struct {
	fakeEvalVectorStore
	membrane *membrane.Membrane
}

func (s *stoppingEvalVectorStore) SearchByEmbedding(ctx context.Context, query []float32, limit int) ([]string, error) {
	ids, err := s.fakeEvalVectorStore.SearchByEmbedding(ctx, query, limit)
	if s.membrane != nil {
		_ = s.membrane.Stop()
		s.membrane = nil
	}
	return ids, err
}

type scriptedEvalEmbeddingClient struct {
	errForText string
	err        error
	texts      []string
}

type fakeEvalMembrane struct {
	startErr error
	stopErr  error
	response *ingestion.CaptureMemoryResponse
}

func (f *fakeEvalMembrane) Start(context.Context) error {
	return f.startErr
}

func (f *fakeEvalMembrane) Stop() error {
	return f.stopErr
}

func (f *fakeEvalMembrane) CaptureMemory(context.Context, ingestion.CaptureMemoryRequest) (*ingestion.CaptureMemoryResponse, error) {
	if f.response != nil {
		return f.response, nil
	}
	return &ingestion.CaptureMemoryResponse{}, nil
}

func (f *fakeEvalMembrane) RetrieveGraph(context.Context, *retrieval.RetrieveGraphRequest) (*retrieval.RetrieveGraphResponse, error) {
	return &retrieval.RetrieveGraphResponse{}, nil
}

func (c *scriptedEvalEmbeddingClient) Embed(_ context.Context, text string) ([]float32, error) {
	c.texts = append(c.texts, text)
	if c.err != nil && (c.errForText == "" || strings.Contains(text, c.errForText)) {
		return nil, c.err
	}
	return []float32{float32(len(c.texts)), 0.5}, nil
}

func TestMetricHelpers(t *testing.T) {
	got := []string{"miss", "b", "a"}
	expected := []string{"a", "b"}

	recall, precision, mrr, ndcg := computeMetrics(got, expected, 2)
	if recall != 0.5 {
		t.Fatalf("recall = %v, want 0.5", recall)
	}
	if precision != 0.5 {
		t.Fatalf("precision = %v, want 0.5", precision)
	}
	if mrr != 0.5 {
		t.Fatalf("mrr = %v, want 0.5", mrr)
	}
	if ndcg <= 0 || ndcg > 1 {
		t.Fatalf("ndcg = %v, want in (0,1]", ndcg)
	}

	emptyPrecision := precisionAtK(nil, expected, 5)
	if math.IsNaN(emptyPrecision) || emptyPrecision != 0 {
		t.Fatalf("precisionAtK empty = %v, want 0", emptyPrecision)
	}
	if precision := precisionAtK([]string{"a"}, expected, 2); precision != 0.5 {
		t.Fatalf("precisionAtK short result = %v, want denominator k", precision)
	}
	if precision := precisionAtK([]string{"a"}, expected, 0); precision != 0 {
		t.Fatalf("precisionAtK zero k = %v, want 0", precision)
	}
	if recall := recallAtK(nil, nil, 5); recall != 1 {
		t.Fatalf("recallAtK no expected = %v, want 1", recall)
	}
	if ndcg := ndcgAtK(nil, nil, 5); ndcg != 1 {
		t.Fatalf("ndcgAtK no expected = %v, want 1", ndcg)
	}
	if ndcg := ndcgAtK([]string{"a"}, expected, 0); ndcg != 0 {
		t.Fatalf("ndcgAtK zero k = %v, want 0", ndcg)
	}
	if mrr := mrrAtK([]string{"miss"}, expected, 3); mrr != 0 {
		t.Fatalf("mrrAtK no match = %v, want 0", mrr)
	}
	if mrr := mrrAtK([]string{"a"}, expected, 0); mrr != 0 {
		t.Fatalf("mrrAtK zero k = %v, want 0", mrr)
	}
	if got := queryLimit(-1, -1); got != 0 {
		t.Fatalf("queryLimit negative = %d, want 0", got)
	}
	if got := queryLimit(0, 3); got != 3 {
		t.Fatalf("queryLimit default = %d, want 3", got)
	}
}

func TestPrintRowHandlesPositiveAndNegativeDelta(t *testing.T) {
	printRow("recall", 0.2, 0.4)
	printRow("precision", 0.4, 0.2)
}

type evalExitCode int

func TestMainReportsCLIError(t *testing.T) {
	oldArgs := os.Args
	oldStderr := evalStderr
	oldExit := evalExit
	defer func() {
		os.Args = oldArgs
		evalStderr = oldStderr
		evalExit = oldExit
	}()

	var stderr bytes.Buffer
	os.Args = []string{"membrane-eval"}
	evalStderr = &stderr
	evalExit = func(code int) {
		panic(evalExitCode(code))
	}

	defer func() {
		got := recover()
		if got != evalExitCode(1) {
			t.Fatalf("main panic = %#v, want exit code 1", got)
		}
		if !strings.Contains(stderr.String(), "config") {
			t.Fatalf("stderr = %q, want config error", stderr.String())
		}
	}()
	main()
	t.Fatalf("main returned without exiting")
}

func TestEvalCLIDependenciesDefaults(t *testing.T) {
	deps := evalCLIDependencies{}.withDefaults()
	if deps.newMembrane == nil || deps.openVectorStore == nil || deps.newEmbeddingClient == nil {
		t.Fatalf("withDefaults returned incomplete dependencies: %+v", deps)
	}

	cfg := membrane.DefaultConfig()
	cfg.DBPath = filepath.Join(t.TempDir(), "membrane.db")
	m, err := deps.newMembrane(cfg)
	if err != nil {
		t.Fatalf("default newMembrane: %v", err)
	}
	t.Cleanup(func() { _ = m.Stop() })

	if client := deps.newEmbeddingClient("http://127.0.0.1:1/embeddings", "test-model", "key", 3); client == nil {
		t.Fatalf("default newEmbeddingClient = nil")
	}

	store, err := deps.openVectorStore("", postgres.EmbeddingConfig{})
	if err == nil && store != nil {
		t.Cleanup(func() { _ = store.Close() })
	}
	if err == nil || !strings.Contains(err.Error(), "dsn is required") {
		t.Fatalf("default openVectorStore empty dsn error = %v, want dsn is required", err)
	}
}

func TestRunEvalCLIValidationAndLoadErrors(t *testing.T) {
	ctx := context.Background()

	if err := runEvalCLI(ctx, nil, nil); err == nil || !strings.Contains(err.Error(), "config") {
		t.Fatalf("runEvalCLI missing config = %v, want config error", err)
	}
	if err := runEvalCLI(ctx, []string{"-bad-flag"}, nil); err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
		t.Fatalf("runEvalCLI bad flag = %v, want parse error", err)
	}
	if err := runEvalCLI(ctx, []string{
		"-postgres-dsn", "postgres://example/db",
		"-dataset", filepath.Join(t.TempDir(), "missing.jsonl"),
	}, func(key string) string {
		if key == "MEMBRANE_EMBEDDING_API_KEY" {
			return "env-key"
		}
		return ""
	}); err == nil || !strings.Contains(err.Error(), "load dataset") {
		t.Fatalf("runEvalCLI missing dataset = %v, want load dataset error", err)
	}

	emptyDataset := filepath.Join(t.TempDir(), "empty.jsonl")
	if err := os.WriteFile(emptyDataset, nil, 0o600); err != nil {
		t.Fatalf("WriteFile empty dataset: %v", err)
	}
	if err := runEvalCLI(ctx, []string{
		"-postgres-dsn", "://bad-dsn",
		"-embedding-api-key", "flag-key",
		"-dataset", emptyDataset,
	}, nil); err == nil || !strings.Contains(err.Error(), "create membrane") {
		t.Fatalf("runEvalCLI invalid postgres = %v, want create membrane error", err)
	}
}

func TestRunEvalCLIWithInjectedDependencies(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "dataset.jsonl")
	body := []byte(`
{"type":"record","memory_type":"semantic","key":"fact","text":"Orchid deploys to staging","sensitivity":"low","scope":"project:alpha","subject":"Orchid","predicate":"deploys_to","object":"staging","tags":["deploy"]}
{"type":"query","key":"q1","text":"Where does Orchid deploy?","expected":["fact"],"k":1,"trust":{"max_sensitivity":"low","authenticated":true,"actor_id":"tester","scopes":["project:alpha"]},"memory_types":["semantic"]}
`)
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("WriteFile dataset: %v", err)
	}

	store := &fakeEvalVectorStore{}
	client := &scriptedEvalEmbeddingClient{}
	deps := evalCLIDependencies{
		newMembrane: func(_ *membrane.Config) (evalMembrane, error) {
			cfg := membrane.DefaultConfig()
			cfg.DBPath = filepath.Join(t.TempDir(), "membrane.db")
			return membrane.New(cfg)
		},
		openVectorStore: func(dsn string, cfg postgres.EmbeddingConfig) (evalVectorStore, error) {
			if dsn != "postgres://fake/db" || cfg.Dimensions != 7 || cfg.Model != "test-model" {
				t.Fatalf("openVectorStore args = %q/%+v, want configured DSN/model/dims", dsn, cfg)
			}
			return store, nil
		},
		newEmbeddingClient: func(endpoint, model, apiKey string, dimensions int) embedding.Client {
			if endpoint != "http://embed.example" || model != "test-model" || apiKey != "env-key" || dimensions != 7 {
				t.Fatalf("newEmbeddingClient args = %q/%q/%q/%d, want configured values", endpoint, model, apiKey, dimensions)
			}
			return client
		},
	}

	err := runEvalCLIWithDeps(ctx, []string{
		"-postgres-dsn", "postgres://fake/db",
		"-dataset", path,
		"-embedding-endpoint", "http://embed.example",
		"-embedding-model", "test-model",
		"-embedding-dimensions", "7",
	}, func(key string) string {
		if key == "MEMBRANE_EMBEDDING_API_KEY" {
			return "env-key"
		}
		return ""
	}, deps)
	if err != nil {
		t.Fatalf("runEvalCLIWithDeps: %v", err)
	}
	if !store.closed {
		t.Fatalf("vector store was not closed")
	}
	if len(store.storedIDs) != 1 || len(store.storedVectors[0]) != 2 || store.storedModels[0] != "test-model" {
		t.Fatalf("stored embeddings = ids:%v vectors:%v models:%v, want one test-model embedding", store.storedIDs, store.storedVectors, store.storedModels)
	}
	if len(client.texts) != 2 {
		t.Fatalf("embedding client texts = %v, want record and query embeddings", client.texts)
	}
}

func TestRunEvalCLIInjectedDependencyErrors(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "dataset.jsonl")
	body := []byte(`
{"type":"record","memory_type":"semantic","key":"fact","text":"Orchid deploys to staging","sensitivity":"low","scope":"project:alpha","subject":"Orchid","predicate":"deploys_to","object":"staging"}
{"type":"query","key":"q1","text":"Where does Orchid deploy?","expected":["fact"],"k":1,"trust":{"max_sensitivity":"low","authenticated":true,"actor_id":"tester","scopes":["project:alpha"]},"memory_types":["semantic"]}
`)
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("WriteFile dataset: %v", err)
	}
	baseDeps := func(store evalVectorStore, client embedding.Client) evalCLIDependencies {
		return evalCLIDependencies{
			newMembrane: func(_ *membrane.Config) (evalMembrane, error) {
				cfg := membrane.DefaultConfig()
				cfg.DBPath = filepath.Join(t.TempDir(), "membrane.db")
				return membrane.New(cfg)
			},
			openVectorStore: func(string, postgres.EmbeddingConfig) (evalVectorStore, error) {
				return store, nil
			},
			newEmbeddingClient: func(string, string, string, int) embedding.Client {
				return client
			},
		}
	}
	args := []string{"-postgres-dsn", "postgres://fake/db", "-dataset", path, "-embedding-api-key", "flag-key"}

	storeErr := errors.New("store failed")
	if err := runEvalCLIWithDeps(ctx, args, nil, baseDeps(&fakeEvalVectorStore{storeErr: storeErr}, &scriptedEvalEmbeddingClient{})); err == nil || !strings.Contains(err.Error(), "store embedding fact") {
		t.Fatalf("runEvalCLI store error = %v, want store embedding error", err)
	}

	embedErr := errors.New("embed failed")
	if err := runEvalCLIWithDeps(ctx, args, nil, baseDeps(&fakeEvalVectorStore{}, &scriptedEvalEmbeddingClient{errForText: "Orchid", err: embedErr})); err == nil || !strings.Contains(err.Error(), "embed fact") {
		t.Fatalf("runEvalCLI record embed error = %v, want embed fact error", err)
	}
	if err := runEvalCLIWithDeps(ctx, args, nil, baseDeps(&fakeEvalVectorStore{}, &scriptedEvalEmbeddingClient{errForText: "Where", err: embedErr})); err == nil || !strings.Contains(err.Error(), "embed query q1") {
		t.Fatalf("runEvalCLI query embed error = %v, want embed query error", err)
	}

	searchErr := errors.New("search failed")
	if err := runEvalCLIWithDeps(ctx, args, nil, baseDeps(&fakeEvalVectorStore{searchErr: searchErr}, &scriptedEvalEmbeddingClient{})); err == nil || !strings.Contains(err.Error(), "run rag eval") {
		t.Fatalf("runEvalCLI rag search error = %v, want run rag eval wrapper", err)
	}

	openErr := errors.New("open failed")
	err := runEvalCLIWithDeps(ctx, args, nil, evalCLIDependencies{
		newMembrane: baseDeps(nil, nil).newMembrane,
		openVectorStore: func(string, postgres.EmbeddingConfig) (evalVectorStore, error) {
			return nil, openErr
		},
	})
	if err == nil || !strings.Contains(err.Error(), "open pgstore") {
		t.Fatalf("runEvalCLI open store error = %v, want open pgstore error", err)
	}
}

func TestRunEvalCLIWrapsStartErrors(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "dataset.jsonl")
	if err := os.WriteFile(path, []byte(`{"type":"query","key":"q1","text":"empty","expected":[]}`+"\n"), 0o600); err != nil {
		t.Fatalf("WriteFile dataset: %v", err)
	}
	args := []string{"-postgres-dsn", "postgres://fake/db", "-dataset", path, "-embedding-api-key", "flag-key"}

	startErr := errors.New("start failed")
	if err := runEvalCLIWithDeps(ctx, args, nil, evalCLIDependencies{
		newMembrane: func(*membrane.Config) (evalMembrane, error) {
			return &fakeEvalMembrane{startErr: startErr}, nil
		},
	}); err == nil || !strings.Contains(err.Error(), "start membrane") || !errors.Is(err, startErr) {
		t.Fatalf("runEvalCLI start error = %v, want wrapped start error", err)
	}

	cleanupErr := errors.New("cleanup failed")
	if err := runEvalCLIWithDeps(ctx, args, nil, evalCLIDependencies{
		newMembrane: func(*membrane.Config) (evalMembrane, error) {
			return &fakeEvalMembrane{startErr: startErr, stopErr: cleanupErr}, nil
		},
	}); err == nil || !strings.Contains(err.Error(), "cleanup") || !errors.Is(err, startErr) {
		t.Fatalf("runEvalCLI start cleanup error = %v, want start and cleanup wrapper", err)
	}
}

func TestRunEvalCLIWrapsIngestAndMembraneEvalErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("ingest", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "dataset.jsonl")
		body := []byte(`{"type":"record","memory_type":"unknown","key":"bad","text":"bad"}` + "\n")
		if err := os.WriteFile(path, body, 0o600); err != nil {
			t.Fatalf("WriteFile dataset: %v", err)
		}
		err := runEvalCLIWithDeps(ctx, []string{
			"-postgres-dsn", "postgres://fake/db",
			"-dataset", path,
			"-embedding-api-key", "flag-key",
		}, nil, evalCLIDependencies{
			newMembrane: func(_ *membrane.Config) (evalMembrane, error) {
				cfg := membrane.DefaultConfig()
				cfg.DBPath = filepath.Join(t.TempDir(), "membrane.db")
				return membrane.New(cfg)
			},
			openVectorStore: func(string, postgres.EmbeddingConfig) (evalVectorStore, error) {
				return &fakeEvalVectorStore{}, nil
			},
			newEmbeddingClient: func(string, string, string, int) embedding.Client {
				return &scriptedEvalEmbeddingClient{}
			},
		})
		if err == nil || !strings.Contains(err.Error(), "ingest bad") {
			t.Fatalf("runEvalCLI ingest error = %v, want ingest bad wrapper", err)
		}
	})

	t.Run("membrane eval", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "dataset.jsonl")
		body := []byte(`
{"type":"record","memory_type":"semantic","key":"fact","text":"Orchid deploys to staging","sensitivity":"low","scope":"project:alpha","subject":"Orchid","predicate":"deploys_to","object":"staging"}
{"type":"query","key":"q1","text":"Where does Orchid deploy?","expected":["fact"],"k":1,"trust":{"max_sensitivity":"low","authenticated":true,"actor_id":"tester","scopes":["project:alpha"]},"memory_types":["semantic"]}
`)
		if err := os.WriteFile(path, body, 0o600); err != nil {
			t.Fatalf("WriteFile dataset: %v", err)
		}
		var m *membrane.Membrane
		store := &stoppingEvalVectorStore{}
		err := runEvalCLIWithDeps(ctx, []string{
			"-postgres-dsn", "postgres://fake/db",
			"-dataset", path,
			"-embedding-api-key", "flag-key",
		}, nil, evalCLIDependencies{
			newMembrane: func(_ *membrane.Config) (evalMembrane, error) {
				cfg := membrane.DefaultConfig()
				cfg.DBPath = filepath.Join(t.TempDir(), "membrane.db")
				var err error
				m, err = membrane.New(cfg)
				return m, err
			},
			openVectorStore: func(string, postgres.EmbeddingConfig) (evalVectorStore, error) {
				store.membrane = m
				return store, nil
			},
			newEmbeddingClient: func(string, string, string, int) embedding.Client {
				return &scriptedEvalEmbeddingClient{}
			},
		})
		if err == nil || !strings.Contains(err.Error(), "run membrane eval") {
			t.Fatalf("runEvalCLI membrane eval error = %v, want run membrane eval wrapper", err)
		}
	})
}

func TestDatasetAndParseHelpers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dataset.jsonl")
	data := []byte(`
{"type":"record","memory_type":"semantic","key":"fact-1","text":"Orchid deploys to staging","sensitivity":"HIGH","scope":"project:alpha","subject":"Orchid","predicate":"deploys_to","object":"staging"}
{"type":"query","key":"query-1","text":"Where does Orchid deploy?","expected":["fact-1"],"trust":{"max_sensitivity":" high ","authenticated":true,"actor_id":"","scopes":["project:alpha"]}}
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	loaded, err := loadDataset(path, 7)
	if err != nil {
		t.Fatalf("loadDataset: %v", err)
	}
	if len(loaded.records) != 1 || loaded.records[0].Key != "fact-1" {
		t.Fatalf("records = %+v, want fact-1", loaded.records)
	}
	if len(loaded.queries) != 1 || loaded.queries[0].K != 7 {
		t.Fatalf("queries = %+v, want default k applied", loaded.queries)
	}

	trust := toTrustContext(loaded.queries[0].Trust)
	if trust.ActorID != "eval" || trust.MaxSensitivity != schema.SensitivityHigh || len(trust.Scopes) != 1 {
		t.Fatalf("trust = %+v, want normalized defaults", trust)
	}

	if got := parseSensitivity("HYPER"); got != schema.SensitivityHyper {
		t.Fatalf("parseSensitivity hyper = %q", got)
	}
	if got := parseSensitivity(" public "); got != schema.SensitivityPublic {
		t.Fatalf("parseSensitivity public = %q", got)
	}
	if got := parseSensitivity("medium"); got != schema.SensitivityMedium {
		t.Fatalf("parseSensitivity medium = %q", got)
	}
	if got := parseSensitivity("unknown"); got != schema.SensitivityLow {
		t.Fatalf("parseSensitivity unknown = %q, want low", got)
	}
	if got := parseMemoryTypes([]string{"semantic", "working"}); len(got) != 2 || got[0] != schema.MemoryTypeSemantic || got[1] != schema.MemoryTypeWorking {
		t.Fatalf("parseMemoryTypes = %+v, want semantic/working", got)
	}
	if len(allMemoryTypes()) != 5 {
		t.Fatalf("allMemoryTypes len = %d, want 5", len(allMemoryTypes()))
	}
	if ids, err := resolveExpected(loaded.queries[0], map[string]string{"fact-1": "record-id"}); err != nil || len(ids) != 1 || ids[0] != "record-id" {
		t.Fatalf("resolveExpected = %+v, want record-id", ids)
	}
	if _, err := resolveExpected(queryEntry{Key: "missing-query", Expected: []string{"missing"}}, map[string]string{}); err == nil {
		t.Fatalf("resolveExpected missing key error = nil, want error")
	}
}

func TestLoadDatasetRejectsInvalidEntries(t *testing.T) {
	dir := t.TempDir()
	for _, tc := range []struct {
		name string
		body string
	}{
		{name: "invalid JSON", body: `{`},
		{name: "invalid record", body: `{"type":"record","tags":123}`},
		{name: "invalid query", body: `{"type":"query","k":"bad"}`},
		{name: "unknown type", body: `{"type":"other"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(dir, tc.name+".jsonl")
			if err := os.WriteFile(path, []byte(tc.body), 0o600); err != nil {
				t.Fatalf("WriteFile: %v", err)
			}
			if _, err := loadDataset(path, 5); err == nil {
				t.Fatalf("loadDataset error = nil, want error")
			}
		})
	}
}

func TestLoadDatasetScannerError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dataset.jsonl")
	if err := os.WriteFile(path, bytes.Repeat([]byte("x"), 1024*1024+1), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := loadDataset(path, 5); err == nil {
		t.Fatalf("loadDataset scanner error = nil, want line-too-long error")
	}
}

func TestLoadDatasetMissingFile(t *testing.T) {
	if _, err := loadDataset(filepath.Join(t.TempDir(), "missing.jsonl"), 5); err == nil {
		t.Fatalf("loadDataset missing file error = nil, want error")
	}
}

func TestCaptureAndRetrieveRootRecordsUseMembraneGraph(t *testing.T) {
	ctx := context.Background()
	m := newEvalTestMembrane(t)

	rec, err := captureObservationRecord(ctx, m, "tester", "Orchid", "deploys_to", "staging", []string{"deploy"}, "project:alpha", schema.SensitivityLow)
	if err != nil {
		t.Fatalf("captureObservationRecord: %v", err)
	}
	resp, err := retrieveRootRecords(ctx, m, &retrieval.RetrieveRequest{
		TaskDescriptor: "Orchid staging deploy",
		Trust:          retrieval.NewTrustContext(schema.SensitivityLow, true, "tester", []string{"project:alpha"}),
		MemoryTypes:    []schema.MemoryType{schema.MemoryTypeSemantic},
		Limit:          1,
	})
	if err != nil {
		t.Fatalf("retrieveRootRecords: %v", err)
	}
	if len(resp.Records) != 1 || resp.Records[0].ID != rec.ID {
		t.Fatalf("records = %+v, want captured semantic root %s", resp.Records, rec.ID)
	}
	if _, err := retrieveRootRecords(ctx, m, nil); err == nil {
		t.Fatalf("retrieveRootRecords nil request error = nil, want error")
	}
	if _, err := retrieveRootRecords(ctx, m, &retrieval.RetrieveRequest{}); err == nil || !strings.Contains(err.Error(), "trust context is required") {
		t.Fatalf("retrieveRootRecords missing trust error = %v, want trust error", err)
	}
}

func TestCaptureEventAndWorkingStateRecords(t *testing.T) {
	ctx := context.Background()
	m := newEvalTestMembrane(t)

	event, err := captureEventRecord(ctx, m, "tester", "deploy", "deploy-1", "Deploy completed", []string{"deploy"}, "project:alpha", schema.SensitivityLow)
	if err != nil {
		t.Fatalf("captureEventRecord: %v", err)
	}
	if event == nil || event.Type != schema.MemoryTypeEpisodic {
		t.Fatalf("event = %+v, want episodic record", event)
	}

	working, err := captureWorkingStateRecord(ctx, m, "tester", "thread-1", schema.TaskStateExecuting, []string{"ship"}, []string{"work"}, "project:alpha", schema.SensitivityLow)
	if err != nil {
		t.Fatalf("captureWorkingStateRecord: %v", err)
	}
	if working == nil || working.Type != schema.MemoryTypeWorking {
		t.Fatalf("working = %+v, want working record", working)
	}

	if _, err := captureEventRecord(ctx, m, "", "deploy", "deploy-2", "Deploy failed", nil, "project:alpha", schema.SensitivityLow); err == nil {
		t.Fatalf("captureEventRecord empty source error = nil, want validation error")
	}
	if _, err := captureObservationRecord(ctx, m, "", "Orchid", "deploys_to", "staging", nil, "project:alpha", schema.SensitivityLow); err == nil {
		t.Fatalf("captureObservationRecord empty source error = nil, want validation error")
	}
	if _, err := captureWorkingStateRecord(ctx, m, "", "thread-1", schema.TaskStateExecuting, nil, nil, "project:alpha", schema.SensitivityLow); err == nil {
		t.Fatalf("captureWorkingStateRecord empty source error = nil, want validation error")
	}
}

func TestIngestRecordCoversAllMemoryTypes(t *testing.T) {
	ctx := context.Background()
	m := newEvalTestMembrane(t)
	store := &fakeRecordCreator{}

	tests := []recordEntry{
		{
			MemoryType:  "semantic",
			Key:         "fact",
			Subject:     "Orchid",
			Predicate:   "deploys_to",
			Object:      "staging",
			Sensitivity: "low",
			Scope:       "project:alpha",
		},
		{
			MemoryType:  "episodic",
			Key:         "event",
			EventKind:   "deploy",
			Ref:         "deploy-1",
			Summary:     "Deploy completed",
			Sensitivity: "medium",
			Scope:       "project:alpha",
		},
		{
			MemoryType:  "working",
			Key:         "working",
			ThreadID:    "thread-1",
			State:       string(schema.TaskStateExecuting),
			NextActions: []string{"ship"},
			Sensitivity: "high",
			Scope:       "project:alpha",
		},
		{
			MemoryType:  "competence",
			Key:         "skill",
			SkillName:   "debug oom",
			Triggers:    []string{"oom"},
			Steps:       []string{"capture heap profile"},
			Sensitivity: "public",
			Scope:       "project:alpha",
		},
		{
			MemoryType: "plan_graph",
			Key:        "plan",
			Intent:     "deploy",
			Nodes:      []planNodeEntry{{ID: "start", Op: "build"}},
			Edges:      []planEdgeEntry{{From: "start", To: "finish"}},
			Scope:      "project:alpha",
		},
	}
	for _, rec := range tests {
		t.Run(rec.MemoryType, func(t *testing.T) {
			id, err := ingestRecord(ctx, m, store, rec)
			if err != nil {
				t.Fatalf("ingestRecord: %v", err)
			}
			if id == "" {
				t.Fatalf("ingestRecord ID is empty")
			}
		})
	}
	if len(store.records) != 2 {
		t.Fatalf("fake store records = %d, want competence and plan graph records", len(store.records))
	}
	if store.records[0].Type != schema.MemoryTypeCompetence || store.records[1].Type != schema.MemoryTypePlanGraph {
		t.Fatalf("fake store record types = %s/%s, want competence/plan_graph", store.records[0].Type, store.records[1].Type)
	}

	if _, err := ingestRecord(ctx, m, store, recordEntry{MemoryType: "unknown"}); err == nil {
		t.Fatalf("ingestRecord unknown memory type error = nil, want error")
	}

	createErr := errors.New("create failed")
	if _, err := ingestRecord(ctx, m, &fakeRecordCreator{err: createErr}, recordEntry{
		MemoryType: "competence",
		SkillName:  "debug oom",
	}); !errors.Is(err, createErr) {
		t.Fatalf("ingestRecord competence create error = %v, want %v", err, createErr)
	}
	if _, err := ingestRecord(ctx, m, &fakeRecordCreator{err: createErr}, recordEntry{
		MemoryType: "plan_graph",
		Key:        "plan",
	}); !errors.Is(err, createErr) {
		t.Fatalf("ingestRecord plan create error = %v, want %v", err, createErr)
	}

	for _, rec := range []recordEntry{
		{MemoryType: "semantic", Subject: "Orchid", Predicate: "deploys_to", Object: "staging"},
		{MemoryType: "episodic", EventKind: "deploy", Ref: "deploy-1", Summary: "Deploy completed"},
		{MemoryType: "working", ThreadID: "thread-1", State: string(schema.TaskStateExecuting)},
	} {
		t.Run("stopped "+rec.MemoryType, func(t *testing.T) {
			stopped := newEvalTestMembrane(t)
			if err := stopped.Stop(); err != nil {
				t.Fatalf("Stop: %v", err)
			}
			if _, err := ingestRecord(ctx, stopped, store, rec); err == nil {
				t.Fatalf("ingestRecord stopped %s error = nil, want error", rec.MemoryType)
			}
		})
	}
}

func TestRunMembraneEvalAndRAGEvalWithLocalFakes(t *testing.T) {
	ctx := context.Background()
	m := newEvalTestMembrane(t)
	rec, err := captureObservationRecord(ctx, m, "tester", "Orchid", "deploys_to", "staging", []string{"deploy"}, "project:alpha", schema.SensitivityLow)
	if err != nil {
		t.Fatalf("captureObservationRecord: %v", err)
	}
	data := &dataset{
		queries: []queryEntry{{
			Key:         "q1",
			Text:        "Orchid staging deploy",
			Expected:    []string{"fact"},
			K:           1,
			Trust:       trustEntry{MaxSensitivity: "low", Authenticated: true, ActorID: "tester", Scopes: []string{"project:alpha"}},
			MemoryTypes: []string{"semantic"},
		}},
	}
	idByKey := map[string]string{"fact": rec.ID}
	queryEmbeddings := map[string][]float32{"q1": []float32{0.1, 0.2}}

	memMetrics, err := runMembraneEval(ctx, m, data, idByKey, queryEmbeddings, 5, false)
	if err != nil {
		t.Fatalf("runMembraneEval: %v", err)
	}
	if memMetrics.recall != 1 || memMetrics.precision != 1 {
		t.Fatalf("runMembraneEval metrics = %+v, want perfect recall and precision", memMetrics)
	}

	searcher := &fakeEmbeddingSearcher{ids: []string{"not-visible", rec.ID}}
	ragMetrics, err := runRAGEval(ctx, m, searcher, data, idByKey, queryEmbeddings, 5, false, 10)
	if err != nil {
		t.Fatalf("runRAGEval: %v", err)
	}
	if ragMetrics.recall != 1 || ragMetrics.precision != 1 {
		t.Fatalf("runRAGEval metrics = %+v, want perfect recall and precision", ragMetrics)
	}
	if searcher.calls != 1 || searcher.lastLimit != 10 || len(searcher.query) != 2 {
		t.Fatalf("fake searcher calls/limit/query = %d/%d/%v, want one call with totalRecords limit and query", searcher.calls, searcher.lastLimit, searcher.query)
	}

	if _, err := runMembraneEval(ctx, m, data, idByKey, queryEmbeddings, 5, true); err != nil {
		t.Fatalf("runMembraneEval verbose: %v", err)
	}
	verboseSearcher := &fakeEmbeddingSearcher{ids: []string{rec.ID, "ignored-after-k"}}
	if _, err := runRAGEval(ctx, m, verboseSearcher, data, idByKey, queryEmbeddings, 5, true, 10); err != nil {
		t.Fatalf("runRAGEval verbose: %v", err)
	}
}

func TestRunEvalHandlesEmptyDatasets(t *testing.T) {
	ctx := context.Background()
	m := newEvalTestMembrane(t)
	data := &dataset{}
	searcher := &fakeEmbeddingSearcher{}

	memMetrics, err := runMembraneEval(ctx, m, data, nil, nil, 5, false)
	if err != nil {
		t.Fatalf("runMembraneEval empty: %v", err)
	}
	if memMetrics != (evalMetrics{}) {
		t.Fatalf("runMembraneEval empty metrics = %+v, want zero metrics", memMetrics)
	}
	ragMetrics, err := runRAGEval(ctx, m, searcher, data, nil, nil, 5, false, 10)
	if err != nil {
		t.Fatalf("runRAGEval empty: %v", err)
	}
	if ragMetrics != (evalMetrics{}) {
		t.Fatalf("runRAGEval empty metrics = %+v, want zero metrics", ragMetrics)
	}
	if searcher.calls != 0 {
		t.Fatalf("empty RAG eval search calls = %d, want 0", searcher.calls)
	}

	memMetrics, err = runMembraneEval(ctx, m, nil, nil, nil, 5, false)
	if err != nil {
		t.Fatalf("runMembraneEval nil: %v", err)
	}
	if memMetrics != (evalMetrics{}) {
		t.Fatalf("runMembraneEval nil metrics = %+v, want zero metrics", memMetrics)
	}
	ragMetrics, err = runRAGEval(ctx, m, searcher, nil, nil, nil, 5, false, 10)
	if err != nil {
		t.Fatalf("runRAGEval nil: %v", err)
	}
	if ragMetrics != (evalMetrics{}) {
		t.Fatalf("runRAGEval nil metrics = %+v, want zero metrics", ragMetrics)
	}
}

func TestRunEvalPropagatesRecoverableErrors(t *testing.T) {
	ctx := context.Background()
	m := newEvalTestMembrane(t)
	rec, err := captureObservationRecord(ctx, m, "tester", "Orchid", "deploys_to", "staging", nil, "project:alpha", schema.SensitivityLow)
	if err != nil {
		t.Fatalf("captureObservationRecord: %v", err)
	}
	data := &dataset{queries: []queryEntry{{
		Key:      "q1",
		Text:     "Orchid staging deploy",
		Expected: []string{"fact"},
		K:        1,
		Trust:    trustEntry{MaxSensitivity: "low", Authenticated: true, ActorID: "tester", Scopes: []string{"project:alpha"}},
	}}}
	idByKey := map[string]string{"fact": rec.ID}

	searchErr := errors.New("vector down")
	if _, err := runRAGEval(ctx, m, &fakeEmbeddingSearcher{err: searchErr}, data, idByKey, map[string][]float32{"q1": []float32{1}}, 5, false, 10); !errors.Is(err, searchErr) {
		t.Fatalf("runRAGEval search error = %v, want %v", err, searchErr)
	}
	if _, err := runRAGEval(ctx, m, &fakeEmbeddingSearcher{}, data, idByKey, map[string][]float32{}, 5, false, 10); err == nil || !strings.Contains(err.Error(), "missing query embedding") {
		t.Fatalf("runRAGEval missing embedding error = %v, want missing embedding error", err)
	}
	if _, err := runMembraneEval(ctx, m, data, idByKey, map[string][]float32{}, 5, false); err == nil || !strings.Contains(err.Error(), "missing query embedding") {
		t.Fatalf("runMembraneEval missing embedding error = %v, want missing embedding error", err)
	}
	if _, err := runMembraneEval(ctx, m, data, map[string]string{}, nil, 5, false); err == nil {
		t.Fatalf("runMembraneEval missing expected error = nil, want error")
	}
}

func TestRunEvalPropagatesRetrieveAndExpectedErrors(t *testing.T) {
	ctx := context.Background()
	data := &dataset{queries: []queryEntry{{
		Key:      "q1",
		Text:     "Orchid staging deploy",
		Expected: []string{"fact"},
		K:        1,
		Trust:    trustEntry{MaxSensitivity: "low", Authenticated: true, ActorID: "tester", Scopes: []string{"project:alpha"}},
	}}}

	stopped := newEvalTestMembrane(t)
	if err := stopped.Stop(); err != nil {
		t.Fatalf("Stop stopped membrane: %v", err)
	}
	if _, err := runRAGEval(ctx, stopped, &fakeEmbeddingSearcher{}, data, map[string]string{"fact": "rec-1"}, map[string][]float32{"q1": []float32{1}}, 5, false, 1); err == nil || !strings.Contains(err.Error(), "rag retrieve q1") {
		t.Fatalf("runRAGEval retrieve error = %v, want rag retrieve wrapper", err)
	}

	stopped = newEvalTestMembrane(t)
	if err := stopped.Stop(); err != nil {
		t.Fatalf("Stop stopped membrane: %v", err)
	}
	if _, err := runMembraneEval(ctx, stopped, data, map[string]string{"fact": "rec-1"}, map[string][]float32{"q1": []float32{1}}, 5, false); err == nil || !strings.Contains(err.Error(), "membrane retrieve q1") {
		t.Fatalf("runMembraneEval retrieve error = %v, want membrane retrieve wrapper", err)
	}

	m := newEvalTestMembrane(t)
	if _, err := runRAGEval(ctx, m, &fakeEmbeddingSearcher{}, data, map[string]string{}, map[string][]float32{"q1": []float32{1}}, 5, false, 1); err == nil || !strings.Contains(err.Error(), "unknown key") {
		t.Fatalf("runRAGEval expected-key error = %v, want unknown key error", err)
	}
	if _, err := runMembraneEval(ctx, m, data, map[string]string{}, map[string][]float32{"q1": []float32{1}}, 5, false); err == nil || !strings.Contains(err.Error(), "unknown key") {
		t.Fatalf("runMembraneEval expected-key error = %v, want unknown key error", err)
	}
}

func TestCaptureObservationRecordRequiresSemanticCreatedRecord(t *testing.T) {
	ctx := context.Background()
	primary := schema.NewMemoryRecord("primary", schema.MemoryTypeEpisodic, schema.SensitivityLow, &schema.EpisodicPayload{Kind: "episodic"})
	entity := schema.NewMemoryRecord("entity", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{Kind: "entity", CanonicalName: "Orchid"})
	m := &fakeEvalMembrane{response: &ingestion.CaptureMemoryResponse{
		PrimaryRecord:  primary,
		CreatedRecords: []*schema.MemoryRecord{nil, entity},
	}}
	if rec, err := captureObservationRecord(ctx, m, "tester", "Orchid", "deploys_to", "staging", nil, "", schema.SensitivityLow); err == nil || rec != nil || !strings.Contains(err.Error(), "did not produce semantic") {
		t.Fatalf("captureObservationRecord no semantic = %+v, %v; want semantic record error", rec, err)
	}
}

func newEvalTestMembrane(t *testing.T) *membrane.Membrane {
	t.Helper()
	cfg := membrane.DefaultConfig()
	cfg.DBPath = filepath.Join(t.TempDir(), "membrane.db")
	m, err := membrane.New(cfg)
	if err != nil {
		t.Fatalf("membrane.New: %v", err)
	}
	t.Cleanup(func() { _ = m.Stop() })
	return m
}
