package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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

type fakeLifecycleEmbeddingClient struct {
	texts []string
	vec   []float32
}

func (f *fakeLifecycleEmbeddingClient) Embed(_ context.Context, text string) ([]float32, error) {
	f.texts = append(f.texts, text)
	return append([]float32(nil), f.vec...), nil
}

type fakeLifecycleVectorStore struct {
	storedID     string
	storedVector []float32
	storedModel  string
	ids          []string
	lastQuery    []float32
	lastLimit    int
}

type failingLifecycleEmbeddingClient struct{}

func (failingLifecycleEmbeddingClient) Embed(context.Context, string) ([]float32, error) {
	return nil, errors.New("embed failed")
}

type failingLifecycleVectorStore struct {
	storeErr  error
	searchErr error
}

func (f failingLifecycleVectorStore) StoreTriggerEmbedding(context.Context, string, []float32, string) error {
	return f.storeErr
}

func (f failingLifecycleVectorStore) SearchByEmbedding(context.Context, []float32, int) ([]string, error) {
	return nil, f.searchErr
}

type scriptedLifecycleEmbeddingClient struct{}

func (scriptedLifecycleEmbeddingClient) Embed(_ context.Context, text string) ([]float32, error) {
	lower := strings.ToLower(text)
	switch {
	case strings.Contains(lower, "what database"):
		return []float32{101}, nil
	case strings.Contains(lower, "mysql"):
		return []float32{1}, nil
	case strings.Contains(lower, "postgresql"):
		return []float32{2}, nil
	case strings.Contains(lower, "how do i debug"):
		return []float32{102}, nil
	case strings.Contains(lower, "restarting pods"):
		return []float32{3}, nil
	case strings.Contains(lower, "pprof heap"):
		return []float32{4}, nil
	case strings.Contains(lower, "current api rate"):
		return []float32{103}, nil
	case strings.Contains(lower, "50 requests"):
		return []float32{5}, nil
	case strings.Contains(lower, "200 requests"):
		return []float32{6}, nil
	case strings.Contains(lower, "where do we deploy"):
		return []float32{104}, nil
	case strings.Contains(lower, "heroku"):
		return []float32{7}, nil
	case strings.Contains(lower, "kubernetes"):
		return []float32{8}, nil
	default:
		return []float32{99}, nil
	}
}

type scriptedLifecycleVectorStore struct {
	byCode map[float32]string
	closed bool
}

type tieLifecycleVectorStore struct {
	scriptedLifecycleVectorStore
}

type failAtLifecycleEmbeddingClient struct {
	failAt int
	calls  int
}

type failAtLifecycleVectorStore struct {
	scriptedLifecycleVectorStore
	failStoreAt  int
	failSearchAt int
	stopStoreAt  int
	stopSearchAt int
	membrane     *membrane.Membrane
	storeCalls   int
	searchCalls  int
}

type fakeLifecycleMembrane struct {
	startErr       error
	stopErr        error
	captureErrAt   int
	reinforceErrAt int
	penalizeErrAt  int
	captureCalls   int
	reinforceCalls int
	penalizeCalls  int
	response       *ingestion.CaptureMemoryResponse
}

func (s *scriptedLifecycleVectorStore) StoreTriggerEmbedding(_ context.Context, recordID string, embedding []float32, _ string) error {
	if s.byCode == nil {
		s.byCode = map[float32]string{}
	}
	if len(embedding) > 0 {
		s.byCode[embedding[0]] = recordID
	}
	return nil
}

func (f *fakeLifecycleMembrane) Start(context.Context) error {
	return f.startErr
}

func (f *fakeLifecycleMembrane) Stop() error {
	return f.stopErr
}

func (f *fakeLifecycleMembrane) CaptureMemory(context.Context, ingestion.CaptureMemoryRequest) (*ingestion.CaptureMemoryResponse, error) {
	f.captureCalls++
	if f.captureCalls == f.captureErrAt {
		return nil, errors.New("scripted capture failed")
	}
	if f.response != nil {
		return f.response, nil
	}
	rec := schema.NewMemoryRecord(fmt.Sprintf("capture-%d", f.captureCalls), schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "subject",
		Predicate: "predicate",
		Object:    "object",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	return &ingestion.CaptureMemoryResponse{CreatedRecords: []*schema.MemoryRecord{rec}}, nil
}

func (f *fakeLifecycleMembrane) RetrieveGraph(context.Context, *retrieval.RetrieveGraphRequest) (*retrieval.RetrieveGraphResponse, error) {
	return &retrieval.RetrieveGraphResponse{}, nil
}

func (f *fakeLifecycleMembrane) Supersede(_ context.Context, _ string, newRec *schema.MemoryRecord, _, _ string) (*schema.MemoryRecord, error) {
	return newRec, nil
}

func (f *fakeLifecycleMembrane) Retract(context.Context, string, string, string) error {
	return nil
}

func (f *fakeLifecycleMembrane) Reinforce(context.Context, string, string, string) error {
	f.reinforceCalls++
	if f.reinforceCalls == f.reinforceErrAt {
		return errors.New("scripted reinforce failed")
	}
	return nil
}

func (f *fakeLifecycleMembrane) Penalize(context.Context, string, float64, string, string) error {
	f.penalizeCalls++
	if f.penalizeCalls == f.penalizeErrAt {
		return errors.New("scripted penalize failed")
	}
	return nil
}

func (f *failAtLifecycleEmbeddingClient) Embed(ctx context.Context, text string) ([]float32, error) {
	f.calls++
	if f.calls == f.failAt {
		return nil, errors.New("scripted embed failed")
	}
	return scriptedLifecycleEmbeddingClient{}.Embed(ctx, text)
}

func (f *failAtLifecycleVectorStore) StoreTriggerEmbedding(ctx context.Context, recordID string, embedding []float32, model string) error {
	f.storeCalls++
	if f.storeCalls == f.failStoreAt {
		return errors.New("scripted store failed")
	}
	if err := f.scriptedLifecycleVectorStore.StoreTriggerEmbedding(ctx, recordID, embedding, model); err != nil {
		return err
	}
	if f.storeCalls == f.stopStoreAt && f.membrane != nil {
		_ = f.membrane.Stop()
	}
	return nil
}

func (f *failAtLifecycleVectorStore) SearchByEmbedding(ctx context.Context, query []float32, limit int) ([]string, error) {
	f.searchCalls++
	if f.searchCalls == f.failSearchAt {
		return nil, errors.New("scripted search failed")
	}
	ids, err := f.scriptedLifecycleVectorStore.SearchByEmbedding(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	if f.searchCalls == f.stopSearchAt && f.membrane != nil {
		_ = f.membrane.Stop()
	}
	return ids, nil
}

func (s *scriptedLifecycleVectorStore) SearchByEmbedding(_ context.Context, query []float32, _ int) ([]string, error) {
	if len(query) == 0 {
		return nil, nil
	}
	ids := func(codes ...float32) []string {
		out := make([]string, 0, len(codes))
		for _, code := range codes {
			if id := s.byCode[code]; id != "" {
				out = append(out, id)
			}
		}
		return out
	}
	switch query[0] {
	case 101:
		return ids(1, 2, 99), nil
	case 102:
		return ids(3, 4), nil
	case 103:
		return ids(5, 6), nil
	case 104:
		return ids(7, 8), nil
	default:
		return nil, nil
	}
}

func (s *tieLifecycleVectorStore) SearchByEmbedding(_ context.Context, query []float32, _ int) ([]string, error) {
	if len(query) == 0 {
		return nil, nil
	}
	ids := func(codes ...float32) []string {
		out := make([]string, 0, len(codes))
		for _, code := range codes {
			if id := s.byCode[code]; id != "" {
				out = append(out, id)
			}
		}
		return out
	}
	switch query[0] {
	case 101:
		return ids(2), nil
	case 102:
		return ids(4, 3), nil
	case 103:
		return ids(6), nil
	case 104:
		return ids(8, 7), nil
	default:
		return nil, nil
	}
}

func (s *scriptedLifecycleVectorStore) Close() error {
	s.closed = true
	return nil
}

func (f *fakeLifecycleVectorStore) StoreTriggerEmbedding(_ context.Context, recordID string, embedding []float32, model string) error {
	f.storedID = recordID
	f.storedVector = append([]float32(nil), embedding...)
	f.storedModel = model
	return nil
}

func (f *fakeLifecycleVectorStore) SearchByEmbedding(_ context.Context, query []float32, limit int) ([]string, error) {
	f.lastQuery = append([]float32(nil), query...)
	f.lastLimit = limit
	return append([]string(nil), f.ids...), nil
}

func (f *fakeLifecycleVectorStore) Close() error {
	return nil
}

type lifecycleExitCode int

func TestMainReportsCLIError(t *testing.T) {
	oldArgs := os.Args
	oldStderr := lifecycleStderr
	oldExit := lifecycleExit
	defer func() {
		os.Args = oldArgs
		lifecycleStderr = oldStderr
		lifecycleExit = oldExit
	}()

	var stderr bytes.Buffer
	os.Args = []string{"membrane-eval-lifecycle"}
	lifecycleStderr = &stderr
	lifecycleExit = func(code int) {
		panic(lifecycleExitCode(code))
	}

	defer func() {
		got := recover()
		if got != lifecycleExitCode(1) {
			t.Fatalf("main panic = %#v, want exit code 1", got)
		}
		if !strings.Contains(stderr.String(), "config") {
			t.Fatalf("stderr = %q, want config error", stderr.String())
		}
	}()
	main()
	t.Fatalf("main returned without exiting")
}

func TestLifecycleLabelAndContainsHelpers(t *testing.T) {
	if got := label("a", "a", "b"); got != "A (old/wrong/stale)" {
		t.Fatalf("label A = %q", got)
	}
	if got := label("b", "a", "b"); got != "B (correct/new/fresh)" {
		t.Fatalf("label B = %q", got)
	}
	if got := label("short", "a", "b"); got != "other: short" {
		t.Fatalf("label short unknown = %q", got)
	}
	if got := label("123456789", "a", "b"); got != "other: 12345678" {
		t.Fatalf("label long unknown = %q", got)
	}
	if !contains([]string{"x", "y"}, "y") || contains([]string{"x"}, "z") {
		t.Fatalf("contains helper returned unexpected result")
	}
	if !containsRec([]*schema.MemoryRecord{{ID: "rec-1"}}, "rec-1") || containsRec([]*schema.MemoryRecord{{ID: "rec-1"}}, "rec-2") {
		t.Fatalf("containsRec helper returned unexpected result")
	}
	if containsRec([]*schema.MemoryRecord{nil}, "rec-1") {
		t.Fatalf("containsRec matched a nil record")
	}
	if trust := fullTrust(); trust.MaxSensitivity != schema.SensitivityHyper || !trust.Authenticated || trust.ActorID != "eval" {
		t.Fatalf("fullTrust = %+v, want hyper authenticated eval trust", trust)
	}
}

func TestLifecycleScenarioOutcomeHelpers(t *testing.T) {
	assertResult := func(t *testing.T, name, gotMessage string, got, want lifecycleEvalResult) {
		t.Helper()
		if got != want {
			t.Fatalf("%s result = %+v, want %+v", name, got, want)
		}
		if strings.TrimSpace(gotMessage) == "" {
			t.Fatalf("%s message is empty", name)
		}
	}

	msg, result := databaseScenarioOutcome(false, true)
	assertResult(t, "database membrane win", msg, result, lifecycleEvalResult{memWins: 1})
	msg, result = databaseScenarioOutcome(false, false)
	assertResult(t, "database tie", msg, result, lifecycleEvalResult{ties: 1})
	msg, result = databaseScenarioOutcome(true, false)
	assertResult(t, "database rag win", msg, result, lifecycleEvalResult{ragWins: 1})

	msg, result = topResultScenarioOutcome("preferred", "other", "preferred", "mem wins", "tie")
	assertResult(t, "top result membrane win", msg, result, lifecycleEvalResult{memWins: 1})
	msg, result = topResultScenarioOutcome("preferred", "preferred", "preferred", "mem wins", "tie")
	assertResult(t, "top result tie", msg, result, lifecycleEvalResult{ties: 1})
	msg, result = topResultScenarioOutcome("other", "preferred", "preferred", "mem wins", "tie")
	assertResult(t, "top result rag win", msg, result, lifecycleEvalResult{ragWins: 1})

	msg, result = supersessionScenarioOutcome(true, false, true)
	assertResult(t, "supersession membrane win", msg, result, lifecycleEvalResult{memWins: 1})
	msg, result = supersessionScenarioOutcome(true, false, false)
	assertResult(t, "supersession tie", msg, result, lifecycleEvalResult{ties: 1})
	msg, result = supersessionScenarioOutcome(false, true, true)
	assertResult(t, "supersession partial tie", msg, result, lifecycleEvalResult{ties: 1})

	total := lifecycleEvalResult{ragWins: 1}
	total.add(lifecycleEvalResult{memWins: 2, ties: 3})
	if total != (lifecycleEvalResult{ragWins: 1, memWins: 2, ties: 3}) {
		t.Fatalf("add result = %+v, want accumulated counts", total)
	}
}

func TestRunLifecycleCLIValidationErrors(t *testing.T) {
	ctx := context.Background()

	if err := runLifecycleCLI(ctx, nil, nil); err == nil || !strings.Contains(err.Error(), "config") {
		t.Fatalf("runLifecycleCLI missing config = %v, want config error", err)
	}
	if err := runLifecycleCLI(ctx, []string{"-bad-flag"}, nil); err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
		t.Fatalf("runLifecycleCLI bad flag = %v, want parse error", err)
	}
	if err := runLifecycleCLI(ctx, []string{"-postgres-dsn", "://bad-dsn"}, func(key string) string {
		if key == "MEMBRANE_EMBEDDING_API_KEY" {
			return "env-key"
		}
		return ""
	}); err == nil || !strings.Contains(err.Error(), "create membrane") {
		t.Fatalf("runLifecycleCLI postgres open path = %v, want create/open error before eval", err)
	}
}

func TestLifecycleCLIDependenciesDefaults(t *testing.T) {
	deps := lifecycleCLIDependencies{}.withDefaults()
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

func TestRunLifecycleCLIWithInjectedDependencies(t *testing.T) {
	ctx := context.Background()
	store := &scriptedLifecycleVectorStore{}
	deps := lifecycleCLIDependencies{
		newMembrane: func(_ *membrane.Config) (lifecycleMembrane, error) {
			cfg := membrane.DefaultConfig()
			cfg.DBPath = filepath.Join(t.TempDir(), "membrane.db")
			return membrane.New(cfg)
		},
		openVectorStore: func(dsn string, cfg postgres.EmbeddingConfig) (lifecycleCLIVectorStore, error) {
			if dsn != "postgres://fake/db" || cfg.Dimensions != 9 || cfg.Model != "life-model" {
				t.Fatalf("openVectorStore args = %q/%+v, want configured DSN/model/dims", dsn, cfg)
			}
			return store, nil
		},
		newEmbeddingClient: func(endpoint, model, apiKey string, dimensions int) embedding.Client {
			if endpoint != "http://embed.example" || model != "life-model" || apiKey != "env-key" || dimensions != 9 {
				t.Fatalf("newEmbeddingClient args = %q/%q/%q/%d, want configured values", endpoint, model, apiKey, dimensions)
			}
			return scriptedLifecycleEmbeddingClient{}
		},
	}

	err := runLifecycleCLIWithDeps(ctx, []string{
		"-postgres-dsn", "postgres://fake/db",
		"-embedding-endpoint", "http://embed.example",
		"-embedding-model", "life-model",
		"-embedding-dimensions", "9",
	}, func(key string) string {
		if key == "MEMBRANE_EMBEDDING_API_KEY" {
			return "env-key"
		}
		return ""
	}, deps)
	if err != nil {
		t.Fatalf("runLifecycleCLIWithDeps: %v", err)
	}
	if !store.closed {
		t.Fatalf("vector store was not closed")
	}
}

func TestRunLifecycleCLIInjectedOpenError(t *testing.T) {
	ctx := context.Background()
	openErr := errors.New("open failed")
	err := runLifecycleCLIWithDeps(ctx, []string{
		"-postgres-dsn", "postgres://fake/db",
		"-embedding-api-key", "flag-key",
	}, nil, lifecycleCLIDependencies{
		newMembrane: func(_ *membrane.Config) (lifecycleMembrane, error) {
			cfg := membrane.DefaultConfig()
			cfg.DBPath = filepath.Join(t.TempDir(), "membrane.db")
			return membrane.New(cfg)
		},
		openVectorStore: func(string, postgres.EmbeddingConfig) (lifecycleCLIVectorStore, error) {
			return nil, openErr
		},
	})
	if err == nil || !strings.Contains(err.Error(), "open pgstore") {
		t.Fatalf("runLifecycleCLI open store error = %v, want open pgstore error", err)
	}
}

func TestRunLifecycleCLIInjectedMembraneError(t *testing.T) {
	ctx := context.Background()
	createErr := errors.New("create failed")
	err := runLifecycleCLIWithDeps(ctx, []string{
		"-postgres-dsn", "postgres://fake/db",
		"-embedding-api-key", "flag-key",
	}, nil, lifecycleCLIDependencies{
		newMembrane: func(_ *membrane.Config) (lifecycleMembrane, error) {
			return nil, createErr
		},
	})
	if err == nil || !strings.Contains(err.Error(), "create membrane") {
		t.Fatalf("runLifecycleCLI membrane error = %v, want create membrane wrapper", err)
	}
}

func TestRunLifecycleCLIWrapsStartErrors(t *testing.T) {
	ctx := context.Background()
	args := []string{"-postgres-dsn", "postgres://fake/db", "-embedding-api-key", "flag-key"}
	startErr := errors.New("start failed")
	if err := runLifecycleCLIWithDeps(ctx, args, nil, lifecycleCLIDependencies{
		newMembrane: func(*membrane.Config) (lifecycleMembrane, error) {
			return &fakeLifecycleMembrane{startErr: startErr}, nil
		},
	}); err == nil || !strings.Contains(err.Error(), "start membrane") || !errors.Is(err, startErr) {
		t.Fatalf("runLifecycleCLI start error = %v, want wrapped start error", err)
	}

	cleanupErr := errors.New("cleanup failed")
	if err := runLifecycleCLIWithDeps(ctx, args, nil, lifecycleCLIDependencies{
		newMembrane: func(*membrane.Config) (lifecycleMembrane, error) {
			return &fakeLifecycleMembrane{startErr: startErr, stopErr: cleanupErr}, nil
		},
	}); err == nil || !strings.Contains(err.Error(), "cleanup") || !errors.Is(err, startErr) {
		t.Fatalf("runLifecycleCLI start cleanup error = %v, want start and cleanup wrapper", err)
	}
}

func TestRunLifecycleCLIWrapsEvalErrors(t *testing.T) {
	ctx := context.Background()
	store := &scriptedLifecycleVectorStore{}
	err := runLifecycleCLIWithDeps(ctx, []string{
		"-postgres-dsn", "postgres://fake/db",
		"-embedding-api-key", "flag-key",
	}, nil, lifecycleCLIDependencies{
		newMembrane: func(_ *membrane.Config) (lifecycleMembrane, error) {
			cfg := membrane.DefaultConfig()
			cfg.DBPath = filepath.Join(t.TempDir(), "membrane.db")
			return membrane.New(cfg)
		},
		openVectorStore: func(string, postgres.EmbeddingConfig) (lifecycleCLIVectorStore, error) {
			return store, nil
		},
		newEmbeddingClient: func(string, string, string, int) embedding.Client {
			return failingLifecycleEmbeddingClient{}
		},
	})
	if err == nil || !strings.Contains(err.Error(), "run lifecycle eval") {
		t.Fatalf("runLifecycleCLI eval error = %v, want run lifecycle eval wrapper", err)
	}
	if !store.closed {
		t.Fatalf("vector store was not closed after eval error")
	}
}

func TestLifecycleEmbedRecordAndRagSearchUseInterfaces(t *testing.T) {
	ctx := context.Background()
	client := &fakeLifecycleEmbeddingClient{vec: []float32{0.25, 0.75}}
	store := &fakeLifecycleVectorStore{ids: []string{"a", "b", "c"}}

	if err := embedRecord(ctx, client, store, "rec-1", "record text", "model-a"); err != nil {
		t.Fatalf("embedRecord: %v", err)
	}
	if len(client.texts) != 1 || client.texts[0] != "record text" {
		t.Fatalf("embedded texts = %+v, want record text", client.texts)
	}
	if store.storedID != "rec-1" || store.storedModel != "model-a" || len(store.storedVector) != 2 {
		t.Fatalf("stored embedding = id:%q model:%q vector:%v, want rec-1/model-a/vector", store.storedID, store.storedModel, store.storedVector)
	}

	top, err := ragSearch(ctx, store, []float32{1, 0}, 2)
	if err != nil {
		t.Fatalf("ragSearch: %v", err)
	}
	if len(top) != 2 || top[0] != "a" || top[1] != "b" {
		t.Fatalf("ragSearch top = %+v, want first two IDs", top)
	}
	if store.lastLimit != 100 || len(store.lastQuery) != 2 {
		t.Fatalf("ragSearch limit/query = %d/%v, want widened limit 100 and query", store.lastLimit, store.lastQuery)
	}

	if _, err := ragSearch(ctx, store, []float32{0}, 150); err != nil {
		t.Fatalf("ragSearch large k: %v", err)
	}
	if store.lastLimit != 150 {
		t.Fatalf("ragSearch large k limit = %d, want 150", store.lastLimit)
	}
	top, err = ragSearch(ctx, store, []float32{1, 0}, -1)
	if err != nil || len(top) != 0 {
		t.Fatalf("ragSearch negative k = %+v, %v; want empty nil", top, err)
	}
}

func TestLifecycleEmbeddingHelpersPropagateErrors(t *testing.T) {
	ctx := context.Background()
	client := &fakeLifecycleEmbeddingClient{vec: []float32{1}}

	if err := embedRecord(ctx, failingLifecycleEmbeddingClient{}, &fakeLifecycleVectorStore{}, "rec-1", "text", "model"); err == nil || !strings.Contains(err.Error(), "embed rec-1") {
		t.Fatalf("embedRecord embed error = %v, want wrapped embed error", err)
	}
	if err := embedRecord(ctx, client, failingLifecycleVectorStore{storeErr: errors.New("store failed")}, "rec-1", "text", "model"); err == nil || !strings.Contains(err.Error(), "store embedding rec-1") {
		t.Fatalf("embedRecord store error = %v, want wrapped store error", err)
	}
	if _, err := ragSearch(ctx, failingLifecycleVectorStore{searchErr: errors.New("search failed")}, []float32{1}, 3); err == nil || !strings.Contains(err.Error(), "rag search") {
		t.Fatalf("ragSearch error = %v, want wrapped search error", err)
	}
}

func TestRunLifecycleEvalWithScriptedVectorSearch(t *testing.T) {
	ctx := context.Background()
	m := newLifecycleTestMembrane(t)
	result, err := runLifecycleEval(ctx, m, scriptedLifecycleEmbeddingClient{}, &scriptedLifecycleVectorStore{}, "test-model")
	if err != nil {
		t.Fatalf("runLifecycleEval: %v", err)
	}

	if got := result.memWins + result.ragWins + result.ties; got != 4 {
		t.Fatalf("scenario count = %d, want 4; result = %+v", got, result)
	}
	if result.memWins == 0 {
		t.Fatalf("runLifecycleEval result = %+v, want at least one Membrane win", result)
	}
}

func TestRunLifecycleEvalTieOutcomes(t *testing.T) {
	ctx := context.Background()
	m := newLifecycleTestMembrane(t)
	result, err := runLifecycleEval(ctx, m, scriptedLifecycleEmbeddingClient{}, &tieLifecycleVectorStore{}, "test-model")
	if err != nil {
		t.Fatalf("runLifecycleEval: %v", err)
	}

	if result.ties == 0 {
		t.Fatalf("runLifecycleEval result = %+v, want at least one tie", result)
	}
}

func TestRunLifecycleEvalPropagatesEmbeddingErrors(t *testing.T) {
	ctx := context.Background()
	m := newLifecycleTestMembrane(t)
	_, err := runLifecycleEval(ctx, m, failingLifecycleEmbeddingClient{}, &scriptedLifecycleVectorStore{}, "test-model")
	if err == nil || !strings.Contains(err.Error(), "embed") {
		t.Fatalf("runLifecycleEval error = %v, want embedded failure", err)
	}
}

func TestRunLifecycleEvalPropagatesInitialCaptureError(t *testing.T) {
	ctx := context.Background()
	m := newLifecycleTestMembrane(t)
	if err := m.Stop(); err != nil {
		t.Fatalf("Stop membrane: %v", err)
	}
	_, err := runLifecycleEval(ctx, m, scriptedLifecycleEmbeddingClient{}, &scriptedLifecycleVectorStore{}, "test-model")
	if err == nil || !strings.Contains(err.Error(), "capture wrong database record") {
		t.Fatalf("runLifecycleEval initial capture error = %v, want capture wrong database record", err)
	}
}

func TestRunLifecycleEvalPropagatesScriptedMembraneErrors(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name           string
		captureErrAt   int
		reinforceErrAt int
		penalizeErrAt  int
		want           string
	}{
		{name: "correct database capture", captureErrAt: 2, want: "capture correct database record"},
		{name: "noise ingest", captureErrAt: 3, want: "ingest noise"},
		{name: "weak oom capture", captureErrAt: 6, want: "capture weak oom procedure"},
		{name: "strong oom capture", captureErrAt: 7, want: "capture strong oom procedure"},
		{name: "weak oom penalize", penalizeErrAt: 1, want: "penalize weak oom procedure"},
		{name: "old rate limit capture", captureErrAt: 8, want: "capture old rate limit"},
		{name: "stale deployment capture", captureErrAt: 9, want: "capture stale deployment target"},
		{name: "fresh deployment capture", captureErrAt: 10, want: "capture fresh deployment target"},
		{name: "stale deployment penalize", penalizeErrAt: 2, want: "penalize stale deployment target"},
		{name: "fresh deployment reinforce", reinforceErrAt: 6, want: "reinforce fresh deployment target"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &fakeLifecycleMembrane{
				captureErrAt:   tt.captureErrAt,
				reinforceErrAt: tt.reinforceErrAt,
				penalizeErrAt:  tt.penalizeErrAt,
			}
			_, err := runLifecycleEval(ctx, m, scriptedLifecycleEmbeddingClient{}, &scriptedLifecycleVectorStore{}, "test-model")
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("runLifecycleEval error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestLifecycleCaptureObservationRecordRequiresSemanticCreatedRecord(t *testing.T) {
	ctx := context.Background()
	primary := schema.NewMemoryRecord("primary", schema.MemoryTypeEpisodic, schema.SensitivityLow, &schema.EpisodicPayload{Kind: "episodic"})
	entity := schema.NewMemoryRecord("entity", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{Kind: "entity", CanonicalName: "Orchid"})
	m := &fakeLifecycleMembrane{response: &ingestion.CaptureMemoryResponse{
		PrimaryRecord:  primary,
		CreatedRecords: []*schema.MemoryRecord{nil, entity},
	}}
	if rec, err := captureObservationRecord(ctx, m, "tester", "Orchid", "deploys_to", "staging", nil, "", schema.SensitivityLow); err == nil || rec != nil || !strings.Contains(err.Error(), "did not produce semantic") {
		t.Fatalf("captureObservationRecord no semantic = %+v, %v; want semantic record error", rec, err)
	}
}

func TestRunLifecycleEvalPropagatesVectorStoreErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("store embedding", func(t *testing.T) {
		m := newLifecycleTestMembrane(t)
		_, err := runLifecycleEval(ctx, m, scriptedLifecycleEmbeddingClient{}, failingLifecycleVectorStore{storeErr: errors.New("store failed")}, "test-model")
		if err == nil || !strings.Contains(err.Error(), "store embedding") {
			t.Fatalf("runLifecycleEval store error = %v, want store embedding error", err)
		}
	})

	t.Run("search", func(t *testing.T) {
		m := newLifecycleTestMembrane(t)
		_, err := runLifecycleEval(ctx, m, scriptedLifecycleEmbeddingClient{}, failingLifecycleVectorStore{searchErr: errors.New("search failed")}, "test-model")
		if err == nil || !strings.Contains(err.Error(), "rag search") {
			t.Fatalf("runLifecycleEval search error = %v, want rag search error", err)
		}
	})
}

func TestRunLifecycleEvalPropagatesLateEmbeddingErrors(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name   string
		failAt int
		want   string
	}{
		{name: "correct database record", failAt: 2, want: "embed"},
		{name: "noise record", failAt: 3, want: "embed"},
		{name: "database query", failAt: 6, want: "What database"},
		{name: "weak oom procedure", failAt: 7, want: "embed"},
		{name: "strong oom procedure", failAt: 8, want: "embed"},
		{name: "oom query", failAt: 9, want: "How do I debug"},
		{name: "old rate limit", failAt: 10, want: "embed"},
		{name: "new rate limit", failAt: 11, want: "embed"},
		{name: "rate limit query", failAt: 12, want: "current API rate"},
		{name: "stale deployment", failAt: 13, want: "embed"},
		{name: "fresh deployment", failAt: 14, want: "embed"},
		{name: "deployment query", failAt: 15, want: "Where do we deploy"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newLifecycleTestMembrane(t)
			client := &failAtLifecycleEmbeddingClient{failAt: tt.failAt}
			_, err := runLifecycleEval(ctx, m, client, &scriptedLifecycleVectorStore{}, "test-model")
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("runLifecycleEval error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestRunLifecycleEvalPropagatesLateVectorSearchErrors(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name         string
		failSearchAt int
	}{
		{name: "oom search", failSearchAt: 2},
		{name: "rate limit search", failSearchAt: 3},
		{name: "deployment search", failSearchAt: 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newLifecycleTestMembrane(t)
			store := &failAtLifecycleVectorStore{failSearchAt: tt.failSearchAt}
			_, err := runLifecycleEval(ctx, m, scriptedLifecycleEmbeddingClient{}, store, "test-model")
			if err == nil || !strings.Contains(err.Error(), "rag search") {
				t.Fatalf("runLifecycleEval search error = %v, want rag search error", err)
			}
		})
	}
}

func TestRunLifecycleEvalPropagatesMembraneOperationErrors(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name         string
		stopStoreAt  int
		stopSearchAt int
		want         string
	}{
		{name: "retract", stopStoreAt: 5, want: "retract wrong database record"},
		{name: "database retrieve", stopSearchAt: 1, want: "retrieve database scenario records"},
		{name: "reinforce", stopStoreAt: 7, want: "reinforce strong oom procedure"},
		{name: "oom retrieve", stopSearchAt: 2, want: "retrieve oom scenario records"},
		{name: "supersede", stopStoreAt: 8, want: "supersede old rate limit"},
		{name: "rate limit retrieve", stopSearchAt: 3, want: "retrieve rate limit scenario records"},
		{name: "deployment retrieve", stopSearchAt: 4, want: "retrieve deployment scenario records"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newLifecycleTestMembrane(t)
			store := &failAtLifecycleVectorStore{
				stopStoreAt:  tt.stopStoreAt,
				stopSearchAt: tt.stopSearchAt,
				membrane:     m,
			}
			_, err := runLifecycleEval(ctx, m, scriptedLifecycleEmbeddingClient{}, store, "test-model")
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("runLifecycleEval operation error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestLifecycleCaptureNoiseAndRetrieveRootRecords(t *testing.T) {
	ctx := context.Background()
	m := newLifecycleTestMembrane(t)

	rec, err := captureObservationRecord(ctx, m, "tester", "Orchid", "deploys_to", "staging", []string{"deploy"}, "project:alpha", schema.SensitivityLow)
	if err != nil {
		t.Fatalf("captureObservationRecord: %v", err)
	}
	noiseIDs, err := ingestNoise(ctx, m, []string{"team uses Redis", "singleword"})
	if err != nil {
		t.Fatalf("ingestNoise: %v", err)
	}
	if len(noiseIDs) != 2 || noiseIDs[0] == "" || noiseIDs[1] == "" {
		t.Fatalf("ingestNoise = %+v, want two IDs", noiseIDs)
	}

	resp, err := retrieveRootRecords(ctx, m, &retrieval.RetrieveRequest{
		TaskDescriptor: "Orchid staging deploy",
		Trust:          fullTrust(),
		MemoryTypes:    []schema.MemoryType{schema.MemoryTypeSemantic},
		Limit:          10,
	})
	if err != nil {
		t.Fatalf("retrieveRootRecords: %v", err)
	}
	if !containsRec(resp.Records, rec.ID) {
		t.Fatalf("records = %+v, want captured semantic record %s", resp.Records, rec.ID)
	}
	if _, err := retrieveRootRecords(ctx, m, nil); err == nil {
		t.Fatalf("retrieveRootRecords nil request error = nil, want error")
	}
	if _, err := retrieveRootRecords(ctx, m, &retrieval.RetrieveRequest{}); err == nil || !strings.Contains(err.Error(), "trust context is required") {
		t.Fatalf("retrieveRootRecords missing trust error = %v, want trust error", err)
	}
	if _, err := captureObservationRecord(ctx, m, "", "Orchid", "deploys_to", "staging", nil, "project:alpha", schema.SensitivityLow); err == nil {
		t.Fatalf("captureObservationRecord empty source error = nil, want validation error")
	}

	stopped := newLifecycleTestMembrane(t)
	if err := stopped.Stop(); err != nil {
		t.Fatalf("Stop stopped membrane: %v", err)
	}
	if _, err := ingestNoise(ctx, stopped, []string{"team uses Redis"}); err == nil || !strings.Contains(err.Error(), "ingest noise") {
		t.Fatalf("ingestNoise stopped membrane error = %v, want wrapped ingest noise error", err)
	}
}

func newLifecycleTestMembrane(t *testing.T) *membrane.Membrane {
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
