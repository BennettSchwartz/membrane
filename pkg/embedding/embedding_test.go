package embedding

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
	"github.com/BennettSchwartz/membrane/pkg/storage/sqlite"
)

type stringerValue string

func (s stringerValue) String() string {
	return "stringer:" + string(s)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type errorReadCloser struct{}

func (errorReadCloser) Read([]byte) (int, error) {
	return 0, errors.New("read failed")
}

func (errorReadCloser) Close() error {
	return nil
}

type fakeEmbeddingClient struct {
	texts   []string
	vectors map[string][]float32
	errors  map[string]error
}

func (f *fakeEmbeddingClient) Embed(_ context.Context, text string) ([]float32, error) {
	f.texts = append(f.texts, text)
	if err := f.errors[text]; err != nil {
		return nil, err
	}
	vec := f.vectors[text]
	if len(vec) == 0 {
		vec = []float32{1, 0}
	}
	return append([]float32(nil), vec...), nil
}

type fakeVectorStore struct {
	embeddings map[string][]float32
	models     map[string]string
	getErrs    map[string]error
	storeErr   error
}

func newFakeVectorStore() *fakeVectorStore {
	return &fakeVectorStore{
		embeddings: make(map[string][]float32),
		models:     make(map[string]string),
		getErrs:    make(map[string]error),
	}
}

func (f *fakeVectorStore) StoreTriggerEmbedding(_ context.Context, recordID string, embedding []float32, model string) error {
	if f.storeErr != nil {
		return f.storeErr
	}
	f.embeddings[recordID] = append([]float32(nil), embedding...)
	f.models[recordID] = model
	return nil
}

func (f *fakeVectorStore) GetTriggerEmbedding(_ context.Context, recordID string) ([]float32, error) {
	if err := f.getErrs[recordID]; err != nil {
		return nil, err
	}
	return append([]float32(nil), f.embeddings[recordID]...), nil
}

func newTestStore(t *testing.T) storage.Store {
	t.Helper()

	store, err := sqlite.Open(":memory:", "")
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite store: %v", err)
		}
	})

	return store
}

func mustCreate(t *testing.T, ctx context.Context, store storage.Store, rec *schema.MemoryRecord) {
	t.Helper()
	if err := store.Create(ctx, rec); err != nil {
		t.Fatalf("create %s: %v", rec.ID, err)
	}
}

func semanticRecord(id string, object any) *schema.MemoryRecord {
	return schema.NewMemoryRecord(id, schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "Membrane",
		Predicate: "supports",
		Object:    object,
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
}

func TestHTTPClientEmbedSendsRequestAndParsesResponse(t *testing.T) {
	var gotRequest map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Fatalf("content-type = %q, want application/json", got)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Fatalf("authorization = %q, want bearer token", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"embedding":[0.25,-0.5,1]}]}`))
	}))
	defer server.Close()

	client := NewHTTPClient(server.URL, "embed-model", "test-token", 3)
	got, err := client.Embed(context.Background(), "search text")
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}

	if len(got) != 3 || got[0] != 0.25 || got[1] != -0.5 || got[2] != 1 {
		t.Fatalf("embedding = %#v, want parsed vector", got)
	}
	if gotRequest["model"] != "embed-model" || gotRequest["input"] != "search text" || gotRequest["dimensions"] != float64(3) {
		t.Fatalf("request body = %#v", gotRequest)
	}
}

func TestHTTPClientEmbedErrors(t *testing.T) {
	for _, tc := range []struct {
		name       string
		statusCode int
		body       string
		want       string
	}{
		{name: "non-2xx", statusCode: http.StatusBadGateway, body: "upstream down", want: "embedding request failed with 502: upstream down"},
		{name: "invalid json", statusCode: http.StatusOK, body: "{", want: "parse embedding response"},
		{name: "missing embedding", statusCode: http.StatusOK, body: `{"data":[{"embedding":[]}]}`, want: "did not include an embedding"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.statusCode)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()

			client := NewHTTPClient(server.URL, "model", "", 0)
			_, err := client.Embed(context.Background(), "text")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Embed error = %v, want containing %q", err, tc.want)
			}
		})
	}

	t.Run("request failed", func(t *testing.T) {
		client := NewHTTPClient("http://example.test", "model", "", 0)
		client.http = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("transport failed")
		})}
		_, err := client.Embed(context.Background(), "text")
		if err == nil || !strings.Contains(err.Error(), "embedding request failed") {
			t.Fatalf("Embed transport error = %v, want embedding request failed", err)
		}
	})

	t.Run("read failed", func(t *testing.T) {
		client := NewHTTPClient("http://example.test", "model", "", 0)
		client.http = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       errorReadCloser{},
				Header:     make(http.Header),
			}, nil
		})}
		_, err := client.Embed(context.Background(), "text")
		if err == nil || !strings.Contains(err.Error(), "read embedding response") {
			t.Fatalf("Embed read error = %v, want read embedding response", err)
		}
	})
}

func TestHTTPClientEmbedRequestErrors(t *testing.T) {
	client := NewHTTPClient("http://[::1", "model", "", 0)
	_, err := client.Embed(context.Background(), "text")
	if err == nil || !strings.Contains(err.Error(), "new embedding request") {
		t.Fatalf("Embed invalid endpoint error = %v, want request creation error", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	url := server.URL
	server.Close()

	client = NewHTTPClient(url, "model", "", 0)
	_, err = client.Embed(context.Background(), "text")
	if err == nil || !strings.Contains(err.Error(), "embedding request failed") {
		t.Fatalf("Embed closed server error = %v, want request failure", err)
	}
}

func TestServiceEmbedRecordUsesSemanticObjectText(t *testing.T) {
	ctx := context.Background()
	client := &fakeEmbeddingClient{
		vectors: map[string][]float32{
			`Membrane supports {"nested":true,"score":3}`: {0.1, 0.2},
		},
	}
	vectors := newFakeVectorStore()
	svc := NewService(client, nil, vectors, "model-a")

	rec := semanticRecord("semantic-map", map[string]any{"score": 3, "nested": true})
	if err := svc.EmbedRecord(ctx, rec); err != nil {
		t.Fatalf("EmbedRecord: %v", err)
	}

	wantText := `Membrane supports {"nested":true,"score":3}`
	if len(client.texts) != 1 || client.texts[0] != wantText {
		t.Fatalf("embedded texts = %#v, want %q", client.texts, wantText)
	}
	if got := vectors.embeddings[rec.ID]; len(got) != 2 || got[0] != 0.1 || got[1] != 0.2 {
		t.Fatalf("stored embedding = %#v, want fake vector", got)
	}
	if got := vectors.models[rec.ID]; got != "model-a" {
		t.Fatalf("stored model = %q, want model-a", got)
	}
}

func TestServiceEmbedRecordGuardsAndErrors(t *testing.T) {
	ctx := context.Background()
	rec := semanticRecord("semantic-blank", nil)
	rec.Payload = &schema.EntityPayload{Kind: "entity", CanonicalName: "not embedded"}
	client := &fakeEmbeddingClient{}
	vectors := newFakeVectorStore()

	if err := (*Service)(nil).EmbedRecord(ctx, semanticRecord("nil-service", "value")); err != nil {
		t.Fatalf("nil service EmbedRecord: %v", err)
	}
	if err := NewService(nil, nil, vectors, "model").EmbedRecord(ctx, semanticRecord("nil-client", "value")); err != nil {
		t.Fatalf("nil client EmbedRecord: %v", err)
	}
	if err := NewService(client, nil, vectors, "model").EmbedRecord(ctx, nil); err != nil {
		t.Fatalf("nil record EmbedRecord: %v", err)
	}
	if err := NewService(client, nil, vectors, "model").EmbedRecord(ctx, rec); err != nil {
		t.Fatalf("blank trigger EmbedRecord: %v", err)
	}
	if len(client.texts) != 0 || len(vectors.embeddings) != 0 {
		t.Fatalf("blank/guarded embeds touched client=%v vectors=%v, want no work", client.texts, vectors.embeddings)
	}

	embedErr := errors.New("embed failed")
	failingClient := &fakeEmbeddingClient{errors: map[string]error{"Membrane supports value": embedErr}}
	if err := NewService(failingClient, nil, vectors, "model").EmbedRecord(ctx, semanticRecord("embed-error", "value")); !errors.Is(err, embedErr) {
		t.Fatalf("EmbedRecord embed error = %v, want %v", err, embedErr)
	}

	storeErr := errors.New("store failed")
	failingStore := newFakeVectorStore()
	failingStore.storeErr = storeErr
	if err := NewService(&fakeEmbeddingClient{}, nil, failingStore, "model").EmbedRecord(ctx, semanticRecord("store-error", "value")); !errors.Is(err, storeErr) {
		t.Fatalf("EmbedRecord store error = %v, want %v", err, storeErr)
	}
}

func TestTriggerTextByPayloadType(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload schema.Payload
		want    string
	}{
		{
			name: "episodic most recent summary",
			payload: &schema.EpisodicPayload{Timeline: []schema.TimelineEvent{
				{EventKind: "tool", Ref: "first", Summary: "first summary"},
				{EventKind: "tool", Ref: "second", Summary: "second summary"},
			}},
			want: "second summary",
		},
		{
			name:    "episodic event fallback",
			payload: &schema.EpisodicPayload{Timeline: []schema.TimelineEvent{{EventKind: "tool_call", Ref: "cmd-1"}}},
			want:    "tool_call cmd-1",
		},
		{
			name:    "empty episodic",
			payload: &schema.EpisodicPayload{},
			want:    "",
		},
		{
			name: "working memory",
			payload: &schema.WorkingPayload{
				ContextSummary: "fix build",
				State:          schema.TaskStateExecuting,
				NextActions:    []string{"run tests", "ship"},
			},
			want: "fix build executing run tests ship",
		},
		{
			name:    "working omits empty state",
			payload: &schema.WorkingPayload{NextActions: []string{"answer blocker"}},
			want:    "answer blocker",
		},
		{
			name:    "semantic string object",
			payload: &schema.SemanticPayload{Subject: "Go", Predicate: "is", Object: "typed"},
			want:    "Go is typed",
		},
		{
			name:    "semantic nil object",
			payload: &schema.SemanticPayload{Subject: "Go", Predicate: "has", Object: nil},
			want:    "Go has",
		},
		{
			name: "competence with triggers",
			payload: &schema.CompetencePayload{
				SkillName: "debug",
				Triggers:  []schema.Trigger{{Signal: "panic"}, {Signal: ""}, {Signal: "timeout"}},
			},
			want: "debug panic timeout",
		},
		{
			name:    "competence without skill name trims signal text",
			payload: &schema.CompetencePayload{Triggers: []schema.Trigger{{Signal: "panic"}}},
			want:    "panic",
		},
		{
			name:    "competence skill fallback",
			payload: &schema.CompetencePayload{SkillName: "deploy"},
			want:    "deploy",
		},
		{
			name:    "plan graph intent",
			payload: &schema.PlanGraphPayload{Intent: "release safely", Nodes: []schema.PlanNode{{Op: "ignored"}}},
			want:    "release safely",
		},
		{
			name:    "plan graph ops fallback",
			payload: &schema.PlanGraphPayload{Nodes: []schema.PlanNode{{Op: "test"}, {Op: ""}, {Op: "deploy"}}},
			want:    "test deploy",
		},
		{
			name: "unknown entity payload",
			payload: &schema.EntityPayload{
				CanonicalName: "Membrane",
			},
			want: "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := schema.NewMemoryRecord("rec", schema.MemoryTypeSemantic, schema.SensitivityLow, tc.payload)
			if got := triggerText(rec); got != tc.want {
				t.Fatalf("triggerText = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSemanticObjectText(t *testing.T) {
	if got := semanticObjectText("plain"); got != "plain" {
		t.Fatalf("string object = %q, want plain", got)
	}
	if got := semanticObjectText(stringerValue("value")); got != "stringer:value" {
		t.Fatalf("stringer object = %q, want stringer:value", got)
	}
	if got := semanticObjectText(map[string]any{"b": 2, "a": 1}); got != `{"a":1,"b":2}` {
		t.Fatalf("map object = %q, want deterministic JSON", got)
	}
	if got := semanticObjectText(nil); got != "" {
		t.Fatalf("nil object = %q, want empty", got)
	}
	if got := semanticObjectText(func() {}); got == "" {
		t.Fatalf("fallback object text should not be empty")
	}
}

func TestServiceEmbedQuery(t *testing.T) {
	ctx := context.Background()
	client := &fakeEmbeddingClient{vectors: map[string][]float32{"query": {0.3, 0.4}}}
	svc := NewService(client, nil, newFakeVectorStore(), "model")

	got, err := svc.EmbedQuery(ctx, "query")
	if err != nil {
		t.Fatalf("EmbedQuery: %v", err)
	}
	if len(got) != 2 || got[0] != 0.3 || got[1] != 0.4 {
		t.Fatalf("query embedding = %#v, want fake vector", got)
	}

	got, err = svc.EmbedQuery(ctx, "  ")
	if err != nil {
		t.Fatalf("blank EmbedQuery: %v", err)
	}
	if got != nil {
		t.Fatalf("blank EmbedQuery = %#v, want nil", got)
	}
	if len(client.texts) != 1 {
		t.Fatalf("Embed called %d times, want 1", len(client.texts))
	}
}

func TestSimilarity(t *testing.T) {
	vectors := newFakeVectorStore()
	vectors.embeddings["same"] = []float32{1, 0}
	vectors.embeddings["opposite"] = []float32{-1, 0}
	vectors.embeddings["zero"] = []float32{0, 0}
	vectors.embeddings["mismatch"] = []float32{1}
	vectors.getErrs["error"] = errors.New("lookup failed")
	svc := NewService(nil, nil, vectors, "model")
	query := []float32{1, 0}

	for _, tc := range []struct {
		id      string
		want    float64
		wantOK  bool
		epsilon float64
	}{
		{id: "same", want: 1, wantOK: true},
		{id: "opposite", want: 0, wantOK: true},
		{id: "zero", want: 0.5, wantOK: true},
		{id: "mismatch", want: 0.5, wantOK: false},
		{id: "missing", want: 0.5, wantOK: false},
		{id: "error", want: 0.5, wantOK: false},
	} {
		t.Run(tc.id, func(t *testing.T) {
			got, ok := svc.Similarity(context.Background(), tc.id, query)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if math.Abs(got-tc.want) > 0.0001 {
				t.Fatalf("similarity = %.4f, want %.4f", got, tc.want)
			}
		})
	}

	if got, ok := (*Service)(nil).Similarity(context.Background(), "same", query); ok || got != 0.5 {
		t.Fatalf("nil service similarity = %.4f, %v; want 0.5, false", got, ok)
	}
	if got, ok := NewService(nil, nil, nil, "model").Similarity(context.Background(), "same", query); ok || got != 0.5 {
		t.Fatalf("nil vector store similarity = %.4f, %v; want 0.5, false", got, ok)
	}
	if got, ok := svc.Similarity(context.Background(), "same", nil); ok || got != 0.5 {
		t.Fatalf("empty query similarity = %.4f, %v; want 0.5, false", got, ok)
	}
}

func TestClamp01(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value float64
		want  float64
	}{
		{name: "nan", value: math.NaN(), want: 0.5},
		{name: "below zero", value: -0.01, want: 0},
		{name: "above one", value: 1.01, want: 1},
		{name: "in range", value: 0.42, want: 0.42},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := clamp01(tc.value); got != tc.want {
				t.Fatalf("clamp01(%v) = %v, want %v", tc.value, got, tc.want)
			}
		})
	}
}

func TestBackfillMissingSkipsExistingUnembeddableAndFailedRecords(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	toEmbed := semanticRecord("needs-embedding", "structured memory")
	existing := schema.NewMemoryRecord("existing", schema.MemoryTypeCompetence, schema.SensitivityLow, &schema.CompetencePayload{
		Kind:      "competence",
		SkillName: "debug-builds",
		Triggers:  []schema.Trigger{{Signal: "build failure"}},
		Recipe:    []schema.RecipeStep{{Step: "inspect logs"}},
	})
	empty := schema.NewMemoryRecord("empty", schema.MemoryTypeEpisodic, schema.SensitivityLow, &schema.EpisodicPayload{
		Kind:     "episodic",
		Timeline: []schema.TimelineEvent{},
	})
	failing := schema.NewMemoryRecord("failing", schema.MemoryTypePlanGraph, schema.SensitivityLow, &schema.PlanGraphPayload{
		Kind:   "plan_graph",
		PlanID: "plan-fail",
		Intent: "fail embedding",
		Nodes:  []schema.PlanNode{{ID: "n1", Op: "build"}},
	})

	for _, rec := range []*schema.MemoryRecord{toEmbed, existing, empty, failing} {
		mustCreate(t, ctx, store, rec)
	}

	client := &fakeEmbeddingClient{
		vectors: map[string][]float32{"Membrane supports structured memory": {0.9, 0.1}},
		errors:  map[string]error{"fail embedding": errors.New("temporary embedding outage")},
	}
	vectors := newFakeVectorStore()
	vectors.embeddings[existing.ID] = []float32{0.2, 0.8}
	svc := NewService(client, store, vectors, "model-b")

	count, err := svc.BackfillMissing(ctx)
	if err != nil {
		t.Fatalf("BackfillMissing: %v", err)
	}
	if count != 1 {
		t.Fatalf("BackfillMissing count = %d, want 1", count)
	}
	if got := vectors.embeddings[toEmbed.ID]; len(got) != 2 || got[0] != 0.9 || got[1] != 0.1 {
		t.Fatalf("new embedding = %#v, want stored vector", got)
	}
	if _, ok := vectors.embeddings[empty.ID]; ok {
		t.Fatalf("empty episodic record should not be counted or stored")
	}
	if _, ok := vectors.embeddings[failing.ID]; ok {
		t.Fatalf("failing record should not be stored")
	}
	if len(client.texts) != 2 {
		t.Fatalf("embedded texts = %#v, want only missing embeddable and failing records", client.texts)
	}
}

func TestBackfillMissingReturnsLookupErrors(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	rec := semanticRecord("lookup-error", "value")
	mustCreate(t, ctx, store, rec)

	vectors := newFakeVectorStore()
	vectors.getErrs[rec.ID] = errors.New("vector lookup failed")
	svc := NewService(&fakeEmbeddingClient{}, store, vectors, "model")

	count, err := svc.BackfillMissing(ctx)
	if err == nil || !strings.Contains(err.Error(), "check embedding for lookup-error") {
		t.Fatalf("BackfillMissing error = %v, want lookup error", err)
	}
	if count != 0 {
		t.Fatalf("BackfillMissing count = %d, want 0", count)
	}
}

func TestBackfillMissingGuardsAndListErrors(t *testing.T) {
	ctx := context.Background()
	if count, err := (*Service)(nil).BackfillMissing(ctx); count != 0 || err != nil {
		t.Fatalf("nil service BackfillMissing = %d/%v, want 0/nil", count, err)
	}
	if count, err := NewService(nil, nil, nil, "model").BackfillMissing(ctx); count != 0 || err != nil {
		t.Fatalf("missing dependencies BackfillMissing = %d/%v, want 0/nil", count, err)
	}

	store := newTestStore(t)
	if err := store.Close(); err != nil {
		t.Fatalf("Close store: %v", err)
	}
	count, err := NewService(&fakeEmbeddingClient{}, store, newFakeVectorStore(), "model").BackfillMissing(ctx)
	if err == nil || !strings.Contains(err.Error(), "list episodic for embedding backfill") {
		t.Fatalf("closed store BackfillMissing = %d/%v, want list episodic error", count, err)
	}
	if count != 0 {
		t.Fatalf("closed store BackfillMissing count = %d, want 0", count)
	}
}
