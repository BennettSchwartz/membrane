package retrieval

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

type fakeEmbeddingService struct {
	vector []float32
	calls  int
}

func (f *fakeEmbeddingService) EmbedQuery(_ context.Context, _ string) ([]float32, error) {
	f.calls++
	return append([]float32(nil), f.vector...), nil
}

type fakeVectorRanker struct {
	ids     []string
	err     error
	queries [][]float32
	limits  []int
}

func (f *fakeVectorRanker) SearchByEmbedding(_ context.Context, query []float32, limit int) ([]string, error) {
	f.queries = append(f.queries, append([]float32(nil), query...))
	f.limits = append(f.limits, limit)
	if f.err != nil {
		return nil, f.err
	}
	return append([]string(nil), f.ids...), nil
}

type failingRetrievalStore struct {
	storage.Store
	getErr  error
	listErr error
}

func (s *failingRetrievalStore) Get(context.Context, string) (*schema.MemoryRecord, error) {
	return nil, s.getErr
}

func (s *failingRetrievalStore) ListByType(context.Context, schema.MemoryType) ([]*schema.MemoryRecord, error) {
	return nil, s.listErr
}

func newSemanticRetrievalRecord(id string, salience float64, sensitivity schema.Sensitivity) *schema.MemoryRecord {
	now := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	rec := schema.NewMemoryRecord(id, schema.MemoryTypeSemantic, sensitivity, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   id,
		Predicate: "is",
		Object:    "retrievable",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	rec.CreatedAt = now
	rec.UpdatedAt = now
	rec.Lifecycle.LastReinforcedAt = now
	rec.Salience = salience
	return rec
}

func TestRetrieveUsesVectorRankerForAllRecordTypes(t *testing.T) {
	_, store := newGraphTestService(t)
	ctx := context.Background()

	lowSalience := newSemanticRetrievalRecord("low-salience-vector-match", 0.1, schema.SensitivityLow)
	highSalience := newSemanticRetrievalRecord("high-salience-second", 0.9, schema.SensitivityLow)
	for _, rec := range []*schema.MemoryRecord{lowSalience, highSalience} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	embedding := &fakeEmbeddingService{vector: []float32{1, 2, 3}}
	ranker := &fakeVectorRanker{ids: []string{lowSalience.ID, highSalience.ID}}
	svc := NewServiceWithVectorRanker(store, nil, embedding, ranker)

	resp, err := svc.Retrieve(ctx, &RetrieveRequest{
		TaskDescriptor: "match by vector",
		Trust:          NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes:    []schema.MemoryType{schema.MemoryTypeSemantic},
	})
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}

	if got := idsOf(resp.Records); !reflect.DeepEqual(got, []string{lowSalience.ID, highSalience.ID}) {
		t.Fatalf("record order = %v, want vector order", got)
	}
	if embedding.calls != 1 {
		t.Fatalf("EmbedQuery calls = %d, want 1", embedding.calls)
	}
	if len(ranker.limits) != 1 || ranker.limits[0] != 500 {
		t.Fatalf("vector search limits = %v, want [500]", ranker.limits)
	}
}

func TestRetrieveRejectsNilRequest(t *testing.T) {
	svc, _ := newGraphTestService(t)

	if _, err := svc.Retrieve(context.Background(), nil); !errors.Is(err, ErrNilTrust) {
		t.Fatalf("Retrieve nil request error = %v, want ErrNilTrust", err)
	}
}

func TestRetrievePropagatesStoreErrors(t *testing.T) {
	ctx := context.Background()
	listErr := errors.New("list failed")
	svc := NewService(&failingRetrievalStore{listErr: listErr}, nil)
	if _, err := svc.Retrieve(ctx, &RetrieveRequest{
		Trust:       NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
	}); !errors.Is(err, listErr) {
		t.Fatalf("Retrieve list error = %v, want %v", err, listErr)
	}

	getErr := errors.New("get failed")
	svc = NewService(&failingRetrievalStore{getErr: getErr}, nil)
	if _, err := svc.RetrieveByID(ctx, "missing", NewTrustContext(schema.SensitivityLow, true, "tester", nil)); !errors.Is(err, getErr) {
		t.Fatalf("RetrieveByID get error = %v, want %v", err, getErr)
	}

	svc = NewService(&failingRetrievalStore{listErr: listErr}, nil)
	if _, err := svc.RetrieveByType(ctx, schema.MemoryTypeSemantic, NewTrustContext(schema.SensitivityLow, true, "tester", nil)); !errors.Is(err, listErr) {
		t.Fatalf("RetrieveByType list error = %v, want %v", err, listErr)
	}
}

func TestRetrieveUsesPrecomputedQueryEmbedding(t *testing.T) {
	_, store := newGraphTestService(t)
	ctx := context.Background()

	first := newSemanticRetrievalRecord("first", 0.1, schema.SensitivityLow)
	second := newSemanticRetrievalRecord("second", 0.9, schema.SensitivityLow)
	for _, rec := range []*schema.MemoryRecord{first, second} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	embedding := &fakeEmbeddingService{vector: []float32{9, 9}}
	ranker := &fakeVectorRanker{ids: []string{first.ID, second.ID}}
	svc := NewServiceWithVectorRanker(store, nil, embedding, ranker)

	resp, err := svc.Retrieve(ctx, &RetrieveRequest{
		QueryEmbedding: []float32{4, 5, 6},
		Trust:          NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes:    []schema.MemoryType{schema.MemoryTypeSemantic},
	})
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	if got := idsOf(resp.Records); !reflect.DeepEqual(got, []string{first.ID, second.ID}) {
		t.Fatalf("record order = %v, want vector order", got)
	}
	if embedding.calls != 0 {
		t.Fatalf("EmbedQuery calls = %d, want 0 for precomputed embedding", embedding.calls)
	}
	if len(ranker.queries) != 1 || !reflect.DeepEqual(ranker.queries[0], []float32{4, 5, 6}) {
		t.Fatalf("ranker queries = %#v, want precomputed embedding", ranker.queries)
	}
}

func TestRetrieveVectorRankerErrorFallsBackToSalience(t *testing.T) {
	_, store := newGraphTestService(t)
	ctx := context.Background()

	lowSalience := newSemanticRetrievalRecord("low-salience", 0.1, schema.SensitivityLow)
	highSalience := newSemanticRetrievalRecord("high-salience", 0.9, schema.SensitivityLow)
	for _, rec := range []*schema.MemoryRecord{lowSalience, highSalience} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	svc := NewServiceWithVectorRanker(
		store,
		nil,
		&fakeEmbeddingService{vector: []float32{1}},
		&fakeVectorRanker{err: errors.New("vector index unavailable")},
	)
	resp, err := svc.Retrieve(ctx, &RetrieveRequest{
		TaskDescriptor: "fallback",
		Trust:          NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes:    []schema.MemoryType{schema.MemoryTypeSemantic},
	})
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	if got := idsOf(resp.Records); !reflect.DeepEqual(got, []string{highSalience.ID, lowSalience.ID}) {
		t.Fatalf("record order = %v, want salience fallback order", got)
	}
}

func TestRetrieveAppliesMinSalienceSelectionRankingAndLimit(t *testing.T) {
	_, store := newGraphTestService(t)
	ctx := context.Background()

	strong := competenceCandidate("strong-competence", 0.9, 9, 1)
	strong.Salience = 0.7
	weak := competenceCandidate("weak-competence", 0.2, 1, 9)
	weak.Salience = 0.6
	filtered := competenceCandidate("filtered-competence", 1.0, 10, 0)
	filtered.Salience = 0.1
	for _, rec := range []*schema.MemoryRecord{weak, strong, filtered} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	svc := NewService(store, NewSelector(0.2))
	resp, err := svc.Retrieve(ctx, &RetrieveRequest{
		TaskDescriptor: "select competence",
		Trust:          NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes:    []schema.MemoryType{schema.MemoryTypeCompetence},
		MinSalience:    0.5,
		Limit:          1,
	})
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	if len(resp.Records) != 1 || resp.Records[0].ID != strong.ID {
		t.Fatalf("records = %v, want strong competence only", idsOf(resp.Records))
	}
	if resp.Selection == nil || len(resp.Selection.Selected) != 2 {
		t.Fatalf("selection = %+v, want two candidates after salience filter", resp.Selection)
	}
}

func TestRankByVectorPromotesSelection(t *testing.T) {
	selected := &schema.MemoryRecord{ID: "selected-competence", Salience: 0.2}
	vectorBest := &schema.MemoryRecord{ID: "vector-best", Salience: 0.9}
	remainder := &schema.MemoryRecord{ID: "remainder", Salience: 0.5}
	ranker := &fakeVectorRanker{ids: []string{vectorBest.ID, selected.ID}}

	got := rankByVector(
		context.Background(),
		[]*schema.MemoryRecord{selected, vectorBest, remainder},
		ranker,
		[]float32{1},
		&SelectionResult{Selected: []*schema.MemoryRecord{selected}},
	)

	if gotIDs := idsOf(got); !reflect.DeepEqual(gotIDs, []string{selected.ID, vectorBest.ID, remainder.ID}) {
		t.Fatalf("ranked IDs = %v, want selected record promoted before vector order", gotIDs)
	}
}

func TestRankByVectorFallbacksAndSkipsExternalSelection(t *testing.T) {
	low := &schema.MemoryRecord{ID: "low", Salience: 0.1}
	high := &schema.MemoryRecord{ID: "high", Salience: 0.9}
	mid := &schema.MemoryRecord{ID: "mid", Salience: 0.5}

	fallback := rankByVector(context.Background(), []*schema.MemoryRecord{low, high}, &fakeVectorRanker{}, []float32{1}, nil)
	if gotIDs := idsOf(fallback); !reflect.DeepEqual(gotIDs, []string{high.ID, low.ID}) {
		t.Fatalf("empty vector results = %v, want salience fallback order", gotIDs)
	}

	external := &schema.MemoryRecord{ID: "external-selection", Salience: 1}
	ranked := rankByVector(
		context.Background(),
		[]*schema.MemoryRecord{low, high, mid},
		&fakeVectorRanker{ids: []string{"outside", low.ID}},
		[]float32{1},
		&SelectionResult{Selected: []*schema.MemoryRecord{external, low, low}},
	)
	if gotIDs := idsOf(ranked); !reflect.DeepEqual(gotIDs, []string{low.ID, high.ID, mid.ID}) {
		t.Fatalf("external/duplicate selection ranked IDs = %v, want low then salience-sorted remainder", gotIDs)
	}

	if got := rankByVector(context.Background(), nil, &fakeVectorRanker{ids: []string{"unused"}}, []float32{1}, nil); got != nil {
		t.Fatalf("empty records rank = %#v, want nil", got)
	}
}

func TestRetrieveByTypeFiltersTrustAndSorts(t *testing.T) {
	svc, store := newGraphTestService(t)
	ctx := context.Background()

	low := newSemanticRetrievalRecord("low", 0.9, schema.SensitivityLow)
	redacted := newSemanticRetrievalRecord("redacted", 0.8, schema.SensitivityMedium)
	denied := newSemanticRetrievalRecord("denied", 1.0, schema.SensitivityHyper)
	for _, rec := range []*schema.MemoryRecord{low, redacted, denied} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	records, err := svc.RetrieveByType(ctx, schema.MemoryTypeSemantic, NewTrustContext(schema.SensitivityLow, true, "tester", nil))
	if err != nil {
		t.Fatalf("RetrieveByType: %v", err)
	}
	if got := idsOf(records); !reflect.DeepEqual(got, []string{low.ID, redacted.ID}) {
		t.Fatalf("records = %v, want accessible and redacted records sorted by salience", got)
	}
	if records[1].Payload != nil {
		t.Fatalf("redacted payload = %#v, want nil", records[1].Payload)
	}

	if _, err := svc.RetrieveByType(ctx, schema.MemoryTypeSemantic, nil); !errors.Is(err, ErrNilTrust) {
		t.Fatalf("RetrieveByType nil trust error = %v, want ErrNilTrust", err)
	}
}

func idsOf(records []*schema.MemoryRecord) []string {
	ids := make([]string, 0, len(records))
	for _, rec := range records {
		ids = append(ids, rec.ID)
	}
	return ids
}
