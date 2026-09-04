package retrieval

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

type fakeEmbeddingService struct {
	vector []float32
	err    error
	calls  int
}

func (f *fakeEmbeddingService) EmbedQuery(_ context.Context, _ string) ([]float32, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
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

type fakeCandidateVectorRanker struct {
	fakeVectorRanker
	candidateIDs    [][]string
	candidateLimits []int
}

func (f *fakeCandidateVectorRanker) SearchByEmbeddingCandidates(_ context.Context, query []float32, recordIDs []string, limit int) ([]string, error) {
	f.queries = append(f.queries, append([]float32(nil), query...))
	f.candidateIDs = append(f.candidateIDs, append([]string(nil), recordIDs...))
	f.candidateLimits = append(f.candidateLimits, limit)
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

type failingBoundedRetrievalStore struct {
	*failingRetrievalStore
}

func (s *failingBoundedRetrievalStore) ListBounded(context.Context, storage.ListOptions) (storage.BoundedListResult, error) {
	return storage.BoundedListResult{}, s.listErr
}

func (s *failingRetrievalStore) Get(context.Context, string) (*schema.MemoryRecord, error) {
	return nil, s.getErr
}

func (s *failingRetrievalStore) List(context.Context, storage.ListOptions) ([]*schema.MemoryRecord, error) {
	return nil, s.listErr
}

func (s *failingRetrievalStore) ListByType(context.Context, schema.MemoryType) ([]*schema.MemoryRecord, error) {
	return nil, s.listErr
}

type boundedRetrievalStore struct {
	storage.Store
	records         []*schema.MemoryRecord
	listOptions     []storage.ListOptions
	listByTypeCalls int
}

func TestRetrieveFailsClosedWithoutBoundedListStore(t *testing.T) {
	legacyErr := errors.New("legacy List must not be called")
	store := &failingRetrievalStore{listErr: legacyErr}
	_, err := NewService(store, nil).Retrieve(context.Background(), &RetrieveRequest{
		Trust: NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		Limit: 1,
	})
	if !errors.Is(err, ErrBoundedRetrievalUnsupported) {
		t.Fatalf("Retrieve error = %v, want ErrBoundedRetrievalUnsupported", err)
	}
}

func (s *boundedRetrievalStore) List(_ context.Context, opts storage.ListOptions) ([]*schema.MemoryRecord, error) {
	return nil, errors.New("legacy List must not be used for bounded retrieval")
}

func (s *boundedRetrievalStore) ListBounded(_ context.Context, opts storage.ListOptions) (storage.BoundedListResult, error) {
	s.listOptions = append(s.listOptions, opts)
	return storage.BoundedListResult{Records: append([]*schema.MemoryRecord(nil), s.records...)}, nil
}

func (s *boundedRetrievalStore) ListByType(context.Context, schema.MemoryType) ([]*schema.MemoryRecord, error) {
	s.listByTypeCalls++
	return nil, errors.New("unbounded ListByType must not be used for a bounded retrieval")
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

func TestRetrievePushesBoundedCandidateWorkIntoStorage(t *testing.T) {
	input := newSemanticRetrievalRecord("bounded-result", 0.8, schema.SensitivityLow)
	input.Relations = []schema.Relation{{Predicate: "related", TargetID: "neighbor"}}
	input.AuditLog = []schema.AuditEntry{{Action: schema.AuditActionReinforce, Actor: "fixture"}}
	input.Provenance.Sources = []schema.ProvenanceSource{{Kind: schema.ProvenanceKindObservation, Ref: "fixture"}}
	store := &boundedRetrievalStore{records: []*schema.MemoryRecord{input}}
	svc := NewService(store, nil)

	resp, err := svc.Retrieve(context.Background(), &RetrieveRequest{
		Trust:       NewTrustContext(schema.SensitivityLow, true, "tester", []string{"project:alpha"}),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		MinSalience: 0.25,
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	if store.listByTypeCalls != 0 {
		t.Fatalf("ListByType calls = %d, want 0 for bounded retrieval", store.listByTypeCalls)
	}
	if len(store.listOptions) != 1 {
		t.Fatalf("List calls = %d, want one bounded storage query", len(store.listOptions))
	}
	if got := store.listOptions[0].Limit; got <= 0 || got > 10_000 {
		t.Fatalf("storage candidate limit = %d, want 1..10000", got)
	}
	if got := store.listOptions[0].MinSalience; got != 0.25 {
		t.Fatalf("storage min salience = %v, want 0.25", got)
	}
	if !store.listOptions[0].OmitRelations || !store.listOptions[0].OmitHistory {
		t.Fatalf("storage projection = %+v, want bounded relation/history omission", store.listOptions[0])
	}
	if !resp.Projection.RelationsOmitted || !resp.Projection.HistoryOmitted {
		t.Fatalf("response projection = %+v, want relation/history omission metadata", resp.Projection)
	}
	if got := idsOf(resp.Records); !reflect.DeepEqual(got, []string{"bounded-result"}) {
		t.Fatalf("record IDs = %v, want bounded result", got)
	}
	if got := resp.Records[0]; len(got.Relations) != 0 || len(got.AuditLog) != 0 || len(got.Provenance.Sources) != 0 {
		t.Fatalf("bounded record leaked store-ignored fields: relations=%d audit=%d provenance=%d", len(got.Relations), len(got.AuditLog), len(got.Provenance.Sources))
	}
	if len(input.Relations) != 1 || len(input.AuditLog) != 1 || len(input.Provenance.Sources) != 1 {
		t.Fatal("bounded response sanitization mutated the store-owned input record")
	}

	defaultLimited := &boundedRetrievalStore{records: store.records}
	if _, err := NewService(defaultLimited, nil).Retrieve(context.Background(), &RetrieveRequest{
		Trust: NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		Limit: 0,
	}); err != nil {
		t.Fatalf("Retrieve with default limit: %v", err)
	}
	if len(defaultLimited.listOptions) != 1 || defaultLimited.listOptions[0].Limit != maxRetrievalCandidates {
		t.Fatalf("default storage candidate limit = %+v, want %d", defaultLimited.listOptions, maxRetrievalCandidates)
	}
}

func TestRetrieveAppliesAggregateProjectedByteBudget(t *testing.T) {
	const payloadBytes = 6 << 20
	records := make([]*schema.MemoryRecord, 3)
	for i := range records {
		records[i] = newSemanticRetrievalRecord(fmt.Sprintf("byte-budget-%d", i), 1-float64(i)*0.1, schema.SensitivityLow)
		records[i].Payload.(*schema.SemanticPayload).Object = strings.Repeat(string(rune('a'+i)), payloadBytes)
	}
	store := &boundedRetrievalStore{records: records}
	resp, err := NewService(store, nil).Retrieve(context.Background(), &RetrieveRequest{
		Trust:       NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	if len(store.listOptions) != 1 || store.listOptions[0].MaxHydratedBytes != MaxProjectedResponseBytes {
		t.Fatalf("storage hydration byte budget = %+v, want %d", store.listOptions, MaxProjectedResponseBytes)
	}
	if got := idsOf(resp.Records); !reflect.DeepEqual(got, []string{"byte-budget-0", "byte-budget-1"}) {
		t.Fatalf("byte-budgeted IDs = %v, want deterministic two-record prefix", got)
	}
	if !resp.Projection.RecordsTruncated {
		t.Fatalf("projection = %+v, want records_truncated metadata", resp.Projection)
	}
	if diagnosticByCode(resp.Diagnostics, DiagnosticResponseByteLimitApplied) == nil {
		t.Fatalf("diagnostics = %+v, want response byte-limit diagnostic", resp.Diagnostics)
	}
}

func TestRetrieveCountsSelectionRecordCopiesAgainstResponseByteBudget(t *testing.T) {
	records := []*schema.MemoryRecord{
		competenceCandidate("selection-byte-0", 0.9, 9, 1),
		competenceCandidate("selection-byte-1", 0.8, 8, 2),
	}
	for i, record := range records {
		record.Payload.(*schema.CompetencePayload).SkillName = strings.Repeat(string(rune('a'+i)), 6<<20)
	}
	resp, err := NewService(&boundedRetrievalStore{records: records}, NewSelector(0.2)).Retrieve(context.Background(), &RetrieveRequest{
		TaskDescriptor: "select large competence",
		Trust:          NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes:    []schema.MemoryType{schema.MemoryTypeCompetence},
		Limit:          2,
	})
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	if len(resp.Records) != 2 {
		t.Fatalf("records = %d, want both primary response records", len(resp.Records))
	}
	if resp.Selection == nil || len(resp.Selection.Selected) != 0 || len(resp.Selection.Scores) != 0 {
		t.Fatalf("selection = %+v, want duplicate record copies removed by remaining byte budget", resp.Selection)
	}
	if !resp.Projection.RecordsTruncated || diagnosticByCode(resp.Diagnostics, DiagnosticResponseByteLimitApplied) == nil {
		t.Fatalf("projection/diagnostics = %+v / %+v, want truthful byte truncation metadata", resp.Projection, resp.Diagnostics)
	}
}

func TestRetrieveByteEstimatorIncludesNonPayloadRecordFields(t *testing.T) {
	records := make([]*schema.MemoryRecord, 3)
	for i := range records {
		records[i] = newSemanticRetrievalRecord(fmt.Sprintf("metadata-byte-%d", i), 1-float64(i)*0.1, schema.SensitivityLow)
		records[i].Tags = []string{strings.Repeat(string(rune('a'+i)), 6<<20)}
	}
	resp, err := NewService(&boundedRetrievalStore{records: records}, nil).Retrieve(context.Background(), &RetrieveRequest{
		Trust:       NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		Limit:       3,
	})
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	if got := idsOf(resp.Records); !reflect.DeepEqual(got, []string{"metadata-byte-0", "metadata-byte-1"}) {
		t.Fatalf("metadata-byte-bounded IDs = %v, want deterministic two-record prefix", got)
	}
	if !resp.Projection.RecordsTruncated || diagnosticByCode(resp.Diagnostics, DiagnosticResponseByteLimitApplied) == nil {
		t.Fatalf("projection/diagnostics = %+v / %+v, want metadata byte truncation", resp.Projection, resp.Diagnostics)
	}
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

func TestLegacyVectorRankerSearchWindowUsesHardCandidateCeiling(t *testing.T) {
	records := make([]*schema.MemoryRecord, 2_000)
	for i := range records {
		records[i] = newSemanticRetrievalRecord(fmt.Sprintf("legacy-vector-%04d", i), 0.5, schema.SensitivityLow)
	}
	ranker := &fakeVectorRanker{ids: []string{records[0].ID}}
	if _, diagnostic := rankByVector(context.Background(), records, ranker, []float32{1}, nil); diagnostic != nil {
		t.Fatalf("rankByVector diagnostic = %+v, want none", diagnostic)
	}
	if len(ranker.limits) != 1 || ranker.limits[0] != maxRetrievalCandidates {
		t.Fatalf("legacy vector search limit = %v, want hard ceiling %d", ranker.limits, maxRetrievalCandidates)
	}
}

func TestRetrieveUsesCandidateRestrictedVectorRankerWhenAvailable(t *testing.T) {
	_, store := newGraphTestService(t)
	ctx := context.Background()

	first := newSemanticRetrievalRecord("candidate-vector-first", 0.1, schema.SensitivityLow)
	second := newSemanticRetrievalRecord("candidate-vector-second", 0.9, schema.SensitivityLow)
	for _, rec := range []*schema.MemoryRecord{first, second} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	embedding := &fakeEmbeddingService{vector: []float32{4, 5, 6}}
	ranker := &fakeCandidateVectorRanker{fakeVectorRanker: fakeVectorRanker{ids: []string{first.ID, second.ID}}}
	svc := NewServiceWithVectorRanker(store, nil, embedding, ranker)

	resp, err := svc.Retrieve(ctx, &RetrieveRequest{
		TaskDescriptor: "candidate vector ranking",
		Trust:          NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes:    []schema.MemoryType{schema.MemoryTypeSemantic},
	})
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}

	if got := idsOf(resp.Records); !reflect.DeepEqual(got, []string{first.ID, second.ID}) {
		t.Fatalf("record order = %v, want candidate vector order", got)
	}
	if len(ranker.limits) != 0 {
		t.Fatalf("global vector search limits = %v, want candidate search only", ranker.limits)
	}
	if len(ranker.candidateLimits) != 1 || ranker.candidateLimits[0] != 2 {
		t.Fatalf("candidate search limits = %v, want [2]", ranker.candidateLimits)
	}
	if len(ranker.candidateIDs) != 1 || !reflect.DeepEqual(ranker.candidateIDs[0], []string{second.ID, first.ID}) {
		t.Fatalf("candidate IDs = %v, want salience-ordered candidate set", ranker.candidateIDs)
	}
}

func TestRetrieveRejectsNilRequest(t *testing.T) {
	svc, _ := newGraphTestService(t)

	if _, err := svc.Retrieve(context.Background(), nil); !errors.Is(err, ErrNilTrust) {
		t.Fatalf("Retrieve nil request error = %v, want ErrNilTrust", err)
	}
}

func TestMemoryTypeLayersCanonicalizesAndValidates(t *testing.T) {
	got, err := memoryTypeLayers([]schema.MemoryType{
		schema.MemoryTypeEpisodic,
		schema.MemoryTypeSemantic,
		schema.MemoryTypeSemantic,
		schema.MemoryTypeEntity,
	})
	if err != nil {
		t.Fatalf("memoryTypeLayers: %v", err)
	}
	want := []schema.MemoryType{schema.MemoryTypeEntity, schema.MemoryTypeSemantic, schema.MemoryTypeEpisodic}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("layers = %v, want canonical de-duplicated order %v", got, want)
	}

	if got, err := memoryTypeLayers(nil); err != nil || !reflect.DeepEqual(got, layerOrder) {
		t.Fatalf("default layers = %v, %v; want layerOrder", got, err)
	}

	if _, err := memoryTypeLayers([]schema.MemoryType{"unknown"}); err == nil || !strings.Contains(err.Error(), "invalid memory type") {
		t.Fatalf("invalid memory type error = %v, want invalid memory type", err)
	}
}

func TestRetrieveDeduplicatesMemoryTypes(t *testing.T) {
	_, store := newGraphTestService(t)
	ctx := context.Background()

	rec := newSemanticRetrievalRecord("semantic-once", 0.7, schema.SensitivityLow)
	if err := store.Create(ctx, rec); err != nil {
		t.Fatalf("Create: %v", err)
	}

	svc := NewService(store, nil)
	resp, err := svc.Retrieve(ctx, &RetrieveRequest{
		Trust:       NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic, schema.MemoryTypeSemantic},
	})
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	if got := idsOf(resp.Records); !reflect.DeepEqual(got, []string{rec.ID}) {
		t.Fatalf("records = %v, want one semantic record despite duplicate filters", got)
	}

	if _, err := svc.Retrieve(ctx, &RetrieveRequest{
		Trust:       NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes: []schema.MemoryType{"unknown"},
	}); err == nil || !strings.Contains(err.Error(), "invalid memory type") {
		t.Fatalf("invalid memory type retrieve error = %v, want invalid memory type", err)
	}
}

func TestRetrievePropagatesStoreErrors(t *testing.T) {
	ctx := context.Background()
	listErr := errors.New("list failed")
	svc := NewService(&failingBoundedRetrievalStore{failingRetrievalStore: &failingRetrievalStore{listErr: listErr}}, nil)
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

func TestRetrieveRejectsInvalidPrecomputedQueryEmbedding(t *testing.T) {
	_, store := newGraphTestService(t)
	ctx := context.Background()

	rec := newSemanticRetrievalRecord("invalid-precomputed-vector", 0.7, schema.SensitivityLow)
	if err := store.Create(ctx, rec); err != nil {
		t.Fatalf("Create: %v", err)
	}
	ranker := &fakeVectorRanker{ids: []string{rec.ID}}
	svc := NewServiceWithVectorRanker(store, nil, nil, ranker)

	for _, queryEmbedding := range [][]float32{
		{0, 0, 0},
		{1, float32(math.NaN()), 0},
		{1, float32(math.Inf(1)), 0},
	} {
		if _, err := svc.Retrieve(ctx, &RetrieveRequest{
			QueryEmbedding: queryEmbedding,
			Trust:          NewTrustContext(schema.SensitivityLow, true, "tester", nil),
			MemoryTypes:    []schema.MemoryType{schema.MemoryTypeSemantic},
		}); err == nil || !strings.Contains(err.Error(), "query_embedding") {
			t.Fatalf("Retrieve invalid precomputed embedding %v error = %v, want query_embedding validation error", queryEmbedding, err)
		}
	}
	if len(ranker.queries) != 0 {
		t.Fatalf("ranker queries = %v, want no vector search for invalid precomputed embeddings", ranker.queries)
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
	if len(resp.Diagnostics) != 1 {
		t.Fatalf("diagnostics = %+v, want one vector-rank failure", resp.Diagnostics)
	}
	if resp.Diagnostics[0].Code != DiagnosticVectorRankFailed {
		t.Fatalf("diagnostic code = %q, want %q", resp.Diagnostics[0].Code, DiagnosticVectorRankFailed)
	}
	if resp.Diagnostics[0].Message == "" {
		t.Fatal("diagnostic message is empty, want vector error detail")
	}
}

func TestRetrieveEmbeddingErrorFallsBackToSalienceWithDiagnostic(t *testing.T) {
	_, store := newGraphTestService(t)
	ctx := context.Background()

	lowSalience := newSemanticRetrievalRecord("embedding-low-salience", 0.1, schema.SensitivityLow)
	highSalience := newSemanticRetrievalRecord("embedding-high-salience", 0.9, schema.SensitivityLow)
	for _, rec := range []*schema.MemoryRecord{lowSalience, highSalience} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	embedding := &fakeEmbeddingService{err: errors.New("embedding service unavailable")}
	svc := NewServiceWithEmbedding(store, nil, embedding)
	resp, err := svc.Retrieve(ctx, &RetrieveRequest{
		TaskDescriptor: "fallback when embedding is down",
		Trust:          NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes:    []schema.MemoryType{schema.MemoryTypeSemantic},
	})
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	if embedding.calls != 1 {
		t.Fatalf("EmbedQuery calls = %d, want 1", embedding.calls)
	}
	if got := idsOf(resp.Records); !reflect.DeepEqual(got, []string{highSalience.ID, lowSalience.ID}) {
		t.Fatalf("record order = %v, want salience fallback order", got)
	}
	if len(resp.Diagnostics) != 1 {
		t.Fatalf("diagnostics = %+v, want one embedding failure", resp.Diagnostics)
	}
	if resp.Diagnostics[0].Code != DiagnosticEmbeddingQueryFailed {
		t.Fatalf("diagnostic code = %q, want %q", resp.Diagnostics[0].Code, DiagnosticEmbeddingQueryFailed)
	}
	if resp.Diagnostics[0].Message == "" {
		t.Fatal("diagnostic message is empty, want embedding error detail")
	}
}

func TestRetrieveInvalidGeneratedQueryEmbeddingFallsBackWithDiagnostic(t *testing.T) {
	_, store := newGraphTestService(t)
	ctx := context.Background()

	lowSalience := newSemanticRetrievalRecord("generated-low-salience", 0.1, schema.SensitivityLow)
	highSalience := newSemanticRetrievalRecord("generated-high-salience", 0.9, schema.SensitivityLow)
	for _, rec := range []*schema.MemoryRecord{lowSalience, highSalience} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	embedding := &fakeEmbeddingService{vector: []float32{0, 0, 0}}
	ranker := &fakeVectorRanker{ids: []string{lowSalience.ID, highSalience.ID}}
	svc := NewServiceWithVectorRanker(store, nil, embedding, ranker)
	resp, err := svc.Retrieve(ctx, &RetrieveRequest{
		TaskDescriptor: "fallback when embedding is invalid",
		Trust:          NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes:    []schema.MemoryType{schema.MemoryTypeSemantic},
	})
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	if got := idsOf(resp.Records); !reflect.DeepEqual(got, []string{highSalience.ID, lowSalience.ID}) {
		t.Fatalf("record order = %v, want salience fallback order", got)
	}
	if len(resp.Diagnostics) != 1 || resp.Diagnostics[0].Code != DiagnosticEmbeddingQueryFailed {
		t.Fatalf("diagnostics = %+v, want embedding_query_failed", resp.Diagnostics)
	}
	if len(ranker.queries) != 0 {
		t.Fatalf("ranker queries = %v, want no vector search for invalid generated embedding", ranker.queries)
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
	if resp.Selection == nil || len(resp.Selection.Selected) != 1 || resp.Selection.Selected[0].ID != strong.ID {
		t.Fatalf("selection = %+v, want the one response-bounded selected candidate", resp.Selection)
	}
	if len(resp.Selection.Scores) != 1 {
		t.Fatalf("selection scores = %+v, want one response-bounded score", resp.Selection.Scores)
	}
	if _, ok := resp.Selection.Scores[weak.ID]; ok {
		t.Fatalf("selection scores = %+v, want omitted candidate %q removed", resp.Selection.Scores, weak.ID)
	}
}

func TestRetrieveRejectsInvalidMinSalienceThreshold(t *testing.T) {
	svc, _ := newGraphTestService(t)
	ctx := context.Background()

	for _, minSalience := range []float64{-0.1, math.NaN(), math.Inf(1), 1.1} {
		if _, err := svc.Retrieve(ctx, &RetrieveRequest{
			Trust:       NewTrustContext(schema.SensitivityLow, true, "tester", nil),
			MinSalience: minSalience,
		}); err == nil || !strings.Contains(err.Error(), "min_salience") {
			t.Fatalf("Retrieve MinSalience %v error = %v, want min_salience validation", minSalience, err)
		}
	}
}

func TestRankByVectorPromotesSelection(t *testing.T) {
	selected := &schema.MemoryRecord{ID: "selected-competence", Salience: 0.2}
	vectorBest := &schema.MemoryRecord{ID: "vector-best", Salience: 0.9}
	remainder := &schema.MemoryRecord{ID: "remainder", Salience: 0.5}
	ranker := &fakeVectorRanker{ids: []string{vectorBest.ID, selected.ID}}

	got, diagnostic := rankByVector(
		context.Background(),
		[]*schema.MemoryRecord{selected, vectorBest, remainder},
		ranker,
		[]float32{1},
		&SelectionResult{Selected: []*schema.MemoryRecord{selected}},
	)
	if diagnostic != nil {
		t.Fatalf("diagnostic = %+v, want nil", diagnostic)
	}

	if gotIDs := idsOf(got); !reflect.DeepEqual(gotIDs, []string{selected.ID, vectorBest.ID, remainder.ID}) {
		t.Fatalf("ranked IDs = %v, want selected record promoted before vector order", gotIDs)
	}
}

func TestRankByVectorScoresAfterFilteringExternalIDs(t *testing.T) {
	vectorBestLowSalience := &schema.MemoryRecord{ID: "vector-best-low-salience", Salience: 0.1}
	vectorSecondHighSalience := &schema.MemoryRecord{ID: "vector-second-high-salience", Salience: 0.9}
	rankedIDs := make([]string, 0, 102)
	for i := 0; i < 100; i++ {
		rankedIDs = append(rankedIDs, "external-"+string(rune('a'+i%26)))
	}
	rankedIDs = append(rankedIDs, vectorBestLowSalience.ID, vectorSecondHighSalience.ID)

	got, diagnostic := rankByVector(
		context.Background(),
		[]*schema.MemoryRecord{vectorSecondHighSalience, vectorBestLowSalience},
		&fakeVectorRanker{ids: rankedIDs},
		[]float32{1},
		nil,
	)
	if diagnostic != nil {
		t.Fatalf("diagnostic = %+v, want nil", diagnostic)
	}

	if gotIDs := idsOf(got); !reflect.DeepEqual(gotIDs, []string{vectorBestLowSalience.ID, vectorSecondHighSalience.ID}) {
		t.Fatalf("ranked IDs = %v, want filtered vector order to beat salience", gotIDs)
	}
}

func TestRankByVectorFallbacksAndSkipsExternalSelection(t *testing.T) {
	low := &schema.MemoryRecord{ID: "low", Salience: 0.1}
	high := &schema.MemoryRecord{ID: "high", Salience: 0.9}
	mid := &schema.MemoryRecord{ID: "mid", Salience: 0.5}

	fallback, diagnostic := rankByVector(context.Background(), []*schema.MemoryRecord{low, high}, &fakeVectorRanker{}, []float32{1}, nil)
	if diagnostic != nil {
		t.Fatalf("empty vector diagnostic = %+v, want nil", diagnostic)
	}
	if gotIDs := idsOf(fallback); !reflect.DeepEqual(gotIDs, []string{high.ID, low.ID}) {
		t.Fatalf("empty vector results = %v, want salience fallback order", gotIDs)
	}

	external := &schema.MemoryRecord{ID: "external-selection", Salience: 1}
	ranked, diagnostic := rankByVector(
		context.Background(),
		[]*schema.MemoryRecord{low, high, mid},
		&fakeVectorRanker{ids: []string{"outside", low.ID}},
		[]float32{1},
		&SelectionResult{Selected: []*schema.MemoryRecord{external, low, low}},
	)
	if diagnostic != nil {
		t.Fatalf("ranked diagnostic = %+v, want nil", diagnostic)
	}
	if gotIDs := idsOf(ranked); !reflect.DeepEqual(gotIDs, []string{low.ID, high.ID, mid.ID}) {
		t.Fatalf("external/duplicate selection ranked IDs = %v, want low then salience-sorted remainder", gotIDs)
	}

	if got, diagnostic := rankByVector(context.Background(), nil, &fakeVectorRanker{ids: []string{"unused"}}, []float32{1}, nil); got != nil || diagnostic != nil {
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
