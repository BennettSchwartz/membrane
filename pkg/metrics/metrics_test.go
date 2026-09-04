package metrics

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/retrieval"
	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

type metricsStore struct {
	records        []*schema.MemoryRecord
	err            error
	embeddingStats storage.EmbeddingStats
	embeddingErr   error
	listCalls      int
}

func (s *metricsStore) Create(context.Context, *schema.MemoryRecord) error { return nil }
func (s *metricsStore) Get(context.Context, string) (*schema.MemoryRecord, error) {
	return nil, storage.ErrNotFound
}
func (s *metricsStore) Update(context.Context, *schema.MemoryRecord) error { return nil }
func (s *metricsStore) Delete(context.Context, string) error               { return nil }
func (s *metricsStore) List(context.Context, storage.ListOptions) ([]*schema.MemoryRecord, error) {
	s.listCalls++
	if s.err != nil {
		return nil, s.err
	}
	return append([]*schema.MemoryRecord(nil), s.records...), nil
}

type aggregateMetricsStore struct {
	*metricsStore
	aggregate storage.MetricsAggregate
	filters   []storage.MetricsFilter
}

func (s *aggregateMetricsStore) AggregateMetrics(_ context.Context, filter storage.MetricsFilter) (storage.MetricsAggregate, error) {
	s.filters = append(s.filters, filter)
	return s.aggregate, nil
}

func TestCollectForTrustUsesPolicyFilteredStorageAggregation(t *testing.T) {
	store := &aggregateMetricsStore{
		metricsStore: &metricsStore{},
		aggregate: storage.MetricsAggregate{
			TotalRecords:      2,
			RecordsByType:     map[string]int{string(schema.MemoryTypeSemantic): 2},
			AvgSalience:       0.75,
			EmbeddedRecords:   1,
			EmbeddingModel:    "filtered-model",
			EmbeddingCoverage: 0.5,
		},
	}
	trust := retrieval.NewTrustContext(schema.SensitivityLow, true, "grpc", []string{"project:alpha"})

	snapshot, err := NewCollector(store).CollectForTrust(context.Background(), trust)
	if err != nil {
		t.Fatalf("CollectForTrust: %v", err)
	}
	if store.listCalls != 0 {
		t.Fatalf("record hydration calls = %d, want 0 when aggregate provider is available", store.listCalls)
	}
	if len(store.filters) != 1 {
		t.Fatalf("aggregate filters = %+v, want one", store.filters)
	}
	filter := store.filters[0]
	if filter.MaxSensitivity != schema.SensitivityLow || !filter.IncludeUnscoped || !reflect.DeepEqual(filter.Scopes, []string{"project:alpha"}) {
		t.Fatalf("aggregate filter = %+v, want low/project:alpha plus unscoped", filter)
	}
	if snapshot.TotalRecords != 2 || snapshot.RecordsByType[string(schema.MemoryTypeSemantic)] != 2 || snapshot.AvgSalience != 0.75 {
		t.Fatalf("snapshot = %+v, want provider aggregate", snapshot)
	}
}

func TestCollectForTrustRejectsNilTrust(t *testing.T) {
	if _, err := NewCollector(&metricsStore{}).CollectForTrust(context.Background(), nil); !errors.Is(err, retrieval.ErrNilTrust) {
		t.Fatalf("CollectForTrust nil error = %v, want ErrNilTrust", err)
	}
}
func (s *metricsStore) ListByType(context.Context, schema.MemoryType) ([]*schema.MemoryRecord, error) {
	return nil, nil
}
func (s *metricsStore) UpdateSalience(context.Context, string, float64) error { return nil }
func (s *metricsStore) AddAuditEntry(context.Context, string, schema.AuditEntry) error {
	return nil
}
func (s *metricsStore) AddRelation(context.Context, string, schema.Relation) error { return nil }
func (s *metricsStore) GetRelations(context.Context, string) ([]schema.Relation, error) {
	return nil, nil
}
func (s *metricsStore) Begin(context.Context) (storage.Transaction, error) { return nil, nil }
func (s *metricsStore) Close() error                                       { return nil }

type embeddingMetricsStore struct {
	*metricsStore
}

func (s *embeddingMetricsStore) EmbeddingStats(context.Context) (storage.EmbeddingStats, error) {
	if s.embeddingErr != nil {
		return storage.EmbeddingStats{}, s.embeddingErr
	}
	return s.embeddingStats, nil
}

func TestCollectComputesSnapshot(t *testing.T) {
	now := time.Now().UTC()
	store := &embeddingMetricsStore{metricsStore: &metricsStore{
		embeddingStats: storage.EmbeddingStats{Model: "text-embedding-current", TotalRecords: 6, EmbeddedRecords: 4},
		records: []*schema.MemoryRecord{
			{
				ID:          "semantic-1",
				Type:        schema.MemoryTypeSemantic,
				Sensitivity: schema.SensitivityLow,
				Confidence:  0.8,
				Salience:    0.1,
				CreatedAt:   now.Add(-2 * time.Hour),
				Lifecycle:   schema.Lifecycle{Pinned: true},
				AuditLog: []schema.AuditEntry{
					{Action: schema.AuditActionReinforce},
					{Action: schema.AuditActionRevise},
				},
				Payload: &schema.SemanticPayload{
					Kind:      "semantic",
					Subject:   "Go",
					Predicate: "is",
					Object:    "typed",
					Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
				},
			},
			{
				ID:         "competence-1",
				Type:       schema.MemoryTypeCompetence,
				Confidence: 0.6,
				Salience:   0.3,
				CreatedAt:  now.Add(-48 * time.Hour),
				AuditLog:   []schema.AuditEntry{{Action: schema.AuditActionFork}},
				Payload: &schema.CompetencePayload{
					Kind:      "competence",
					SkillName: "debug",
					Triggers:  []schema.Trigger{{Signal: "panic"}},
					Recipe:    []schema.RecipeStep{{Step: "read logs"}},
					Performance: &schema.PerformanceStats{
						SuccessRate: 0.75,
					},
				},
			},
			{
				ID:         "plan-1",
				Type:       schema.MemoryTypePlanGraph,
				Confidence: 1.0,
				Salience:   0.5,
				CreatedAt:  now.Add(-30 * time.Minute),
				Payload: &schema.PlanGraphPayload{
					Kind:    "plan_graph",
					PlanID:  "plan-1",
					Version: "1",
					Nodes:   []schema.PlanNode{{ID: "n1", Op: "test"}},
					Edges:   []schema.PlanEdge{},
					Metrics: &schema.PlanMetrics{ExecutionCount: 4},
				},
			},
			{
				ID:         "entity-1",
				Type:       schema.MemoryTypeEntity,
				Confidence: 0.6,
				Salience:   0.5,
				CreatedAt:  now.Add(-1 * time.Hour),
				Payload: &schema.EntityPayload{
					Kind:          "entity",
					CanonicalName: "Project Orchid",
					PrimaryType:   schema.EntityTypeProject,
				},
			},
			{
				ID:         "working-1",
				Type:       schema.MemoryTypeWorking,
				Confidence: 0.4,
				Salience:   0.7,
				CreatedAt:  now.Add(-25 * time.Hour),
				Payload:    &schema.WorkingPayload{Kind: "working", ThreadID: "thread", State: schema.TaskStateExecuting},
			},
			{
				ID:         "episode-1",
				Type:       schema.MemoryTypeEpisodic,
				Confidence: 0.2,
				Salience:   0.9,
				CreatedAt:  now.Add(-72 * time.Hour),
				Payload:    &schema.EpisodicPayload{Kind: "episodic"},
			},
		}}}

	snapshot, err := NewCollector(store).Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if snapshot.TotalRecords != 6 {
		t.Fatalf("TotalRecords = %d, want 6", snapshot.TotalRecords)
	}
	if snapshot.RecordsByType[string(schema.MemoryTypeSemantic)] != 1 ||
		snapshot.RecordsByType[string(schema.MemoryTypeEntity)] != 1 ||
		snapshot.RecordsByType[string(schema.MemoryTypeCompetence)] != 1 ||
		snapshot.RecordsByType[string(schema.MemoryTypePlanGraph)] != 1 {
		t.Fatalf("RecordsByType = %#v, want counts by type", snapshot.RecordsByType)
	}
	if len(snapshot.RecordsByType) != 6 {
		t.Fatalf("RecordsByType keys = %#v, want all six memory types", snapshot.RecordsByType)
	}
	if snapshot.ActiveRecords != 6 {
		t.Fatalf("ActiveRecords = %d, want 6", snapshot.ActiveRecords)
	}
	if snapshot.PinnedRecords != 1 {
		t.Fatalf("PinnedRecords = %d, want 1", snapshot.PinnedRecords)
	}
	if snapshot.TotalAuditEntries != 3 {
		t.Fatalf("TotalAuditEntries = %d, want 3", snapshot.TotalAuditEntries)
	}
	if snapshot.SalienceDistribution["0.0-0.2"] != 1 ||
		snapshot.SalienceDistribution["0.2-0.4"] != 1 ||
		snapshot.SalienceDistribution["0.4-0.6"] != 2 ||
		snapshot.SalienceDistribution["0.6-0.8"] != 1 ||
		snapshot.SalienceDistribution["0.8-1.0"] != 1 {
		t.Fatalf("SalienceDistribution = %#v, want expected bucket counts", snapshot.SalienceDistribution)
	}
	if !nearlyEqual(snapshot.AvgSalience, 0.5) {
		t.Fatalf("AvgSalience = %.3f, want 0.5", snapshot.AvgSalience)
	}
	if !nearlyEqual(snapshot.AvgConfidence, 0.6) {
		t.Fatalf("AvgConfidence = %.3f, want 0.6", snapshot.AvgConfidence)
	}
	if !nearlyEqual(snapshot.MemoryGrowthRate, 0.5) {
		t.Fatalf("MemoryGrowthRate = %.3f, want 0.5", snapshot.MemoryGrowthRate)
	}
	if !nearlyEqual(snapshot.RetrievalUsefulness, 1.0/3.0) {
		t.Fatalf("RetrievalUsefulness = %.3f, want 1/3", snapshot.RetrievalUsefulness)
	}
	if !nearlyEqual(snapshot.RevisionRate, 2.0/3.0) {
		t.Fatalf("RevisionRate = %.3f, want 2/3", snapshot.RevisionRate)
	}
	if snapshot.CompetenceSuccessRate != 0.75 {
		t.Fatalf("CompetenceSuccessRate = %.2f, want 0.75", snapshot.CompetenceSuccessRate)
	}
	if snapshot.PlanReuseFrequency != 4 {
		t.Fatalf("PlanReuseFrequency = %.2f, want 4", snapshot.PlanReuseFrequency)
	}
	if snapshot.EmbeddingModel != "text-embedding-current" {
		t.Fatalf("EmbeddingModel = %q, want configured model", snapshot.EmbeddingModel)
	}
	if snapshot.EmbeddedRecords != 4 || snapshot.MissingEmbeddings != 2 || !nearlyEqual(snapshot.EmbeddingCoverage, 4.0/6.0) {
		t.Fatalf("embedding stats = model:%q embedded:%d missing:%d coverage:%.3f, want 4/2/0.667", snapshot.EmbeddingModel, snapshot.EmbeddedRecords, snapshot.MissingEmbeddings, snapshot.EmbeddingCoverage)
	}
}

func TestCollectEmptyStore(t *testing.T) {
	snapshot, err := NewCollector(&metricsStore{}).Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect empty: %v", err)
	}
	if snapshot.TotalRecords != 0 || snapshot.AvgSalience != 0 || snapshot.MemoryGrowthRate != 0 {
		t.Fatalf("empty snapshot = %+v, want zero-valued aggregate metrics", snapshot)
	}
	if len(snapshot.RecordsByType) != 6 || snapshot.RecordsByType[string(schema.MemoryTypeEntity)] != 0 {
		t.Fatalf("empty RecordsByType = %#v, want zero counts for all memory types", snapshot.RecordsByType)
	}
	if len(snapshot.SalienceDistribution) != 5 {
		t.Fatalf("empty SalienceDistribution = %#v, want initialized buckets", snapshot.SalienceDistribution)
	}
}

func TestCollectPropagatesListError(t *testing.T) {
	want := errors.New("list failed")
	_, err := NewCollector(&metricsStore{err: want}).Collect(context.Background())
	if err == nil || !errors.Is(err, want) {
		t.Fatalf("Collect error = %v, want wrapping %v", err, want)
	}
}

func TestCollectPropagatesEmbeddingStatsError(t *testing.T) {
	want := errors.New("embedding stats failed")
	_, err := NewCollector(&embeddingMetricsStore{metricsStore: &metricsStore{embeddingErr: want}}).Collect(context.Background())
	if err == nil || !errors.Is(err, want) {
		t.Fatalf("Collect embedding stats error = %v, want wrapping %v", err, want)
	}
}

func TestNormalizeEmbeddingStats(t *testing.T) {
	for _, tc := range []struct {
		name         string
		stats        storage.EmbeddingStats
		wantEmbedded int
		wantMissing  int
		wantCoverage float64
		wantNearly   bool
	}{
		{
			name:         "normal",
			stats:        storage.EmbeddingStats{TotalRecords: 6, EmbeddedRecords: 4},
			wantEmbedded: 4,
			wantMissing:  2,
			wantCoverage: 4.0 / 6.0,
			wantNearly:   true,
		},
		{
			name:         "embedded above total",
			stats:        storage.EmbeddingStats{TotalRecords: 3, EmbeddedRecords: 8},
			wantEmbedded: 3,
			wantMissing:  0,
			wantCoverage: 1,
		},
		{
			name:         "negative embedded",
			stats:        storage.EmbeddingStats{TotalRecords: 3, EmbeddedRecords: -2},
			wantEmbedded: 0,
			wantMissing:  3,
			wantCoverage: 0,
		},
		{
			name:         "negative total",
			stats:        storage.EmbeddingStats{TotalRecords: -1, EmbeddedRecords: 2},
			wantEmbedded: 0,
			wantMissing:  0,
			wantCoverage: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			embedded, missing, coverage := normalizeEmbeddingStats(tc.stats)
			if embedded != tc.wantEmbedded || missing != tc.wantMissing {
				t.Fatalf("normalizeEmbeddingStats = embedded:%d missing:%d coverage:%.3f, want embedded:%d missing:%d coverage:%.3f", embedded, missing, coverage, tc.wantEmbedded, tc.wantMissing, tc.wantCoverage)
			}
			if tc.wantNearly {
				if !nearlyEqual(coverage, tc.wantCoverage) {
					t.Fatalf("coverage = %.3f, want %.3f", coverage, tc.wantCoverage)
				}
				return
			}
			if coverage != tc.wantCoverage {
				t.Fatalf("coverage = %.3f, want %.3f", coverage, tc.wantCoverage)
			}
		})
	}
}

func nearlyEqual(a, b float64) bool {
	return math.Abs(a-b) < 0.0001
}
