package metrics

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

type metricsStore struct {
	records []*schema.MemoryRecord
	err     error
}

func (s *metricsStore) Create(context.Context, *schema.MemoryRecord) error { return nil }
func (s *metricsStore) Get(context.Context, string) (*schema.MemoryRecord, error) {
	return nil, storage.ErrNotFound
}
func (s *metricsStore) Update(context.Context, *schema.MemoryRecord) error { return nil }
func (s *metricsStore) Delete(context.Context, string) error               { return nil }
func (s *metricsStore) List(context.Context, storage.ListOptions) ([]*schema.MemoryRecord, error) {
	if s.err != nil {
		return nil, s.err
	}
	return append([]*schema.MemoryRecord(nil), s.records...), nil
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

func TestCollectComputesSnapshot(t *testing.T) {
	now := time.Now().UTC()
	store := &metricsStore{records: []*schema.MemoryRecord{
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
	}}

	snapshot, err := NewCollector(store).Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if snapshot.TotalRecords != 5 {
		t.Fatalf("TotalRecords = %d, want 5", snapshot.TotalRecords)
	}
	if snapshot.RecordsByType[string(schema.MemoryTypeSemantic)] != 1 ||
		snapshot.RecordsByType[string(schema.MemoryTypeCompetence)] != 1 ||
		snapshot.RecordsByType[string(schema.MemoryTypePlanGraph)] != 1 {
		t.Fatalf("RecordsByType = %#v, want counts by type", snapshot.RecordsByType)
	}
	if snapshot.ActiveRecords != 5 {
		t.Fatalf("ActiveRecords = %d, want 5", snapshot.ActiveRecords)
	}
	if snapshot.PinnedRecords != 1 {
		t.Fatalf("PinnedRecords = %d, want 1", snapshot.PinnedRecords)
	}
	if snapshot.TotalAuditEntries != 3 {
		t.Fatalf("TotalAuditEntries = %d, want 3", snapshot.TotalAuditEntries)
	}
	if snapshot.SalienceDistribution["0.0-0.2"] != 1 ||
		snapshot.SalienceDistribution["0.2-0.4"] != 1 ||
		snapshot.SalienceDistribution["0.4-0.6"] != 1 ||
		snapshot.SalienceDistribution["0.6-0.8"] != 1 ||
		snapshot.SalienceDistribution["0.8-1.0"] != 1 {
		t.Fatalf("SalienceDistribution = %#v, want one record per bucket", snapshot.SalienceDistribution)
	}
	if !nearlyEqual(snapshot.AvgSalience, 0.5) {
		t.Fatalf("AvgSalience = %.3f, want 0.5", snapshot.AvgSalience)
	}
	if !nearlyEqual(snapshot.AvgConfidence, 0.6) {
		t.Fatalf("AvgConfidence = %.3f, want 0.6", snapshot.AvgConfidence)
	}
	if !nearlyEqual(snapshot.MemoryGrowthRate, 0.4) {
		t.Fatalf("MemoryGrowthRate = %.3f, want 0.4", snapshot.MemoryGrowthRate)
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
}

func TestCollectEmptyStore(t *testing.T) {
	snapshot, err := NewCollector(&metricsStore{}).Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect empty: %v", err)
	}
	if snapshot.TotalRecords != 0 || snapshot.AvgSalience != 0 || snapshot.MemoryGrowthRate != 0 {
		t.Fatalf("empty snapshot = %+v, want zero-valued aggregate metrics", snapshot)
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

func nearlyEqual(a, b float64) bool {
	return math.Abs(a-b) < 0.0001
}
