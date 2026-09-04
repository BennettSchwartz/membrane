package retrieval

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
)

type fakeSimilarityProvider struct {
	scores map[string]float64
}

func (p fakeSimilarityProvider) Similarity(_ context.Context, recordID string, _ []float32) (float64, bool) {
	score, ok := p.scores[recordID]
	return score, ok
}

func TestSelectorSelectScoresAndRanksCandidates(t *testing.T) {
	ctx := context.Background()
	selector := NewSelectorWithEmbedding(0.4, fakeSimilarityProvider{scores: map[string]float64{
		"strong": 0.9,
		"weak":   0.2,
	}})

	strong := competenceCandidate("strong", 0.1, 9, 1)
	strong.Lifecycle.LastReinforcedAt = time.Now().Add(time.Minute)
	weak := planCandidate("weak", 0.8, 0.6)
	weak.Lifecycle.LastReinforcedAt = time.Now().Add(-90 * 24 * time.Hour)
	noPayload := &schema.MemoryRecord{ID: "no-payload", Confidence: 1}

	result := selector.Select(ctx, []*schema.MemoryRecord{weak, noPayload, strong}, []float32{1, 2})
	if got := idsOf(result.Selected); !reflect.DeepEqual(got, []string{strong.ID, noPayload.ID, weak.ID}) {
		t.Fatalf("selected IDs = %v, want strong/no-payload/weak", got)
	}
	if result.Confidence <= 0 || result.NeedsMore {
		t.Fatalf("selection result = %+v, want confident selection", result)
	}
	if result.Scores[strong.ID] <= result.Scores[weak.ID] {
		t.Fatalf("scores = %+v, want strong above weak", result.Scores)
	}
}

func TestSelectorEmptySingleAndSuccessRateDefaults(t *testing.T) {
	selector := NewSelector(0.75)
	empty := selector.Select(context.Background(), nil, nil)
	if len(empty.Selected) != 0 || empty.Confidence != 0 || !empty.NeedsMore {
		t.Fatalf("empty selection = %+v, want no candidates with needs_more", empty)
	}

	single := competenceCandidate("single", 0.9, 1, 0)
	single.Lifecycle.LastReinforcedAt = time.Now().Add(-time.Minute)
	result := selector.Select(context.Background(), []*schema.MemoryRecord{single}, nil)
	if len(result.Selected) != 1 || result.Selected[0].ID != single.ID || result.Confidence <= 0 || result.NeedsMore {
		t.Fatalf("single selection = %+v, want confident single candidate", result)
	}
	overconfident := competenceCandidate("overconfident", 3, 1, 0)
	overconfident.Lifecycle.LastReinforcedAt = time.Now().Add(time.Minute)
	result = selector.Select(context.Background(), []*schema.MemoryRecord{overconfident}, nil)
	if result.Confidence != 1.0 {
		t.Fatalf("overconfident single confidence = %v, want clamp to 1.0", result.Confidence)
	}

	if got := selector.extractSuccessRate(&schema.MemoryRecord{Payload: &schema.CompetencePayload{Kind: "competence"}}); got != 0.5 {
		t.Fatalf("competence default success rate = %v, want 0.5", got)
	}
	if got := selector.extractSuccessRate(&schema.MemoryRecord{Payload: &schema.PlanGraphPayload{Kind: "plan_graph"}}); got != 0.5 {
		t.Fatalf("plan default success rate = %v, want 0.5", got)
	}
	if got := selector.extractSuccessRate(planCandidate("plan", 1, 0.25)); got != 0.75 {
		t.Fatalf("plan success rate = %v, want 0.75", got)
	}
}

func TestSelectorBreaksScoreTiesByFreshnessThenID(t *testing.T) {
	selector := NewSelector(0.2)
	older := competenceCandidate("a-older", 0.5, 1, 1)
	older.UpdatedAt = time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	older.Lifecycle.LastReinforcedAt = time.Time{}
	fresherB := competenceCandidate("b-fresher", 0.5, 1, 1)
	fresherB.UpdatedAt = time.Date(2026, 5, 1, 11, 0, 0, 0, time.UTC)
	fresherB.Lifecycle.LastReinforcedAt = time.Time{}
	fresherA := competenceCandidate("a-fresher", 0.5, 1, 1)
	fresherA.UpdatedAt = fresherB.UpdatedAt
	fresherA.Lifecycle.LastReinforcedAt = time.Time{}

	result := selector.Select(context.Background(), []*schema.MemoryRecord{older, fresherB, fresherA}, nil)

	if got := idsOf(result.Selected); !reflect.DeepEqual(got, []string{"a-fresher", "b-fresher", "a-older"}) {
		t.Fatalf("selected IDs = %v, want freshness tie-breaker then ID", got)
	}
}

func TestRankRecordsWithSelectionPromotesSelectedThenSalienceSortsRemainder(t *testing.T) {
	selected := &schema.MemoryRecord{ID: "selected", Salience: 0.1}
	high := &schema.MemoryRecord{ID: "high", Salience: 0.9}
	low := &schema.MemoryRecord{ID: "low", Salience: 0.2}

	got := rankRecordsWithSelection(
		[]*schema.MemoryRecord{low, high, selected},
		&SelectionResult{Selected: []*schema.MemoryRecord{selected}},
	)
	if gotIDs := idsOf(got); !reflect.DeepEqual(gotIDs, []string{selected.ID, high.ID, low.ID}) {
		t.Fatalf("ranked IDs = %v, want selected/high/low", gotIDs)
	}
}

func TestRetrieveByIDAndEmbeddingServicePath(t *testing.T) {
	_, store := newGraphTestService(t)
	ctx := context.Background()

	record := newSemanticRetrievalRecord("retrievable", 0.8, schema.SensitivityLow)
	record.AuditLog = []schema.AuditEntry{{Action: schema.AuditActionReinforce, Actor: "fixture"}}
	record.Provenance.Sources = []schema.ProvenanceSource{{Kind: schema.ProvenanceKindObservation, Ref: "fixture"}}
	denied := newSemanticRetrievalRecord("denied", 0.9, schema.SensitivityHyper)
	for _, rec := range []*schema.MemoryRecord{record, denied} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}
	if err := store.AddRelation(ctx, record.ID, schema.Relation{Predicate: "related", TargetID: denied.ID}); err != nil {
		t.Fatalf("AddRelation: %v", err)
	}

	embedding := &fakeEmbeddingService{vector: []float32{1, 2, 3}}
	svc := NewServiceWithEmbedding(store, nil, embedding)
	resp, err := svc.Retrieve(ctx, &RetrieveRequest{
		TaskDescriptor: "trigger embedding path",
		Trust:          NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes:    []schema.MemoryType{schema.MemoryTypeSemantic},
		Limit:          1,
	})
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	if embedding.calls != 1 {
		t.Fatalf("EmbedQuery calls = %d, want 1", embedding.calls)
	}
	if len(resp.Records) != 1 || resp.Records[0].ID != record.ID {
		t.Fatalf("Retrieve records = %+v, want accessible record", resp.Records)
	}

	got, err := svc.RetrieveByID(ctx, record.ID, NewTrustContext(schema.SensitivityLow, true, "tester", nil))
	if err != nil {
		t.Fatalf("RetrieveByID: %v", err)
	}
	if got.ID != record.ID {
		t.Fatalf("RetrieveByID ID = %q, want %q", got.ID, record.ID)
	}
	if len(got.Relations) != 1 || len(got.AuditLog) != 1 || len(got.Provenance.Sources) != 1 {
		t.Fatalf("RetrieveByID projection = relations:%d audit:%d provenance:%d, want complete record", len(got.Relations), len(got.AuditLog), len(got.Provenance.Sources))
	}
	if _, err := svc.RetrieveByID(ctx, record.ID, nil); !errors.Is(err, ErrNilTrust) {
		t.Fatalf("RetrieveByID nil trust err = %v, want ErrNilTrust", err)
	}
	if _, err := svc.RetrieveByID(ctx, denied.ID, NewTrustContext(schema.SensitivityLow, true, "tester", nil)); !errors.Is(err, ErrAccessDenied) {
		t.Fatalf("RetrieveByID denied err = %v, want ErrAccessDenied", err)
	}
}

func competenceCandidate(id string, confidence float64, success, failure int) *schema.MemoryRecord {
	rec := schema.NewMemoryRecord(id, schema.MemoryTypeCompetence, schema.SensitivityLow, &schema.CompetencePayload{
		Kind:      "competence",
		SkillName: id,
		Performance: &schema.PerformanceStats{
			SuccessCount: int64(success),
			FailureCount: int64(failure),
		},
	})
	rec.Confidence = confidence
	return rec
}

func planCandidate(id string, confidence, failureRate float64) *schema.MemoryRecord {
	rec := schema.NewMemoryRecord(id, schema.MemoryTypePlanGraph, schema.SensitivityLow, &schema.PlanGraphPayload{
		Kind:   "plan_graph",
		PlanID: id,
		Metrics: &schema.PlanMetrics{
			ExecutionCount: 4,
			FailureRate:    failureRate,
		},
	})
	rec.Confidence = confidence
	return rec
}
