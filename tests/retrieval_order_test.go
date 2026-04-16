package tests_test

import (
	"context"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/retrieval"
	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage/sqlite"
)

type stubEmbeddingService struct {
	query  []float32
	scores map[string]float64
}

func (s stubEmbeddingService) EmbedQuery(context.Context, string) ([]float32, error) {
	return s.query, nil
}

func (s stubEmbeddingService) Similarity(_ context.Context, recordID string, _ []float32) (float64, bool) {
	score, ok := s.scores[recordID]
	return score, ok
}

func TestRetrievePromotesSelectedRecordsWhenTaskDescriptorProvided(t *testing.T) {
	store, err := sqlite.Open(":memory:", "")
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	now := time.Now().UTC()
	semantic := schema.NewMemoryRecord("semantic", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "user",
		Predicate: "prefers",
		Object:    "go",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	semantic.Salience = 0.99
	semantic.UpdatedAt = now

	best := schema.NewMemoryRecord("best", schema.MemoryTypeCompetence, schema.SensitivityLow, &schema.CompetencePayload{
		Kind:      "competence",
		SkillName: "best",
		Triggers:  []schema.Trigger{{Signal: "compile"}},
		Recipe:    []schema.RecipeStep{{Step: "do it"}},
		Performance: &schema.PerformanceStats{
			SuccessCount: 9,
			FailureCount: 1,
			SuccessRate:  0.9,
		},
	})
	best.Salience = 0.40
	best.Confidence = 0.40
	best.Lifecycle.LastReinforcedAt = now

	worse := schema.NewMemoryRecord("worse", schema.MemoryTypeCompetence, schema.SensitivityLow, &schema.CompetencePayload{
		Kind:      "competence",
		SkillName: "worse",
		Triggers:  []schema.Trigger{{Signal: "compile"}},
		Recipe:    []schema.RecipeStep{{Step: "do it"}},
		Performance: &schema.PerformanceStats{
			SuccessCount: 9,
			FailureCount: 1,
			SuccessRate:  0.9,
		},
	})
	worse.Salience = 0.80
	worse.Confidence = 0.95
	worse.Lifecycle.LastReinforcedAt = now

	for _, rec := range []*schema.MemoryRecord{semantic, best, worse} {
		if err := store.Create(context.Background(), rec); err != nil {
			t.Fatalf("Create(%s): %v", rec.ID, err)
		}
	}

	emb := stubEmbeddingService{
		query: []float32{1, 0, 0},
		scores: map[string]float64{
			"best":  0.95,
			"worse": 0.10,
		},
	}
	selector := retrieval.NewSelectorWithEmbedding(0.2, emb)
	svc := retrieval.NewServiceWithEmbedding(store, selector, emb)

	resp, err := svc.Retrieve(context.Background(), &retrieval.RetrieveRequest{
		TaskDescriptor: "compile code",
		Trust:          retrieval.NewTrustContext(schema.SensitivityHyper, true, "tester", nil),
		Limit:          10,
	})
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	if len(resp.Records) < 2 {
		t.Fatalf("expected at least 2 records, got %d", len(resp.Records))
	}
	if resp.Records[0].ID != "best" {
		t.Fatalf("expected selected competence record to be promoted first, got %s", resp.Records[0].ID)
	}
}
