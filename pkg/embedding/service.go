package embedding

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/GustyCube/membrane/pkg/schema"
	"github.com/GustyCube/membrane/pkg/storage"
)

// EmbeddingStore is the narrow vector-storage interface needed by Service.
type EmbeddingStore interface {
	StoreTriggerEmbedding(ctx context.Context, recordID string, embedding []float32, model string) error
	GetTriggerEmbedding(ctx context.Context, recordID string) ([]float32, error)
}

// Service coordinates query-time and write-time embedding operations.
type Service struct {
	client      Client
	store       storage.Store
	vectorStore EmbeddingStore
	model       string
}

// NewService creates a new embedding service.
func NewService(client Client, store storage.Store, vectorStore EmbeddingStore, model string) *Service {
	return &Service{
		client:      client,
		store:       store,
		vectorStore: vectorStore,
		model:       model,
	}
}

// EmbedRecord generates and persists the trigger embedding for a record when supported.
func (s *Service) EmbedRecord(ctx context.Context, rec *schema.MemoryRecord) error {
	if s == nil || rec == nil || s.client == nil || s.vectorStore == nil {
		return nil
	}
	text := triggerText(rec)
	if text == "" {
		return nil
	}
	embedding, err := s.client.Embed(ctx, text)
	if err != nil {
		return err
	}
	return s.vectorStore.StoreTriggerEmbedding(ctx, rec.ID, embedding, s.model)
}

// EmbedQuery generates an embedding for the given task descriptor.
func (s *Service) EmbedQuery(ctx context.Context, taskDescriptor string) ([]float32, error) {
	if s == nil || s.client == nil || strings.TrimSpace(taskDescriptor) == "" {
		return nil, nil
	}
	return s.client.Embed(ctx, taskDescriptor)
}

// Similarity returns the cosine similarity in [0, 1] between query and a stored embedding.
func (s *Service) Similarity(ctx context.Context, recordID string, query []float32) (float64, bool) {
	if s == nil || s.vectorStore == nil || len(query) == 0 {
		return 0.5, false
	}
	stored, err := s.vectorStore.GetTriggerEmbedding(ctx, recordID)
	if err != nil || len(stored) == 0 || len(stored) != len(query) {
		return 0.5, false
	}
	return cosineSimilarity(stored, query), true
}

// BackfillMissing computes embeddings for existing competence and plan graph records that do not have one.
func (s *Service) BackfillMissing(ctx context.Context) (int, error) {
	if s == nil || s.store == nil || s.vectorStore == nil || s.client == nil {
		return 0, nil
	}

	var total int
	for _, memType := range []schema.MemoryType{schema.MemoryTypeCompetence, schema.MemoryTypePlanGraph} {
		records, err := s.store.ListByType(ctx, memType)
		if err != nil {
			return total, fmt.Errorf("list %s for embedding backfill: %w", memType, err)
		}
		for _, rec := range records {
			existing, err := s.vectorStore.GetTriggerEmbedding(ctx, rec.ID)
			if err != nil {
				return total, fmt.Errorf("check embedding for %s: %w", rec.ID, err)
			}
			if len(existing) > 0 {
				continue
			}
			if err := s.EmbedRecord(ctx, rec); err != nil {
				continue
			}
			total++
		}
	}
	return total, nil
}

func triggerText(rec *schema.MemoryRecord) string {
	switch payload := rec.Payload.(type) {
	case *schema.CompetencePayload:
		signals := make([]string, 0, len(payload.Triggers))
		for _, trig := range payload.Triggers {
			if trig.Signal != "" {
				signals = append(signals, trig.Signal)
			}
		}
		if len(signals) > 0 {
			return strings.Join(signals, " ")
		}
		return payload.SkillName
	case *schema.PlanGraphPayload:
		if payload.Intent != "" {
			return payload.Intent
		}
		ops := make([]string, 0, len(payload.Nodes))
		for _, node := range payload.Nodes {
			if node.Op != "" {
				ops = append(ops, node.Op)
			}
		}
		return strings.Join(ops, " ")
	default:
		return ""
	}
}

func cosineSimilarity(a, b []float32) float64 {
	var dot, normA, normB float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		normA += float64(a[i]) * float64(a[i])
		normB += float64(b[i]) * float64(b[i])
	}
	if normA == 0 || normB == 0 {
		return 0.5
	}
	sim := dot / (math.Sqrt(normA) * math.Sqrt(normB))
	sim = (sim + 1.0) / 2.0
	if sim < 0 {
		return 0
	}
	if sim > 1 {
		return 1
	}
	return sim
}
