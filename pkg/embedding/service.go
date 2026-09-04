package embedding

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
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
	if s == nil || s.vectorStore == nil || !usableEmbeddingVector(query) {
		return 0.5, false
	}
	stored, err := s.vectorStore.GetTriggerEmbedding(ctx, recordID)
	if err != nil || len(stored) != len(query) || !usableEmbeddingVector(stored) {
		return 0.5, false
	}
	return cosineSimilarity(stored, query), true
}

// BackfillMissing computes embeddings for existing embeddable records that do
// not have an embedding for the configured model.
func (s *Service) BackfillMissing(ctx context.Context) (int, error) {
	if s == nil || s.store == nil || s.vectorStore == nil || s.client == nil {
		return 0, nil
	}

	var total int
	var embedErrs []error
	for _, memType := range []schema.MemoryType{
		schema.MemoryTypeEpisodic,
		schema.MemoryTypeWorking,
		schema.MemoryTypeEntity,
		schema.MemoryTypeSemantic,
		schema.MemoryTypeCompetence,
		schema.MemoryTypePlanGraph,
	} {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		records, err := s.store.ListByType(ctx, memType)
		if err != nil {
			return total, fmt.Errorf("list %s for embedding backfill: %w", memType, err)
		}
		for _, rec := range records {
			if err := ctx.Err(); err != nil {
				return total, err
			}
			existing, err := s.vectorStore.GetTriggerEmbedding(ctx, rec.ID)
			if err != nil {
				return total, fmt.Errorf("check embedding for %s: %w", rec.ID, err)
			}
			if len(existing) > 0 {
				continue
			}
			if triggerText(rec) == "" {
				continue
			}
			if err := s.EmbedRecord(ctx, rec); err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return total, ctxErr
				}
				embedErrs = append(embedErrs, fmt.Errorf("%s: %w", rec.ID, err))
				continue
			}
			total++
		}
	}
	if len(embedErrs) > 0 {
		return total, fmt.Errorf("embedding backfill failed for %d record(s): %w", len(embedErrs), errors.Join(embedErrs...))
	}
	return total, nil
}

func triggerText(rec *schema.MemoryRecord) string {
	switch payload := rec.Payload.(type) {
	case *schema.EpisodicPayload:
		// Use the summary from the most recent timeline event.
		for i := len(payload.Timeline) - 1; i >= 0; i-- {
			if payload.Timeline[i].Summary != "" {
				return payload.Timeline[i].Summary
			}
		}
		// Fall back to event kind + ref.
		if len(payload.Timeline) > 0 {
			ev := payload.Timeline[0]
			return ev.EventKind + " " + ev.Ref
		}
		return ""
	case *schema.WorkingPayload:
		parts := make([]string, 0, 3)
		if payload.ContextSummary != "" {
			parts = append(parts, payload.ContextSummary)
		}
		if payload.State != "" {
			parts = append(parts, string(payload.State))
		}
		if len(payload.NextActions) > 0 {
			parts = append(parts, strings.Join(payload.NextActions, " "))
		}
		return strings.Join(parts, " ")
	case *schema.SemanticPayload:
		return strings.TrimSpace(payload.Subject + " " + payload.Predicate + " " + semanticObjectText(payload.Object))
	case *schema.EntityPayload:
		return entityText(payload)
	case *schema.CompetencePayload:
		signals := make([]string, 0, len(payload.Triggers))
		for _, trig := range payload.Triggers {
			if trig.Signal != "" {
				signals = append(signals, trig.Signal)
			}
		}
		if len(signals) > 0 {
			return strings.TrimSpace(payload.SkillName + " " + strings.Join(signals, " "))
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

func entityText(payload *schema.EntityPayload) string {
	if payload == nil {
		return ""
	}
	parts := make([]string, 0, 2+len(payload.Types)+len(payload.Aliases)+len(payload.Identifiers))
	appendEntityTextPart := func(value string) {
		value = strings.TrimSpace(value)
		if value != "" {
			parts = append(parts, value)
		}
	}
	appendEntityTextPart(payload.CanonicalName)
	appendEntityTextPart(payload.PrimaryType)
	for _, entityType := range payload.Types {
		appendEntityTextPart(entityType)
	}
	for _, alias := range payload.Aliases {
		appendEntityTextPart(alias.Value)
	}
	for _, identifier := range payload.Identifiers {
		namespace := schema.NormalizeEntityIdentifierNamespace(identifier.Namespace)
		value := strings.TrimSpace(identifier.Value)
		if namespace != "" && value != "" {
			parts = append(parts, namespace+":"+value)
		}
	}
	appendEntityTextPart(payload.Summary)
	return strings.Join(parts, " ")
}

func semanticObjectText(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case fmt.Stringer:
		return v.String()
	default:
		data, err := json.Marshal(v)
		if err == nil {
			return string(data)
		}
		return fmt.Sprint(v)
	}
}

func cosineSimilarity(a, b []float32) float64 {
	if len(a) == 0 || len(a) != len(b) || !usableEmbeddingVector(a) || !usableEmbeddingVector(b) {
		return 0.5
	}
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
	return clamp01(sim)
}

func usableEmbeddingVector(vec []float32) bool {
	if len(vec) == 0 {
		return false
	}
	var nonZero bool
	for _, value := range vec {
		v := float64(value)
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return false
		}
		if value != 0 {
			nonZero = true
		}
	}
	return nonZero
}

func clamp01(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0.5
	}
	if value < 0 {
		return 0
	}
	if value > 1 {
		return 1
	}
	return value
}
