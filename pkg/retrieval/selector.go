package retrieval

import (
	"context"
	"math"
	"sort"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
)

// SelectionResult holds the outcome of multi-solution selection per RFC 15A.11.
// When multiple competence records or plan graphs match a query, the selector
// ranks them and reports confidence in the selection.
type SelectionResult struct {
	// Selected contains the ranked candidates in descending score order.
	Selected []*schema.MemoryRecord

	// Confidence is the selection confidence in [0, 1].
	// Computed as the score gap between the best and second-best candidate,
	// normalized by the best score.
	Confidence float64

	// NeedsMore is true when confidence falls below the selector's threshold,
	// indicating that additional information or user disambiguation is needed.
	NeedsMore bool

	// Scores contains the computed score for each selected record ID.
	Scores map[string]float64
}

// Selector performs multi-solution selection for competence and plan_graph
// records using three signals: applicability, observed success rate, and
// recency of reinforcement.
type Selector struct {
	confidenceThreshold float64
	embedding           EmbeddingProvider
}

// EmbeddingProvider computes record applicability against a query embedding.
type EmbeddingProvider interface {
	Similarity(ctx context.Context, recordID string, query []float32) (float64, bool)
}

// NewSelector creates a Selector with the given confidence threshold.
// If confidence falls below the threshold, SelectionResult.NeedsMore is set to true.
func NewSelector(confidenceThreshold float64) *Selector {
	return &Selector{
		confidenceThreshold: confidenceThreshold,
	}
}

// NewSelectorWithEmbedding creates a selector that uses embeddings for applicability when available.
func NewSelectorWithEmbedding(confidenceThreshold float64, embedding EmbeddingProvider) *Selector {
	return &Selector{
		confidenceThreshold: confidenceThreshold,
		embedding:           embedding,
	}
}

// Select ranks candidate records and returns a SelectionResult.
// Candidates are scored using three equally-weighted signals:
//  1. Applicability conditions - trigger/constraint match (approximated by confidence field).
//  2. Observed success rate - from PerformanceStats (success_count / total).
//  3. Recency of reinforcement - more recently reinforced records score higher.
//
// If fewer than one candidate is provided, the result has zero confidence.
func (s *Selector) Select(ctx context.Context, candidates []*schema.MemoryRecord, queryEmbedding []float32) *SelectionResult {
	if len(candidates) == 0 {
		return &SelectionResult{
			Selected:   nil,
			Confidence: 0,
			NeedsMore:  true,
			Scores:     map[string]float64{},
		}
	}

	if len(candidates) == 1 {
		score := s.scoreRecord(ctx, candidates[0], queryEmbedding)
		conf := score // single candidate: confidence is the score itself
		if conf > 1.0 {
			conf = 1.0
		}
		return &SelectionResult{
			Selected:   candidates,
			Confidence: conf,
			NeedsMore:  conf < s.confidenceThreshold,
			Scores:     map[string]float64{candidates[0].ID: score},
		}
	}

	// Score all candidates.
	type scored struct {
		record *schema.MemoryRecord
		score  float64
	}
	items := make([]scored, len(candidates))
	for i, c := range candidates {
		items[i] = scored{record: c, score: s.scoreRecord(ctx, c, queryEmbedding)}
	}

	// Sort by score descending.
	sort.Slice(items, func(i, j int) bool {
		return items[i].score > items[j].score
	})

	// Build ranked result.
	ranked := make([]*schema.MemoryRecord, len(items))
	scores := make(map[string]float64, len(items))
	for i, item := range items {
		ranked[i] = item.record
		scores[item.record.ID] = item.score
	}

	// Compute confidence as the normalized gap between best and second-best.
	best := items[0].score
	secondBest := items[1].score
	var confidence float64
	if best > 0 {
		confidence = (best - secondBest) / best
	}

	return &SelectionResult{
		Selected:   ranked,
		Confidence: confidence,
		NeedsMore:  confidence < s.confidenceThreshold,
		Scores:     scores,
	}
}

// scoreRecord computes a composite score for a candidate record using
// three equally-weighted signals.
func (s *Selector) scoreRecord(ctx context.Context, record *schema.MemoryRecord, queryEmbedding []float32) float64 {
	// If payload is nil, return neutral score.
	if record.Payload == nil {
		return 0.5
	}

	// Signal 1: Applicability - use the record's Confidence field as a proxy
	// for how well this record's triggers/constraints match.
	applicability := record.Confidence
	if s.embedding != nil && len(queryEmbedding) > 0 {
		if sim, ok := s.embedding.Similarity(ctx, record.ID, queryEmbedding); ok {
			applicability = sim
		}
	}

	// Signal 2: Observed success rate from PerformanceStats.
	successRate := s.extractSuccessRate(record)

	// Signal 3: Recency of reinforcement - normalized to [0, 1] based on
	// how recently the record was reinforced. Records reinforced within the
	// last hour score 1.0; those older than 30 days score near 0.
	recency := s.computeRecency(record)

	// Equal weighting across all three signals.
	return (applicability + successRate + recency) / 3.0
}

// extractSuccessRate pulls the success rate from a competence or plan_graph payload.
func (s *Selector) extractSuccessRate(record *schema.MemoryRecord) float64 {
	switch p := record.Payload.(type) {
	case *schema.CompetencePayload:
		if p.Performance != nil {
			total := p.Performance.SuccessCount + p.Performance.FailureCount
			if total > 0 {
				return float64(p.Performance.SuccessCount) / float64(total)
			}
		}
	case *schema.PlanGraphPayload:
		if p.Metrics != nil && p.Metrics.ExecutionCount > 0 {
			return 1.0 - p.Metrics.FailureRate
		}
	}
	// Default: no data means neutral score.
	return 0.5
}

// computeRecency returns a [0, 1] score based on how recently the record
// was reinforced. Uses an exponential decay with a 30-day reference window.
func (s *Selector) computeRecency(record *schema.MemoryRecord) float64 {
	// If never reinforced (zero value), return neutral score.
	if record.Lifecycle.LastReinforcedAt.IsZero() {
		return 0.5
	}
	elapsed := time.Since(record.Lifecycle.LastReinforcedAt).Seconds()
	// Handle future timestamps: treat as fully recent.
	if elapsed <= 0 {
		return 1.0
	}
	// 30-day half-life for recency scoring.
	const halfLifeSeconds = 30 * 24 * 3600.0
	// Exponential decay: score = 0.5^(elapsed / halfLife)
	// Use the identity: 0.5^x = exp(-x * ln(2))
	return math.Exp(-elapsed * math.Log(2) / halfLifeSeconds)
}
