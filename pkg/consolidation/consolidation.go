// Package consolidation analyzes episodic and working memory to extract
// durable knowledge. RFC Section 10, 15.7: Consolidation promotes raw
// experience into semantic facts, competence records, and plan graphs.
// Consolidation is automatic (RFC 15B) and requires no user approval.
// Promoted knowledge remains subject to decay and revision.
package consolidation

import (
	"context"
	"fmt"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

// ConsolidationResult tracks what was created or updated during a
// consolidation run across all sub-consolidators.
type ConsolidationResult struct {
	// EpisodicCompressed is the number of episodic records whose salience
	// was reduced (compressed) because they exceeded the age threshold.
	EpisodicCompressed int

	// SemanticExtracted is the number of new semantic memory records
	// created from episodic observations.
	SemanticExtracted int

	// SemanticTriplesExtracted is the number of new semantic facts created
	// by the LLM-backed semantic extractor.
	SemanticTriplesExtracted int

	// CompetenceExtracted is the number of new competence records
	// created from repeated successful episodic patterns.
	CompetenceExtracted int

	// PlanGraphsExtracted is the number of new plan graph records
	// extracted from episodic tool graphs.
	PlanGraphsExtracted int

	// DuplicatesResolved is the number of duplicate records that were
	// identified and resolved (e.g., by reinforcing existing records).
	DuplicatesResolved int

	// ExtractionSkipped is the number of claimed episodic records that
	// could not be fully processed and will be retried later.
	ExtractionSkipped int
}

// Service is the top-level consolidation service that orchestrates all
// sub-consolidators. It runs episodic compression, semantic extraction,
// competence extraction, and plan graph extraction in sequence.
type Service struct {
	store      storage.Store
	embedder   Embedder
	episodic   *EpisodicConsolidator
	semantic   *SemanticConsolidator
	extractor  *SemanticExtractor
	competence *CompetenceConsolidator
	plangraph  *PlanGraphConsolidator
}

// Embedder stores embeddings for newly created durable records when configured.
type Embedder interface {
	EmbedRecord(ctx context.Context, rec *schema.MemoryRecord) error
}

// NewService creates a new consolidation Service backed by the given store.
func NewService(store storage.Store) *Service {
	return newService(store, nil, nil)
}

// NewServiceWithEmbedder creates a new consolidation Service with embedding support.
func NewServiceWithEmbedder(store storage.Store, embedder Embedder) *Service {
	return newService(store, embedder, nil)
}

// NewServiceWithExtractor creates a new consolidation Service with semantic extraction.
// embedder may be nil when record embeddings are not configured.
func NewServiceWithExtractor(
	store storage.Store,
	embedder Embedder,
	reinforcer Reinforcer,
	llm LLMClient,
	extractionStore ExtractionStore,
) *Service {
	return newService(
		store,
		embedder,
		NewSemanticExtractor(store, extractionStore, reinforcer, llm),
	)
}

func newService(store storage.Store, embedder Embedder, extractor *SemanticExtractor) *Service {
	competence := NewCompetenceConsolidator(store)
	plangraph := NewPlanGraphConsolidator(store)
	if embedder != nil {
		competence = NewCompetenceConsolidatorWithEmbedder(store, embedder)
		plangraph = NewPlanGraphConsolidatorWithEmbedder(store, embedder)
	}

	return &Service{
		store:      store,
		embedder:   embedder,
		episodic:   NewEpisodicConsolidator(store),
		semantic:   NewSemanticConsolidator(store),
		extractor:  extractor,
		competence: competence,
		plangraph:  plangraph,
	}
}

// RunAll executes every consolidation pipeline in sequence and returns
// a combined result. If any sub-consolidator fails the run is aborted
// and the error is returned together with any partial results collected
// so far.
func (s *Service) RunAll(ctx context.Context) (*ConsolidationResult, error) {
	result := &ConsolidationResult{}

	episodicCount, err := s.episodic.Consolidate(ctx)
	if err != nil {
		return result, fmt.Errorf("episodic consolidation: %w", err)
	}
	result.EpisodicCompressed = episodicCount

	semanticCount, semanticReinforced, err := s.semantic.Consolidate(ctx)
	if err != nil {
		return result, fmt.Errorf("semantic consolidation: %w", err)
	}
	result.SemanticExtracted = semanticCount
	result.DuplicatesResolved += semanticReinforced

	if s.extractor != nil {
		extracted, skipped, err := s.extractor.Extract(ctx)
		if err != nil {
			return result, fmt.Errorf("semantic extraction: %w", err)
		}
		result.SemanticTriplesExtracted = extracted
		result.ExtractionSkipped = skipped
	}

	competenceCount, competenceReinforced, err := s.competence.Consolidate(ctx)
	if err != nil {
		return result, fmt.Errorf("competence consolidation: %w", err)
	}
	result.CompetenceExtracted = competenceCount
	result.DuplicatesResolved += competenceReinforced

	planGraphCount, err := s.plangraph.Consolidate(ctx)
	if err != nil {
		return result, fmt.Errorf("plan graph consolidation: %w", err)
	}
	result.PlanGraphsExtracted = planGraphCount

	return result, nil
}
