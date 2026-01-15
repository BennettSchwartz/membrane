// Package consolidation analyzes episodic and working memory to extract
// durable knowledge. RFC Section 10, 15.7: Consolidation promotes raw
// experience into semantic facts, competence records, and plan graphs.
// Consolidation is automatic (RFC 15B) and requires no user approval.
// Promoted knowledge remains subject to decay and revision.
package consolidation

import (
	"context"
	"fmt"

	"github.com/GustyCube/membrane/pkg/storage"
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

	// CompetenceExtracted is the number of new competence records
	// created from repeated successful episodic patterns.
	CompetenceExtracted int

	// PlanGraphsExtracted is the number of new plan graph records
	// extracted from episodic tool graphs.
	PlanGraphsExtracted int

	// DuplicatesResolved is the number of duplicate records that were
	// identified and resolved (e.g., by reinforcing existing records).
	DuplicatesResolved int
}

// Service is the top-level consolidation service that orchestrates all
// sub-consolidators. It runs episodic compression, semantic extraction,
// competence extraction, and plan graph extraction in sequence.
type Service struct {
	store      storage.Store
	episodic   *EpisodicConsolidator
	semantic   *SemanticConsolidator
	competence *CompetenceConsolidator
	plangraph  *PlanGraphConsolidator
}

// NewService creates a new consolidation Service backed by the given store.
func NewService(store storage.Store) *Service {
	return &Service{
		store:      store,
		episodic:   NewEpisodicConsolidator(store),
		semantic:   NewSemanticConsolidator(store),
		competence: NewCompetenceConsolidator(store),
		plangraph:  NewPlanGraphConsolidator(store),
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

	semanticCount, err := s.semantic.Consolidate(ctx)
	if err != nil {
		return result, fmt.Errorf("semantic consolidation: %w", err)
	}
	result.SemanticExtracted = semanticCount

	competenceCount, err := s.competence.Consolidate(ctx)
	if err != nil {
		return result, fmt.Errorf("competence consolidation: %w", err)
	}
	result.CompetenceExtracted = competenceCount

	planGraphCount, err := s.plangraph.Consolidate(ctx)
	if err != nil {
		return result, fmt.Errorf("plan graph consolidation: %w", err)
	}
	result.PlanGraphsExtracted = planGraphCount

	return result, nil
}
