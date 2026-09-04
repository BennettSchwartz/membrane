package consolidation

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

// minToolGraphNodes is the minimum number of tool nodes an episodic
// tool graph must contain for it to be considered complex enough to
// extract as a plan graph.
const minToolGraphNodes = 3

// PlanGraphConsolidator extracts reusable plan graphs from episodic
// tool graphs. Only tool graphs with at least minToolGraphNodes nodes
// are promoted, ensuring that trivial single-tool invocations are not
// turned into plans.
type PlanGraphConsolidator struct {
	store    storage.Store
	embedder Embedder
}

// NewPlanGraphConsolidator creates a PlanGraphConsolidator backed by store.
func NewPlanGraphConsolidator(store storage.Store) *PlanGraphConsolidator {
	return &PlanGraphConsolidator{store: store}
}

// NewPlanGraphConsolidatorWithEmbedder creates a plan graph consolidator with embedding support.
func NewPlanGraphConsolidatorWithEmbedder(store storage.Store, embedder Embedder) *PlanGraphConsolidator {
	return &PlanGraphConsolidator{store: store, embedder: embedder}
}

// Consolidate finds episodic records with complex tool graphs (at
// least minToolGraphNodes nodes) and extracts them as plan graph
// records. It returns the number of new plan graphs created.
func (c *PlanGraphConsolidator) Consolidate(ctx context.Context) (int, error) {
	episodics, err := c.store.ListByType(ctx, schema.MemoryTypeEpisodic)
	if err != nil {
		return 0, err
	}

	// Load existing plan graphs to avoid creating duplicates for the
	// same episodic source.
	planGraphs, err := c.store.ListByType(ctx, schema.MemoryTypePlanGraph)
	if err != nil {
		return 0, err
	}

	// Build a set of episodic record IDs that already have a plan
	// graph derived from them.
	derivedFrom := make(map[string][]*schema.MemoryRecord)
	for _, pg := range planGraphs {
		rels, err := c.store.GetRelations(ctx, pg.ID)
		if err != nil {
			return 0, fmt.Errorf("load plan graph relations %s: %w", pg.ID, err)
		}
		for _, rel := range rels {
			if schema.NormalizeGraphPredicate(rel.Predicate) == schema.GraphPredicateDerivedFrom {
				derivedFrom[rel.TargetID] = append(derivedFrom[rel.TargetID], pg)
			}
		}
	}

	now := time.Now().UTC()
	created := 0

	for _, rec := range episodics {
		ep, ok := rec.Payload.(*schema.EpisodicPayload)
		if !ok {
			continue
		}

		// Only extract from complex tool graphs.
		if len(ep.ToolGraph) < minToolGraphNodes {
			continue
		}

		// Skip if we already extracted a plan graph from this episode.
		if priorPlans := derivedFrom[rec.ID]; len(priorPlans) > 0 {
			for _, prior := range priorPlans {
				if err := storage.WithTransaction(ctx, c.store, func(tx storage.Transaction) error {
					current, err := storage.GetDerivedDestination(ctx, tx, prior.ID)
					if err != nil {
						return err
					}
					if current.Type != schema.MemoryTypePlanGraph {
						return fmt.Errorf("plan destination changed")
					}
					oldSensitivity := current.Sensitivity
					if err := storage.ApplyDerivedSourcePolicy(ctx, tx, current, []*schema.MemoryRecord{rec}); err != nil {
						return err
					}
					if current.Sensitivity == oldSensitivity {
						return nil
					}
					if err := storage.PruneDerivedInverseRelations(ctx, tx, current); err != nil {
						return err
					}
					if err := tx.Update(ctx, current); err != nil {
						return err
					}
					return tx.AddAuditEntry(ctx, current.ID, schema.AuditEntry{Action: schema.AuditActionReinforce, Actor: "consolidation/plangraph", Timestamp: now, Rationale: "Raised classification to cover source evidence"})
				}); err != nil {
					return created, err
				}
			}
			continue
		}

		// Convert tool graph nodes to plan nodes and edges.
		nodes, edges := convertToolGraphToPlan(ep.ToolGraph)

		planID := uuid.New().String()
		payload := &schema.PlanGraphPayload{
			Kind:    "plan_graph",
			PlanID:  planID,
			Version: "1",
			Intent:  inferIntent(ep),
			Nodes:   nodes,
			Edges:   edges,
			Metrics: &schema.PlanMetrics{
				ExecutionCount: 1,
				LastExecutedAt: &now,
			},
		}

		newRec := schema.NewMemoryRecord(
			uuid.New().String(),
			schema.MemoryTypePlanGraph,
			rec.Sensitivity,
			payload,
		)
		newRec.Confidence = 0.7
		newRec.Scope = rec.Scope
		newRec.Tags = []string{"consolidated", "auto-plangraph"}
		newRec.Provenance = schema.Provenance{
			Sources: []schema.ProvenanceSource{
				{
					Kind:      schema.ProvenanceKindToolCall,
					Ref:       rec.ID,
					CreatedBy: "consolidation/plangraph",
					Timestamp: now,
				},
			},
			CreatedBy: "consolidation/plangraph",
		}
		newRec.AuditLog = []schema.AuditEntry{
			{
				Action:    schema.AuditActionCreate,
				Actor:     "consolidation/plangraph",
				Timestamp: now,
				Rationale: fmt.Sprintf("Extracted plan graph from episodic record %s (%d nodes)", rec.ID, len(ep.ToolGraph)),
			},
		}

		candidates := snapshotEntityCandidates(ctx, c.store, newRec.Scope, planEntityTerms(nodes)...)
		err := storage.WithTransaction(ctx, c.store, func(tx storage.Transaction) error {
			if err := storage.ApplyDerivedSourcePolicy(ctx, tx, newRec, []*schema.MemoryRecord{rec}); err != nil {
				return err
			}
			entityEdges := linkRecordToEntityTerms(ctx, entityStoreInTransaction(candidates, tx), newRec, planEntityTerms(nodes), schema.GraphPredicateUses, schema.GraphPredicateUsedBy, now)
			if err := tx.Create(ctx, newRec); err != nil {
				return err
			}
			rel := schema.Relation{
				Predicate: schema.GraphPredicateDerivedFrom,
				TargetID:  rec.ID,
				Weight:    1.0,
				CreatedAt: now,
			}
			if err := tx.AddRelation(ctx, newRec.ID, rel); err != nil {
				return err
			}
			for _, edge := range entityEdges {
				if edge.SourceID == newRec.ID {
					continue
				}
				if err := tx.AddRelation(ctx, edge.SourceID, schema.Relation{
					Predicate: edge.Predicate,
					TargetID:  edge.TargetID,
					Weight:    edge.Weight,
					CreatedAt: edge.CreatedAt,
				}); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return created, err
		}
		if c.embedder != nil {
			_ = c.embedder.EmbedRecord(ctx, newRec)
		}

		derivedFrom[rec.ID] = []*schema.MemoryRecord{newRec}
		created++
	}

	return created, nil
}

func planEntityTerms(nodes []schema.PlanNode) []string {
	terms := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if node.Op != "" {
			terms = append(terms, node.Op)
		}
		for _, key := range []string{"tool", "command", "repo", "repository", "file", "service", "package"} {
			if value, ok := node.Params[key].(string); ok && value != "" {
				terms = append(terms, value)
			}
		}
	}
	return terms
}

// convertToolGraphToPlan converts episodic ToolNodes into PlanNodes
// and PlanEdges. Dependency relationships from ToolNode.DependsOn are
// translated into control-flow edges.
func convertToolGraphToPlan(toolNodes []schema.ToolNode) ([]schema.PlanNode, []schema.PlanEdge) {
	nodes := make([]schema.PlanNode, 0, len(toolNodes))
	edges := make([]schema.PlanEdge, 0)

	for _, tn := range toolNodes {
		nodes = append(nodes, schema.PlanNode{
			ID:     tn.ID,
			Op:     tn.Tool,
			Params: tn.Args,
		})

		for _, dep := range tn.DependsOn {
			edges = append(edges, schema.PlanEdge{
				From: dep,
				To:   tn.ID,
				Kind: schema.EdgeKindControl,
			})
		}
	}

	return nodes, edges
}

// inferIntent produces a simple intent label from an episodic payload.
// If the episode has timeline events the first event kind is used;
// otherwise a generic label is returned.
func inferIntent(ep *schema.EpisodicPayload) string {
	for _, event := range ep.Timeline {
		if event.EventKind != "" {
			return event.EventKind
		}
	}
	return "unknown"
}
