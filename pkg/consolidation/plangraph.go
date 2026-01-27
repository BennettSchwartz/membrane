package consolidation

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/GustyCube/membrane/pkg/schema"
	"github.com/GustyCube/membrane/pkg/storage"
)

// minToolGraphNodes is the minimum number of tool nodes an episodic
// tool graph must contain for it to be considered complex enough to
// extract as a plan graph.
const minToolGraphNodes = 3

// PlanGraphConsolidator extracts reusable plan graphs from episodic
// tool graphs. Only tool graphs with more than minToolGraphNodes nodes
// are promoted, ensuring that trivial single-tool invocations are not
// turned into plans.
type PlanGraphConsolidator struct {
	store storage.Store
}

// NewPlanGraphConsolidator creates a PlanGraphConsolidator backed by store.
func NewPlanGraphConsolidator(store storage.Store) *PlanGraphConsolidator {
	return &PlanGraphConsolidator{store: store}
}

// Consolidate finds episodic records with complex tool graphs (more
// than minToolGraphNodes nodes) and extracts them as plan graph
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
	derivedFrom := make(map[string]bool)
	for _, pg := range planGraphs {
		rels, err := c.store.GetRelations(ctx, pg.ID)
		if err != nil {
			continue
		}
		for _, rel := range rels {
			if rel.Predicate == "derived_from" {
				derivedFrom[rel.TargetID] = true
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
		if derivedFrom[rec.ID] {
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

		err := storage.WithTransaction(ctx, c.store, func(tx storage.Transaction) error {
			if err := tx.Create(ctx, newRec); err != nil {
				return err
			}
			rel := schema.Relation{
				Predicate: "derived_from",
				TargetID:  rec.ID,
				Weight:    1.0,
				CreatedAt: now,
			}
			return tx.AddRelation(ctx, newRec.ID, rel)
		})
		if err != nil {
			return created, err
		}

		derivedFrom[rec.ID] = true
		created++
	}

	return created, nil
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
	if len(ep.Timeline) > 0 {
		return ep.Timeline[0].EventKind
	}
	return "unknown"
}
