package consolidation

import (
	"context"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

func canonicalizeSemanticRecordEntities(ctx context.Context, store storage.Store, rec *schema.MemoryRecord) []schema.GraphEdge {
	payload, ok := rec.Payload.(*schema.SemanticPayload)
	if !ok || payload == nil {
		return nil
	}
	lookup, ok := store.(storage.EntityLookup)
	if !ok {
		return nil
	}
	ts := rec.CreatedAt
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	edges := make([]schema.GraphEdge, 0, 4)
	if entity := findEntityByTerm(ctx, lookup, payload.Subject, rec.Scope); entity != nil {
		payload.Subject = entity.ID
		rec.Relations = append(rec.Relations, schema.Relation{
			Predicate: "subject_entity",
			TargetID:  entity.ID,
			Weight:    1.0,
			CreatedAt: ts,
		})
		edges = append(edges, schema.GraphEdge{SourceID: rec.ID, Predicate: "subject_entity", TargetID: entity.ID, Weight: 1.0, CreatedAt: ts})
		edges = append(edges, schema.GraphEdge{SourceID: entity.ID, Predicate: "fact_subject_of", TargetID: rec.ID, Weight: 1.0, CreatedAt: ts})
	}
	if object, ok := payload.Object.(string); ok {
		if entity := findEntityByTerm(ctx, lookup, object, rec.Scope); entity != nil {
			payload.Object = entity.ID
			rec.Relations = append(rec.Relations, schema.Relation{
				Predicate: "object_entity",
				TargetID:  entity.ID,
				Weight:    1.0,
				CreatedAt: ts,
			})
			edges = append(edges, schema.GraphEdge{SourceID: rec.ID, Predicate: "object_entity", TargetID: entity.ID, Weight: 1.0, CreatedAt: ts})
			edges = append(edges, schema.GraphEdge{SourceID: entity.ID, Predicate: "fact_object_of", TargetID: rec.ID, Weight: 1.0, CreatedAt: ts})
		}
	}
	rec.Payload = payload
	return edges
}

func findEntityByTerm(ctx context.Context, lookup storage.EntityLookup, term, scope string) *schema.MemoryRecord {
	matches, err := lookup.FindEntitiesByTerm(ctx, term, scope, 1)
	if err != nil || len(matches) == 0 || matches[0] == nil || matches[0].Type != schema.MemoryTypeEntity {
		return nil
	}
	return matches[0]
}

func linkRecordToEntityTerms(ctx context.Context, store storage.Store, rec *schema.MemoryRecord, terms []string, predicate, inverse string, ts time.Time) []schema.GraphEdge {
	lookup, ok := store.(storage.EntityLookup)
	if !ok || rec == nil {
		return nil
	}
	edges := make([]schema.GraphEdge, 0, len(terms)*2)
	seen := make(map[string]struct{}, len(terms))
	for _, term := range terms {
		entity := findEntityByTerm(ctx, lookup, term, rec.Scope)
		if entity == nil {
			continue
		}
		if _, ok := seen[entity.ID]; ok {
			continue
		}
		seen[entity.ID] = struct{}{}
		rec.Relations = append(rec.Relations, schema.Relation{
			Predicate: predicate,
			TargetID:  entity.ID,
			Weight:    1.0,
			CreatedAt: ts,
		})
		edges = append(edges, schema.GraphEdge{SourceID: rec.ID, Predicate: predicate, TargetID: entity.ID, Weight: 1.0, CreatedAt: ts})
		edges = append(edges, schema.GraphEdge{SourceID: entity.ID, Predicate: inverse, TargetID: rec.ID, Weight: 1.0, CreatedAt: ts})
	}
	return edges
}
