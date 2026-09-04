package postgres

import (
	"context"
	"testing"

	"github.com/BennettSchwartz/membrane/pkg/schema"
)

func TestBoundedRelationsHydrateSelectedNumericIDs(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	for _, id := range []string{"a", "b", "c"} {
		if err := store.Create(ctx, newSemanticRecord(id)); err != nil {
			t.Fatal(err)
		}
	}
	for _, edge := range []struct {
		from, to string
		weight   float64
	}{{"a", "c", 0.8}, {"a", "b", 0.3}, {"b", "c", 0.4}} {
		if err := store.AddRelation(ctx, edge.from, schema.Relation{Predicate: "related_to", TargetID: edge.to, Weight: edge.weight}); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("outgoing", func(t *testing.T) {
		result, err := store.GetRelationsBounded(ctx, "a", 2, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Relations) != 2 || result.Relations[0].TargetID != "c" || result.Relations[1].TargetID != "b" || result.ProjectedBytes <= 0 {
			t.Fatalf("outgoing relation selection was not hydrated in rank order: %+v", result)
		}
	})
	t.Run("incoming", func(t *testing.T) {
		result, err := store.GetIncomingRelationsBounded(ctx, "c", 2, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Edges) != 2 || result.Edges[0].SourceID != "a" || result.Edges[1].SourceID != "b" || result.ProjectedBytes <= 0 {
			t.Fatalf("incoming relation selection was not hydrated in rank order: %+v", result)
		}
	})
}
