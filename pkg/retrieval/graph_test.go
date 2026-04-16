package retrieval

import (
	"context"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	sqlitestore "github.com/BennettSchwartz/membrane/pkg/storage/sqlite"
)

func newGraphTestService(t *testing.T) (*Service, *sqlitestore.SQLiteStore) {
	t.Helper()

	store, err := sqlitestore.Open(":memory:", "")
	if err != nil {
		t.Fatalf("Open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return NewService(store, nil), store
}

func TestRetrieveGraphReranksEntityRootsAndExpandsNeighbors(t *testing.T) {
	svc, store := newGraphTestService(t)
	ctx := context.Background()
	now := time.Date(2026, 4, 8, 12, 0, 0, 0, time.UTC)

	entity := schema.NewMemoryRecord("entity-1", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Orchid",
		EntityKind:    schema.EntityKindProject,
		Aliases:       []string{"staging deploy target"},
		Summary:       "Orchid entity",
	})
	entity.CreatedAt = now
	entity.UpdatedAt = now
	entity.Salience = 0.1
	if err := store.Create(ctx, entity); err != nil {
		t.Fatalf("Create entity: %v", err)
	}

	episode := schema.NewMemoryRecord("episodic-1", schema.MemoryTypeEpisodic, schema.SensitivityLow, &schema.EpisodicPayload{
		Kind: "episodic",
		Timeline: []schema.TimelineEvent{{
			T:         now,
			EventKind: "event",
			Ref:       "evt-1",
			Summary:   "Used Orchid during rollout verification",
		}},
	})
	episode.CreatedAt = now
	episode.UpdatedAt = now
	episode.Salience = 1.0
	if err := store.Create(ctx, episode); err != nil {
		t.Fatalf("Create episode: %v", err)
	}
	if err := store.AddRelation(ctx, entity.ID, schema.Relation{TargetID: episode.ID, Predicate: "mentioned_in", Weight: 1.0, CreatedAt: now}); err != nil {
		t.Fatalf("AddRelation entity->episode: %v", err)
	}
	if err := store.AddRelation(ctx, episode.ID, schema.Relation{TargetID: entity.ID, Predicate: "mentions_entity", Weight: 1.0, CreatedAt: now}); err != nil {
		t.Fatalf("AddRelation episode->entity: %v", err)
	}

	resp, err := svc.RetrieveGraph(ctx, &RetrieveGraphRequest{
		TaskDescriptor: "Orchid",
		Trust:          NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		RootLimit:      1,
		NodeLimit:      2,
		EdgeLimit:      4,
		MaxHops:        1,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph: %v", err)
	}

	if len(resp.RootIDs) != 1 || resp.RootIDs[0] != entity.ID {
		t.Fatalf("RootIDs = %v, want [%s]", resp.RootIDs, entity.ID)
	}
	if len(resp.Nodes) != 2 {
		t.Fatalf("Nodes len = %d, want 2", len(resp.Nodes))
	}
	if !resp.Nodes[0].Root || resp.Nodes[0].Record.ID != entity.ID {
		t.Fatalf("First node = %+v, want root entity node", resp.Nodes[0])
	}
	if len(resp.Edges) != 1 || resp.Edges[0].TargetID != episode.ID {
		t.Fatalf("Edges = %+v, want expansion edge to episode", resp.Edges)
	}
}

func TestRetrieveGraphRedactsNeighborPayloadAndInterpretation(t *testing.T) {
	svc, store := newGraphTestService(t)
	ctx := context.Background()
	now := time.Date(2026, 4, 8, 12, 0, 0, 0, time.UTC)

	entity := schema.NewMemoryRecord("entity-2", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Orchid",
		EntityKind:    schema.EntityKindProject,
		Aliases:       []string{"orchid"},
		Summary:       "Orchid entity",
	})
	entity.CreatedAt = now
	entity.UpdatedAt = now
	if err := store.Create(ctx, entity); err != nil {
		t.Fatalf("Create entity: %v", err)
	}

	episode := schema.NewMemoryRecord("episodic-2", schema.MemoryTypeEpisodic, schema.SensitivityMedium, &schema.EpisodicPayload{
		Kind: "episodic",
		Timeline: []schema.TimelineEvent{{
			T:         now,
			EventKind: "event",
			Ref:       "evt-2",
			Summary:   "Sensitive Orchid rollout note",
		}},
	})
	episode.CreatedAt = now
	episode.UpdatedAt = now
	episode.Interpretation = &schema.Interpretation{
		Status:       schema.InterpretationStatusResolved,
		Summary:      "Sensitive Orchid interpretation",
		ProposedType: schema.MemoryTypeSemantic,
	}
	if err := store.Create(ctx, episode); err != nil {
		t.Fatalf("Create episode: %v", err)
	}
	if err := store.AddRelation(ctx, entity.ID, schema.Relation{TargetID: episode.ID, Predicate: "mentioned_in", Weight: 1.0, CreatedAt: now}); err != nil {
		t.Fatalf("AddRelation: %v", err)
	}

	resp, err := svc.RetrieveGraph(ctx, &RetrieveGraphRequest{
		TaskDescriptor: "Orchid",
		Trust:          NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		RootLimit:      1,
		NodeLimit:      2,
		EdgeLimit:      2,
		MaxHops:        1,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph: %v", err)
	}

	var redacted *schema.MemoryRecord
	for _, node := range resp.Nodes {
		if node.Record.ID == episode.ID {
			redacted = node.Record
			break
		}
	}
	if redacted == nil {
		t.Fatalf("expected redacted neighbor for %s", episode.ID)
	}
	if redacted.Payload != nil {
		t.Fatalf("Payload = %+v, want nil after redaction", redacted.Payload)
	}
	if redacted.Interpretation != nil {
		t.Fatalf("Interpretation = %+v, want nil after redaction", redacted.Interpretation)
	}
}
