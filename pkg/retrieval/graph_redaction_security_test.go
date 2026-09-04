package retrieval

import (
	"context"
	"testing"

	"github.com/BennettSchwartz/membrane/internal/teststore"
	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

type redactionRelationStore struct {
	*teststore.MemoryStore
	lookupIDs []string
}

func (s *redactionRelationStore) GetRelationsBounded(ctx context.Context, id string, limit int, maxBytes int64) (storage.BoundedRelationResult, error) {
	s.lookupIDs = append(s.lookupIDs, id)
	return s.MemoryStore.GetRelationsBounded(ctx, id, limit, maxBytes)
}

func TestRetrieveGraphKeepsRedactedRelationshipsPrivate(t *testing.T) {
	for _, tc := range []struct {
		name                string
		rootSensitivity     schema.Sensitivity
		neighborSensitivity schema.Sensitivity
		edges               []schema.GraphEdge
		rootLimit           int
		wantNodes           int
		wantEdges           int
	}{
		{
			name:            "redacted root is terminal",
			rootSensitivity: schema.SensitivityMedium, neighborSensitivity: schema.SensitivityLow,
			edges:     []schema.GraphEdge{{SourceID: "root", Predicate: "private_relation", TargetID: "neighbor", Weight: 1}},
			wantNodes: 1,
		},
		{
			name:            "readable edge reaches redacted terminal neighbor",
			rootSensitivity: schema.SensitivityLow, neighborSensitivity: schema.SensitivityMedium,
			edges: []schema.GraphEdge{
				{SourceID: "root", Predicate: "public_reference", TargetID: "neighbor", Weight: 1},
				{SourceID: "neighbor", Predicate: "private_relation", TargetID: "further", Weight: 1},
			},
			wantNodes: 2, wantEdges: 1,
		},
		{
			name:            "incoming edge from redacted source is private",
			rootSensitivity: schema.SensitivityLow, neighborSensitivity: schema.SensitivityMedium,
			edges:     []schema.GraphEdge{{SourceID: "neighbor", Predicate: "private_relation", TargetID: "root", Weight: 1}},
			wantNodes: 1,
		},
		{
			name:            "incoming edge from readable source is preserved",
			rootSensitivity: schema.SensitivityLow, neighborSensitivity: schema.SensitivityLow,
			edges:     []schema.GraphEdge{{SourceID: "neighbor", Predicate: "public_reference", TargetID: "root", Weight: 1}},
			wantNodes: 2, wantEdges: 1,
		},
		{
			name:            "relationship from existing redacted root is private",
			rootSensitivity: schema.SensitivityLow, neighborSensitivity: schema.SensitivityMedium,
			edges:     []schema.GraphEdge{{SourceID: "neighbor", Predicate: "private_relation", TargetID: "root", Weight: 1}},
			rootLimit: 2, wantNodes: 2,
		},
		{
			name:            "relationship between readable roots is preserved",
			rootSensitivity: schema.SensitivityLow, neighborSensitivity: schema.SensitivityLow,
			edges:     []schema.GraphEdge{{SourceID: "neighbor", Predicate: "public_reference", TargetID: "root", Weight: 1}},
			rootLimit: 2, wantNodes: 2, wantEdges: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := &redactionRelationStore{MemoryStore: teststore.NewMemoryStore()}
			root := newSemanticRetrievalRecord("root", 1, tc.rootSensitivity)
			neighbor := newSemanticRetrievalRecord("neighbor", 0.2, tc.neighborSensitivity)
			further := newSemanticRetrievalRecord("further", 0.1, schema.SensitivityLow)
			records := []*schema.MemoryRecord{root, neighbor, further}
			for _, record := range records {
				if err := store.Create(ctx, record); err != nil {
					t.Fatal(err)
				}
			}
			for _, edge := range tc.edges {
				if err := store.AddRelation(ctx, edge.SourceID, schema.Relation{Predicate: edge.Predicate, TargetID: edge.TargetID, Weight: edge.Weight}); err != nil {
					t.Fatal(err)
				}
			}
			trust := NewTrustContext(schema.SensitivityLow, true, "reader", nil)
			resp, err := NewService(store, nil).RetrieveGraph(ctx, &RetrieveGraphRequest{
				TaskDescriptor: "Orchid", Trust: trust, MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
				RootLimit: max(1, tc.rootLimit), NodeLimit: 3, EdgeLimit: 8, MaxHops: 2,
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(resp.Nodes) != tc.wantNodes || len(resp.Edges) != tc.wantEdges {
				t.Fatalf("nodes/edges = %d/%d, want %d/%d: %+v", len(resp.Nodes), len(resp.Edges), tc.wantNodes, tc.wantEdges, resp)
			}
			for _, node := range resp.Nodes {
				if !trust.Allows(node.Record) && (node.Record.Payload != nil || node.Record.Interpretation != nil || len(node.Record.Relations) != 0) {
					t.Fatalf("redacted node exposes content: %+v", node.Record)
				}
			}
			for _, edge := range resp.Edges {
				if edge.Predicate == "private_relation" {
					t.Fatalf("graph exposes redacted source relationship: %+v", edge)
				}
			}
			for _, record := range records {
				if trust.Allows(record) {
					continue
				}
				for _, id := range store.lookupIDs {
					if id == record.ID {
						t.Fatalf("graph ranking or expansion hydrated redacted record %s relationships", id)
					}
				}
			}
		})
	}
}
