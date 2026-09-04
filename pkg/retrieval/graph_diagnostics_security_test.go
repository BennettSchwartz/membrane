package retrieval

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/BennettSchwartz/membrane/internal/teststore"
	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

type diagnosticNeighborStore struct {
	*teststore.MemoryStore
	id     string
	err    error
	absent bool
}

func (s *diagnosticNeighborStore) ListBounded(ctx context.Context, opts storage.ListOptions) (storage.BoundedListResult, error) {
	if opts.ID == s.id {
		if s.err != nil {
			return storage.BoundedListResult{}, s.err
		}
		if s.absent {
			return storage.BoundedListResult{}, nil
		}
	}
	return s.MemoryStore.ListBounded(ctx, opts)
}

func TestRetrieveGraphDoesNotDiscloseHiddenIncomingNeighbors(t *testing.T) {
	const hiddenID = "private-neighbor-id"
	for _, tc := range []struct {
		name        string
		sensitivity schema.Sensitivity
		scope       string
		absent      bool
		err         error
		diagnostic  bool
	}{
		{name: "high", sensitivity: schema.SensitivityHigh, scope: "allowed"},
		{name: "hyper", sensitivity: schema.SensitivityHyper, scope: "allowed"},
		{name: "wrong_scope", sensitivity: schema.SensitivityLow, scope: "private"},
		{name: "absent", sensitivity: schema.SensitivityLow, scope: "allowed", absent: true},
		{name: "not_found_error", sensitivity: schema.SensitivityLow, scope: "allowed", err: fmt.Errorf("%s: %w", hiddenID, storage.ErrNotFound)},
		{name: "access_denied_error", sensitivity: schema.SensitivityLow, scope: "allowed", err: fmt.Errorf("%s: %w", hiddenID, ErrAccessDenied)},
		{name: "operational_error", sensitivity: schema.SensitivityLow, scope: "allowed", err: fmt.Errorf("database lookup for %s: private database detail", hiddenID), diagnostic: true},
		{name: "redacted_source_relationship", sensitivity: schema.SensitivityMedium, scope: "allowed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			_, base := newGraphTestService(t)
			root := newSemanticRetrievalRecord("visible-root", 1, schema.SensitivityLow)
			root.Scope = "allowed"
			neighbor := newSemanticRetrievalRecord(hiddenID, 0.1, tc.sensitivity)
			neighbor.Scope = tc.scope
			for _, rec := range []*schema.MemoryRecord{root, neighbor} {
				if err := base.Create(ctx, rec); err != nil {
					t.Fatal(err)
				}
			}
			if err := base.AddRelation(ctx, neighbor.ID, schema.Relation{Predicate: "supports", TargetID: root.ID, Weight: 1}); err != nil {
				t.Fatal(err)
			}
			store := &diagnosticNeighborStore{MemoryStore: base, id: hiddenID, err: tc.err, absent: tc.absent}
			resp, err := NewService(store, nil).RetrieveGraph(ctx, &RetrieveGraphRequest{
				Trust:       NewTrustContext(schema.SensitivityLow, true, "reader", []string{"allowed"}),
				MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
				RootLimit:   1, NodeLimit: 2, EdgeLimit: 4, MaxHops: 1,
			})
			if err != nil {
				t.Fatal(err)
			}
			diagnostic := diagnosticByCode(resp.Diagnostics, DiagnosticGraphExpandFailed)
			if (diagnostic != nil) != tc.diagnostic {
				t.Fatalf("diagnostics = %+v, want operational diagnostic %t", resp.Diagnostics, tc.diagnostic)
			}
			if len(resp.Nodes) != 1 || len(resp.Edges) != 0 {
				t.Fatalf("nodes/edges = %d/%d, want visible root only", len(resp.Nodes), len(resp.Edges))
			}
			encoded, err := json.Marshal(resp)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(encoded), hiddenID) || strings.Contains(string(encoded), "private database detail") {
				t.Fatalf("response discloses private neighbor details: %s", encoded)
			}
		})
	}
}
