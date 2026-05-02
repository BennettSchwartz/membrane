package retrieval

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
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

type countingGetStore struct {
	*sqlitestore.SQLiteStore
	gets int
}

func (s *countingGetStore) Get(ctx context.Context, id string) (*schema.MemoryRecord, error) {
	s.gets++
	return s.SQLiteStore.Get(ctx, id)
}

type graphStoreWithoutLookup struct {
	storage.Store
}

type graphLookupStore struct {
	storage.Store
	matchesByScope map[string][]*schema.MemoryRecord
	errScopes      map[string]error
	limits         []int
}

func (s *graphLookupStore) FindEntitiesByTerm(_ context.Context, _ string, scope string, limit int) ([]*schema.MemoryRecord, error) {
	s.limits = append(s.limits, limit)
	if err := s.errScopes[scope]; err != nil {
		return nil, err
	}
	return s.matchesByScope[scope], nil
}

func (*graphLookupStore) FindEntityByIdentifier(context.Context, string, string, string) (*schema.MemoryRecord, error) {
	return nil, storage.ErrNotFound
}

type relationErrorStore struct {
	*sqlitestore.SQLiteStore
	err error
}

func (s *relationErrorStore) GetRelations(context.Context, string) ([]schema.Relation, error) {
	return nil, s.err
}

type missingTargetGetStore struct {
	*sqlitestore.SQLiteStore
	missingID string
}

func (s *missingTargetGetStore) Get(ctx context.Context, id string) (*schema.MemoryRecord, error) {
	if id == s.missingID {
		return nil, storage.ErrNotFound
	}
	return s.SQLiteStore.Get(ctx, id)
}

type duplicateRelationStore struct {
	*sqlitestore.SQLiteStore
	rels map[string][]schema.Relation
}

func (s *duplicateRelationStore) GetRelations(ctx context.Context, id string) ([]schema.Relation, error) {
	if rels, ok := s.rels[id]; ok {
		return rels, nil
	}
	return s.SQLiteStore.GetRelations(ctx, id)
}

func TestRetrieveGraphReranksEntityRootsAndExpandsNeighbors(t *testing.T) {
	svc, store := newGraphTestService(t)
	ctx := context.Background()
	now := time.Date(2026, 4, 8, 12, 0, 0, 0, time.UTC)

	entity := schema.NewMemoryRecord("entity-1", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Orchid",
		PrimaryType:   schema.EntityTypeProject,
		Types:         []string{schema.EntityTypeProject},
		Aliases:       []schema.EntityAlias{{Value: "staging deploy target"}},
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

func TestRetrieveGraphEdgeLimitDuplicateAndHopOrderingBranches(t *testing.T) {
	_, base := newGraphTestService(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 13, 30, 0, 0, time.UTC)

	root := newSemanticRetrievalRecord("graph-branch-root", 1.0, schema.SensitivityLow)
	first := newSemanticRetrievalRecord("graph-branch-first", 0.9, schema.SensitivityLow)
	second := newSemanticRetrievalRecord("graph-branch-second", 0.8, schema.SensitivityLow)
	for _, rec := range []*schema.MemoryRecord{root, first, second} {
		if err := base.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}
	if err := base.AddRelation(ctx, root.ID, schema.Relation{Predicate: "related", TargetID: first.ID, Weight: 1, CreatedAt: now}); err != nil {
		t.Fatalf("AddRelation root->first: %v", err)
	}
	if err := base.AddRelation(ctx, root.ID, schema.Relation{Predicate: "supports", TargetID: second.ID, Weight: 1, CreatedAt: now}); err != nil {
		t.Fatalf("AddRelation root->second: %v", err)
	}

	limited, err := NewService(base, nil).RetrieveGraph(ctx, &RetrieveGraphRequest{
		Trust:       NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		RootLimit:   1,
		NodeLimit:   3,
		EdgeLimit:   1,
		MaxHops:     1,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph limited: %v", err)
	}
	if len(limited.Edges) != 1 {
		t.Fatalf("limited edges = %+v, want one edge", limited.Edges)
	}

	duplicateSvc := NewService(&duplicateRelationStore{
		SQLiteStore: base,
		rels: map[string][]schema.Relation{
			root.ID: {
				{Predicate: "related", TargetID: first.ID, Weight: 1, CreatedAt: now},
				{Predicate: "related", TargetID: first.ID, Weight: 1, CreatedAt: now},
			},
		},
	}, nil)
	deduped, err := duplicateSvc.RetrieveGraph(ctx, &RetrieveGraphRequest{
		Trust:       NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		RootLimit:   1,
		NodeLimit:   3,
		EdgeLimit:   3,
		MaxHops:     1,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph duplicate: %v", err)
	}
	if len(deduped.Edges) != 1 {
		t.Fatalf("deduped edges = %+v, want one edge after duplicate suppression", deduped.Edges)
	}

	leaf := newSemanticRetrievalRecord("graph-branch-leaf", 0.7, schema.SensitivityLow)
	if err := base.Create(ctx, leaf); err != nil {
		t.Fatalf("Create leaf: %v", err)
	}
	if err := base.AddRelation(ctx, first.ID, schema.Relation{Predicate: "related", TargetID: leaf.ID, Weight: 1, CreatedAt: now}); err != nil {
		t.Fatalf("AddRelation first->leaf: %v", err)
	}
	multihop, err := NewService(base, nil).RetrieveGraph(ctx, &RetrieveGraphRequest{
		Trust:       NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		RootLimit:   1,
		NodeLimit:   4,
		EdgeLimit:   4,
		MaxHops:     2,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph multihop: %v", err)
	}
	if len(multihop.Nodes) < 3 || multihop.Nodes[1].Hop > multihop.Nodes[2].Hop {
		t.Fatalf("multihop nodes = %+v, want lower-hop neighbors sorted first", multihop.Nodes)
	}
}

func TestRetrieveGraphGuardsDefaultsAndRelationErrors(t *testing.T) {
	svc, base := newGraphTestService(t)
	ctx := context.Background()

	if _, err := svc.RetrieveGraph(ctx, nil); !errors.Is(err, ErrNilTrust) {
		t.Fatalf("RetrieveGraph nil request error = %v, want ErrNilTrust", err)
	}

	first := newSemanticRetrievalRecord("graph-limit-first", 0.9, schema.SensitivityLow)
	second := newSemanticRetrievalRecord("graph-limit-second", 0.8, schema.SensitivityLow)
	for _, rec := range []*schema.MemoryRecord{first, second} {
		if err := base.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	limited, err := svc.RetrieveGraph(ctx, &RetrieveGraphRequest{
		Trust:       NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		RootLimit:   2,
		NodeLimit:   1,
		MaxHops:     -1,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph limited defaults: %v", err)
	}
	if len(limited.RootIDs) != 1 || len(limited.Nodes) != 1 {
		t.Fatalf("limited graph roots/nodes = %v/%d, want one root and one node", limited.RootIDs, len(limited.Nodes))
	}
	if len(limited.Edges) != 0 {
		t.Fatalf("limited graph edges = %+v, want none when max hops clamps to zero", limited.Edges)
	}

	relationErrSvc := NewService(&relationErrorStore{SQLiteStore: base, err: errors.New("relations failed")}, nil)
	resp, err := relationErrSvc.RetrieveGraph(ctx, &RetrieveGraphRequest{
		Trust:       NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		RootLimit:   1,
		NodeLimit:   2,
		MaxHops:     1,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph relation error: %v", err)
	}
	if len(resp.Nodes) != 1 || len(resp.Edges) != 0 {
		t.Fatalf("relation-error graph nodes/edges = %d/%+v, want root only and no edges", len(resp.Nodes), resp.Edges)
	}

	errorSvc := NewService(&failingRetrievalStore{listErr: errors.New("list failed")}, nil)
	if _, err := errorSvc.RetrieveGraph(ctx, &RetrieveGraphRequest{
		Trust: NewTrustContext(schema.SensitivityLow, true, "tester", nil),
	}); err == nil || !errors.Is(err, errorSvc.store.(*failingRetrievalStore).listErr) {
		t.Fatalf("RetrieveGraph retrieve error = %v, want list error", err)
	}
}

func TestRetrieveGraphExpansionSkipsMissingDeniedDuplicateAndLimitedEdges(t *testing.T) {
	_, store := newGraphTestService(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 13, 0, 0, 0, time.UTC)

	root := newSemanticRetrievalRecord("graph-root", 1.0, schema.SensitivityLow)
	allowed := newSemanticRetrievalRecord("graph-allowed", 0.8, schema.SensitivityLow)
	denied := newSemanticRetrievalRecord("graph-denied", 0.7, schema.SensitivityHyper)
	missingTarget := schema.NewMemoryRecord("graph-missing-target", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "missing",
	})
	for _, rec := range []*schema.MemoryRecord{root, allowed, denied, missingTarget} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}
	for _, rel := range []schema.Relation{
		{Predicate: "related", TargetID: missingTarget.ID, Weight: 1, CreatedAt: now},
		{Predicate: "related", TargetID: denied.ID, Weight: 1, CreatedAt: now},
		{Predicate: "related", TargetID: allowed.ID, Weight: 1, CreatedAt: now},
		{Predicate: "related", TargetID: allowed.ID, Weight: 1, CreatedAt: now},
	} {
		if err := store.AddRelation(ctx, root.ID, rel); err != nil {
			t.Fatalf("AddRelation: %v", err)
		}
	}
	svc := NewService(&missingTargetGetStore{SQLiteStore: store, missingID: missingTarget.ID}, nil)

	resp, err := svc.RetrieveGraph(ctx, &RetrieveGraphRequest{
		Trust:       NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		RootLimit:   1,
		NodeLimit:   2,
		EdgeLimit:   1,
		MaxHops:     1,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph: %v", err)
	}
	if len(resp.Edges) != 1 || resp.Edges[0].TargetID != allowed.ID {
		t.Fatalf("Edges = %+v, want single allowed edge", resp.Edges)
	}
	if len(resp.Nodes) != 2 {
		t.Fatalf("Nodes len = %d, want root plus allowed target", len(resp.Nodes))
	}

	deduped, err := svc.RetrieveGraph(ctx, &RetrieveGraphRequest{
		Trust:       NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		RootLimit:   1,
		NodeLimit:   2,
		EdgeLimit:   4,
		MaxHops:     1,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph dedupe: %v", err)
	}
	if len(deduped.Edges) != 1 || deduped.Edges[0].TargetID != allowed.ID {
		t.Fatalf("deduped edges = %+v, want one allowed edge", deduped.Edges)
	}

	nodeLimited, err := svc.RetrieveGraph(ctx, &RetrieveGraphRequest{
		Trust:       NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		RootLimit:   1,
		NodeLimit:   1,
		EdgeLimit:   4,
		MaxHops:     1,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph node-limited: %v", err)
	}
	if len(nodeLimited.Nodes) != 1 || len(nodeLimited.Edges) != 0 {
		t.Fatalf("node-limited graph nodes/edges = %d/%+v, want root only", len(nodeLimited.Nodes), nodeLimited.Edges)
	}
}

func TestEntityRootCandidatesGuardsAndTrustFiltering(t *testing.T) {
	_, base := newGraphTestService(t)
	ctx := context.Background()
	allowed := schema.NewMemoryRecord("entity-allowed", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Orchid",
		PrimaryType:   schema.EntityTypeProject,
	})
	allowed.Scope = "project:alpha"
	redacted := schema.NewMemoryRecord("entity-redacted", schema.MemoryTypeEntity, schema.SensitivityMedium, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Sensitive Orchid",
		PrimaryType:   schema.EntityTypeProject,
	})
	redacted.Scope = "project:alpha"
	filtered := schema.NewMemoryRecord("entity-filtered", schema.MemoryTypeEntity, schema.SensitivityHigh, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Hidden Orchid",
		PrimaryType:   schema.EntityTypeProject,
	})
	filtered.Scope = "project:alpha"

	noLookupSvc := NewService(&graphStoreWithoutLookup{Store: base}, nil)
	if got := noLookupSvc.entityRootCandidates(ctx, &RetrieveGraphRequest{
		TaskDescriptor: "Orchid",
		Trust:          NewTrustContext(schema.SensitivityLow, true, "tester", nil),
	}); got != nil {
		t.Fatalf("entityRootCandidates without lookup = %+v, want nil", got)
	}

	lookupStore := &graphLookupStore{
		Store: base,
		matchesByScope: map[string][]*schema.MemoryRecord{
			"project:alpha": {allowed, redacted, filtered, nil},
		},
		errScopes: map[string]error{"project:error": errors.New("lookup failed")},
	}
	svc := NewService(lookupStore, nil)

	for _, req := range []*RetrieveGraphRequest{
		nil,
		{TaskDescriptor: "  ", Trust: NewTrustContext(schema.SensitivityLow, true, "tester", nil)},
		{TaskDescriptor: "Orchid", Trust: NewTrustContext(schema.SensitivityLow, true, "tester", nil), MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic}},
	} {
		if got := svc.entityRootCandidates(ctx, req); got != nil {
			t.Fatalf("guarded entityRootCandidates(%+v) = %+v, want nil", req, got)
		}
	}

	got := svc.entityRootCandidates(ctx, &RetrieveGraphRequest{
		TaskDescriptor: "Orchid",
		Trust:          NewTrustContext(schema.SensitivityLow, true, "tester", []string{"project:error", "project:alpha"}),
		MemoryTypes:    []schema.MemoryType{schema.MemoryTypeEntity},
		RootLimit:      7,
	})
	if len(got) != 2 {
		t.Fatalf("entityRootCandidates len = %d, want allowed plus redacted records: %+v", len(got), got)
	}
	if got[0].ID != allowed.ID || got[0].Payload == nil {
		t.Fatalf("allowed candidate = %+v, want unredacted %s", got[0], allowed.ID)
	}
	if got[1].ID != redacted.ID || got[1].Payload != nil {
		t.Fatalf("redacted candidate = %+v, want redacted %s", got[1], redacted.ID)
	}
	if len(lookupStore.limits) == 0 || lookupStore.limits[len(lookupStore.limits)-1] != 7 {
		t.Fatalf("lookup limits = %v, want RootLimit passed through", lookupStore.limits)
	}
}

func TestRetrieveGraphSkipsFetchForKnownTargetNodes(t *testing.T) {
	_, base := newGraphTestService(t)
	store := &countingGetStore{SQLiteStore: base}
	svc := NewService(store, nil)
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 13, 0, 0, 0, time.UTC)

	first := newSemanticRetrievalRecord("known-first", 0.9, schema.SensitivityLow)
	second := newSemanticRetrievalRecord("known-second", 0.8, schema.SensitivityLow)
	for _, rec := range []*schema.MemoryRecord{first, second} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}
	if err := store.AddRelation(ctx, first.ID, schema.Relation{Predicate: "related", TargetID: second.ID, Weight: 1, CreatedAt: now}); err != nil {
		t.Fatalf("AddRelation first->second: %v", err)
	}
	if err := store.AddRelation(ctx, second.ID, schema.Relation{Predicate: "related", TargetID: first.ID, Weight: 1, CreatedAt: now}); err != nil {
		t.Fatalf("AddRelation second->first: %v", err)
	}

	resp, err := svc.RetrieveGraph(ctx, &RetrieveGraphRequest{
		Trust:       NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		RootLimit:   2,
		NodeLimit:   2,
		EdgeLimit:   4,
		MaxHops:     1,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph: %v", err)
	}
	if len(resp.Edges) != 2 {
		t.Fatalf("Edges = %+v, want two known-target edges", resp.Edges)
	}
	if store.gets != 0 {
		t.Fatalf("store.Get calls = %d, want 0 for targets already in node set", store.gets)
	}
}

func TestRetrieveGraphUsesPrecomputedQueryEmbedding(t *testing.T) {
	_, store := newGraphTestService(t)
	ctx := context.Background()

	first := newSemanticRetrievalRecord("graph-vector-first", 0.1, schema.SensitivityLow)
	second := newSemanticRetrievalRecord("graph-vector-second", 0.9, schema.SensitivityLow)
	for _, rec := range []*schema.MemoryRecord{first, second} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	embedding := &fakeEmbeddingService{vector: []float32{9, 9}}
	ranker := &fakeVectorRanker{ids: []string{first.ID, second.ID}}
	svc := NewServiceWithVectorRanker(store, nil, embedding, ranker)

	resp, err := svc.RetrieveGraph(ctx, &RetrieveGraphRequest{
		TaskDescriptor: "semantic graph vector query",
		QueryEmbedding: []float32{4, 5, 6},
		Trust:          NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes:    []schema.MemoryType{schema.MemoryTypeSemantic},
		RootLimit:      2,
		NodeLimit:      2,
		MaxHops:        0,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph: %v", err)
	}
	if got := resp.RootIDs; len(got) != 2 || got[0] != first.ID || got[1] != second.ID {
		t.Fatalf("RootIDs = %v, want vector-ranked roots [%s %s]", got, first.ID, second.ID)
	}
	if embedding.calls != 0 {
		t.Fatalf("EmbedQuery calls = %d, want 0 for precomputed embedding", embedding.calls)
	}
	if len(ranker.queries) != 1 || len(ranker.queries[0]) != 3 || ranker.queries[0][0] != 4 {
		t.Fatalf("ranker queries = %#v, want precomputed embedding", ranker.queries)
	}
}

func TestRetrieveGraphRedactsNeighborPayloadAndInterpretation(t *testing.T) {
	svc, store := newGraphTestService(t)
	ctx := context.Background()
	now := time.Date(2026, 4, 8, 12, 0, 0, 0, time.UTC)

	entity := schema.NewMemoryRecord("entity-2", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Orchid",
		PrimaryType:   schema.EntityTypeProject,
		Types:         []string{schema.EntityTypeProject},
		Aliases:       []schema.EntityAlias{{Value: "orchid"}},
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

func TestRetrieveGraphBoostsSemanticRootsThroughLinkedEntity(t *testing.T) {
	svc, store := newGraphTestService(t)
	ctx := context.Background()
	now := time.Date(2026, 4, 8, 12, 0, 0, 0, time.UTC)

	entity := schema.NewMemoryRecord("entity-3", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Orchid",
		PrimaryType:   schema.EntityTypeProject,
		Types:         []string{schema.EntityTypeProject},
		Aliases:       []schema.EntityAlias{{Value: "orchid deploy target"}},
		Summary:       "Orchid project entity",
	})
	entity.CreatedAt = now
	entity.UpdatedAt = now
	if err := store.Create(ctx, entity); err != nil {
		t.Fatalf("Create entity: %v", err)
	}

	semantic := schema.NewMemoryRecord("semantic-entity-linked", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   entity.ID,
		Predicate: "deploy_target_for",
		Object:    "staging",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	semantic.CreatedAt = now
	semantic.UpdatedAt = now
	semantic.Salience = 0.2
	if err := store.Create(ctx, semantic); err != nil {
		t.Fatalf("Create semantic: %v", err)
	}
	if err := store.AddRelation(ctx, semantic.ID, schema.Relation{TargetID: entity.ID, Predicate: "subject_entity", Weight: 1.0, CreatedAt: now}); err != nil {
		t.Fatalf("AddRelation semantic->entity: %v", err)
	}
	if err := store.AddRelation(ctx, entity.ID, schema.Relation{TargetID: semantic.ID, Predicate: "fact_subject_of", Weight: 1.0, CreatedAt: now}); err != nil {
		t.Fatalf("AddRelation entity->semantic: %v", err)
	}

	unrelated := schema.NewMemoryRecord("semantic-unrelated", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "billing",
		Predicate: "owned_by",
		Object:    "finance",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	unrelated.CreatedAt = now
	unrelated.UpdatedAt = now
	unrelated.Salience = 1.0
	if err := store.Create(ctx, unrelated); err != nil {
		t.Fatalf("Create unrelated semantic: %v", err)
	}

	resp, err := svc.RetrieveGraph(ctx, &RetrieveGraphRequest{
		TaskDescriptor: "Orchid deploy target",
		Trust:          NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes:    []schema.MemoryType{schema.MemoryTypeSemantic},
		RootLimit:      1,
		NodeLimit:      3,
		EdgeLimit:      4,
		MaxHops:        1,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph: %v", err)
	}

	if len(resp.RootIDs) != 1 || resp.RootIDs[0] != semantic.ID {
		t.Fatalf("RootIDs = %v, want [%s]", resp.RootIDs, semantic.ID)
	}
	foundEntityNeighbor := false
	for _, node := range resp.Nodes {
		if node.Record != nil && node.Record.ID == entity.ID && node.Hop == 1 {
			foundEntityNeighbor = true
			break
		}
	}
	if !foundEntityNeighbor {
		t.Fatalf("Nodes = %+v, want linked entity neighbor", resp.Nodes)
	}
}

func TestRootBoostAndRelatedEntityEdgeCases(t *testing.T) {
	svc, store := newGraphTestService(t)
	ctx := context.Background()
	trust := NewTrustContext(schema.SensitivityLow, true, "tester", nil)

	target := schema.NewMemoryRecord("entity-target", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Borealis",
		PrimaryType:   schema.EntityTypeProject,
		Types:         []string{schema.EntityTypeProject},
		Aliases:       []schema.EntityAlias{{Value: "compute cluster"}},
	})
	nonEntity := newSemanticRetrievalRecord("semantic-target", 0.5, schema.SensitivityLow)
	denied := schema.NewMemoryRecord("entity-denied", schema.MemoryTypeEntity, schema.SensitivityHigh, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Secret",
		PrimaryType:   schema.EntityTypeProject,
	})
	for _, rec := range []*schema.MemoryRecord{target, nonEntity, denied} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	rec := schema.NewMemoryRecord("root", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Orchid",
		PrimaryType:   schema.EntityTypeProject,
		Types:         []string{schema.EntityTypeProject},
		Aliases:       []schema.EntityAlias{{Value: "deploy target"}},
	})
	rec.Interpretation = &schema.Interpretation{Mentions: []schema.Mention{{
		Surface: "rollout",
		Aliases: []string{"flower service"},
	}}}
	rec.Relations = []schema.Relation{{Predicate: "supports", TargetID: target.ID}}

	cache := map[string]*schema.EntityPayload{}
	if got := svc.rootBoost(ctx, nil, "orchid", trust, cache); got != 0 {
		t.Fatalf("nil rootBoost = %v, want 0", got)
	}
	score := svc.rootBoost(ctx, rec, "orchid deploy target project rollout flower borealis compute cluster", trust, cache)
	if score < 340 {
		t.Fatalf("rootBoost score = %v, want entity, mention, and relation boosts", score)
	}
	aliasMention := &schema.MemoryRecord{
		Interpretation: &schema.Interpretation{Mentions: []schema.Mention{{
			Surface: "unmatched",
			Aliases: []string{"flower service"},
		}}},
	}
	if got := svc.rootBoost(ctx, aliasMention, "flower service", trust, cache); got < 20 {
		t.Fatalf("alias mention rootBoost = %v, want alias boost", got)
	}
	if got := svc.relatedEntity(ctx, target.ID, trust, cache); got == nil || got.CanonicalName != "Borealis" {
		t.Fatalf("relatedEntity cached target = %+v, want Borealis", got)
	}
	if got := svc.relatedEntity(ctx, "", trust, cache); got != nil {
		t.Fatalf("relatedEntity empty id = %+v, want nil", got)
	}
	if got := svc.relatedEntity(ctx, "missing", trust, cache); got != nil {
		t.Fatalf("relatedEntity missing = %+v, want nil", got)
	}
	if got := svc.relatedEntity(ctx, "missing", trust, cache); got != nil {
		t.Fatalf("relatedEntity cached missing = %+v, want nil", got)
	}
	if got := svc.relatedEntity(ctx, nonEntity.ID, trust, cache); got != nil {
		t.Fatalf("relatedEntity non-entity = %+v, want nil", got)
	}
	if got := svc.relatedEntity(ctx, denied.ID, trust, cache); got != nil {
		t.Fatalf("relatedEntity denied = %+v, want nil", got)
	}
	if got := svc.relatedEntity(ctx, target.ID, nil, map[string]*schema.EntityPayload{}); got == nil || got.CanonicalName != "Borealis" {
		t.Fatalf("relatedEntity nil trust = %+v, want Borealis", got)
	}
}

func TestUniqueRecordsSkipsNilAndDuplicates(t *testing.T) {
	first := &schema.MemoryRecord{ID: "first"}
	second := &schema.MemoryRecord{ID: "second"}
	got := uniqueRecords([]*schema.MemoryRecord{nil, first, second, first, nil})
	if len(got) != 2 || got[0] != first || got[1] != second {
		t.Fatalf("uniqueRecords = %+v, want first and second only", got)
	}
}
