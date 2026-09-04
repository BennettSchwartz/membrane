package retrieval

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/internal/teststore"
	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

func newGraphTestService(t *testing.T) (*Service, *teststore.MemoryStore) {
	t.Helper()

	store := teststore.NewMemoryStore()
	t.Cleanup(func() { _ = store.Close() })
	return NewService(store, nil), store
}

func diagnosticByCode(diagnostics []RetrievalDiagnostic, code string) *RetrievalDiagnostic {
	for i := range diagnostics {
		if diagnostics[i].Code == code {
			return &diagnostics[i]
		}
	}
	return nil
}

type countingGetStore struct {
	*teststore.MemoryStore
	gets         int
	exactGets    int
	relationGets int
}

type boundedNeighborStore struct {
	*teststore.MemoryStore
	neighborID      string
	exactResult     *storage.BoundedListResult
	exactResults    map[string]storage.BoundedListResult
	exactCalls      []storage.ListOptions
	legacyGraphGets int
}

func (s *boundedNeighborStore) ListBounded(ctx context.Context, opts storage.ListOptions) (storage.BoundedListResult, error) {
	if opts.ID != "" {
		s.exactCalls = append(s.exactCalls, opts)
		if result, ok := s.exactResults[opts.ID]; ok {
			return result, nil
		}
	}
	if opts.ID == s.neighborID {
		if s.exactResult != nil {
			return *s.exactResult, nil
		}
	}
	return s.MemoryStore.ListBounded(ctx, opts)
}

func (s *boundedNeighborStore) GetGraphRecord(ctx context.Context, id string) (*schema.MemoryRecord, error) {
	s.legacyGraphGets++
	return s.MemoryStore.GetGraphRecord(ctx, id)
}

func (s *countingGetStore) Get(ctx context.Context, id string) (*schema.MemoryRecord, error) {
	s.gets++
	return s.MemoryStore.Get(ctx, id)
}

func (s *countingGetStore) GetGraphRecord(ctx context.Context, id string) (*schema.MemoryRecord, error) {
	s.gets++
	return s.MemoryStore.GetGraphRecord(ctx, id)
}

func (s *countingGetStore) ListBounded(ctx context.Context, opts storage.ListOptions) (storage.BoundedListResult, error) {
	if opts.ID != "" {
		s.exactGets++
	}
	return s.MemoryStore.ListBounded(ctx, opts)
}

func (s *countingGetStore) GetRelations(ctx context.Context, id string) ([]schema.Relation, error) {
	s.relationGets++
	return s.MemoryStore.GetRelations(ctx, id)
}

func (s *countingGetStore) List(ctx context.Context, opts storage.ListOptions) ([]*schema.MemoryRecord, error) {
	// Exercise compatibility with stores that ignore the additive graph
	// hydration hint and return relations inline.
	opts.OmitRelations = false
	return s.MemoryStore.List(ctx, opts)
}

type graphStoreWithoutLookup struct {
	storage.Store
}

func (s *graphStoreWithoutLookup) ListBounded(ctx context.Context, opts storage.ListOptions) (storage.BoundedListResult, error) {
	return s.Store.(storage.BoundedListStore).ListBounded(ctx, opts)
}

type unboundedGraphStore struct {
	storage.Store
	gets         int
	relationGets int
}

func (s *unboundedGraphStore) ListBounded(ctx context.Context, opts storage.ListOptions) (storage.BoundedListResult, error) {
	return s.Store.(storage.BoundedListStore).ListBounded(ctx, opts)
}

func (s *unboundedGraphStore) Get(ctx context.Context, id string) (*schema.MemoryRecord, error) {
	s.gets++
	return s.Store.Get(ctx, id)
}

func (s *unboundedGraphStore) GetRelations(ctx context.Context, id string) ([]schema.Relation, error) {
	s.relationGets++
	return s.Store.GetRelations(ctx, id)
}

type legacyEntityOnlyStore struct {
	storage.Store
	termCalls int
}

func (s *legacyEntityOnlyStore) ListBounded(ctx context.Context, opts storage.ListOptions) (storage.BoundedListResult, error) {
	return s.Store.(storage.BoundedListStore).ListBounded(ctx, opts)
}

func (s *legacyEntityOnlyStore) FindEntitiesByTerm(context.Context, string, string, int) ([]*schema.MemoryRecord, error) {
	s.termCalls++
	return nil, nil
}

func (s *legacyEntityOnlyStore) FindEntityByIdentifier(context.Context, string, string, string) (*schema.MemoryRecord, error) {
	s.termCalls++
	return nil, storage.ErrNotFound
}

type graphLookupStore struct {
	storage.Store
	matchesByScope          map[string][]*schema.MemoryRecord
	allScopeMatches         []*schema.MemoryRecord
	errScopes               map[string]error
	identifierMatches       map[string]*schema.MemoryRecord
	allScopeIdentifiers     map[string]*schema.MemoryRecord
	limits                  []int
	allScopeLimits          []int
	identifierCalls         []string
	allScopeIdentifierCalls []string
	hydrationBudgets        []int64
	boundedProjectedBytes   int64
}

func (s *graphLookupStore) ListBounded(ctx context.Context, opts storage.ListOptions) (storage.BoundedListResult, error) {
	return s.Store.(storage.BoundedListStore).ListBounded(ctx, opts)
}

func (s *graphLookupStore) FindEntitiesByTerm(_ context.Context, _ string, scope string, limit int) ([]*schema.MemoryRecord, error) {
	s.limits = append(s.limits, limit)
	if err := s.errScopes[scope]; err != nil {
		return nil, err
	}
	return s.matchesByScope[scope], nil
}

func (s *graphLookupStore) FindEntityByIdentifier(_ context.Context, namespace, value, scope string) (*schema.MemoryRecord, error) {
	key := namespace + "\x00" + value + "\x00" + scope
	s.identifierCalls = append(s.identifierCalls, key)
	if rec := s.identifierMatches[key]; rec != nil {
		return rec, nil
	}
	return nil, storage.ErrNotFound
}

func (s *graphLookupStore) FindGraphEntitiesByTerm(ctx context.Context, term, scope string, limit int) ([]*schema.MemoryRecord, error) {
	return s.FindEntitiesByTerm(ctx, term, scope, limit)
}

func (s *graphLookupStore) FindGraphEntityByIdentifier(ctx context.Context, namespace, value, scope string) (*schema.MemoryRecord, error) {
	return s.FindEntityByIdentifier(ctx, namespace, value, scope)
}

func (s *graphLookupStore) FindGraphEntitiesByTermBounded(ctx context.Context, term, scope string, limit int, maxBytes int64) (storage.BoundedGraphEntityResult, error) {
	s.hydrationBudgets = append(s.hydrationBudgets, maxBytes)
	records, err := s.FindGraphEntitiesByTerm(ctx, term, scope, limit)
	return storage.BoundedGraphEntityResult{Records: records, ProjectedBytes: s.boundedProjectedBytes}, err
}

func (s *graphLookupStore) FindGraphEntityByIdentifierBounded(ctx context.Context, namespace, value, scope string, maxBytes int64) (storage.BoundedGraphEntityResult, error) {
	s.hydrationBudgets = append(s.hydrationBudgets, maxBytes)
	rec, err := s.FindGraphEntityByIdentifier(ctx, namespace, value, scope)
	if err != nil {
		return storage.BoundedGraphEntityResult{}, err
	}
	return storage.BoundedGraphEntityResult{Records: []*schema.MemoryRecord{rec}, ProjectedBytes: s.boundedProjectedBytes}, nil
}

func (s *graphLookupStore) FindEntitiesByTermAllScopes(_ context.Context, _ string, limit int) ([]*schema.MemoryRecord, error) {
	s.allScopeLimits = append(s.allScopeLimits, limit)
	return s.allScopeMatches, nil
}

func (s *graphLookupStore) FindEntityByIdentifierAllScopes(_ context.Context, namespace, value string) (*schema.MemoryRecord, error) {
	key := namespace + "\x00" + value
	s.allScopeIdentifierCalls = append(s.allScopeIdentifierCalls, key)
	if rec := s.allScopeIdentifiers[key]; rec != nil {
		return rec, nil
	}
	return nil, storage.ErrNotFound
}

func (s *graphLookupStore) FindGraphEntitiesByTermAllScopes(ctx context.Context, term string, limit int) ([]*schema.MemoryRecord, error) {
	return s.FindEntitiesByTermAllScopes(ctx, term, limit)
}

func (s *graphLookupStore) FindGraphEntityByIdentifierAllScopes(ctx context.Context, namespace, value string) (*schema.MemoryRecord, error) {
	return s.FindEntityByIdentifierAllScopes(ctx, namespace, value)
}

func (s *graphLookupStore) FindGraphEntitiesByTermAllScopesBounded(ctx context.Context, term string, limit int, maxBytes int64) (storage.BoundedGraphEntityResult, error) {
	s.hydrationBudgets = append(s.hydrationBudgets, maxBytes)
	records, err := s.FindGraphEntitiesByTermAllScopes(ctx, term, limit)
	return storage.BoundedGraphEntityResult{Records: records, ProjectedBytes: s.boundedProjectedBytes}, err
}

func (s *graphLookupStore) FindGraphEntityByIdentifierAllScopesBounded(ctx context.Context, namespace, value string, maxBytes int64) (storage.BoundedGraphEntityResult, error) {
	s.hydrationBudgets = append(s.hydrationBudgets, maxBytes)
	rec, err := s.FindGraphEntityByIdentifierAllScopes(ctx, namespace, value)
	if err != nil {
		return storage.BoundedGraphEntityResult{}, err
	}
	return storage.BoundedGraphEntityResult{Records: []*schema.MemoryRecord{rec}, ProjectedBytes: s.boundedProjectedBytes}, nil
}

type relationErrorStore struct {
	*teststore.MemoryStore
	err error
}

func (s *relationErrorStore) GetRelations(context.Context, string) ([]schema.Relation, error) {
	return nil, s.err
}

func (s *relationErrorStore) GetRelationsLimited(context.Context, string, int) ([]schema.Relation, error) {
	return nil, s.err
}

func (s *relationErrorStore) GetRelationsBounded(context.Context, string, int, int64) (storage.BoundedRelationResult, error) {
	return storage.BoundedRelationResult{}, s.err
}

type incomingRelationErrorStore struct {
	*teststore.MemoryStore
	err error
}

func (s *incomingRelationErrorStore) GetIncomingRelations(context.Context, string) ([]schema.GraphEdge, error) {
	return nil, s.err
}

func (s *incomingRelationErrorStore) GetIncomingRelationsLimited(context.Context, string, int) ([]schema.GraphEdge, error) {
	return nil, s.err
}

func (s *incomingRelationErrorStore) GetIncomingRelationsBounded(context.Context, string, int, int64) (storage.BoundedIncomingRelationResult, error) {
	return storage.BoundedIncomingRelationResult{}, s.err
}

type missingTargetGetStore struct {
	*teststore.MemoryStore
	missingID string
}

func (s *missingTargetGetStore) Get(ctx context.Context, id string) (*schema.MemoryRecord, error) {
	if id == s.missingID {
		return nil, storage.ErrNotFound
	}
	return s.MemoryStore.Get(ctx, id)
}

func (s *missingTargetGetStore) GetGraphRecord(ctx context.Context, id string) (*schema.MemoryRecord, error) {
	if id == s.missingID {
		return nil, storage.ErrNotFound
	}
	return s.MemoryStore.GetGraphRecord(ctx, id)
}

func (s *missingTargetGetStore) ListBounded(ctx context.Context, opts storage.ListOptions) (storage.BoundedListResult, error) {
	if opts.ID == s.missingID {
		return storage.BoundedListResult{Records: []*schema.MemoryRecord{}}, nil
	}
	return s.MemoryStore.ListBounded(ctx, opts)
}

type duplicateRelationStore struct {
	*teststore.MemoryStore
	rels map[string][]schema.Relation
}

func (s *duplicateRelationStore) GetRelations(ctx context.Context, id string) ([]schema.Relation, error) {
	if rels, ok := s.rels[id]; ok {
		return rels, nil
	}
	return s.MemoryStore.GetRelations(ctx, id)
}

func (s *duplicateRelationStore) GetRelationsLimited(ctx context.Context, id string, limit int) ([]schema.Relation, error) {
	if relations, ok := s.rels[id]; ok {
		relations = append([]schema.Relation(nil), relations...)
		prioritizeRelations(relations)
		if limit <= 0 {
			return []schema.Relation{}, nil
		}
		if len(relations) > limit {
			relations = relations[:limit]
		}
		return relations, nil
	}
	return s.MemoryStore.GetRelationsLimited(ctx, id, limit)
}

func (s *duplicateRelationStore) GetRelationsBounded(ctx context.Context, id string, limit int, maxBytes int64) (storage.BoundedRelationResult, error) {
	if relations, ok := s.rels[id]; ok {
		return sanitizeBoundedRelations(storage.BoundedRelationResult{Relations: relations}, limit, maxBytes), nil
	}
	return s.MemoryStore.GetRelationsBounded(ctx, id, limit, maxBytes)
}

func (s *duplicateRelationStore) List(ctx context.Context, opts storage.ListOptions) ([]*schema.MemoryRecord, error) {
	records, err := s.MemoryStore.List(ctx, opts)
	if err != nil {
		return nil, err
	}
	for _, rec := range records {
		if _, ok := s.rels[rec.ID]; ok {
			rec.Relations = nil
		}
	}
	return records, nil
}

func (s *duplicateRelationStore) ListByType(ctx context.Context, memType schema.MemoryType) ([]*schema.MemoryRecord, error) {
	return s.List(ctx, storage.ListOptions{Type: memType})
}

type boundedRelationStore struct {
	*teststore.MemoryStore
	outgoingLimits []int
	incomingLimits []int
	unboundedReads int
}

type countingEmptyGraphStore struct {
	*teststore.MemoryStore
	outgoingCalls int
	incomingCalls int
}

func (s *countingEmptyGraphStore) GetRelationsLimited(ctx context.Context, id string, limit int) ([]schema.Relation, error) {
	s.outgoingCalls++
	return s.MemoryStore.GetRelationsLimited(ctx, id, limit)
}

func (s *countingEmptyGraphStore) GetRelationsBounded(ctx context.Context, id string, limit int, maxBytes int64) (storage.BoundedRelationResult, error) {
	s.outgoingCalls++
	return s.MemoryStore.GetRelationsBounded(ctx, id, limit, maxBytes)
}

func (s *countingEmptyGraphStore) GetIncomingRelationsLimited(ctx context.Context, id string, limit int) ([]schema.GraphEdge, error) {
	s.incomingCalls++
	return s.MemoryStore.GetIncomingRelationsLimited(ctx, id, limit)
}

func (s *countingEmptyGraphStore) GetIncomingRelationsBounded(ctx context.Context, id string, limit int, maxBytes int64) (storage.BoundedIncomingRelationResult, error) {
	s.incomingCalls++
	return s.MemoryStore.GetIncomingRelationsBounded(ctx, id, limit, maxBytes)
}

func (s *boundedRelationStore) List(ctx context.Context, opts storage.ListOptions) ([]*schema.MemoryRecord, error) {
	records, err := s.MemoryStore.List(ctx, opts)
	if err != nil {
		return nil, err
	}
	for _, rec := range records {
		rec.Relations = nil
	}
	return records, nil
}

func (s *boundedRelationStore) GetRelations(ctx context.Context, id string) ([]schema.Relation, error) {
	s.unboundedReads++
	return s.MemoryStore.GetRelations(ctx, id)
}

func (s *boundedRelationStore) GetIncomingRelations(ctx context.Context, id string) ([]schema.GraphEdge, error) {
	s.unboundedReads++
	return s.MemoryStore.GetIncomingRelations(ctx, id)
}

func (s *boundedRelationStore) GetRelationsLimited(ctx context.Context, id string, limit int) ([]schema.Relation, error) {
	s.outgoingLimits = append(s.outgoingLimits, limit)
	relations, err := s.MemoryStore.GetRelations(ctx, id)
	if err != nil {
		return nil, err
	}
	prioritizeRelations(relations)
	if len(relations) > limit {
		relations = relations[:limit]
	}
	return relations, nil
}

func (s *boundedRelationStore) GetRelationsBounded(ctx context.Context, id string, limit int, maxBytes int64) (storage.BoundedRelationResult, error) {
	s.outgoingLimits = append(s.outgoingLimits, limit)
	return s.MemoryStore.GetRelationsBounded(ctx, id, limit, maxBytes)
}

func (s *boundedRelationStore) GetIncomingRelationsLimited(ctx context.Context, id string, limit int) ([]schema.GraphEdge, error) {
	s.incomingLimits = append(s.incomingLimits, limit)
	edges, err := s.MemoryStore.GetIncomingRelations(ctx, id)
	if err != nil {
		return nil, err
	}
	prioritizeGraphEdges(edges)
	if len(edges) > limit {
		edges = edges[:limit]
	}
	return edges, nil
}

func (s *boundedRelationStore) GetIncomingRelationsBounded(ctx context.Context, id string, limit int, maxBytes int64) (storage.BoundedIncomingRelationResult, error) {
	s.incomingLimits = append(s.incomingLimits, limit)
	return s.MemoryStore.GetIncomingRelationsBounded(ctx, id, limit, maxBytes)
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

func TestRetrieveGraphExpandsIncomingRelationsWhenReverseEdgeIsMissing(t *testing.T) {
	svc, store := newGraphTestService(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 7, 10, 30, 0, 0, time.UTC)

	entity := schema.NewMemoryRecord("entity-inbound-orchid", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Orchid",
		PrimaryType:   schema.EntityTypeProject,
		Aliases:       []schema.EntityAlias{{Value: "Project Orchid"}},
	})
	entity.CreatedAt = now
	entity.UpdatedAt = now
	entity.Salience = 0.1

	semantic := schema.NewMemoryRecord("semantic-inbound-orchid", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   entity.ID,
		Predicate: "deploy_target_for",
		Object:    "staging",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	semantic.CreatedAt = now
	semantic.UpdatedAt = now
	semantic.Salience = 1.0

	for _, rec := range []*schema.MemoryRecord{entity, semantic} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}
	if err := store.AddRelation(ctx, semantic.ID, schema.Relation{
		Predicate: schema.GraphPredicateSubjectEntity,
		TargetID:  entity.ID,
		Weight:    1,
		CreatedAt: now,
	}); err != nil {
		t.Fatalf("AddRelation semantic->entity: %v", err)
	}

	resp, err := svc.RetrieveGraph(ctx, &RetrieveGraphRequest{
		TaskDescriptor: "Project Orchid",
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
		t.Fatalf("RootIDs = %v, want entity root %s", resp.RootIDs, entity.ID)
	}
	if got := graphNodeIDs(resp.Nodes); len(got) != 2 || got[0] != entity.ID || got[1] != semantic.ID {
		t.Fatalf("Nodes = %v, want root entity plus inbound semantic neighbor", got)
	}
	if len(resp.Edges) != 1 || resp.Edges[0].SourceID != semantic.ID || resp.Edges[0].TargetID != entity.ID {
		t.Fatalf("Edges = %+v, want stored incoming semantic->entity edge", resp.Edges)
	}
}

func TestRetrieveGraphReportsIncomingRelationErrorAndKeepsOutgoingEdges(t *testing.T) {
	_, store := newGraphTestService(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 7, 11, 15, 0, 0, time.UTC)

	root := newSemanticRetrievalRecord("graph-incoming-error-root", 1.0, schema.SensitivityLow)
	neighbor := newSemanticRetrievalRecord("graph-incoming-error-neighbor", 0.8, schema.SensitivityLow)
	for _, rec := range []*schema.MemoryRecord{root, neighbor} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}
	if err := store.AddRelation(ctx, root.ID, schema.Relation{
		Predicate: "supports",
		TargetID:  neighbor.ID,
		Weight:    1,
		CreatedAt: now,
	}); err != nil {
		t.Fatalf("AddRelation root->neighbor: %v", err)
	}

	svc := NewService(&incomingRelationErrorStore{
		MemoryStore: store,
		err:         errors.New("incoming lookup failed"),
	}, nil)
	resp, err := svc.RetrieveGraph(ctx, &RetrieveGraphRequest{
		Trust:       NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		RootLimit:   1,
		NodeLimit:   2,
		EdgeLimit:   4,
		MaxHops:     1,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph: %v", err)
	}
	if got := graphNodeIDs(resp.Nodes); len(got) != 2 || got[0] != root.ID || got[1] != neighbor.ID {
		t.Fatalf("Nodes = %v, want root plus outgoing neighbor", got)
	}
	if len(resp.Edges) != 1 || resp.Edges[0].TargetID != neighbor.ID {
		t.Fatalf("Edges = %+v, want preserved outgoing edge", resp.Edges)
	}
	diagnostic := diagnosticByCode(resp.Diagnostics, DiagnosticGraphExpandFailed)
	if diagnostic == nil {
		t.Fatalf("diagnostics = %+v, want incoming graph expansion diagnostic", resp.Diagnostics)
	}
	if diagnostic.Message != "some graph relationships could not be retrieved" {
		t.Fatalf("diagnostic message = %q, want generic operational failure", diagnostic.Message)
	}
}

func TestRetrieveGraphPropagatesRetrievalDiagnostics(t *testing.T) {
	_, store := newGraphTestService(t)
	ctx := context.Background()

	record := newSemanticRetrievalRecord("graph-diagnostic-root", 0.8, schema.SensitivityLow)
	if err := store.Create(ctx, record); err != nil {
		t.Fatalf("Create record: %v", err)
	}

	svc := NewServiceWithEmbedding(store, nil, &fakeEmbeddingService{err: errors.New("embedding service unavailable")})
	resp, err := svc.RetrieveGraph(ctx, &RetrieveGraphRequest{
		TaskDescriptor: "diagnostic graph retrieval",
		Trust:          NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes:    []schema.MemoryType{schema.MemoryTypeSemantic},
		RootLimit:      1,
		NodeLimit:      1,
		MaxHops:        0,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph: %v", err)
	}
	diagnostic := diagnosticByCode(resp.Diagnostics, DiagnosticEmbeddingQueryFailed)
	if diagnostic == nil {
		t.Fatalf("diagnostics = %+v, want embedding failure", resp.Diagnostics)
	}
}

func TestRetrieveGraphFindsEntityRootFromTaskDescriptorPhrase(t *testing.T) {
	svc, store := newGraphTestService(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)

	entity := schema.NewMemoryRecord("entity-descriptor-orchid", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Orchid",
		PrimaryType:   schema.EntityTypeProject,
		Aliases:       []schema.EntityAlias{{Value: "Project Orchid"}},
	})
	entity.CreatedAt = now
	entity.UpdatedAt = now
	entity.Salience = 0.05
	if err := store.Create(ctx, entity); err != nil {
		t.Fatalf("Create entity: %v", err)
	}

	for i := 0; i < 4; i++ {
		rec := newSemanticRetrievalRecord("semantic-distractor-"+strconv.Itoa(i), 1.0-float64(i)*0.01, schema.SensitivityLow)
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create distractor: %v", err)
		}
	}

	matches, err := store.FindEntitiesByTerm(ctx, "debug project orchid rollout failure", "", 1)
	if err != nil {
		t.Fatalf("FindEntitiesByTerm: %v", err)
	}
	if len(matches) != 1 || matches[0].ID != entity.ID {
		t.Fatalf("FindEntitiesByTerm = %+v, want descriptor-matched entity %s", matches, entity.ID)
	}

	candidates := svc.entityRootCandidates(ctx, &RetrieveGraphRequest{
		TaskDescriptor: "debug project orchid rollout failure",
		Trust:          NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		RootLimit:      1,
	})
	if len(candidates) != 1 || candidates[0].ID != entity.ID {
		t.Fatalf("entityRootCandidates = %+v, want descriptor-matched entity %s", candidates, entity.ID)
	}

	resp, err := svc.RetrieveGraph(ctx, &RetrieveGraphRequest{
		TaskDescriptor: "debug project orchid rollout failure",
		Trust:          NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		RootLimit:      1,
		NodeLimit:      1,
		MaxHops:        0,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph: %v", err)
	}
	if len(resp.RootIDs) != 1 || resp.RootIDs[0] != entity.ID {
		t.Fatalf("RootIDs = %v, want descriptor-matched entity %s", resp.RootIDs, entity.ID)
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
		MemoryStore: base,
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

func TestRetrieveGraphPrioritizesHigherWeightRelationsWhenEdgeLimited(t *testing.T) {
	_, base := newGraphTestService(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 6, 10, 0, 0, 0, time.UTC)

	root := newSemanticRetrievalRecord("graph-weight-root", 1.0, schema.SensitivityLow)
	low := newSemanticRetrievalRecord("graph-weight-low", 0.7, schema.SensitivityLow)
	high := newSemanticRetrievalRecord("graph-weight-high", 0.6, schema.SensitivityLow)
	for _, rec := range []*schema.MemoryRecord{root, low, high} {
		if err := base.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	svc := NewService(&duplicateRelationStore{
		MemoryStore: base,
		rels: map[string][]schema.Relation{
			root.ID: {
				{Predicate: "weakly_related", TargetID: low.ID, Weight: 0.2, CreatedAt: now},
				{Predicate: "strongly_related", TargetID: high.ID, Weight: 0.95, CreatedAt: now.Add(time.Second)},
			},
		},
	}, nil)

	resp, err := svc.RetrieveGraph(ctx, &RetrieveGraphRequest{
		Trust:       NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		RootLimit:   1,
		NodeLimit:   3,
		EdgeLimit:   1,
		MaxHops:     1,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph: %v", err)
	}
	if len(resp.Edges) != 1 {
		t.Fatalf("Edges = %+v, want exactly one prioritized edge", resp.Edges)
	}
	if resp.Edges[0].TargetID != high.ID {
		t.Fatalf("Edges = %+v, want high-weight target %s", resp.Edges, high.ID)
	}
}

func TestRetrieveGraphCapsNodeRelationMetadataAtEdgeLimit(t *testing.T) {
	svc, store := newGraphTestService(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)

	root := newSemanticRetrievalRecord("bounded-relations-root", 1, schema.SensitivityLow)
	if err := store.Create(ctx, root); err != nil {
		t.Fatalf("Create root: %v", err)
	}
	for idx, id := range []string{"bounded-relations-a", "bounded-relations-b", "bounded-relations-c"} {
		target := newSemanticRetrievalRecord(id, 0.1-float64(idx)*0.01, schema.SensitivityLow)
		if err := store.Create(ctx, target); err != nil {
			t.Fatalf("Create %s: %v", id, err)
		}
		if err := store.AddRelation(ctx, root.ID, schema.Relation{
			Predicate: "related",
			TargetID:  id,
			Weight:    0.7 + float64(idx)*0.1,
			CreatedAt: now.Add(time.Duration(idx) * time.Minute),
		}); err != nil {
			t.Fatalf("AddRelation root->%s: %v", id, err)
		}
	}

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
	if len(resp.Edges) > 1 {
		t.Fatalf("top-level edges = %d, want at most EdgeLimit 1", len(resp.Edges))
	}
	for _, node := range resp.Nodes {
		if got := len(node.Record.Relations); got > 1 {
			t.Fatalf("node %s relation metadata = %d, want at most EdgeLimit 1", node.Record.ID, got)
		}
	}
}

func TestRetrieveGraphReportsBoundedProjectionWithoutNormalWarning(t *testing.T) {
	svc, store := newGraphTestService(t)
	ctx := context.Background()
	root := newSemanticRetrievalRecord("bounded-history-root", 1, schema.SensitivityLow)
	root.AuditLog = make([]schema.AuditEntry, 200)
	root.Provenance.Sources = make([]schema.ProvenanceSource, 200)
	for i := 0; i < 200; i++ {
		root.AuditLog[i] = schema.AuditEntry{Action: schema.AuditActionReinforce, Actor: "fixture"}
		root.Provenance.Sources[i] = schema.ProvenanceSource{
			Kind: schema.ProvenanceKindObservation,
			Ref:  "fixture-" + strconv.Itoa(i),
		}
	}
	if err := store.Create(ctx, root); err != nil {
		t.Fatalf("Create root: %v", err)
	}

	resp, err := svc.RetrieveGraph(ctx, &RetrieveGraphRequest{
		Trust:       NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		RootLimit:   1,
		NodeLimit:   1,
		EdgeLimit:   1,
		MaxHops:     -1,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph: %v", err)
	}
	if len(resp.Nodes) != 1 {
		t.Fatalf("nodes = %d, want one root", len(resp.Nodes))
	}
	if got := len(resp.Nodes[0].Record.AuditLog); got != 0 {
		t.Fatalf("graph audit history = %d, want omitted", got)
	}
	if got := len(resp.Nodes[0].Record.Provenance.Sources); got != 0 {
		t.Fatalf("graph provenance history = %d, want omitted", got)
	}
	if !resp.Projection.RelationsOmitted || resp.Projection.RelationsTruncated || !resp.Projection.HistoryOmitted {
		t.Fatalf("projection = %+v, want roots-only relation/history omission", resp.Projection)
	}
	if diagnosticByCode(resp.Diagnostics, DiagnosticGraphHistoryOmitted) != nil {
		t.Fatalf("diagnostics = %+v, want no warning for normal graph projection", resp.Diagnostics)
	}
}

func TestRetrieveGraphPrefersFiniteRelationLookups(t *testing.T) {
	_, base := newGraphTestService(t)
	store := &boundedRelationStore{MemoryStore: base}
	svc := NewService(store, nil)
	ctx := context.Background()
	now := time.Date(2026, 8, 16, 13, 0, 0, 0, time.UTC)

	root := newSemanticRetrievalRecord("finite-relations-root", 1, schema.SensitivityLow)
	target := newSemanticRetrievalRecord("finite-relations-target", 0.1, schema.SensitivityLow)
	for _, rec := range []*schema.MemoryRecord{root, target} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}
	if err := store.AddRelation(ctx, root.ID, schema.Relation{
		Predicate: "related", TargetID: target.ID, Weight: 1, CreatedAt: now,
	}); err != nil {
		t.Fatalf("AddRelation root->target: %v", err)
	}

	resp, err := svc.RetrieveGraph(ctx, &RetrieveGraphRequest{
		Trust:       NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		RootLimit:   1,
		NodeLimit:   2,
		EdgeLimit:   2,
		MaxHops:     1,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph: %v", err)
	}
	if len(resp.Edges) != 1 {
		t.Fatalf("edges = %+v, want root relation", resp.Edges)
	}
	if store.unboundedReads != 0 {
		t.Fatalf("unbounded relation reads = %d, want zero", store.unboundedReads)
	}
	if len(store.outgoingLimits) == 0 || len(store.incomingLimits) == 0 {
		t.Fatalf("bounded lookup limits outgoing=%v incoming=%v, want both paths", store.outgoingLimits, store.incomingLimits)
	}
	for _, limit := range append(append([]int(nil), store.outgoingLimits...), store.incomingLimits...) {
		if limit <= 0 || limit > 2 {
			t.Fatalf("bounded relation lookup limit = %d, want 1..EdgeLimit", limit)
		}
	}
}

func TestRetrieveGraphDoesNotUseUnboundedLegacyGraphFallbacks(t *testing.T) {
	_, base := newGraphTestService(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 16, 14, 0, 0, 0, time.UTC)
	root := newSemanticRetrievalRecord("legacy-fallback-root", 1, schema.SensitivityLow)
	target := newSemanticRetrievalRecord("legacy-fallback-target", 0.1, schema.SensitivityHigh)
	for _, rec := range []*schema.MemoryRecord{root, target} {
		if err := base.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}
	if err := base.AddRelation(ctx, root.ID, schema.Relation{
		Predicate: "related", TargetID: target.ID, Weight: 1, CreatedAt: now,
	}); err != nil {
		t.Fatalf("AddRelation root->target: %v", err)
	}

	legacy := &unboundedGraphStore{Store: base}
	svc := NewService(legacy, nil)
	resp, err := svc.RetrieveGraph(ctx, &RetrieveGraphRequest{
		Trust:       NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		RootLimit:   1,
		NodeLimit:   2,
		EdgeLimit:   2,
		MaxHops:     1,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph: %v", err)
	}
	if legacy.gets != 0 || legacy.relationGets != 0 {
		t.Fatalf("legacy graph reads: Get=%d GetRelations=%d, want zero unbounded calls", legacy.gets, legacy.relationGets)
	}
	if diagnosticByCode(resp.Diagnostics, DiagnosticGraphExpandFailed) == nil {
		t.Fatalf("diagnostics = %+v, want bounded-lookup compatibility diagnostic", resp.Diagnostics)
	}
}

func TestRetrieveGraphNeighborUsesExactBoundedProjectionAndRemainingBudget(t *testing.T) {
	base := teststore.NewMemoryStore()
	t.Cleanup(func() { _ = base.Close() })
	ctx := context.Background()
	now := time.Date(2026, 8, 17, 8, 0, 0, 0, time.UTC)
	root := newSemanticRetrievalRecord("bounded-neighbor-root", 1, schema.SensitivityLow)
	root.Scope = "project:alpha"
	target := schema.NewMemoryRecord("bounded-neighbor-target", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Bounded Neighbor",
		PrimaryType:   schema.EntityTypeProject,
	})
	target.Scope = root.Scope
	for _, rec := range []*schema.MemoryRecord{root, target} {
		if err := base.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}
	if err := base.AddRelation(ctx, root.ID, schema.Relation{
		Predicate: "related", TargetID: target.ID, Weight: 1, CreatedAt: now,
	}); err != nil {
		t.Fatalf("AddRelation: %v", err)
	}

	store := &boundedNeighborStore{MemoryStore: base, neighborID: target.ID}
	resp, err := NewService(store, nil).RetrieveGraph(ctx, &RetrieveGraphRequest{
		Trust:       NewTrustContext(schema.SensitivityLow, true, "tester", []string{root.Scope}),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		RootLimit:   1,
		NodeLimit:   2,
		EdgeLimit:   1,
		MaxHops:     1,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph: %v", err)
	}
	if store.legacyGraphGets != 0 {
		t.Fatalf("legacy GetGraphRecord calls = %d, want zero", store.legacyGraphGets)
	}
	if len(store.exactCalls) != 1 {
		t.Fatalf("exact bounded calls = %+v, want one neighbor lookup", store.exactCalls)
	}
	projectedRoot := *root
	projectedRoot.Relations = nil
	projectedRoot.AuditLog = nil
	projectedRoot.Provenance.Sources = nil
	wantBudget := MaxProjectedResponseBytes - storage.ProjectedRecordBytes(&projectedRoot, MaxProjectedResponseBytes)
	opts := store.exactCalls[0]
	if opts.ID != target.ID || opts.Limit != 1 || !opts.OmitRelations || !opts.OmitHistory ||
		opts.MaxHydratedBytes != wantBudget || opts.MaxSensitivity != retrievalHydrationSensitivity(schema.SensitivityLow) ||
		len(opts.Scopes) != 1 || opts.Scopes[0] != root.Scope || !opts.IncludeUnscoped {
		t.Fatalf("bounded neighbor options = %+v, want exact projected lookup with remaining budget %d", opts, wantBudget)
	}
	if len(resp.Nodes) != 2 || len(resp.Edges) != 1 || resp.Edges[0].TargetID != target.ID {
		t.Fatalf("graph nodes/edges = %d/%+v, want preserved neighbor output", len(resp.Nodes), resp.Edges)
	}
}

func TestRetrieveGraphNeighborPreflightTruncationStopsBeforeLegacyHydration(t *testing.T) {
	base := teststore.NewMemoryStore()
	t.Cleanup(func() { _ = base.Close() })
	ctx := context.Background()
	root := newSemanticRetrievalRecord("bounded-neighbor-truncated-root", 1, schema.SensitivityLow)
	target := schema.NewMemoryRecord("bounded-neighbor-truncated-target", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Too Large",
		PrimaryType:   schema.EntityTypeProject,
		Summary:       strings.Repeat("x", 2<<20),
	})
	for _, rec := range []*schema.MemoryRecord{root, target} {
		if err := base.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}
	if err := base.AddRelation(ctx, root.ID, schema.Relation{Predicate: "related", TargetID: target.ID, Weight: 1}); err != nil {
		t.Fatalf("AddRelation: %v", err)
	}

	store := &boundedNeighborStore{
		MemoryStore: base,
		neighborID:  target.ID,
		exactResult: &storage.BoundedListResult{HydrationBytesTruncated: true},
	}
	resp, err := NewService(store, nil).RetrieveGraph(ctx, &RetrieveGraphRequest{
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
	if store.legacyGraphGets != 0 || len(store.exactCalls) != 1 {
		t.Fatalf("bounded/legacy calls = %d/%d, want 1/0", len(store.exactCalls), store.legacyGraphGets)
	}
	if len(resp.Nodes) != 1 || len(resp.Edges) != 0 {
		t.Fatalf("graph nodes/edges = %d/%+v, want oversized neighbor omitted", len(resp.Nodes), resp.Edges)
	}
	if !resp.Projection.RecordsTruncated || diagnosticByCode(resp.Diagnostics, DiagnosticResponseByteLimitApplied) == nil {
		t.Fatalf("projection/diagnostics = %+v/%+v, want byte-limit truncation", resp.Projection, resp.Diagnostics)
	}
}

func TestRetrieveGraphNeighborDishonestProjectionIsRemeasured(t *testing.T) {
	base := teststore.NewMemoryStore()
	t.Cleanup(func() { _ = base.Close() })
	ctx := context.Background()
	root := newSemanticRetrievalRecord("dishonest-neighbor-root", 1, schema.SensitivityLow)
	target := schema.NewMemoryRecord("dishonest-neighbor-target", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Dishonest Projection",
		PrimaryType:   schema.EntityTypeProject,
		Summary:       strings.Repeat("x", int(MaxProjectedResponseBytes)),
	})
	for _, rec := range []*schema.MemoryRecord{root, target} {
		if err := base.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}
	if err := base.AddRelation(ctx, root.ID, schema.Relation{Predicate: "related", TargetID: target.ID, Weight: 1}); err != nil {
		t.Fatalf("AddRelation: %v", err)
	}
	store := &boundedNeighborStore{
		MemoryStore: base,
		neighborID:  target.ID,
		exactResult: &storage.BoundedListResult{Records: []*schema.MemoryRecord{target}, ProjectedBytes: 0},
	}
	resp, err := NewService(store, nil).RetrieveGraph(ctx, &RetrieveGraphRequest{
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
	if store.legacyGraphGets != 0 || len(store.exactCalls) != 1 {
		t.Fatalf("bounded/legacy calls = %d/%d, want 1/0", len(store.exactCalls), store.legacyGraphGets)
	}
	if len(resp.Nodes) != 1 || len(resp.Edges) != 0 || !resp.Projection.RecordsTruncated {
		t.Fatalf("dishonest oversized graph = nodes %d edges %+v projection %+v", len(resp.Nodes), resp.Edges, resp.Projection)
	}
}

func TestRetrieveGraphRelatedEntityUsesSharedBoundedScoringBudget(t *testing.T) {
	base := teststore.NewMemoryStore()
	t.Cleanup(func() { _ = base.Close() })
	ctx := context.Background()
	root := newSemanticRetrievalRecord("bounded-score-root", 1, schema.SensitivityLow)
	root.Scope = "project:alpha"
	first := schema.NewMemoryRecord("bounded-score-entity-a", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind: "entity", CanonicalName: "Alpha Project", PrimaryType: schema.EntityTypeProject, Summary: strings.Repeat("a", 64<<10),
	})
	second := schema.NewMemoryRecord("bounded-score-entity-b", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind: "entity", CanonicalName: "Beta Project", PrimaryType: schema.EntityTypeProject, Summary: strings.Repeat("b", 64<<10),
	})
	first.Scope, second.Scope = root.Scope, root.Scope
	for _, rec := range []*schema.MemoryRecord{root, first, second} {
		if err := base.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}
	for _, target := range []*schema.MemoryRecord{first, second} {
		if err := base.AddRelation(ctx, root.ID, schema.Relation{Predicate: "subject_entity", TargetID: target.ID, Weight: 1}); err != nil {
			t.Fatalf("AddRelation %s: %v", target.ID, err)
		}
	}
	store := &boundedNeighborStore{MemoryStore: base}
	if _, err := NewService(store, nil).RetrieveGraph(ctx, &RetrieveGraphRequest{
		TaskDescriptor: "project",
		Trust:          NewTrustContext(schema.SensitivityLow, true, "tester", []string{root.Scope}),
		MemoryTypes:    []schema.MemoryType{schema.MemoryTypeSemantic},
		RootLimit:      1,
		NodeLimit:      1,
		EdgeLimit:      4,
		MaxHops:        0,
	}); err != nil {
		t.Fatalf("RetrieveGraph: %v", err)
	}
	if store.legacyGraphGets != 0 {
		t.Fatalf("legacy GetGraphRecord calls = %d, want zero", store.legacyGraphGets)
	}
	if len(store.exactCalls) != 2 {
		t.Fatalf("exact scoring calls = %+v, want two entity lookups", store.exactCalls)
	}
	if store.exactCalls[0].MaxHydratedBytes <= 0 || store.exactCalls[0].MaxHydratedBytes >= MaxProjectedResponseBytes ||
		store.exactCalls[1].MaxHydratedBytes <= 0 || store.exactCalls[1].MaxHydratedBytes >= store.exactCalls[0].MaxHydratedBytes {
		t.Fatalf("scoring hydration budgets = %d, %d, want one decreasing request-wide budget", store.exactCalls[0].MaxHydratedBytes, store.exactCalls[1].MaxHydratedBytes)
	}
	for _, opts := range store.exactCalls {
		if opts.Limit != 1 || !opts.OmitRelations || !opts.OmitHistory || opts.MaxSensitivity != schema.SensitivityLow ||
			len(opts.Scopes) != 1 || opts.Scopes[0] != root.Scope || !opts.IncludeUnscoped {
			t.Fatalf("bounded scoring options = %+v", opts)
		}
	}
}

func TestRetrieveGraphRelatedEntityOversizeCannotInfluenceReranking(t *testing.T) {
	base := teststore.NewMemoryStore()
	t.Cleanup(func() { _ = base.Close() })
	ctx := context.Background()
	linked := newSemanticRetrievalRecord("oversize-score-linked", 0.1, schema.SensitivityLow)
	unrelated := newSemanticRetrievalRecord("oversize-score-unrelated", 1, schema.SensitivityLow)
	entity := schema.NewMemoryRecord("oversize-score-entity", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind: "entity", CanonicalName: "Orchid", PrimaryType: schema.EntityTypeProject, Summary: strings.Repeat("x", 2<<20),
	})
	for _, rec := range []*schema.MemoryRecord{linked, unrelated, entity} {
		if err := base.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}
	if err := base.AddRelation(ctx, linked.ID, schema.Relation{Predicate: "subject_entity", TargetID: entity.ID, Weight: 1}); err != nil {
		t.Fatalf("AddRelation: %v", err)
	}
	store := &boundedNeighborStore{
		MemoryStore: base,
		neighborID:  entity.ID,
		exactResult: &storage.BoundedListResult{HydrationBytesTruncated: true},
	}
	resp, err := NewService(store, nil).RetrieveGraph(ctx, &RetrieveGraphRequest{
		TaskDescriptor: "Orchid",
		Trust:          NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes:    []schema.MemoryType{schema.MemoryTypeSemantic},
		RootLimit:      1,
		NodeLimit:      1,
		EdgeLimit:      4,
		MaxHops:        0,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph: %v", err)
	}
	if store.legacyGraphGets != 0 || len(store.exactCalls) != 1 {
		t.Fatalf("bounded/legacy scoring calls = %d/%d, want 1/0", len(store.exactCalls), store.legacyGraphGets)
	}
	if len(resp.RootIDs) != 1 || resp.RootIDs[0] != unrelated.ID {
		t.Fatalf("RootIDs = %v, want oversized related entity unable to boost %s", resp.RootIDs, linked.ID)
	}
}

func TestRetrieveGraphCapsRequestWideExaminedNeighborWork(t *testing.T) {
	_, base := newGraphTestService(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 16, 15, 0, 0, 0, time.UTC)
	const roots = 300
	for i := 0; i < roots; i++ {
		root := newSemanticRetrievalRecord("work-budget-root-"+strconv.Itoa(i), 1, schema.SensitivityLow)
		target := newSemanticRetrievalRecord("work-budget-target-"+strconv.Itoa(i), 0.1, schema.SensitivityHigh)
		for _, rec := range []*schema.MemoryRecord{root, target} {
			if err := base.Create(ctx, rec); err != nil {
				t.Fatalf("Create %s: %v", rec.ID, err)
			}
		}
		if err := base.AddRelation(ctx, root.ID, schema.Relation{
			Predicate: "related", TargetID: target.ID, Weight: 1, CreatedAt: now,
		}); err != nil {
			t.Fatalf("AddRelation %s->%s: %v", root.ID, target.ID, err)
		}
	}

	store := &countingGetStore{MemoryStore: base}
	resp, err := NewService(store, nil).RetrieveGraph(ctx, &RetrieveGraphRequest{
		Trust:       NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		RootLimit:   roots,
		NodeLimit:   roots,
		EdgeLimit:   1,
		MaxHops:     1,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph: %v", err)
	}
	if len(resp.Edges) != 0 {
		t.Fatalf("edges = %+v, want denied neighbors omitted", resp.Edges)
	}
	if store.exactGets > 256 || store.gets != 0 {
		t.Fatalf("bounded/legacy examined neighbor records = %d/%d, want bounded cap <= 256 and zero legacy", store.exactGets, store.gets)
	}
}

func TestRetrieveGraphAppliesRequestWideProjectedByteBudget(t *testing.T) {
	_, store := newGraphTestService(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 16, 16, 0, 0, 0, time.UTC)
	root := newSemanticRetrievalRecord("graph-byte-root", 1, schema.SensitivityLow)
	if err := store.Create(ctx, root); err != nil {
		t.Fatalf("Create root: %v", err)
	}
	for i := 0; i < 3; i++ {
		target := schema.NewMemoryRecord("graph-byte-target-"+strconv.Itoa(i), schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
			Kind:          "entity",
			CanonicalName: "large target",
			PrimaryType:   schema.EntityTypeProject,
			Summary:       strings.Repeat(string(rune('a'+i)), 6<<20),
		})
		if err := store.Create(ctx, target); err != nil {
			t.Fatalf("Create target %d: %v", i, err)
		}
		if err := store.AddRelation(ctx, root.ID, schema.Relation{
			Predicate: "related", TargetID: target.ID, Weight: 1, CreatedAt: now,
		}); err != nil {
			t.Fatalf("AddRelation target %d: %v", i, err)
		}
	}

	resp, err := NewService(store, nil).RetrieveGraph(ctx, &RetrieveGraphRequest{
		Trust:       NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		RootLimit:   1,
		NodeLimit:   4,
		EdgeLimit:   3,
		MaxHops:     1,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph: %v", err)
	}
	if got := graphNodeIDs(resp.Nodes); len(got) != 3 || got[0] != root.ID || got[1] != "graph-byte-target-0" || got[2] != "graph-byte-target-1" {
		t.Fatalf("byte-budgeted graph nodes = %v, want root plus deterministic two-target prefix", got)
	}
	if len(resp.Edges) != 2 {
		t.Fatalf("byte-budgeted graph edges = %+v, want two included-neighbor edges", resp.Edges)
	}
	if !resp.Projection.RecordsTruncated {
		t.Fatalf("projection = %+v, want records_truncated metadata", resp.Projection)
	}
	if diagnosticByCode(resp.Diagnostics, DiagnosticResponseByteLimitApplied) == nil {
		t.Fatalf("diagnostics = %+v, want response byte-limit diagnostic", resp.Diagnostics)
	}
}

func TestRetrieveGraphWorkBudgetCountsEmptyStorageLookups(t *testing.T) {
	store := &countingEmptyGraphStore{MemoryStore: teststore.NewMemoryStore()}
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()
	const roots = 300
	for i := 0; i < roots; i++ {
		rec := newSemanticRetrievalRecord("empty-work-root-"+strconv.Itoa(i), 1, schema.SensitivityLow)
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	if _, err := NewService(store, nil).RetrieveGraph(ctx, &RetrieveGraphRequest{
		Trust:       NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		RootLimit:   roots,
		NodeLimit:   roots,
		EdgeLimit:   1,
		MaxHops:     1,
	}); err != nil {
		t.Fatalf("RetrieveGraph: %v", err)
	}
	if calls := store.outgoingCalls + store.incomingCalls; calls > 256 {
		t.Fatalf("empty bounded graph lookups = %d (outgoing=%d incoming=%d), want request-wide cap <= 256", calls, store.outgoingCalls, store.incomingCalls)
	}
}

func TestRetrieveGraphBoundsSelectionToRootBudget(t *testing.T) {
	_, store := newGraphTestService(t)
	ctx := context.Background()
	strong := competenceCandidate("graph-selection-strong", 0.9, 9, 1)
	weak := competenceCandidate("graph-selection-weak", 0.2, 1, 9)
	for _, rec := range []*schema.MemoryRecord{strong, weak} {
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	resp, err := NewService(store, NewSelector(0.2)).RetrieveGraph(ctx, &RetrieveGraphRequest{
		TaskDescriptor: "select competence",
		Trust:          NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes:    []schema.MemoryType{schema.MemoryTypeCompetence},
		RootLimit:      1,
		NodeLimit:      1,
		EdgeLimit:      1,
		MaxHops:        0,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph: %v", err)
	}
	if resp.Selection == nil || len(resp.Selection.Selected) != 1 || len(resp.Selection.Scores) != 1 {
		t.Fatalf("selection = %+v, want one root-budgeted selection and score", resp.Selection)
	}
}

func TestEntityRootCandidatesSkipsUnboundedLegacyLookup(t *testing.T) {
	_, base := newGraphTestService(t)
	store := &legacyEntityOnlyStore{Store: base}
	got := NewService(store, nil).entityRootCandidates(context.Background(), &RetrieveGraphRequest{
		TaskDescriptor: "legacy entity",
		Trust:          NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		RootLimit:      1,
	})
	if len(got) != 0 {
		t.Fatalf("entity candidates = %+v, want no unbounded legacy lookup results", got)
	}
	if store.termCalls != 0 {
		t.Fatalf("legacy entity lookup calls = %d, want zero", store.termCalls)
	}
}

func TestRerankGraphRootsCapsRelationLookupAmplification(t *testing.T) {
	store := &boundedRelationStore{MemoryStore: teststore.NewMemoryStore()}
	t.Cleanup(func() { _ = store.Close() })
	svc := NewService(store, nil)
	records := make([]*schema.MemoryRecord, 200)
	for i := range records {
		records[i] = newSemanticRetrievalRecord("root-rerank-"+strconv.Itoa(i), 0.5, schema.SensitivityLow)
		records[i].Relations = nil
	}

	ranked := svc.rerankGraphRootsLimited(
		context.Background(),
		records,
		"bounded query",
		NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		100,
	)
	if len(ranked) != len(records) {
		t.Fatalf("ranked records = %d, want %d", len(ranked), len(records))
	}
	if got := len(store.outgoingLimits); got > 100 {
		t.Fatalf("root rerank relation lookups = %d, want at most 100", got)
	}
	for _, limit := range store.outgoingLimits {
		if limit <= 0 || limit > 4 {
			t.Fatalf("root rerank per-record relation limit = %d, want 1..4", limit)
		}
	}
}

func TestRerankGraphRootsBoundsNormalizationAndComparisonWork(t *testing.T) {
	svc, _ := newGraphTestService(t)
	identifiers := make([]string, 256)
	for i := range identifiers {
		identifiers[i] = "namespace" + strconv.Itoa(i) + ":value"
	}
	descriptor := strings.Join(identifiers, " ") + " orchid " + strings.Repeat("oversized descriptor padding ", 4_000)
	if len(descriptor) < 100<<10 {
		t.Fatalf("descriptor len = %d, want at least 100 KiB", len(descriptor))
	}

	aliases := make([]schema.EntityAlias, 256)
	mentions := make([]schema.Mention, 256)
	for i := range aliases {
		aliases[i] = schema.EntityAlias{Value: "unmatched alias " + strconv.Itoa(i)}
		mentions[i] = schema.Mention{
			Surface: "unmatched mention " + strconv.Itoa(i),
			Aliases: []string{"unmatched mention alias " + strconv.Itoa(i)},
		}
	}
	records := make([]*schema.MemoryRecord, 2_000)
	for i := range records {
		records[i] = schema.NewMemoryRecord("bounded-root-"+strconv.Itoa(i), schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
			Kind:          "entity",
			CanonicalName: "unmatched canonical",
			Aliases:       aliases,
		})
		records[i].Interpretation = &schema.Interpretation{Mentions: mentions}
	}

	budget := newGraphRootBoostBudget()
	ranked := svc.rerankGraphRootsLimitedWithBudget(
		context.Background(),
		records,
		descriptor,
		NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		0,
		budget,
	)
	if len(ranked) != len(records) {
		t.Fatalf("ranked roots = %d, want %d", len(ranked), len(records))
	}
	if budget.queryNormalizations != 1 {
		t.Fatalf("query normalizations = %d, want exactly 1", budget.queryNormalizations)
	}
	if budget.identifierTokensParsed > maxGraphRootBoostIdentifiers {
		t.Fatalf("parsed identifiers = %d, want at most %d", budget.identifierTokensParsed, maxGraphRootBoostIdentifiers)
	}
	if budget.identifierBytesParsed > maxGraphRootBoostIdentifierParseBytes {
		t.Fatalf("identifier parse bytes = %d, want at most %d", budget.identifierBytesParsed, maxGraphRootBoostIdentifierParseBytes)
	}
	if budget.normalizationBytesUsed > maxGraphRootBoostNormalizationBytes {
		t.Fatalf("normalized bytes = %d, want at most %d", budget.normalizationBytesUsed, maxGraphRootBoostNormalizationBytes)
	}
	if budget.workUsed > maxGraphRootBoostWork || budget.comparisons > maxGraphRootBoostWork {
		t.Fatalf("root boost work = %d comparisons = %d, want each at most %d", budget.workUsed, budget.comparisons, maxGraphRootBoostWork)
	}
	if budget.workRemaining != 0 {
		t.Fatalf("adversarial root boost work remaining = %d, want deterministic budget exhaustion", budget.workRemaining)
	}
}

func TestRerankGraphRootsNormalizesSharedLinkedEntityTermsOnce(t *testing.T) {
	svc, store := newGraphTestService(t)
	ctx := context.Background()
	target := schema.NewMemoryRecord("shared-linked-entity", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Borealis",
		Aliases:       []schema.EntityAlias{{Value: "compute cluster"}},
	})
	if err := store.Create(ctx, target); err != nil {
		t.Fatalf("Create target: %v", err)
	}
	records := make([]*schema.MemoryRecord, maxGraphRootBoostRelationLookups)
	for i := range records {
		records[i] = newSemanticRetrievalRecord("shared-linked-root-"+strconv.Itoa(i), 0.5, schema.SensitivityLow)
		records[i].Relations = []schema.Relation{{Predicate: "subject_entity", TargetID: target.ID, Weight: 1}}
	}

	budget := newGraphRootBoostBudget()
	svc.rerankGraphRootsLimitedWithBudget(
		ctx,
		records,
		"borealis",
		NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		maxGraphRootBoostRelations,
		budget,
	)
	if budget.queryNormalizations != 1 {
		t.Fatalf("query normalizations = %d, want 1", budget.queryNormalizations)
	}
	if budget.normalizationCalls != 3 {
		t.Fatalf("normalization calls = %d, want query plus shared canonical and alias exactly once", budget.normalizationCalls)
	}
	if budget.comparisons != 2*maxGraphRootBoostRelationLookups {
		t.Fatalf("comparisons = %d, want two prepared-term comparisons per relation", budget.comparisons)
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

	if _, err := svc.RetrieveGraph(ctx, &RetrieveGraphRequest{
		Trust:       NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		MaxHops:     -2,
	}); err == nil || !strings.Contains(err.Error(), "max_hops") {
		t.Fatalf("RetrieveGraph invalid max hops error = %v, want max_hops validation error", err)
	}

	relationErrSvc := NewService(&relationErrorStore{MemoryStore: base, err: errors.New("relations failed")}, nil)
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
	diagnostic := diagnosticByCode(resp.Diagnostics, DiagnosticGraphExpandFailed)
	if diagnostic == nil {
		t.Fatalf("relation-error diagnostics = %+v, want graph expansion diagnostic", resp.Diagnostics)
	}
	if diagnostic.Message != "some graph relationships could not be retrieved" {
		t.Fatalf("relation-error diagnostic message = %q, want generic operational failure", diagnostic.Message)
	}

	errorStore := &failingRetrievalStore{listErr: errors.New("list failed")}
	errorSvc := NewService(&failingBoundedRetrievalStore{failingRetrievalStore: errorStore}, nil)
	if _, err := errorSvc.RetrieveGraph(ctx, &RetrieveGraphRequest{
		Trust: NewTrustContext(schema.SensitivityLow, true, "tester", nil),
	}); err == nil || !errors.Is(err, errorStore.listErr) {
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
	svc := NewService(&missingTargetGetStore{MemoryStore: store, missingID: missingTarget.ID}, nil)

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

func TestRetrieveGraphCachesDeniedNeighborLookups(t *testing.T) {
	_, base := newGraphTestService(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 14, 0, 0, 0, time.UTC)

	root := newSemanticRetrievalRecord("graph-cache-root", 1.0, schema.SensitivityLow)
	denied := newSemanticRetrievalRecord("graph-cache-denied", 0.9, schema.SensitivityHyper)
	for _, rec := range []*schema.MemoryRecord{root, denied} {
		if err := base.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}
	for _, rel := range []schema.Relation{
		{Predicate: "supports", TargetID: denied.ID, Weight: 1, CreatedAt: now},
		{Predicate: "depends_on", TargetID: denied.ID, Weight: 0.9, CreatedAt: now},
	} {
		if err := base.AddRelation(ctx, root.ID, rel); err != nil {
			t.Fatalf("AddRelation: %v", err)
		}
	}

	store := &countingGetStore{MemoryStore: base}
	resp, err := NewService(store, nil).RetrieveGraph(ctx, &RetrieveGraphRequest{
		Trust:       NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
		RootLimit:   1,
		NodeLimit:   4,
		EdgeLimit:   4,
		MaxHops:     1,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph: %v", err)
	}
	if len(resp.Edges) != 0 || len(resp.Nodes) != 1 {
		t.Fatalf("graph nodes/edges = %d/%+v, want denied neighbor omitted", len(resp.Nodes), resp.Edges)
	}
	if store.exactGets != 1 || store.gets != 0 {
		t.Fatalf("bounded/legacy record calls = %d/%d, want denied target checked once through bounded lookup", store.exactGets, store.gets)
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
	if len(lookupStore.limits) != 3 || lookupStore.limits[0] != 7 || lookupStore.limits[1] != 7 || lookupStore.limits[2] != 5 {
		t.Fatalf("lookup limits = %v, want failed/full scopes capped at 7 then remaining budget 5", lookupStore.limits)
	}
}

func TestRetrieveGraphUsesEffectiveRootLimitForEntityLookup(t *testing.T) {
	_, base := newGraphTestService(t)
	ctx := context.Background()

	entity := schema.NewMemoryRecord("entity-default-limit", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Default Limit Entity",
		PrimaryType:   schema.EntityTypeProject,
	})
	lookupStore := &graphLookupStore{
		Store:           base,
		allScopeMatches: []*schema.MemoryRecord{entity},
	}
	svc := NewService(lookupStore, nil)

	resp, err := svc.RetrieveGraph(ctx, &RetrieveGraphRequest{
		TaskDescriptor: "Default Limit Entity",
		Trust:          NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		NodeLimit:      1,
		MaxHops:        -1,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph: %v", err)
	}
	if got := lookupStore.allScopeLimits; len(got) != 1 || got[0] != 10 {
		t.Fatalf("all-scope lookup limits = %v, want effective default root limit [10]", got)
	}
	if len(resp.RootIDs) != 1 || resp.RootIDs[0] != entity.ID {
		t.Fatalf("RootIDs = %v, want [%s]", resp.RootIDs, entity.ID)
	}
}

func TestEntityRootCandidatesIncludesGlobalEntitiesForScopedTrust(t *testing.T) {
	_, base := newGraphTestService(t)
	ctx := context.Background()

	global := schema.NewMemoryRecord("entity-global-orchid", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Orchid",
		PrimaryType:   schema.EntityTypeProject,
	})
	scoped := schema.NewMemoryRecord("entity-scoped-orchid", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Scoped Orchid",
		PrimaryType:   schema.EntityTypeProject,
	})
	scoped.Scope = "project:alpha"

	lookupStore := &graphLookupStore{
		Store: base,
		matchesByScope: map[string][]*schema.MemoryRecord{
			"":              {global},
			"project:alpha": {scoped, global},
		},
	}
	svc := NewService(lookupStore, nil)

	got := svc.entityRootCandidates(ctx, &RetrieveGraphRequest{
		TaskDescriptor: "debug orchid rollout",
		Trust:          NewTrustContext(schema.SensitivityLow, true, "tester", []string{"project:alpha"}),
		RootLimit:      2,
	})
	if len(got) != 2 {
		t.Fatalf("entityRootCandidates len = %d, want global and scoped entity matches: %+v", len(got), got)
	}
	if got[0].ID != scoped.ID || got[1].ID != global.ID {
		t.Fatalf("entityRootCandidates IDs = [%s, %s], want scoped match before global fallback [%s, %s]", got[0].ID, got[1].ID, scoped.ID, global.ID)
	}
}

func TestEntityRootCandidatesUsesAllScopeLookupForUnrestrictedTrust(t *testing.T) {
	_, base := newGraphTestService(t)
	ctx := context.Background()

	scoped := schema.NewMemoryRecord("entity-scoped-all-scope-orchid", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Scoped Orchid",
		PrimaryType:   schema.EntityTypeProject,
	})
	scoped.Scope = "project:alpha"
	global := schema.NewMemoryRecord("entity-global-all-scope-orchid", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Global Orchid",
		PrimaryType:   schema.EntityTypeProject,
	})
	identifier := schema.NewMemoryRecord("entity-identifier-all-scope-orchid", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Identifier Orchid",
		PrimaryType:   schema.EntityTypeRepository,
		Identifiers: []schema.EntityIdentifier{{
			Namespace: "github",
			Value:     "BennettSchwartz/orchid",
		}},
	})
	identifier.Scope = "project:beta"

	lookupStore := &graphLookupStore{
		Store:           base,
		allScopeMatches: []*schema.MemoryRecord{scoped, global},
		allScopeIdentifiers: map[string]*schema.MemoryRecord{
			"github\x00BennettSchwartz/orchid": identifier,
		},
	}
	svc := NewService(lookupStore, nil)

	got := svc.entityRootCandidates(ctx, &RetrieveGraphRequest{
		TaskDescriptor: "debug github:BennettSchwartz/orchid rollout",
		Trust:          NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		RootLimit:      4,
	})
	if len(got) != 3 {
		t.Fatalf("entityRootCandidates len = %d, want scoped, global, and identifier matches: %+v", len(got), got)
	}
	if got[0].ID != scoped.ID || got[1].ID != global.ID || got[2].ID != identifier.ID {
		t.Fatalf("entityRootCandidates IDs = [%s, %s, %s], want all-scope term matches then identifier [%s, %s, %s]", got[0].ID, got[1].ID, got[2].ID, scoped.ID, global.ID, identifier.ID)
	}
	if len(lookupStore.limits) != 0 || len(lookupStore.identifierCalls) != 0 {
		t.Fatalf("scoped lookups = limits %v identifiers %v, want unrestricted graph lookup to avoid global-only path", lookupStore.limits, lookupStore.identifierCalls)
	}
	if got := lookupStore.allScopeLimits; len(got) != 1 || got[0] != 4 {
		t.Fatalf("all-scope limits = %v, want [4]", got)
	}
	if len(lookupStore.allScopeIdentifierCalls) != 1 {
		t.Fatalf("all-scope identifier calls = %v, want one explicit identifier lookup", lookupStore.allScopeIdentifierCalls)
	}
}

func TestEntityRootCandidatesResolveExplicitIdentifierTokens(t *testing.T) {
	_, base := newGraphTestService(t)
	ctx := context.Background()

	entity := schema.NewMemoryRecord("entity-github-orchid", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Project Orchid",
		PrimaryType:   schema.EntityTypeProject,
		Identifiers: []schema.EntityIdentifier{{
			Namespace: "github",
			Value:     "BennettSchwartz/orchid",
		}},
	})
	entity.Scope = "project:alpha"
	lookupStore := &graphLookupStore{
		Store: base,
		matchesByScope: map[string][]*schema.MemoryRecord{
			"project:alpha": {},
			"":              {},
		},
		identifierMatches: map[string]*schema.MemoryRecord{
			"github\x00BennettSchwartz/orchid\x00project:alpha": entity,
		},
	}
	svc := NewService(lookupStore, nil)

	got := svc.entityRootCandidates(ctx, &RetrieveGraphRequest{
		TaskDescriptor: "debug github:BennettSchwartz/orchid deploy failure",
		Trust:          NewTrustContext(schema.SensitivityLow, true, "tester", []string{"project:alpha"}),
		RootLimit:      3,
	})
	if len(got) != 1 || got[0].ID != entity.ID {
		t.Fatalf("entityRootCandidates = %+v, want identifier-resolved entity %s", got, entity.ID)
	}
	if len(lookupStore.identifierCalls) == 0 {
		t.Fatalf("identifierCalls = 0, want FindEntityByIdentifier call for explicit namespace:value token")
	}
}

func TestEntityRootCandidatesCapsScopeAndIdentifierLookupWork(t *testing.T) {
	_, base := newGraphTestService(t)
	lookupStore := &graphLookupStore{Store: base, matchesByScope: map[string][]*schema.MemoryRecord{}}
	scopes := make([]string, 20)
	identifiers := make([]string, 20)
	for i := range scopes {
		scopes[i] = "project:" + strconv.Itoa(i)
		identifiers[i] = "namespace" + strconv.Itoa(i) + ":value"
	}

	got := NewService(lookupStore, nil).entityRootCandidates(context.Background(), &RetrieveGraphRequest{
		TaskDescriptor: strings.Join(identifiers, " "),
		Trust:          NewTrustContext(schema.SensitivityLow, true, "tester", scopes),
		RootLimit:      10_000,
	})
	if len(got) != 0 {
		t.Fatalf("entity candidates = %+v, want none", got)
	}
	lookupCalls := len(lookupStore.limits) + len(lookupStore.identifierCalls)
	if lookupCalls > 128 {
		t.Fatalf("entity lookup calls = %d, want a global budget of at most 128", lookupCalls)
	}
}

func TestEntityRootCandidatesConsumesOneHydrationBudgetAcrossScopes(t *testing.T) {
	_, base := newGraphTestService(t)
	lookupStore := &graphLookupStore{
		Store:                 base,
		matchesByScope:        map[string][]*schema.MemoryRecord{},
		boundedProjectedBytes: 6 << 20,
	}
	result := NewService(lookupStore, nil).entityRootCandidatesBounded(context.Background(), &RetrieveGraphRequest{
		TaskDescriptor: "bounded entity lookup",
		Trust:          NewTrustContext(schema.SensitivityLow, true, "tester", []string{"project:one", "project:two", "project:three"}),
		RootLimit:      10,
	}, MaxProjectedResponseBytes)

	wantBudgets := []int64{16 << 20, 10 << 20, 4 << 20}
	if len(lookupStore.hydrationBudgets) != len(wantBudgets) {
		t.Fatalf("hydration budgets = %v, want %v", lookupStore.hydrationBudgets, wantBudgets)
	}
	for i := range wantBudgets {
		if lookupStore.hydrationBudgets[i] != wantBudgets[i] {
			t.Fatalf("hydration budgets = %v, want %v", lookupStore.hydrationBudgets, wantBudgets)
		}
	}
	if !result.HydrationBytesTruncated || result.ProjectedBytes != MaxProjectedResponseBytes {
		t.Fatalf("bounded entity result = %+v, want conservative exhaustion metadata", result)
	}
}

func TestRetrieveGraphSkipsFetchForKnownTargetNodes(t *testing.T) {
	_, base := newGraphTestService(t)
	store := &countingGetStore{MemoryStore: base}
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
	if store.exactGets != 0 || store.gets != 0 {
		t.Fatalf("bounded/legacy record calls = %d/%d, want 0 for targets already in node set", store.exactGets, store.gets)
	}
	if store.relationGets != 0 {
		t.Fatalf("store.GetRelations calls = %d, want 0 when listed records already include relations", store.relationGets)
	}
}

func TestRetrieveGraphSortsTiedNodesByID(t *testing.T) {
	svc, store := newGraphTestService(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 7, 9, 0, 0, 0, time.UTC)

	root := newSemanticRetrievalRecord("graph-tie-root", 1.0, schema.SensitivityLow)
	if err := store.Create(ctx, root); err != nil {
		t.Fatalf("Create root: %v", err)
	}
	for _, id := range []string{"graph-tie-d", "graph-tie-c", "graph-tie-b", "graph-tie-a"} {
		rec := newSemanticRetrievalRecord(id, 0.5, schema.SensitivityLow)
		if err := store.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", id, err)
		}
		if err := store.AddRelation(ctx, root.ID, schema.Relation{Predicate: "related", TargetID: id, Weight: 1, CreatedAt: now}); err != nil {
			t.Fatalf("AddRelation root->%s: %v", id, err)
		}
	}

	for attempt := 0; attempt < 20; attempt++ {
		resp, err := svc.RetrieveGraph(ctx, &RetrieveGraphRequest{
			Trust:       NewTrustContext(schema.SensitivityLow, true, "tester", nil),
			MemoryTypes: []schema.MemoryType{schema.MemoryTypeSemantic},
			RootLimit:   1,
			NodeLimit:   5,
			EdgeLimit:   4,
			MaxHops:     1,
		})
		if err != nil {
			t.Fatalf("RetrieveGraph: %v", err)
		}
		if got := graphNodeIDs(resp.Nodes); len(got) != 5 || got[1] != "graph-tie-a" || got[2] != "graph-tie-b" || got[3] != "graph-tie-c" || got[4] != "graph-tie-d" {
			t.Fatalf("attempt %d graph node IDs = %v, want root then tied neighbors sorted by ID", attempt, got)
		}
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

func TestRerankGraphRootsBreaksEqualScoresByFreshnessThenID(t *testing.T) {
	svc, _ := newGraphTestService(t)
	ctx := context.Background()
	trust := NewTrustContext(schema.SensitivityLow, true, "tester", nil)
	ts := time.Date(2026, 5, 7, 8, 0, 0, 0, time.UTC)

	t.Run("freshness", func(t *testing.T) {
		newerHighBase := newSemanticRetrievalRecord("root-z-newer-high-base", 0.5, schema.SensitivityLow)
		newerHighBase.UpdatedAt = ts.Add(time.Hour)
		olderBoosted := equalScoreProjectBoostedRecords(newerHighBase, ts)

		ranked := svc.rerankGraphRoots(ctx, olderBoosted, "project", trust)
		if ranked[0].ID != newerHighBase.ID {
			t.Fatalf("rerankGraphRoots first = %s, want fresher equal-score root %s", ranked[0].ID, newerHighBase.ID)
		}
	})

	t.Run("id", func(t *testing.T) {
		oldHighBase := newSemanticRetrievalRecord("root-a-high-base", 0.5, schema.SensitivityLow)
		oldHighBase.UpdatedAt = ts
		records := equalScoreProjectBoostedRecords(oldHighBase, ts)

		ranked := svc.rerankGraphRoots(ctx, records, "project", trust)
		if ranked[0].ID != oldHighBase.ID {
			t.Fatalf("rerankGraphRoots first = %s, want ID-sorted equal-score root %s", ranked[0].ID, oldHighBase.ID)
		}
	})
}

func equalScoreProjectBoostedRecords(highBase *schema.MemoryRecord, boostedUpdatedAt time.Time) []*schema.MemoryRecord {
	records := []*schema.MemoryRecord{highBase}
	for i := 0; i < 14; i++ {
		rec := newSemanticRetrievalRecord("root-padding-"+strconv.Itoa(i), 0.4, schema.SensitivityLow)
		rec.UpdatedAt = boostedUpdatedAt
		records = append(records, rec)
	}
	boosted := schema.NewMemoryRecord("root-z-boosted", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:        "entity",
		PrimaryType: schema.EntityTypeProject,
	})
	boosted.UpdatedAt = boostedUpdatedAt
	records = append(records, boosted)
	return records
}

func graphNodeIDs(nodes []GraphNode) []string {
	ids := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if node.Record == nil {
			ids = append(ids, "")
			continue
		}
		ids = append(ids, node.Record.ID)
	}
	return ids
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

func TestRetrieveGraphBoostsRootsFromStoreFetchedRelations(t *testing.T) {
	_, base := newGraphTestService(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 7, 15, 0, 0, 0, time.UTC)

	entity := schema.NewMemoryRecord("entity-store-fetched-relation", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Orchid",
		PrimaryType:   schema.EntityTypeProject,
	})
	entity.CreatedAt = now
	entity.UpdatedAt = now
	if err := base.Create(ctx, entity); err != nil {
		t.Fatalf("Create entity: %v", err)
	}

	linked := schema.NewMemoryRecord("semantic-store-fetched-link", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   entity.ID,
		Predicate: "deploy_target_for",
		Object:    "staging",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	linked.CreatedAt = now
	linked.UpdatedAt = now
	linked.Salience = 0.2
	unrelated := schema.NewMemoryRecord("semantic-store-fetched-unrelated", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   "billing",
		Predicate: "owned_by",
		Object:    "finance",
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
	})
	unrelated.CreatedAt = now
	unrelated.UpdatedAt = now
	unrelated.Salience = 1
	for _, rec := range []*schema.MemoryRecord{linked, unrelated} {
		if err := base.Create(ctx, rec); err != nil {
			t.Fatalf("Create %s: %v", rec.ID, err)
		}
	}

	svc := NewService(&duplicateRelationStore{
		MemoryStore: base,
		rels: map[string][]schema.Relation{
			linked.ID: {{Predicate: "subject_entity", TargetID: entity.ID, Weight: 1, CreatedAt: now}},
		},
	}, nil)
	resp, err := svc.RetrieveGraph(ctx, &RetrieveGraphRequest{
		TaskDescriptor: "debug orchid deploy target",
		Trust:          NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		MemoryTypes:    []schema.MemoryType{schema.MemoryTypeSemantic},
		RootLimit:      1,
		NodeLimit:      1,
		MaxHops:        0,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph: %v", err)
	}
	if len(resp.RootIDs) != 1 || resp.RootIDs[0] != linked.ID {
		t.Fatalf("RootIDs = %v, want store-fetched relation to boost linked semantic %s over high-salience unrelated %s", resp.RootIDs, linked.ID, unrelated.ID)
	}
}

func TestRetrieveGraphBoostsExplicitIdentifierRoot(t *testing.T) {
	svc, store := newGraphTestService(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 7, 14, 30, 0, 0, time.UTC)

	entity := schema.NewMemoryRecord("entity-explicit-github", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Project Orchid",
		PrimaryType:   schema.EntityTypeRepository,
		Identifiers: []schema.EntityIdentifier{{
			Namespace: "github",
			Value:     "BennettSchwartz/orchid",
		}},
	})
	entity.CreatedAt = now
	entity.UpdatedAt = now
	entity.Salience = 0.1
	if err := store.Create(ctx, entity); err != nil {
		t.Fatalf("Create entity: %v", err)
	}

	unrelated := schema.NewMemoryRecord("semantic-high-salience", schema.MemoryTypeSemantic, schema.SensitivityLow, &schema.SemanticPayload{
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
		t.Fatalf("Create unrelated: %v", err)
	}

	resp, err := svc.RetrieveGraph(ctx, &RetrieveGraphRequest{
		TaskDescriptor: "debug github:BennettSchwartz/orchid release issue",
		Trust:          NewTrustContext(schema.SensitivityLow, true, "tester", nil),
		RootLimit:      1,
		NodeLimit:      1,
		MaxHops:        0,
	})
	if err != nil {
		t.Fatalf("RetrieveGraph: %v", err)
	}

	if len(resp.RootIDs) != 1 || resp.RootIDs[0] != entity.ID {
		t.Fatalf("RootIDs = %v, want explicit identifier root %s", resp.RootIDs, entity.ID)
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
	if got := svc.rootBoost(ctx, nil, "orchid", nil, trust, cache); got != 0 {
		t.Fatalf("nil rootBoost = %v, want 0", got)
	}
	score := svc.rootBoost(ctx, rec, "orchid deploy target project rollout flower borealis compute cluster", nil, trust, cache)
	if score < 340 {
		t.Fatalf("rootBoost score = %v, want entity, mention, and relation boosts", score)
	}
	aliasMention := &schema.MemoryRecord{
		Sensitivity: schema.SensitivityLow,
		Interpretation: &schema.Interpretation{Mentions: []schema.Mention{{
			Surface: "unmatched",
			Aliases: []string{"flower service"},
		}}},
	}
	if got := svc.rootBoost(ctx, aliasMention, "flower service", nil, trust, cache); got < 20 {
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

func TestLexicalMatchUsesEntityTermBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
		query string
	}{
		{name: "short query inside longer word", value: "mongo", query: "go"},
		{name: "short value inside longer word", value: "go", query: "debug mongo migration"},
		{name: "substring crosses word boundary", value: "auth service", query: "oauth services"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if lexicalMatch(tc.value, tc.query) {
				t.Fatalf("lexicalMatch(%q, %q) = true, want false", tc.value, tc.query)
			}
		})
	}
	for _, tc := range []struct {
		name  string
		value string
		query string
	}{
		{name: "exact", value: "Project Orchid", query: "project orchid"},
		{name: "query contains phrase", value: "Project Orchid", query: "debug project orchid rollout"},
		{name: "value contains query phrase", value: "Project Orchid rollout", query: "project orchid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !lexicalMatch(tc.value, tc.query) {
				t.Fatalf("lexicalMatch(%q, %q) = false, want true", tc.value, tc.query)
			}
		})
	}
}

func TestExplicitEntityIdentifierTokens(t *testing.T) {
	got := schema.ParseEntityIdentifierTokens(`check (GitHub:BennettSchwartz/orchid), repo_path:pkg/auth repo_path:pkg/auth https://example.test`)
	if len(got) != 2 {
		t.Fatalf("explicitEntityIdentifierTokens len = %d, want 2: %+v", len(got), got)
	}
	if got[0].Namespace != "github" || got[0].Value != "BennettSchwartz/orchid" {
		t.Fatalf("first identifier = %+v, want normalized github identifier", got[0])
	}
	if got[1].Namespace != "repo_path" || got[1].Value != "pkg/auth" {
		t.Fatalf("second identifier = %+v, want repo_path identifier", got[1])
	}
	if got := schema.ParseEntityIdentifierTokens("mailto:user@example.test bad/ns:value no-value: 12:00"); len(got) != 0 {
		t.Fatalf("explicitEntityIdentifierTokens skipped identifiers = %+v, want none", got)
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
