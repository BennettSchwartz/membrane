package teststore

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

// MemoryStore is a fast test Store implementation. It is intentionally kept in
// internal/testutil so production code cannot grow a second runtime backend.
type MemoryStore struct {
	mu      sync.Mutex
	records map[string]*schema.MemoryRecord
	closed  bool
}

var errMemoryStoreClosed = errors.New("memory store closed")

type entityTermCandidate struct {
	record      *schema.MemoryRecord
	indexedTerm string
	scopeRank   int
	rank        int
	specificity int
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{records: make(map[string]*schema.MemoryRecord)}
}

func capBoundedLookupLimit(limit int) int {
	if limit > storage.MaxBoundedLookupLimit {
		return storage.MaxBoundedLookupLimit
	}
	return limit
}

func (s *MemoryStore) Create(_ context.Context, rec *schema.MemoryRecord) error {
	if err := rec.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errMemoryStoreClosed
	}
	if _, ok := s.records[rec.ID]; ok {
		return storage.ErrAlreadyExists
	}
	clone := cloneRecord(rec)
	normalizeRecordRelationsForStorage(clone)
	s.records[rec.ID] = clone
	return nil
}

func (s *MemoryStore) Get(_ context.Context, id string) (*schema.MemoryRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errMemoryStoreClosed
	}
	rec, ok := s.records[id]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return cloneRecord(rec), nil
}

func (s *MemoryStore) GetGraphRecord(_ context.Context, id string) (*schema.MemoryRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errMemoryStoreClosed
	}
	rec, ok := s.records[id]
	if !ok {
		return nil, storage.ErrNotFound
	}
	clone := cloneRecord(rec)
	clone.Relations = nil
	clone.AuditLog = nil
	clone.Provenance.Sources = nil
	return clone, nil
}

func (s *MemoryStore) Update(_ context.Context, rec *schema.MemoryRecord) error {
	if err := rec.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errMemoryStoreClosed
	}
	if _, ok := s.records[rec.ID]; !ok {
		return storage.ErrNotFound
	}
	clone := cloneRecord(rec)
	normalizeRecordRelationsForStorage(clone)
	s.records[rec.ID] = clone
	return nil
}

func (s *MemoryStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errMemoryStoreClosed
	}
	if _, ok := s.records[id]; !ok {
		return storage.ErrNotFound
	}
	delete(s.records, id)
	return nil
}

func (s *MemoryStore) List(_ context.Context, opts storage.ListOptions) ([]*schema.MemoryRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errMemoryStoreClosed
	}
	return listMemoryRecords(s.records, opts), nil
}

func (s *MemoryStore) ListBounded(_ context.Context, opts storage.ListOptions) (storage.BoundedListResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return storage.BoundedListResult{}, errMemoryStoreClosed
	}
	budget := opts.MaxHydratedBytes
	if budget <= 0 || budget > storage.MaxBoundedHydrationBytes {
		budget = storage.MaxBoundedHydrationBytes
	}
	candidateLimit := int(budget/storage.ProjectedRecordOverheadBytes) + 1
	if candidateLimit > storage.MaxBoundedLookupLimit {
		candidateLimit = storage.MaxBoundedLookupLimit
	}
	if opts.Limit <= 0 || opts.Limit > candidateLimit {
		opts.Limit = candidateLimit
	}
	records := listMemoryRecords(s.records, opts)
	result := storage.BoundedListResult{Records: make([]*schema.MemoryRecord, 0, len(records))}
	remaining := budget
	for _, record := range records {
		projected := storage.ProjectedRecordBytes(record, remaining)
		if projected > remaining {
			result.HydrationBytesTruncated = true
			break
		}
		remaining -= projected
		result.ProjectedBytes += projected
		result.Records = append(result.Records, record)
	}
	return result, nil
}

func (s *MemoryStore) GetAuthorizationMetadata(_ context.Context, ids []string) ([]storage.RecordAuthorizationMetadata, error) {
	if len(ids) > storage.MaxAuthorizationMetadataIDs {
		return nil, storage.ErrAuthorizationMetadataLimit
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errMemoryStoreClosed
	}
	if len(ids) == 0 {
		return []storage.RecordAuthorizationMetadata{}, nil
	}

	seen := make(map[string]struct{}, len(ids))
	metadata := make([]storage.RecordAuthorizationMetadata, 0, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		record, ok := s.records[id]
		if !ok || record == nil {
			continue
		}
		metadata = append(metadata, storage.RecordAuthorizationMetadata{
			ID:          record.ID,
			Scope:       record.Scope,
			Sensitivity: record.Sensitivity,
		})
	}
	sort.Slice(metadata, func(i, j int) bool { return metadata[i].ID < metadata[j].ID })
	return metadata, nil
}

func (s *MemoryStore) ListByType(ctx context.Context, memType schema.MemoryType) ([]*schema.MemoryRecord, error) {
	return s.List(ctx, storage.ListOptions{Type: memType})
}

func (s *MemoryStore) UpdateSalience(_ context.Context, id string, salience float64) error {
	if err := storage.ValidateSalience(salience); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errMemoryStoreClosed
	}
	rec, ok := s.records[id]
	if !ok {
		return storage.ErrNotFound
	}
	rec.Salience = salience
	return nil
}

func (s *MemoryStore) AddAuditEntry(_ context.Context, id string, entry schema.AuditEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errMemoryStoreClosed
	}
	rec, ok := s.records[id]
	if !ok {
		return storage.ErrNotFound
	}
	rec.AuditLog = append(rec.AuditLog, entry)
	return nil
}

func (s *MemoryStore) AddRelation(_ context.Context, sourceID string, rel schema.Relation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errMemoryStoreClosed
	}
	if strings.TrimSpace(sourceID) == "" {
		return &schema.ValidationError{Field: "source_id", Message: "source_id is required for relations"}
	}
	if err := rel.Validate(); err != nil {
		return err
	}
	rec, ok := s.records[sourceID]
	if !ok {
		return storage.ErrNotFound
	}
	if _, ok := s.records[rel.TargetID]; !ok {
		return storage.ErrNotFound
	}
	rel = normalizeRelationForStorage(rel)
	rec.Relations = append(upsertRelation(rec.Relations, rel), rel)
	return nil
}

func (s *MemoryStore) GetRelations(_ context.Context, id string) ([]schema.Relation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errMemoryStoreClosed
	}
	rec, ok := s.records[id]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return append([]schema.Relation(nil), rec.Relations...), nil
}

func (s *MemoryStore) GetRelationsLimited(_ context.Context, id string, limit int) ([]schema.Relation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errMemoryStoreClosed
	}
	rec, ok := s.records[id]
	if !ok {
		return nil, storage.ErrNotFound
	}
	limit = capBoundedLookupLimit(limit)
	relations := append([]schema.Relation(nil), rec.Relations...)
	sort.SliceStable(relations, func(i, j int) bool {
		if relations[i].Weight != relations[j].Weight {
			return relations[i].Weight > relations[j].Weight
		}
		if !relations[i].CreatedAt.Equal(relations[j].CreatedAt) {
			return relations[i].CreatedAt.After(relations[j].CreatedAt)
		}
		if relations[i].Predicate != relations[j].Predicate {
			return relations[i].Predicate < relations[j].Predicate
		}
		return relations[i].TargetID < relations[j].TargetID
	})
	if limit <= 0 {
		return []schema.Relation{}, nil
	}
	if len(relations) > limit {
		relations = relations[:limit]
	}
	return relations, nil
}

func (s *MemoryStore) GetRelationsBounded(ctx context.Context, id string, limit int, maxHydratedBytes int64) (storage.BoundedRelationResult, error) {
	relations, err := s.GetRelationsLimited(ctx, id, limit)
	if err != nil {
		return storage.BoundedRelationResult{}, err
	}
	if maxHydratedBytes > storage.MaxBoundedHydrationBytes {
		maxHydratedBytes = storage.MaxBoundedHydrationBytes
	}
	result := storage.BoundedRelationResult{Relations: make([]schema.Relation, 0, len(relations))}
	remaining := maxHydratedBytes
	for _, relation := range relations {
		projected := storage.ProjectedRelationOverheadBytes + int64(len(relation.Predicate)+len(relation.TargetID))
		if projected > remaining {
			result.HydrationBytesTruncated = true
			break
		}
		remaining -= projected
		result.ProjectedBytes += projected
		result.Relations = append(result.Relations, relation)
	}
	return result, nil
}

func (s *MemoryStore) GetIncomingRelations(_ context.Context, targetID string) ([]schema.GraphEdge, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errMemoryStoreClosed
	}
	if _, ok := s.records[targetID]; !ok {
		return nil, storage.ErrNotFound
	}
	edges := make([]schema.GraphEdge, 0)
	for _, rec := range sortedMemoryRecords(s.records) {
		for _, rel := range rec.Relations {
			if rel.TargetID != targetID {
				continue
			}
			edges = append(edges, schema.GraphEdge{
				SourceID:  rec.ID,
				Predicate: schema.NormalizeGraphPredicate(rel.Predicate),
				TargetID:  rel.TargetID,
				Weight:    rel.Weight,
				CreatedAt: rel.CreatedAt,
			})
		}
	}
	sort.SliceStable(edges, func(i, j int) bool {
		if edges[i].SourceID != edges[j].SourceID {
			return edges[i].SourceID < edges[j].SourceID
		}
		if edges[i].Predicate != edges[j].Predicate {
			return edges[i].Predicate < edges[j].Predicate
		}
		return edges[i].TargetID < edges[j].TargetID
	})
	return edges, nil
}

func (s *MemoryStore) GetIncomingRelationsLimited(_ context.Context, targetID string, limit int) ([]schema.GraphEdge, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errMemoryStoreClosed
	}
	if _, ok := s.records[targetID]; !ok {
		return nil, storage.ErrNotFound
	}
	limit = capBoundedLookupLimit(limit)
	edges := make([]schema.GraphEdge, 0)
	for _, rec := range s.records {
		for _, rel := range rec.Relations {
			if rel.TargetID == targetID {
				edges = append(edges, schema.GraphEdge{
					SourceID: rec.ID, Predicate: schema.NormalizeGraphPredicate(rel.Predicate),
					TargetID: rel.TargetID, Weight: rel.Weight, CreatedAt: rel.CreatedAt,
				})
			}
		}
	}
	sort.SliceStable(edges, func(i, j int) bool {
		if edges[i].Weight != edges[j].Weight {
			return edges[i].Weight > edges[j].Weight
		}
		if !edges[i].CreatedAt.Equal(edges[j].CreatedAt) {
			return edges[i].CreatedAt.After(edges[j].CreatedAt)
		}
		if edges[i].Predicate != edges[j].Predicate {
			return edges[i].Predicate < edges[j].Predicate
		}
		return edges[i].SourceID < edges[j].SourceID
	})
	if limit <= 0 {
		return []schema.GraphEdge{}, nil
	}
	if len(edges) > limit {
		edges = edges[:limit]
	}
	return edges, nil
}

func (s *MemoryStore) GetIncomingRelationsBounded(ctx context.Context, targetID string, limit int, maxHydratedBytes int64) (storage.BoundedIncomingRelationResult, error) {
	edges, err := s.GetIncomingRelationsLimited(ctx, targetID, limit)
	if err != nil {
		return storage.BoundedIncomingRelationResult{}, err
	}
	if maxHydratedBytes > storage.MaxBoundedHydrationBytes {
		maxHydratedBytes = storage.MaxBoundedHydrationBytes
	}
	result := storage.BoundedIncomingRelationResult{Edges: make([]schema.GraphEdge, 0, len(edges))}
	remaining := maxHydratedBytes
	for _, edge := range edges {
		projected := storage.ProjectedRelationOverheadBytes + int64(len(edge.SourceID)+len(edge.Predicate)+len(edge.TargetID))
		if projected > remaining {
			result.HydrationBytesTruncated = true
			break
		}
		remaining -= projected
		result.ProjectedBytes += projected
		result.Edges = append(result.Edges, edge)
	}
	return result, nil
}

func (s *MemoryStore) Begin(_ context.Context) (storage.Transaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errMemoryStoreClosed
	}
	return &memoryTx{
		parent:  s,
		records: cloneRecords(s.records),
	}, nil
}

func (s *MemoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *MemoryStore) FindEntitiesByTerm(_ context.Context, term, scope string, limit int) ([]*schema.MemoryRecord, error) {
	normalized := schema.NormalizeEntityTerm(term)
	if normalized == "" {
		return []*schema.MemoryRecord{}, nil
	}
	if limit <= 0 {
		limit = 10
	}
	limit = capBoundedLookupLimit(limit)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errMemoryStoreClosed
	}
	candidates := make([]entityTermCandidate, 0, limit)
	for _, rec := range sortedMemoryRecords(s.records) {
		payload, ok := rec.Payload.(*schema.EntityPayload)
		if !ok || !scopeMatches(rec.Scope, scope) {
			continue
		}
		best := entityTermCandidate{scopeRank: 2, rank: 3}
		for _, indexed := range entityTerms(payload) {
			rank := schema.EntityTermMatchRank(indexed, normalized)
			if rank >= 3 {
				continue
			}
			candidate := entityTermCandidate{
				record:      rec,
				indexedTerm: schema.NormalizeEntityTerm(indexed),
				scopeRank:   entityScopeRank(rec.Scope, scope),
				rank:        rank,
				specificity: schema.EntityTermMatchSpecificity(indexed, normalized),
			}
			if betterEntityTermCandidate(candidate, best) {
				best = candidate
			}
		}
		if best.record != nil {
			candidates = append(candidates, best)
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		return betterEntityTermCandidate(candidates[i], candidates[j])
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	matches := make([]*schema.MemoryRecord, 0, len(candidates))
	for _, candidate := range candidates {
		matches = append(matches, cloneRecord(candidate.record))
	}
	return matches, nil
}

func (s *MemoryStore) FindGraphEntitiesByTerm(ctx context.Context, term, scope string, limit int) ([]*schema.MemoryRecord, error) {
	records, err := s.FindEntitiesByTerm(ctx, term, scope, limit)
	if err != nil {
		return nil, err
	}
	for _, rec := range records {
		rec.Relations = nil
		rec.AuditLog = nil
		rec.Provenance.Sources = nil
	}
	return records, nil
}

func (s *MemoryStore) FindGraphEntitiesByTermBounded(ctx context.Context, term, scope string, limit int, maxHydratedBytes int64) (storage.BoundedGraphEntityResult, error) {
	records, err := s.FindGraphEntitiesByTerm(ctx, term, scope, limit)
	if err != nil {
		return storage.BoundedGraphEntityResult{}, err
	}
	return boundGraphEntityRecords(records, maxHydratedBytes), nil
}

func (s *MemoryStore) FindEntitiesByTermAllScopes(_ context.Context, term string, limit int) ([]*schema.MemoryRecord, error) {
	normalized := schema.NormalizeEntityTerm(term)
	if normalized == "" {
		return []*schema.MemoryRecord{}, nil
	}
	if limit <= 0 {
		limit = 10
	}
	limit = capBoundedLookupLimit(limit)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errMemoryStoreClosed
	}
	candidates := make([]entityTermCandidate, 0, limit)
	for _, rec := range sortedMemoryRecords(s.records) {
		payload, ok := rec.Payload.(*schema.EntityPayload)
		if !ok {
			continue
		}
		best := entityTermCandidate{rank: 3}
		for _, indexed := range entityTerms(payload) {
			rank := schema.EntityTermMatchRank(indexed, normalized)
			if rank >= 3 {
				continue
			}
			candidate := entityTermCandidate{
				record:      rec,
				indexedTerm: schema.NormalizeEntityTerm(indexed),
				rank:        rank,
				specificity: schema.EntityTermMatchSpecificity(indexed, normalized),
			}
			if betterEntityTermCandidate(candidate, best) {
				best = candidate
			}
		}
		if best.record != nil {
			candidates = append(candidates, best)
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		return betterEntityTermCandidate(candidates[i], candidates[j])
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	matches := make([]*schema.MemoryRecord, 0, len(candidates))
	for _, candidate := range candidates {
		matches = append(matches, cloneRecord(candidate.record))
	}
	return matches, nil
}

func (s *MemoryStore) FindGraphEntitiesByTermAllScopes(ctx context.Context, term string, limit int) ([]*schema.MemoryRecord, error) {
	records, err := s.FindEntitiesByTermAllScopes(ctx, term, limit)
	if err != nil {
		return nil, err
	}
	for _, rec := range records {
		rec.Relations = nil
		rec.AuditLog = nil
		rec.Provenance.Sources = nil
	}
	return records, nil
}

func (s *MemoryStore) FindGraphEntitiesByTermAllScopesBounded(ctx context.Context, term string, limit int, maxHydratedBytes int64) (storage.BoundedGraphEntityResult, error) {
	records, err := s.FindGraphEntitiesByTermAllScopes(ctx, term, limit)
	if err != nil {
		return storage.BoundedGraphEntityResult{}, err
	}
	return boundGraphEntityRecords(records, maxHydratedBytes), nil
}

func betterEntityTermCandidate(left, right entityTermCandidate) bool {
	if left.scopeRank != right.scopeRank {
		return left.scopeRank < right.scopeRank
	}
	if left.rank != right.rank {
		return left.rank < right.rank
	}
	if left.specificity != right.specificity {
		return left.specificity > right.specificity
	}
	if left.indexedTerm != right.indexedTerm {
		return left.indexedTerm < right.indexedTerm
	}
	if left.record == nil || right.record == nil {
		return right.record != nil
	}
	return left.record.ID < right.record.ID
}

func entityScopeRank(recordScope, requested string) int {
	if recordScope == requested {
		return 0
	}
	return 1
}

func (s *MemoryStore) FindEntityByIdentifier(_ context.Context, namespace, value, scope string) (*schema.MemoryRecord, error) {
	namespace = schema.NormalizeEntityIdentifierNamespace(namespace)
	value = strings.TrimSpace(value)
	if namespace == "" || value == "" {
		return nil, storage.ErrNotFound
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errMemoryStoreClosed
	}
	for _, rec := range sortedMemoryRecords(s.records) {
		payload, ok := rec.Payload.(*schema.EntityPayload)
		if !ok || !scopeMatches(rec.Scope, scope) {
			continue
		}
		for _, id := range payload.Identifiers {
			if schema.NormalizeEntityIdentifierNamespace(id.Namespace) == namespace && strings.TrimSpace(id.Value) == value {
				return cloneRecord(rec), nil
			}
		}
	}
	return nil, storage.ErrNotFound
}

func (s *MemoryStore) FindGraphEntityByIdentifier(ctx context.Context, namespace, value, scope string) (*schema.MemoryRecord, error) {
	rec, err := s.FindEntityByIdentifier(ctx, namespace, value, scope)
	if err != nil {
		return nil, err
	}
	rec.Relations = nil
	rec.AuditLog = nil
	rec.Provenance.Sources = nil
	return rec, nil
}

func (s *MemoryStore) FindGraphEntityByIdentifierBounded(ctx context.Context, namespace, value, scope string, maxHydratedBytes int64) (storage.BoundedGraphEntityResult, error) {
	rec, err := s.FindGraphEntityByIdentifier(ctx, namespace, value, scope)
	if err != nil {
		return storage.BoundedGraphEntityResult{}, err
	}
	return boundGraphEntityRecords([]*schema.MemoryRecord{rec}, maxHydratedBytes), nil
}

func (s *MemoryStore) FindEntityByIdentifierAllScopes(_ context.Context, namespace, value string) (*schema.MemoryRecord, error) {
	namespace = schema.NormalizeEntityIdentifierNamespace(namespace)
	value = strings.TrimSpace(value)
	if namespace == "" || value == "" {
		return nil, storage.ErrNotFound
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errMemoryStoreClosed
	}
	for _, rec := range sortedMemoryRecords(s.records) {
		payload, ok := rec.Payload.(*schema.EntityPayload)
		if !ok {
			continue
		}
		for _, id := range payload.Identifiers {
			if schema.NormalizeEntityIdentifierNamespace(id.Namespace) == namespace && strings.TrimSpace(id.Value) == value {
				return cloneRecord(rec), nil
			}
		}
	}
	return nil, storage.ErrNotFound
}

func (s *MemoryStore) FindGraphEntityByIdentifierAllScopes(ctx context.Context, namespace, value string) (*schema.MemoryRecord, error) {
	rec, err := s.FindEntityByIdentifierAllScopes(ctx, namespace, value)
	if err != nil {
		return nil, err
	}
	rec.Relations = nil
	rec.AuditLog = nil
	rec.Provenance.Sources = nil
	return rec, nil
}

func (s *MemoryStore) FindGraphEntityByIdentifierAllScopesBounded(ctx context.Context, namespace, value string, maxHydratedBytes int64) (storage.BoundedGraphEntityResult, error) {
	rec, err := s.FindGraphEntityByIdentifierAllScopes(ctx, namespace, value)
	if err != nil {
		return storage.BoundedGraphEntityResult{}, err
	}
	return boundGraphEntityRecords([]*schema.MemoryRecord{rec}, maxHydratedBytes), nil
}

func boundGraphEntityRecords(records []*schema.MemoryRecord, maxHydratedBytes int64) storage.BoundedGraphEntityResult {
	if maxHydratedBytes <= 0 || maxHydratedBytes > storage.MaxBoundedHydrationBytes {
		maxHydratedBytes = storage.MaxBoundedHydrationBytes
	}
	result := storage.BoundedGraphEntityResult{Records: make([]*schema.MemoryRecord, 0, len(records))}
	remaining := maxHydratedBytes
	for _, rec := range records {
		if rec == nil {
			continue
		}
		payload, _ := json.Marshal(struct {
			Payload        any                    `json:"payload"`
			Interpretation *schema.Interpretation `json:"interpretation"`
		}{Payload: rec.Payload, Interpretation: rec.Interpretation})
		projected := storage.ProjectedRecordOverheadBytes + int64(len(payload))
		if projected > remaining {
			result.HydrationBytesTruncated = true
			break
		}
		remaining -= projected
		result.ProjectedBytes += projected
		result.Records = append(result.Records, rec)
	}
	return result
}

func (s *MemoryStore) FindSemanticExact(_ context.Context, subject, predicate, object string) (*schema.MemoryRecord, error) {
	predicate = schema.NormalizeSemanticPredicate(predicate)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errMemoryStoreClosed
	}
	for _, rec := range sortedMemoryRecords(s.records) {
		payload, ok := rec.Payload.(*schema.SemanticPayload)
		if !ok {
			continue
		}
		if payload.Subject == subject && payload.Predicate == predicate && schema.SemanticObjectKey(payload.Object) == object {
			return cloneRecord(rec), nil
		}
	}
	return nil, nil
}

func (s *MemoryStore) FindSemanticExactInScope(_ context.Context, subject, predicate, object, scope string) (*schema.MemoryRecord, error) {
	predicate = schema.NormalizeSemanticPredicate(predicate)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errMemoryStoreClosed
	}
	for _, rec := range sortedMemoryRecords(s.records) {
		if rec.Scope != scope {
			continue
		}
		payload, ok := rec.Payload.(*schema.SemanticPayload)
		if !ok {
			continue
		}
		if payload.Subject == subject && payload.Predicate == predicate && schema.SemanticObjectKey(payload.Object) == object {
			return cloneRecord(rec), nil
		}
	}
	return nil, nil
}

type memoryTx struct {
	parent  *MemoryStore
	records map[string]*schema.MemoryRecord
	closed  bool
}

func (t *memoryTx) checkClosed() error {
	if t.closed {
		return storage.ErrTxClosed
	}
	return nil
}

func (t *memoryTx) Create(_ context.Context, rec *schema.MemoryRecord) error {
	if err := t.checkClosed(); err != nil {
		return err
	}
	if err := rec.Validate(); err != nil {
		return err
	}
	if _, ok := t.records[rec.ID]; ok {
		return storage.ErrAlreadyExists
	}
	clone := cloneRecord(rec)
	normalizeRecordRelationsForStorage(clone)
	t.records[rec.ID] = clone
	return nil
}

func (t *memoryTx) Get(_ context.Context, id string) (*schema.MemoryRecord, error) {
	if err := t.checkClosed(); err != nil {
		return nil, err
	}
	rec, ok := t.records[id]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return cloneRecord(rec), nil
}

func (t *memoryTx) GetAuthorizationMetadata(_ context.Context, ids []string) ([]storage.RecordAuthorizationMetadata, error) {
	if err := t.checkClosed(); err != nil {
		return nil, err
	}
	if len(ids) > storage.MaxAuthorizationMetadataIDs {
		return nil, storage.ErrAuthorizationMetadataLimit
	}
	seen := make(map[string]struct{}, len(ids))
	metadata := make([]storage.RecordAuthorizationMetadata, 0, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		record, ok := t.records[id]
		if !ok || record == nil {
			continue
		}
		metadata = append(metadata, storage.RecordAuthorizationMetadata{
			ID:          record.ID,
			Scope:       record.Scope,
			Sensitivity: record.Sensitivity,
		})
	}
	sort.Slice(metadata, func(i, j int) bool { return metadata[i].ID < metadata[j].ID })
	return metadata, nil
}

func (t *memoryTx) Update(_ context.Context, rec *schema.MemoryRecord) error {
	if err := t.checkClosed(); err != nil {
		return err
	}
	if err := rec.Validate(); err != nil {
		return err
	}
	if _, ok := t.records[rec.ID]; !ok {
		return storage.ErrNotFound
	}
	clone := cloneRecord(rec)
	normalizeRecordRelationsForStorage(clone)
	t.records[rec.ID] = clone
	return nil
}

func (t *memoryTx) Delete(_ context.Context, id string) error {
	if err := t.checkClosed(); err != nil {
		return err
	}
	if _, ok := t.records[id]; !ok {
		return storage.ErrNotFound
	}
	delete(t.records, id)
	return nil
}

func (t *memoryTx) List(_ context.Context, opts storage.ListOptions) ([]*schema.MemoryRecord, error) {
	if err := t.checkClosed(); err != nil {
		return nil, err
	}
	return listMemoryRecords(t.records, opts), nil
}

func (t *memoryTx) ListByType(ctx context.Context, memType schema.MemoryType) ([]*schema.MemoryRecord, error) {
	return t.List(ctx, storage.ListOptions{Type: memType})
}

func (t *memoryTx) UpdateSalience(_ context.Context, id string, salience float64) error {
	if err := t.checkClosed(); err != nil {
		return err
	}
	if err := storage.ValidateSalience(salience); err != nil {
		return err
	}
	rec, ok := t.records[id]
	if !ok {
		return storage.ErrNotFound
	}
	rec.Salience = salience
	return nil
}

func (t *memoryTx) AddAuditEntry(_ context.Context, id string, entry schema.AuditEntry) error {
	if err := t.checkClosed(); err != nil {
		return err
	}
	rec, ok := t.records[id]
	if !ok {
		return storage.ErrNotFound
	}
	rec.AuditLog = append(rec.AuditLog, entry)
	return nil
}

func (t *memoryTx) AddRelation(_ context.Context, sourceID string, rel schema.Relation) error {
	if err := t.checkClosed(); err != nil {
		return err
	}
	if strings.TrimSpace(sourceID) == "" {
		return &schema.ValidationError{Field: "source_id", Message: "source_id is required for relations"}
	}
	if err := rel.Validate(); err != nil {
		return err
	}
	rec, ok := t.records[sourceID]
	if !ok {
		return storage.ErrNotFound
	}
	if _, ok := t.records[rel.TargetID]; !ok {
		return storage.ErrNotFound
	}
	rel = normalizeRelationForStorage(rel)
	rec.Relations = append(upsertRelation(rec.Relations, rel), rel)
	return nil
}

func (t *memoryTx) GetRelations(_ context.Context, id string) ([]schema.Relation, error) {
	if err := t.checkClosed(); err != nil {
		return nil, err
	}
	rec, ok := t.records[id]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return append([]schema.Relation(nil), rec.Relations...), nil
}

func (t *memoryTx) Commit() error {
	if err := t.checkClosed(); err != nil {
		return err
	}
	t.parent.mu.Lock()
	defer t.parent.mu.Unlock()
	t.parent.records = cloneRecords(t.records)
	t.closed = true
	return nil
}

func (t *memoryTx) Rollback() error {
	if err := t.checkClosed(); err != nil {
		return err
	}
	t.closed = true
	return nil
}

func listMemoryRecords(records map[string]*schema.MemoryRecord, opts storage.ListOptions) []*schema.MemoryRecord {
	all := sortedMemoryRecords(records)
	capacity := len(all)
	if opts.Limit > 0 && opts.Limit < capacity {
		capacity = opts.Limit
	}
	out := make([]*schema.MemoryRecord, 0, capacity)
	matched := 0
	for _, rec := range all {
		if !matchesListOptions(rec, opts) {
			continue
		}
		if matched < opts.Offset {
			matched++
			continue
		}
		clone := cloneRecord(rec)
		if opts.OmitRelations {
			clone.Relations = nil
		}
		if opts.OmitHistory {
			clone.AuditLog = nil
			clone.Provenance.Sources = nil
		}
		out = append(out, clone)
		matched++
		if opts.Limit > 0 && len(out) >= opts.Limit {
			break
		}
	}
	return out
}

func sortedMemoryRecords(records map[string]*schema.MemoryRecord) []*schema.MemoryRecord {
	out := make([]*schema.MemoryRecord, 0, len(records))
	for _, rec := range records {
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Salience != out[j].Salience {
			return out[i].Salience > out[j].Salience
		}
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.After(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

func matchesListOptions(rec *schema.MemoryRecord, opts storage.ListOptions) bool {
	if opts.ID != "" && rec.ID != opts.ID {
		return false
	}
	if opts.Type != "" && rec.Type != opts.Type {
		return false
	}
	if opts.Type == "" && len(opts.Types) > 0 && !containsMemoryType(opts.Types, rec.Type) {
		return false
	}
	if opts.Scope != "" && rec.Scope != opts.Scope {
		return false
	}
	if opts.Scope == "" && len(opts.Scopes) > 0 && !containsScope(opts.Scopes, rec.Scope, opts.IncludeUnscoped) {
		return false
	}
	if opts.Sensitivity != "" && rec.Sensitivity != opts.Sensitivity {
		return false
	}
	if opts.Sensitivity == "" && opts.MaxSensitivity != "" && !sensitivityAtOrBelow(rec.Sensitivity, opts.MaxSensitivity) {
		return false
	}
	if opts.MinSalience > 0 && rec.Salience < opts.MinSalience {
		return false
	}
	if opts.MaxSalience > 0 && rec.Salience > opts.MaxSalience {
		return false
	}
	for _, tag := range opts.Tags {
		if !hasTag(rec.Tags, tag) {
			return false
		}
	}
	return true
}

func containsMemoryType(values []schema.MemoryType, want schema.MemoryType) bool {
	for _, value := range values {
		if value == "*" || value == want {
			return true
		}
	}
	return false
}

func containsScope(values []string, want string, includeUnscoped bool) bool {
	if want == "" && includeUnscoped {
		return true
	}
	for _, value := range values {
		if value == "*" || value == want {
			return true
		}
	}
	return false
}

func sensitivityAtOrBelow(value, max schema.Sensitivity) bool {
	level := func(s schema.Sensitivity) int {
		switch s {
		case schema.SensitivityPublic:
			return 0
		case schema.SensitivityLow:
			return 1
		case schema.SensitivityMedium:
			return 2
		case schema.SensitivityHigh:
			return 3
		case schema.SensitivityHyper:
			return 4
		default:
			return -1
		}
	}
	valueLevel, maxLevel := level(value), level(max)
	return valueLevel >= 0 && maxLevel >= 0 && valueLevel <= maxLevel
}

func hasTag(tags []string, want string) bool {
	for _, tag := range tags {
		if tag == want {
			return true
		}
	}
	return false
}

func upsertRelation(existing []schema.Relation, rel schema.Relation) []schema.Relation {
	rel = normalizeRelationForStorage(rel)
	filtered := existing[:0]
	for _, cur := range existing {
		if schema.NormalizeGraphPredicate(cur.Predicate) == rel.Predicate && cur.TargetID == rel.TargetID {
			continue
		}
		filtered = append(filtered, cur)
	}
	return filtered
}

func normalizeRelationForStorage(rel schema.Relation) schema.Relation {
	rel.Predicate = schema.NormalizeGraphPredicate(rel.Predicate)
	return rel
}

func normalizeRecordRelationsForStorage(rec *schema.MemoryRecord) {
	if semantic, ok := rec.Payload.(*schema.SemanticPayload); ok && semantic != nil {
		semantic.Predicate = schema.NormalizeSemanticPredicate(semantic.Predicate)
		rec.Payload = semantic
	}
	for i := range rec.Relations {
		rec.Relations[i] = normalizeRelationForStorage(rec.Relations[i])
	}
}

func scopeMatches(recordScope, requested string) bool {
	return recordScope == requested || recordScope == ""
}

func entityTerms(payload *schema.EntityPayload) []string {
	terms := make([]string, 0, len(payload.Aliases)+1+(2*len(payload.Identifiers)))
	terms = append(terms, payload.CanonicalName)
	for _, alias := range payload.Aliases {
		terms = append(terms, alias.Value)
	}
	for _, identifier := range payload.Identifiers {
		namespace := schema.NormalizeEntityIdentifierNamespace(identifier.Namespace)
		value := strings.TrimSpace(identifier.Value)
		if value == "" {
			continue
		}
		terms = append(terms, value)
		if namespace != "" {
			terms = append(terms, namespace+":"+value)
		}
	}
	return terms
}

func cloneRecords(records map[string]*schema.MemoryRecord) map[string]*schema.MemoryRecord {
	out := make(map[string]*schema.MemoryRecord, len(records))
	for id, rec := range records {
		out[id] = cloneRecord(rec)
	}
	return out
}

func cloneRecord(rec *schema.MemoryRecord) *schema.MemoryRecord {
	if rec == nil {
		return nil
	}
	data, err := json.Marshal(rec)
	if err != nil {
		panic(err)
	}
	var clone schema.MemoryRecord
	if err := json.Unmarshal(data, &clone); err != nil {
		panic(err)
	}
	return &clone
}

var (
	_ storage.Store                             = (*MemoryStore)(nil)
	_ storage.BoundedListStore                  = (*MemoryStore)(nil)
	_ storage.AuthorizationMetadataStore        = (*MemoryStore)(nil)
	_ storage.IncomingRelationLookup            = (*MemoryStore)(nil)
	_ storage.BoundedRelationLookup             = (*MemoryStore)(nil)
	_ storage.BoundedIncomingRelationLookup     = (*MemoryStore)(nil)
	_ storage.ByteBoundedRelationLookup         = (*MemoryStore)(nil)
	_ storage.ByteBoundedIncomingRelationLookup = (*MemoryStore)(nil)
	_ storage.GraphRecordLookup                 = (*MemoryStore)(nil)
	_ storage.EntityLookup                      = (*MemoryStore)(nil)
	_ storage.EntityLookupAllScopes             = (*MemoryStore)(nil)
	_ storage.GraphEntityLookup                 = (*MemoryStore)(nil)
	_ storage.GraphEntityLookupAllScopes        = (*MemoryStore)(nil)
	_ storage.SemanticLookup                    = (*MemoryStore)(nil)
	_ storage.Transaction                       = (*memoryTx)(nil)
)
