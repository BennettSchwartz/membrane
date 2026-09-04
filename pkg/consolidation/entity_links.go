package consolidation

import (
	"context"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

func canonicalizeSemanticRecordEntities(ctx context.Context, store storage.Store, rec *schema.MemoryRecord) []schema.GraphEdge {
	if rec == nil {
		return nil
	}
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
	if entity := findEntityByTerm(ctx, lookup, payload.Subject, rec.Scope, rec.Sensitivity); entity != nil {
		payload.Subject = entity.ID
		rec.Relations = append(rec.Relations, schema.Relation{
			Predicate: schema.GraphPredicateSubjectEntity,
			TargetID:  entity.ID,
			Weight:    1.0,
			CreatedAt: ts,
		})
		edges = append(edges, schema.GraphEdge{SourceID: rec.ID, Predicate: schema.GraphPredicateSubjectEntity, TargetID: entity.ID, Weight: 1.0, CreatedAt: ts})
		if entity.Scope == rec.Scope && entity.Sensitivity == rec.Sensitivity {
			edges = append(edges, schema.GraphEdge{SourceID: entity.ID, Predicate: schema.GraphPredicateFactSubjectOf, TargetID: rec.ID, Weight: 1.0, CreatedAt: ts})
		}
	}
	if object, ok := payload.Object.(string); ok {
		if entity := findEntityByTerm(ctx, lookup, object, rec.Scope, rec.Sensitivity); entity != nil {
			payload.Object = entity.ID
			rec.Relations = append(rec.Relations, schema.Relation{
				Predicate: schema.GraphPredicateObjectEntity,
				TargetID:  entity.ID,
				Weight:    1.0,
				CreatedAt: ts,
			})
			edges = append(edges, schema.GraphEdge{SourceID: rec.ID, Predicate: schema.GraphPredicateObjectEntity, TargetID: entity.ID, Weight: 1.0, CreatedAt: ts})
			if entity.Scope == rec.Scope && entity.Sensitivity == rec.Sensitivity {
				edges = append(edges, schema.GraphEdge{SourceID: entity.ID, Predicate: schema.GraphPredicateFactObjectOf, TargetID: rec.ID, Weight: 1.0, CreatedAt: ts})
			}
		}
	}
	rec.Payload = payload
	return edges
}

// transactionEntityStore keeps term lookup on the store while binding every
// accepted candidate to locked transaction metadata before its ID is used.
type transactionEntityStore struct {
	storage.Store
	lookup storage.EntityLookup
	tx     storage.Transaction
}

type entityTermKey struct{ term, scope string }

type candidateEntityStore struct {
	storage.Store
	matches map[entityTermKey][]*schema.MemoryRecord
}

// snapshotEntityCandidates performs bounded discovery before opening a write
// transaction. A transaction must still approve every candidate's exact policy.
// Keeping discovery outside avoids a second pool checkout while locks are held.
func snapshotEntityCandidates(ctx context.Context, store storage.Store, scope string, terms ...string) storage.Store {
	lookup, ok := store.(storage.EntityLookup)
	if !ok {
		return store
	}
	result := &candidateEntityStore{Store: store, matches: make(map[entityTermKey][]*schema.MemoryRecord)}
	scopes := []string{scope}
	if scope != "" {
		scopes = append(scopes, "")
	}
	for _, term := range terms {
		for _, searchScope := range scopes {
			key := entityTermKey{term: term, scope: searchScope}
			if _, seen := result.matches[key]; seen {
				continue
			}
			matches, err := lookup.FindEntitiesByTerm(ctx, term, searchScope, storage.MaxAuthorizationMetadataIDs)
			if err != nil {
				result.matches[key] = nil
				continue
			}
			if len(matches) > storage.MaxAuthorizationMetadataIDs {
				matches = matches[:storage.MaxAuthorizationMetadataIDs]
			}
			result.matches[key] = matches
		}
	}
	return result
}

func (s *candidateEntityStore) FindEntitiesByTerm(_ context.Context, term, scope string, _ int) ([]*schema.MemoryRecord, error) {
	return s.matches[entityTermKey{term: term, scope: scope}], nil
}

func (s *candidateEntityStore) FindEntityByIdentifier(context.Context, string, string, string) (*schema.MemoryRecord, error) {
	return nil, storage.ErrNotFound
}

func semanticEntityTerms(subject string, object any) []string {
	terms := []string{subject}
	if value, ok := object.(string); ok {
		terms = append(terms, value)
	}
	return terms
}

func entityStoreInTransaction(store storage.Store, tx storage.Transaction) storage.Store {
	lookup, ok := store.(storage.EntityLookup)
	if !ok {
		return store
	}
	return &transactionEntityStore{Store: store, lookup: lookup, tx: tx}
}

func (s *transactionEntityStore) FindEntitiesByTerm(ctx context.Context, term, scope string, limit int) ([]*schema.MemoryRecord, error) {
	return s.lookup.FindEntitiesByTerm(ctx, term, scope, limit)
}
func (s *transactionEntityStore) FindEntityByIdentifier(ctx context.Context, namespace, value, scope string) (*schema.MemoryRecord, error) {
	return s.lookup.FindEntityByIdentifier(ctx, namespace, value, scope)
}
func (s *transactionEntityStore) GetAuthorizationMetadata(ctx context.Context, ids []string) ([]storage.RecordAuthorizationMetadata, error) {
	lookup, ok := s.tx.(storage.AuthorizationMetadataStore)
	if !ok {
		return nil, storage.ErrAuthorizationMetadataUnsupported
	}
	return lookup.GetAuthorizationMetadata(ctx, ids)
}

func findEntityByTerm(ctx context.Context, lookup storage.EntityLookup, term, scope string, sensitivity schema.Sensitivity) *schema.MemoryRecord {
	policyStore, ok := lookup.(storage.AuthorizationMetadataStore)
	if !ok || !schema.IsValidSensitivity(sensitivity) {
		return nil
	}
	scopes := []string{scope}
	if scope != "" {
		scopes = append(scopes, "")
	}
	for _, searchScope := range scopes {
		matches, err := lookup.FindEntitiesByTerm(ctx, term, searchScope, storage.MaxAuthorizationMetadataIDs)
		if err != nil {
			continue
		}
		for i, candidate := range matches {
			if i >= storage.MaxAuthorizationMetadataIDs {
				break
			}
			if candidate == nil || candidate.Type != schema.MemoryTypeEntity || candidate.ID == "" || candidate.Scope != searchScope || !schema.IsValidSensitivity(candidate.Sensitivity) || sensitivityRank(candidate.Sensitivity) > sensitivityRank(sensitivity) {
				continue
			}
			rows, err := policyStore.GetAuthorizationMetadata(ctx, []string{candidate.ID})
			if err != nil || len(rows) != 1 || rows[0].ID != candidate.ID || rows[0].Scope != candidate.Scope || rows[0].Sensitivity != candidate.Sensitivity {
				continue
			}
			return candidate
		}
	}
	return nil
}

func linkRecordToEntityTerms(ctx context.Context, store storage.Store, rec *schema.MemoryRecord, terms []string, predicate, inverse string, ts time.Time) []schema.GraphEdge {
	lookup, ok := store.(storage.EntityLookup)
	if !ok || rec == nil {
		return nil
	}
	edges := make([]schema.GraphEdge, 0, len(terms)*2)
	seen := make(map[string]struct{}, len(terms))
	for _, term := range terms {
		entity := findEntityByTerm(ctx, lookup, term, rec.Scope, rec.Sensitivity)
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
		if entity.Scope == rec.Scope && entity.Sensitivity == rec.Sensitivity {
			edges = append(edges, schema.GraphEdge{SourceID: entity.ID, Predicate: inverse, TargetID: rec.ID, Weight: 1.0, CreatedAt: ts})
		}
	}
	return edges
}
