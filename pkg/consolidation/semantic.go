package consolidation

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

// SemanticConsolidator extracts semantic facts from episodic records.
// For each episodic record with a successful outcome it checks whether
// a matching semantic record already exists. If not, a new semantic
// memory record is created; if yes, the existing record is reinforced.
type SemanticConsolidator struct {
	store storage.Store
}

// NewSemanticConsolidator creates a SemanticConsolidator backed by store.
func NewSemanticConsolidator(store storage.Store) *SemanticConsolidator {
	return &SemanticConsolidator{store: store}
}

// Consolidate scans episodic records with successful outcomes and
// extracts semantic observations. It returns the number of new semantic
// records created, the number of existing records reinforced, and any error.
func (c *SemanticConsolidator) Consolidate(ctx context.Context) (int, int, error) {
	episodics, err := c.store.ListByType(ctx, schema.MemoryTypeEpisodic)
	if err != nil {
		return 0, 0, err
	}

	// Load existing semantic records for duplicate checking.
	semantics, err := c.store.ListByType(ctx, schema.MemoryTypeSemantic)
	if err != nil {
		return 0, 0, err
	}

	// Build an index of existing semantic observations keyed by exact fact.
	existing := make(map[semanticFactKeyValue]*schema.MemoryRecord, len(semantics))
	for _, s := range semantics {
		sp, ok := s.Payload.(*schema.SemanticPayload)
		if !ok {
			continue
		}
		existing[semanticScopedFactKey(s.Scope, sp.Subject, sp.Predicate, sp.Object)] = s
		existing[semanticScopedObservationFactKey(ctx, c.store, s.Scope, s.Sensitivity, sp.Subject, sp.Predicate, sp.Object)] = s
	}

	now := time.Now().UTC()
	created := 0
	reinforced := 0

	for _, rec := range episodics {
		ep, ok := rec.Payload.(*schema.EpisodicPayload)
		if !ok {
			continue
		}

		// Only process episodes with successful outcomes.
		if ep.Outcome != schema.OutcomeStatusSuccess {
			continue
		}

		// Extract observations from timeline events. Each event with a
		// summary is treated as a potential semantic fact. In the future
		// this would be replaced by LLM-based extraction.
		for _, evt := range ep.Timeline {
			if evt.Summary == "" {
				continue
			}

			subject := evt.EventKind
			predicate := "observed_in"
			object := evt.Summary

			key := semanticScopedObservationFactKey(ctx, c.store, rec.Scope, rec.Sensitivity, subject, predicate, object)
			existingRec, found := existing[key]
			if found {
				gain := 0.1
				current, didReinforce, err := reinforceSemanticFact(ctx, c.store, rec, existingRec, Triple{Subject: subject, Predicate: predicate, Object: object}, "consolidation/semantic", now, &gain)
				if err != nil {
					return created, reinforced, err
				}
				if didReinforce {
					existing[key] = current
					reinforced++
				}
				continue
			}

			// Create a new semantic record.
			payload := &schema.SemanticPayload{
				Kind:      "semantic",
				Subject:   subject,
				Predicate: predicate,
				Object:    object,
				Validity: schema.Validity{
					Mode: schema.ValidityModeGlobal,
				},
				Evidence: []schema.ProvenanceRef{
					{
						SourceType: "episodic",
						SourceID:   rec.ID,
						Timestamp:  now,
					},
				},
				RevisionPolicy: "replace",
			}

			newRec := schema.NewMemoryRecord(
				uuid.New().String(),
				schema.MemoryTypeSemantic,
				rec.Sensitivity,
				payload,
			)
			newRec.Confidence = rec.Confidence
			newRec.Scope = rec.Scope
			newRec.Tags = deriveTags(rec)
			newRec.Provenance = schema.Provenance{
				Sources: []schema.ProvenanceSource{
					{
						Kind:      schema.ProvenanceKindObservation,
						Ref:       rec.ID,
						CreatedBy: "consolidation/semantic",
						Timestamp: now,
					},
				},
				CreatedBy: "consolidation/semantic",
			}
			newRec.AuditLog = []schema.AuditEntry{
				{
					Action:    schema.AuditActionCreate,
					Actor:     "consolidation/semantic",
					Timestamp: now,
					Rationale: fmt.Sprintf("Extracted from episodic record %s", rec.ID),
				},
			}

			candidates := snapshotEntityCandidates(ctx, c.store, newRec.Scope, semanticEntityTerms(subject, object)...)
			err := storage.WithTransaction(ctx, c.store, func(tx storage.Transaction) error {
				if err := storage.ApplyDerivedSourcePolicy(ctx, tx, newRec, []*schema.MemoryRecord{rec}); err != nil {
					return err
				}
				entityEdges := canonicalizeSemanticRecordEntities(ctx, entityStoreInTransaction(candidates, tx), newRec)
				if err := tx.Create(ctx, newRec); err != nil {
					return err
				}
				rel := schema.Relation{
					Predicate: schema.GraphPredicateDerivedFrom,
					TargetID:  rec.ID,
					Weight:    1.0,
					CreatedAt: now,
				}
				if err := tx.AddRelation(ctx, newRec.ID, rel); err != nil {
					return err
				}
				for _, edge := range entityEdges {
					if edge.SourceID == newRec.ID {
						continue
					}
					if err := tx.AddRelation(ctx, edge.SourceID, schema.Relation{
						Predicate: edge.Predicate,
						TargetID:  edge.TargetID,
						Weight:    edge.Weight,
						CreatedAt: edge.CreatedAt,
					}); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				return created, reinforced, err
			}

			// Track in the local index to avoid duplicates within the
			// same consolidation run.
			existing[key] = newRec
			created++
		}
	}

	return created, reinforced, nil
}

func semanticFactHasSource(rec *schema.MemoryRecord, sourceID string) bool {
	if rec == nil || strings.TrimSpace(sourceID) == "" {
		return false
	}
	if payload, ok := rec.Payload.(*schema.SemanticPayload); ok && payload != nil {
		for _, evidence := range payload.Evidence {
			if evidence.SourceID == sourceID {
				return true
			}
		}
	}
	for _, source := range rec.Provenance.Sources {
		if source.Ref == sourceID {
			return true
		}
	}
	return false
}

func appendSemanticSource(rec *schema.MemoryRecord, sourceID string, ts time.Time, actor string) {
	if rec == nil || strings.TrimSpace(sourceID) == "" {
		return
	}
	if !semanticFactHasSource(rec, sourceID) {
		if payload, ok := rec.Payload.(*schema.SemanticPayload); ok && payload != nil {
			payload.Evidence = append(payload.Evidence, schema.ProvenanceRef{
				SourceType: "episodic",
				SourceID:   sourceID,
				Timestamp:  ts,
			})
			rec.Payload = payload
		}
		rec.Provenance.Sources = append(rec.Provenance.Sources, schema.ProvenanceSource{
			Kind:      schema.ProvenanceKindObservation,
			Ref:       sourceID,
			CreatedBy: actor,
			Timestamp: ts,
		})
	}
	if !semanticHasDerivedRelation(rec, sourceID) {
		rec.Relations = append(rec.Relations, schema.Relation{
			Predicate: schema.GraphPredicateDerivedFrom,
			TargetID:  sourceID,
			Weight:    1.0,
			CreatedAt: ts,
		})
	}
	if rec.Provenance.CreatedBy == "" {
		rec.Provenance.CreatedBy = actor
	}
}

func semanticHasDerivedRelation(rec *schema.MemoryRecord, sourceID string) bool {
	if rec == nil || strings.TrimSpace(sourceID) == "" {
		return false
	}
	for _, rel := range rec.Relations {
		if schema.NormalizeGraphPredicate(rel.Predicate) == schema.GraphPredicateDerivedFrom && rel.TargetID == sourceID {
			return true
		}
	}
	return false
}

// deriveTags builds a tag set for a consolidated record from its
// episodic source. It preserves existing tags and adds a consolidation
// marker.
func deriveTags(rec *schema.MemoryRecord) []string {
	tags := make([]string, 0, len(rec.Tags)+1)
	tags = append(tags, "consolidated")
	for _, t := range rec.Tags {
		if !strings.EqualFold(t, "consolidated") {
			tags = append(tags, t)
		}
	}
	return tags
}

type semanticFactKeyValue struct{ scope, subject, predicate, object string }

func semanticFactKey(subject, predicate string, object any) semanticFactKeyValue {
	return semanticScopedFactKey("", subject, predicate, object)
}

func semanticScopedFactKey(scope, subject, predicate string, object any) semanticFactKeyValue {
	return semanticFactKeyValue{
		scope:     strings.TrimSpace(scope),
		subject:   strings.TrimSpace(subject),
		predicate: schema.NormalizeSemanticPredicate(predicate),
		object:    schema.SemanticObjectKey(object),
	}
}

func semanticObservationFactKey(ctx context.Context, store storage.Store, scope string, sensitivity schema.Sensitivity, subject, predicate string, object any) semanticFactKeyValue {
	keySubject := subject
	keyObject := object
	if lookup, ok := store.(storage.EntityLookup); ok {
		if entity := findEntityByTerm(ctx, lookup, subject, scope, sensitivity); entity != nil {
			keySubject = entity.ID
		}
		if objectTerm, ok := object.(string); ok {
			if entity := findEntityByTerm(ctx, lookup, objectTerm, scope, sensitivity); entity != nil {
				keyObject = entity.ID
			}
		}
	}
	return semanticFactKey(keySubject, predicate, keyObject)
}

func semanticScopedObservationFactKey(ctx context.Context, store storage.Store, scope string, sensitivity schema.Sensitivity, subject, predicate string, object any) semanticFactKeyValue {
	key := semanticObservationFactKey(ctx, store, scope, sensitivity, subject, predicate, object)
	key.scope = strings.TrimSpace(scope)
	return key
}
