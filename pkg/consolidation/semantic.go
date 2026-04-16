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
//
// This is a framework/stub that processes records structurally and can
// be extended with LLM-based extraction in the future.
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

	// Build an index of existing semantic observations keyed by
	// subject + predicate for fast lookup.
	type semKey struct{ subject, predicate string }
	existing := make(map[semKey]*schema.MemoryRecord, len(semantics))
	for _, s := range semantics {
		sp, ok := s.Payload.(*schema.SemanticPayload)
		if !ok {
			continue
		}
		existing[semKey{sp.Subject, sp.Predicate}] = s
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

			key := semKey{subject, predicate}
			if existingRec, found := existing[key]; found {
				err := storage.WithTransaction(ctx, c.store, func(tx storage.Transaction) error {
					newSalience := existingRec.Salience + 0.1
					if newSalience > 1.0 {
						newSalience = 1.0
					}
					if err := tx.UpdateSalience(ctx, existingRec.ID, newSalience); err != nil {
						return err
					}
					entry := schema.AuditEntry{
						Action:    schema.AuditActionReinforce,
						Actor:     "consolidation/semantic",
						Timestamp: now,
						Rationale: fmt.Sprintf("Reinforced from episodic record %s", rec.ID),
					}
					return tx.AddAuditEntry(ctx, existingRec.ID, entry)
				})
				if err != nil {
					return created, reinforced, err
				}
				reinforced++
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

			err := storage.WithTransaction(ctx, c.store, func(tx storage.Transaction) error {
				if err := tx.Create(ctx, newRec); err != nil {
					return err
				}
				rel := schema.Relation{
					Predicate: "derived_from",
					TargetID:  rec.ID,
					Weight:    1.0,
					CreatedAt: now,
				}
				return tx.AddRelation(ctx, newRec.ID, rel)
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
