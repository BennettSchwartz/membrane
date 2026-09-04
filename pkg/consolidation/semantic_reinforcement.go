package consolidation

import (
	"context"
	"fmt"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

// reinforceSemanticFact keeps the consumed source snapshot and matching fact
// policy in the same transaction as persistence. Lower new contributions are
// deliberately ignored; known sources only repair classification. A nil gain
// selects the destination's configured reinforcement gain.
func reinforceSemanticFact(ctx context.Context, store storage.Store, source, existing *schema.MemoryRecord, triple Triple, actor string, now time.Time, gain *float64) (*schema.MemoryRecord, bool, error) {
	if source == nil || existing == nil {
		return nil, false, fmt.Errorf("missing semantic source or destination")
	}
	terms := semanticEntityTerms(triple.Subject, triple.Object)
	if payload, ok := existing.Payload.(*schema.SemanticPayload); ok && payload != nil {
		terms = append(terms, semanticEntityTerms(payload.Subject, payload.Object)...)
	}
	candidates := snapshotEntityCandidates(ctx, store, source.Scope, terms...)
	var updated *schema.MemoryRecord
	err := storage.WithTransaction(ctx, store, func(tx storage.Transaction) error {
		current, err := storage.GetDerivedDestination(ctx, tx, existing.ID)
		if err != nil {
			return err
		}
		known := semanticFactHasSource(current, source.ID)
		allowed, err := storage.DerivedSourceMayReinforce(ctx, tx, current, source, known)
		if err != nil || !allowed {
			return err
		}
		oldSensitivity := current.Sensitivity
		if err := storage.ApplyDerivedSourcePolicy(ctx, tx, current, []*schema.MemoryRecord{source}); err != nil {
			return err
		}
		payload, ok := current.Payload.(*schema.SemanticPayload)
		if !ok || payload == nil || current.Type != schema.MemoryTypeSemantic {
			return fmt.Errorf("semantic destination changed")
		}
		entityStore := entityStoreInTransaction(candidates, tx)
		sourceKey := semanticScopedObservationFactKey(ctx, entityStore, current.Scope, current.Sensitivity, triple.Subject, triple.Predicate, triple.Object)
		currentKey := semanticScopedObservationFactKey(ctx, entityStore, current.Scope, current.Sensitivity, payload.Subject, payload.Predicate, payload.Object)
		if currentKey != sourceKey {
			return fmt.Errorf("semantic destination fact changed")
		}
		if known && current.Sensitivity == oldSensitivity {
			return nil
		}
		if !known {
			boost := current.Lifecycle.Decay.ReinforcementGain
			if gain != nil {
				boost = *gain
			}
			current.Salience += boost
			if current.Salience > 1 {
				current.Salience = 1
			}
			current.Lifecycle.LastReinforcedAt = now
			appendSemanticSource(current, source.ID, now, actor)
		}
		current.UpdatedAt = now
		if oldSensitivity != current.Sensitivity {
			if err := storage.PruneDerivedInverseRelations(ctx, tx, current); err != nil {
				return err
			}
		}
		if err := tx.Update(ctx, current); err != nil {
			return err
		}
		if err := tx.AddAuditEntry(ctx, current.ID, schema.AuditEntry{
			Action: schema.AuditActionReinforce, Actor: actor, Timestamp: now,
			Rationale: fmt.Sprintf("Reinforced from episodic record %s", source.ID),
		}); err != nil {
			return err
		}
		updated = current
		return nil
	})
	return updated, updated != nil && err == nil, err
}
