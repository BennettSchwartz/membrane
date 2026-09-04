package storage

import (
	"context"
	"fmt"

	"github.com/BennettSchwartz/membrane/pkg/schema"
)

// DerivedRecordPolicy reads and locks authoritative policy through the
// transaction's field-only metadata contract. Unsupported and missing metadata
// cannot authorize evidence incorporation or graph canonicalization.
func DerivedRecordPolicy(ctx context.Context, tx Transaction, id string) (RecordAuthorizationMetadata, error) {
	lookup, ok := tx.(AuthorizationMetadataStore)
	if !ok {
		return RecordAuthorizationMetadata{}, ErrAuthorizationMetadataUnsupported
	}
	rows, err := lookup.GetAuthorizationMetadata(ctx, []string{id})
	if err != nil {
		return RecordAuthorizationMetadata{}, err
	}
	if len(rows) != 1 || rows[0].ID != id || id == "" {
		return RecordAuthorizationMetadata{}, ErrNotFound
	}
	if !schema.IsValidSensitivity(rows[0].Sensitivity) {
		return RecordAuthorizationMetadata{}, fmt.Errorf("invalid derived record sensitivity")
	}
	return rows[0], nil
}

// GetDerivedDestination reloads the destination after locking its policy, so a
// background snapshot never overwrites newer payload, evidence or labels.
func GetDerivedDestination(ctx context.Context, tx Transaction, id string) (*schema.MemoryRecord, error) {
	policy, err := DerivedRecordPolicy(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	rec, err := tx.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if rec == nil || rec.ID != id || rec.Scope != policy.Scope || rec.Sensitivity != policy.Sensitivity {
		return nil, fmt.Errorf("derived destination policy changed")
	}
	return rec, nil
}

// DerivedSourceMayReinforce admits new evidence only when both the consumed
// snapshot and locked source classification meet the current destination label.
// Lower new contributions are a no-op. Scope drift fails so extraction can retry.
// Known sources may repair
// classification, but callers must not grant them repeat salience or counters.
func DerivedSourceMayReinforce(ctx context.Context, tx Transaction, destination, source *schema.MemoryRecord, known bool) (bool, error) {
	if destination == nil || source == nil || !schema.IsValidSensitivity(destination.Sensitivity) || !schema.IsValidSensitivity(source.Sensitivity) {
		return false, fmt.Errorf("invalid derived source policy")
	}
	policy, err := DerivedRecordPolicy(ctx, tx, source.ID)
	if err != nil {
		return false, err
	}
	if policy.Scope != source.Scope || destination.Scope != policy.Scope {
		return false, fmt.Errorf("incompatible derived source scope")
	}
	if !known && (derivedSensitivityRank(source.Sensitivity) < derivedSensitivityRank(destination.Sensitivity) || derivedSensitivityRank(policy.Sensitivity) < derivedSensitivityRank(destination.Sensitivity)) {
		return false, nil
	}
	return true, nil
}

// ApplyDerivedSourcePolicy preserves scope and raises sensitivity to cover both
// the consumed source snapshots and their authoritative transaction labels.
// A source moved to a different scope must be reconsidered by a later run.
func ApplyDerivedSourcePolicy(ctx context.Context, tx Transaction, destination *schema.MemoryRecord, sources []*schema.MemoryRecord) error {
	if destination == nil || !schema.IsValidSensitivity(destination.Sensitivity) || len(sources) == 0 {
		return fmt.Errorf("invalid derived source policy")
	}
	for _, source := range sources {
		if source == nil || !schema.IsValidSensitivity(source.Sensitivity) {
			return fmt.Errorf("invalid derived source policy")
		}
		policy, err := DerivedRecordPolicy(ctx, tx, source.ID)
		if err != nil {
			return err
		}
		if policy.Scope != source.Scope || destination.Scope != policy.Scope {
			return fmt.Errorf("incompatible derived source scope")
		}
		for _, sensitivity := range []schema.Sensitivity{source.Sensitivity, policy.Sensitivity} {
			if derivedSensitivityRank(sensitivity) > derivedSensitivityRank(destination.Sensitivity) {
				destination.Sensitivity = sensitivity
			}
		}
	}
	return nil
}

// PruneDerivedInverseRelations removes known entity backreferences that a
// sensitivity promotion made unsafe. The derived record's forward links remain
// available to authorized incoming graph traversal.
func PruneDerivedInverseRelations(ctx context.Context, tx Transaction, destination *schema.MemoryRecord) error {
	seen := make(map[string]struct{})
	for _, relation := range destination.Relations {
		var inverse string
		switch schema.NormalizeGraphPredicate(relation.Predicate) {
		case schema.GraphPredicateSubjectEntity:
			inverse = schema.GraphPredicateFactSubjectOf
		case schema.GraphPredicateObjectEntity:
			inverse = schema.GraphPredicateFactObjectOf
		case schema.GraphPredicateUses:
			inverse = schema.GraphPredicateUsedBy
		default:
			continue
		}
		key := relation.TargetID + "\x00" + inverse
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		entity, err := GetDerivedDestination(ctx, tx, relation.TargetID)
		if err != nil {
			return err
		}
		if entity.Type != schema.MemoryTypeEntity || (entity.Scope == destination.Scope && entity.Sensitivity == destination.Sensitivity) {
			continue
		}
		retained := make([]schema.Relation, 0, len(entity.Relations))
		for _, edge := range entity.Relations {
			if edge.TargetID == destination.ID && schema.NormalizeGraphPredicate(edge.Predicate) == inverse {
				continue
			}
			retained = append(retained, edge)
		}
		if len(retained) != len(entity.Relations) {
			entity.Relations = retained
			if err := tx.Update(ctx, entity); err != nil {
				return err
			}
		}
	}
	return nil
}

func derivedSensitivityRank(s schema.Sensitivity) int {
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
