package decay

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

// Service applies decay and reinforcement to memory records.
type Service struct {
	store storage.Store
}

// NewService creates a new decay service backed by the given store.
func NewService(store storage.Store) *Service {
	return &Service{store: store}
}

// ApplyDecay calculates and applies decay to a single record's salience
// based on elapsed time since LastReinforcedAt.
func (s *Service) ApplyDecay(ctx context.Context, id string) error {
	return storage.WithTransaction(ctx, s.store, func(tx storage.Transaction) error {
		record, err := tx.Get(ctx, id)
		if err != nil {
			return fmt.Errorf("decay: get record %s: %w", id, err)
		}

		now := time.Now().UTC()
		elapsed := now.Sub(record.Lifecycle.LastReinforcedAt).Seconds()
		if elapsed < 0 {
			elapsed = 0
		}

		profile := record.Lifecycle.Decay
		newSalience := record.Salience

		if profile.MaxAgeSeconds > 0 {
			ageSeconds := now.Sub(record.CreatedAt).Seconds()
			if ageSeconds >= float64(profile.MaxAgeSeconds) {
				newSalience = 0
			}
		}

		if newSalience > 0 {
			decayFn := GetDecayFunc(profile.Curve)
			newSalience = decayFn(record.Salience, elapsed, profile)
		}

		// Guard against NaN or negative values from misconfigured curves.
		if math.IsNaN(newSalience) || math.IsInf(newSalience, 0) {
			newSalience = profile.MinSalience
		}
		if newSalience < 0 {
			newSalience = 0
		}

		if err := tx.UpdateSalience(ctx, id, newSalience); err != nil {
			return fmt.Errorf("decay: update salience %s: %w", id, err)
		}

		entry := schema.AuditEntry{
			Action:    schema.AuditActionDecay,
			Actor:     "decay-service",
			Timestamp: now,
			Rationale: fmt.Sprintf("decay applied: %.4f -> %.4f (elapsed %.0fs)", record.Salience, newSalience, elapsed),
		}
		if err := tx.AddAuditEntry(ctx, id, entry); err != nil {
			return fmt.Errorf("decay: add audit entry %s: %w", id, err)
		}

		return nil
	})
}

// Reinforce boosts a record's salience by its ReinforcementGain, updates
// LastReinforcedAt, and adds an audit entry.
func (s *Service) Reinforce(ctx context.Context, id string, actor string, rationale string) error {
	return storage.WithTransaction(ctx, s.store, func(tx storage.Transaction) error {
		record, err := tx.Get(ctx, id)
		if err != nil {
			return fmt.Errorf("reinforce: get record %s: %w", id, err)
		}

		gain := record.Lifecycle.Decay.ReinforcementGain
		newSalience := record.Salience + gain
		if newSalience > 1.0 {
			newSalience = 1.0
		}

		now := time.Now().UTC()
		record.Salience = newSalience
		record.Lifecycle.LastReinforcedAt = now
		record.UpdatedAt = now
		if err := tx.Update(ctx, record); err != nil {
			return fmt.Errorf("reinforce: update record %s: %w", id, err)
		}

		entry := schema.AuditEntry{
			Action:    schema.AuditActionReinforce,
			Actor:     actor,
			Timestamp: now,
			Rationale: rationale,
		}
		if err := tx.AddAuditEntry(ctx, id, entry); err != nil {
			return fmt.Errorf("reinforce: add audit entry %s: %w", id, err)
		}

		return nil
	})
}

// ReinforceFromSource boosts a record and records the source evidence when the
// record is a semantic fact. It is used by consolidation paths that reinforce
// an existing durable fact from a specific episodic observation.
func (s *Service) ReinforceFromSource(ctx context.Context, id, sourceType, sourceID, actor, rationale string) error {
	sourceID = strings.TrimSpace(sourceID)
	if sourceID == "" {
		return fmt.Errorf("reinforce: source ID is required")
	}
	sourceType = strings.TrimSpace(sourceType)
	if sourceType == "" {
		sourceType = "observation"
	}

	return storage.WithTransaction(ctx, s.store, func(tx storage.Transaction) error {
		record, err := storage.GetDerivedDestination(ctx, tx, id)
		if err != nil {
			return fmt.Errorf("reinforce: get record %s: %w", id, err)
		}
		policy, err := storage.DerivedRecordPolicy(ctx, tx, sourceID)
		if err != nil {
			return fmt.Errorf("reinforce: source policy: %w", err)
		}
		source := &schema.MemoryRecord{ID: sourceID, Scope: policy.Scope, Sensitivity: policy.Sensitivity}
		oldSensitivity := record.Sensitivity
		knownSource := provenanceHasSource(record.Provenance.Sources, sourceID)
		if payload, ok := record.Payload.(*schema.SemanticPayload); ok && payload != nil {
			knownSource = knownSource || semanticPayloadHasSource(payload, sourceID)
		}
		allowed, err := storage.DerivedSourceMayReinforce(ctx, tx, record, source, knownSource)
		if err != nil || !allowed {
			return err
		}
		if err := storage.ApplyDerivedSourcePolicy(ctx, tx, record, []*schema.MemoryRecord{source}); err != nil {
			return err
		}
		if knownSource && oldSensitivity == record.Sensitivity {
			return nil
		}

		now := time.Now().UTC()
		if !knownSource {
			record.Salience += record.Lifecycle.Decay.ReinforcementGain
			if record.Salience > 1.0 {
				record.Salience = 1.0
			}
			record.Lifecycle.LastReinforcedAt = now
			appendSemanticEvidenceSource(record, sourceType, sourceID, actor, now)
		}
		record.UpdatedAt = now
		if oldSensitivity != record.Sensitivity {
			if err := storage.PruneDerivedInverseRelations(ctx, tx, record); err != nil {
				return err
			}
		}
		if err := tx.Update(ctx, record); err != nil {
			return fmt.Errorf("reinforce: update record %s: %w", id, err)
		}
		if err := tx.AddAuditEntry(ctx, id, schema.AuditEntry{
			Action: schema.AuditActionReinforce, Actor: actor, Timestamp: now, Rationale: rationale,
		}); err != nil {
			return fmt.Errorf("reinforce: add audit entry %s: %w", id, err)
		}
		return nil
	})
}

// Penalize reduces a record's salience by the given amount, floored at
// MinSalience, and adds an audit entry.
func (s *Service) Penalize(ctx context.Context, id string, amount float64, actor string, rationale string) error {
	if amount < 0 || math.IsNaN(amount) || math.IsInf(amount, 0) {
		return fmt.Errorf("penalize: amount must be non-negative and finite")
	}

	return storage.WithTransaction(ctx, s.store, func(tx storage.Transaction) error {
		record, err := tx.Get(ctx, id)
		if err != nil {
			return fmt.Errorf("penalize: get record %s: %w", id, err)
		}

		floor := record.Lifecycle.Decay.MinSalience
		newSalience := record.Salience - amount
		if newSalience < floor {
			newSalience = floor
		}

		if err := tx.UpdateSalience(ctx, id, newSalience); err != nil {
			return fmt.Errorf("penalize: update salience %s: %w", id, err)
		}

		now := time.Now().UTC()
		entry := schema.AuditEntry{
			Action:    schema.AuditActionDecay,
			Actor:     actor,
			Timestamp: now,
			Rationale: fmt.Sprintf("penalty: %s", rationale),
		}
		if err := tx.AddAuditEntry(ctx, id, entry); err != nil {
			return fmt.Errorf("penalize: add audit entry %s: %w", id, err)
		}

		return nil
	})
}

func appendSemanticEvidenceSource(record *schema.MemoryRecord, sourceType, sourceID, actor string, ts time.Time) {
	if record == nil || strings.TrimSpace(sourceID) == "" {
		return
	}
	if payload, ok := record.Payload.(*schema.SemanticPayload); ok && payload != nil && !semanticPayloadHasSource(payload, sourceID) {
		payload.Evidence = append(payload.Evidence, schema.ProvenanceRef{
			SourceType: sourceType,
			SourceID:   sourceID,
			Timestamp:  ts,
		})
		record.Payload = payload
	}
	if !provenanceHasSource(record.Provenance.Sources, sourceID) {
		record.Provenance.Sources = append(record.Provenance.Sources, schema.ProvenanceSource{
			Kind:      provenanceKindForSourceType(sourceType),
			Ref:       sourceID,
			CreatedBy: actor,
			Timestamp: ts,
		})
	}
	if record.Provenance.CreatedBy == "" {
		record.Provenance.CreatedBy = actor
	}
}

func semanticPayloadHasSource(payload *schema.SemanticPayload, sourceID string) bool {
	for _, evidence := range payload.Evidence {
		if evidence.SourceID == sourceID {
			return true
		}
	}
	return false
}

func provenanceHasSource(sources []schema.ProvenanceSource, sourceID string) bool {
	for _, source := range sources {
		if source.Ref == sourceID {
			return true
		}
	}
	return false
}

func provenanceKindForSourceType(sourceType string) schema.ProvenanceKind {
	switch strings.TrimSpace(sourceType) {
	case "event":
		return schema.ProvenanceKindEvent
	case "artifact":
		return schema.ProvenanceKindArtifact
	case "tool", "tool_call":
		return schema.ProvenanceKindToolCall
	case "outcome":
		return schema.ProvenanceKindOutcome
	default:
		return schema.ProvenanceKindObservation
	}
}

// ApplyDecayAll applies decay to all non-pinned records and returns the
// count of records processed.
func (s *Service) ApplyDecayAll(ctx context.Context) (int, error) {
	records, err := s.store.List(ctx, storage.ListOptions{})
	if err != nil {
		return 0, fmt.Errorf("decay-all: list records: %w", err)
	}

	count := 0
	for _, record := range records {
		// Skip pinned records.
		if record.Lifecycle.Pinned {
			continue
		}

		if err := s.ApplyDecay(ctx, record.ID); err != nil {
			return count, fmt.Errorf("decay-all: record %s: %w", record.ID, err)
		}
		count++
	}

	return count, nil
}

// Prune deletes records whose salience has dropped to their floor and whose
// deletion policy is auto_prune. Pinned records are never pruned.
func (s *Service) Prune(ctx context.Context) (int, error) {
	records, err := s.store.List(ctx, storage.ListOptions{})
	if err != nil {
		return 0, fmt.Errorf("prune: list records: %w", err)
	}

	count := 0
	for _, record := range records {
		// Skip pinned records.
		if record.Lifecycle.Pinned {
			continue
		}

		// Skip if deletion policy is not auto_prune.
		if record.Lifecycle.DeletionPolicy != schema.DeletionPolicyAutoPrune {
			continue
		}

		// Check if salience has dropped to floor (effectively zero).
		// Prune if salience <= MinSalience AND salience is very close to zero (< 0.001).
		floor := record.Lifecycle.Decay.MinSalience
		if record.Salience <= floor && record.Salience < 0.001 {
			if err := s.deletePruned(ctx, record.ID); err != nil {
				return count, fmt.Errorf("prune: delete record %s: %w", record.ID, err)
			}
			count++
		}
	}

	return count, nil
}

// deletePruned deletes a record and adds an audit entry documenting the auto-prune action.
func (s *Service) deletePruned(ctx context.Context, id string) error {
	return storage.WithTransaction(ctx, s.store, func(tx storage.Transaction) error {
		// Add audit entry before deletion.
		now := time.Now().UTC()
		entry := schema.AuditEntry{
			Action:    schema.AuditActionDelete,
			Actor:     "decay-service",
			Timestamp: now,
			Rationale: "auto-pruned: salience reached floor",
		}
		if err := tx.AddAuditEntry(ctx, id, entry); err != nil {
			return fmt.Errorf("add audit entry %s: %w", id, err)
		}

		// Delete the record.
		if err := tx.Delete(ctx, id); err != nil {
			return fmt.Errorf("delete record %s: %w", id, err)
		}

		return nil
	})
}
