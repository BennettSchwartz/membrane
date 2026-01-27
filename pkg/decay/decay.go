package decay

import (
	"context"
	"fmt"
	"time"

	"github.com/GustyCube/membrane/pkg/schema"
	"github.com/GustyCube/membrane/pkg/storage"
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

		if err := tx.UpdateSalience(ctx, id, newSalience); err != nil {
			return fmt.Errorf("reinforce: update salience %s: %w", id, err)
		}

		now := time.Now().UTC()
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

// Penalize reduces a record's salience by the given amount, floored at
// MinSalience, and adds an audit entry.
func (s *Service) Penalize(ctx context.Context, id string, amount float64, actor string, rationale string) error {
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
