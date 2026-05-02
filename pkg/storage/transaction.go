package storage

import (
	"context"
	"fmt"
)

// WithTransaction executes fn within a transaction obtained from the given Store.
// If fn returns nil, the transaction is committed. If fn returns an error or
// panics, the transaction is rolled back. The error from fn (or commit) is returned.
func WithTransaction(ctx context.Context, s Store, fn func(tx Transaction) error) (err error) {
	tx, err := s.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	shouldRollback := true
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p) // re-panic after rollback
		}
		if shouldRollback && err != nil {
			_ = tx.Rollback()
		}
	}()

	if err = fn(tx); err != nil {
		return err
	}

	shouldRollback = false
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}
