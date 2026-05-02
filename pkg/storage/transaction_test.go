package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/BennettSchwartz/membrane/pkg/schema"
)

type txTestStore struct {
	tx       *txTestTransaction
	beginErr error
}

func (s *txTestStore) Create(context.Context, *schema.MemoryRecord) error { return nil }
func (s *txTestStore) Get(context.Context, string) (*schema.MemoryRecord, error) {
	return nil, ErrNotFound
}
func (s *txTestStore) Update(context.Context, *schema.MemoryRecord) error { return nil }
func (s *txTestStore) Delete(context.Context, string) error               { return nil }
func (s *txTestStore) List(context.Context, ListOptions) ([]*schema.MemoryRecord, error) {
	return nil, nil
}
func (s *txTestStore) ListByType(context.Context, schema.MemoryType) ([]*schema.MemoryRecord, error) {
	return nil, nil
}
func (s *txTestStore) UpdateSalience(context.Context, string, float64) error { return nil }
func (s *txTestStore) AddAuditEntry(context.Context, string, schema.AuditEntry) error {
	return nil
}
func (s *txTestStore) AddRelation(context.Context, string, schema.Relation) error { return nil }
func (s *txTestStore) GetRelations(context.Context, string) ([]schema.Relation, error) {
	return nil, nil
}
func (s *txTestStore) Begin(context.Context) (Transaction, error) {
	if s.beginErr != nil {
		return nil, s.beginErr
	}
	if s.tx == nil {
		s.tx = &txTestTransaction{}
	}
	return s.tx, nil
}
func (s *txTestStore) Close() error { return nil }

type txTestTransaction struct {
	commitErr     error
	commits       int
	rollbacks     int
	createRecords []*schema.MemoryRecord
}

func (tx *txTestTransaction) Create(_ context.Context, record *schema.MemoryRecord) error {
	tx.createRecords = append(tx.createRecords, record)
	return nil
}
func (tx *txTestTransaction) Get(context.Context, string) (*schema.MemoryRecord, error) {
	return nil, ErrNotFound
}
func (tx *txTestTransaction) Update(context.Context, *schema.MemoryRecord) error { return nil }
func (tx *txTestTransaction) Delete(context.Context, string) error               { return nil }
func (tx *txTestTransaction) List(context.Context, ListOptions) ([]*schema.MemoryRecord, error) {
	return nil, nil
}
func (tx *txTestTransaction) ListByType(context.Context, schema.MemoryType) ([]*schema.MemoryRecord, error) {
	return nil, nil
}
func (tx *txTestTransaction) UpdateSalience(context.Context, string, float64) error { return nil }
func (tx *txTestTransaction) AddAuditEntry(context.Context, string, schema.AuditEntry) error {
	return nil
}
func (tx *txTestTransaction) AddRelation(context.Context, string, schema.Relation) error {
	return nil
}
func (tx *txTestTransaction) GetRelations(context.Context, string) ([]schema.Relation, error) {
	return nil, nil
}
func (tx *txTestTransaction) Commit() error {
	tx.commits++
	return tx.commitErr
}
func (tx *txTestTransaction) Rollback() error {
	tx.rollbacks++
	return nil
}

func TestWithTransactionCommitsOnSuccess(t *testing.T) {
	store := &txTestStore{tx: &txTestTransaction{}}
	err := WithTransaction(context.Background(), store, func(tx Transaction) error {
		return tx.Create(context.Background(), &schema.MemoryRecord{ID: "rec-1"})
	})
	if err != nil {
		t.Fatalf("WithTransaction: %v", err)
	}
	if store.tx.commits != 1 || store.tx.rollbacks != 0 {
		t.Fatalf("commits=%d rollbacks=%d, want 1 commit and 0 rollbacks", store.tx.commits, store.tx.rollbacks)
	}
	if len(store.tx.createRecords) != 1 {
		t.Fatalf("created records = %d, want 1", len(store.tx.createRecords))
	}
}

func TestWithTransactionRollsBackOnBeginFunctionAndCommitErrors(t *testing.T) {
	ctx := context.Background()
	beginErr := errors.New("begin failed")
	store := &txTestStore{beginErr: beginErr}
	if err := WithTransaction(ctx, store, func(Transaction) error { return nil }); !errors.Is(err, beginErr) {
		t.Fatalf("begin error = %v, want %v", err, beginErr)
	}

	fnErr := errors.New("work failed")
	store = &txTestStore{tx: &txTestTransaction{}}
	if err := WithTransaction(ctx, store, func(Transaction) error { return fnErr }); !errors.Is(err, fnErr) {
		t.Fatalf("function error = %v, want %v", err, fnErr)
	}
	if store.tx.commits != 0 || store.tx.rollbacks != 1 {
		t.Fatalf("function error commits=%d rollbacks=%d, want 0/1", store.tx.commits, store.tx.rollbacks)
	}

	commitErr := errors.New("commit failed")
	store = &txTestStore{tx: &txTestTransaction{commitErr: commitErr}}
	if err := WithTransaction(ctx, store, func(Transaction) error { return nil }); !errors.Is(err, commitErr) {
		t.Fatalf("commit error = %v, want %v", err, commitErr)
	}
	if store.tx.commits != 1 || store.tx.rollbacks != 0 {
		t.Fatalf("commit error commits=%d rollbacks=%d, want 1/0", store.tx.commits, store.tx.rollbacks)
	}
}

func TestWithTransactionRollsBackAndRepanics(t *testing.T) {
	store := &txTestStore{tx: &txTestTransaction{}}
	defer func() {
		got := recover()
		if got != "boom" {
			t.Fatalf("recover = %#v, want boom", got)
		}
		if store.tx.commits != 0 || store.tx.rollbacks != 1 {
			t.Fatalf("panic commits=%d rollbacks=%d, want 0/1", store.tx.commits, store.tx.rollbacks)
		}
	}()

	_ = WithTransaction(context.Background(), store, func(Transaction) error {
		panic("boom")
	})
}
