// Package storage defines the Store interface that all storage backends must implement.
// It provides the contract for persisting and retrieving MemoryRecords.
package storage

import (
	"context"
	"errors"

	"github.com/GustyCube/membrane/pkg/schema"
)

// Common storage errors.
var (
	// ErrNotFound is returned when a requested record does not exist.
	ErrNotFound = errors.New("record not found")

	// ErrAlreadyExists is returned when attempting to create a record with a duplicate ID.
	ErrAlreadyExists = errors.New("record already exists")

	// ErrTxClosed is returned when attempting to use a committed or rolled-back transaction.
	ErrTxClosed = errors.New("transaction already closed")
)

// Store is the interface that all storage backends must implement.
// It provides CRUD operations for MemoryRecords along with specialized
// operations for relations, audit entries, and salience updates.
type Store interface {
	// Create persists a new MemoryRecord. Returns ErrAlreadyExists if the ID is taken.
	Create(ctx context.Context, record *schema.MemoryRecord) error

	// Get retrieves a single MemoryRecord by ID. Returns ErrNotFound if it does not exist.
	Get(ctx context.Context, id string) (*schema.MemoryRecord, error)

	// Update replaces an existing MemoryRecord. Returns ErrNotFound if the ID does not exist.
	Update(ctx context.Context, record *schema.MemoryRecord) error

	// Delete removes a MemoryRecord by ID. Returns ErrNotFound if it does not exist.
	Delete(ctx context.Context, id string) error

	// List retrieves MemoryRecords matching the given filter options.
	List(ctx context.Context, opts ListOptions) ([]*schema.MemoryRecord, error)

	// ListByType retrieves all MemoryRecords of a given type.
	ListByType(ctx context.Context, memType schema.MemoryType) ([]*schema.MemoryRecord, error)

	// UpdateSalience sets the salience value for a specific record.
	// Returns ErrNotFound if the record does not exist.
	UpdateSalience(ctx context.Context, id string, salience float64) error

	// AddAuditEntry appends an audit log entry to a record.
	// Returns ErrNotFound if the record does not exist.
	AddAuditEntry(ctx context.Context, id string, entry schema.AuditEntry) error

	// AddRelation adds a relation edge from sourceID to another record.
	// Returns ErrNotFound if the source record does not exist.
	AddRelation(ctx context.Context, sourceID string, rel schema.Relation) error

	// GetRelations retrieves all relations originating from the given record ID.
	// Returns ErrNotFound if the record does not exist.
	GetRelations(ctx context.Context, id string) ([]schema.Relation, error)

	// Begin starts a new transaction. The returned Transaction wraps Store
	// methods and must be committed or rolled back.
	Begin(ctx context.Context) (Transaction, error)

	// Close releases any resources held by the store (e.g., database connections).
	Close() error
}

// ListOptions specifies filters for the List operation.
type ListOptions struct {
	// Type filters records by memory type. Empty means no filter.
	Type schema.MemoryType

	// Tags filters records that have ALL of the specified tags.
	Tags []string

	// Scope filters records by scope. Empty means no filter.
	Scope string

	// Sensitivity filters records by sensitivity level. Empty means no filter.
	Sensitivity schema.Sensitivity

	// MinSalience filters records with salience >= this value.
	// A value of 0 means no minimum filter.
	MinSalience float64

	// MaxSalience filters records with salience <= this value.
	// A value of 0 means no maximum filter.
	MaxSalience float64

	// Limit caps the number of returned records. 0 means no limit.
	Limit int

	// Offset skips the first N records (for pagination).
	Offset int
}

// Transaction wraps Store methods in an atomic transaction.
// Callers must call either Commit or Rollback when done.
// Using the Transaction after Commit or Rollback returns ErrTxClosed.
type Transaction interface {
	// Create persists a new MemoryRecord within the transaction.
	Create(ctx context.Context, record *schema.MemoryRecord) error

	// Get retrieves a single MemoryRecord by ID within the transaction.
	Get(ctx context.Context, id string) (*schema.MemoryRecord, error)

	// Update replaces an existing MemoryRecord within the transaction.
	Update(ctx context.Context, record *schema.MemoryRecord) error

	// Delete removes a MemoryRecord by ID within the transaction.
	Delete(ctx context.Context, id string) error

	// List retrieves MemoryRecords matching the given filter options within the transaction.
	List(ctx context.Context, opts ListOptions) ([]*schema.MemoryRecord, error)

	// ListByType retrieves all MemoryRecords of a given type within the transaction.
	ListByType(ctx context.Context, memType schema.MemoryType) ([]*schema.MemoryRecord, error)

	// UpdateSalience sets the salience value for a specific record within the transaction.
	UpdateSalience(ctx context.Context, id string, salience float64) error

	// AddAuditEntry appends an audit log entry to a record within the transaction.
	AddAuditEntry(ctx context.Context, id string, entry schema.AuditEntry) error

	// AddRelation adds a relation edge within the transaction.
	AddRelation(ctx context.Context, sourceID string, rel schema.Relation) error

	// GetRelations retrieves all relations for a record within the transaction.
	GetRelations(ctx context.Context, id string) ([]schema.Relation, error)

	// Commit atomically applies all operations in the transaction.
	Commit() error

	// Rollback discards all operations in the transaction.
	Rollback() error
}
