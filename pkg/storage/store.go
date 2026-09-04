// Package storage defines the persistence contract implemented by Postgres and
// by small test-only stores.
// It provides the contract for persisting and retrieving MemoryRecords.
package storage

import (
	"context"
	"errors"
	"math"

	"github.com/BennettSchwartz/membrane/pkg/schema"
)

// Common storage errors.
var (
	// ErrNotFound is returned when a requested record does not exist.
	ErrNotFound = errors.New("record not found")

	// ErrAlreadyExists is returned when attempting to create a record with a duplicate ID.
	ErrAlreadyExists = errors.New("record already exists")

	// ErrTxClosed is returned when attempting to use a committed or rolled-back transaction.
	ErrTxClosed = errors.New("transaction already closed")

	// ErrAuthorizationMetadataLimit is returned before allocation or storage
	// work when a policy metadata lookup exceeds its hard target ceiling.
	ErrAuthorizationMetadataLimit = errors.New("authorization metadata target limit exceeded")

	// ErrAuthorizationMetadataUnsupported reports that a store cannot perform
	// the field-only policy lookup required at the network mutation boundary.
	ErrAuthorizationMetadataUnsupported = errors.New("store does not support authorization metadata lookup")
)

// MaxBoundedLookupLimit is the largest row budget accepted by finite graph
// lookup contracts. Stores clamp larger direct calls before allocating or
// issuing a query.
const (
	MaxBoundedLookupLimit = 10_000

	// MaxAuthorizationMetadataIDs is the largest exact-ID set accepted by the
	// field-only policy lookup used at the network mutation boundary.
	MaxAuthorizationMetadataIDs = 100

	// MaxBoundedHydrationBytes is the aggregate payload/interpretation budget
	// for one bounded retrieval request.
	MaxBoundedHydrationBytes int64 = 16 << 20

	// ProjectedRecordOverheadBytes conservatively covers bounded record fields
	// outside payload and interpretation JSON (IDs, scope, tags, and envelope).
	ProjectedRecordOverheadBytes int64 = 512 << 10

	// ProjectedRelationOverheadBytes conservatively covers one relation/edge
	// envelope in addition to its source, predicate, and target strings.
	ProjectedRelationOverheadBytes int64 = 4 << 10
)

// BoundedListResult reports records selected under a storage hydration budget.
type BoundedListResult struct {
	Records                 []*schema.MemoryRecord
	ProjectedBytes          int64
	HydrationBytesTruncated bool
}

// BoundedListStore can expose whether a ListOptions hydration-byte budget
// truncated the candidate prefix before full records were hydrated.
type BoundedListStore interface {
	ListBounded(ctx context.Context, opts ListOptions) (BoundedListResult, error)
}

// RecordAuthorizationMetadata is the minimal persisted record shape needed to
// enforce scope and sensitivity policy without hydrating payload or history.
type RecordAuthorizationMetadata struct {
	ID          string
	Scope       string
	Sensitivity schema.Sensitivity
}

// AuthorizationMetadataStore performs one capped, field-only exact-ID lookup.
// Missing IDs are omitted from the result; callers decide whether missing and
// policy-hidden records need an indistinguishable external disposition.
type AuthorizationMetadataStore interface {
	GetAuthorizationMetadata(ctx context.Context, ids []string) ([]RecordAuthorizationMetadata, error)
}

// ValidateSalience rejects values that cannot safely be written through the
// storage-level salience update shortcut.
func ValidateSalience(salience float64) error {
	if math.IsNaN(salience) || math.IsInf(salience, 0) || salience < 0 {
		return &schema.ValidationError{Field: "salience", Message: "salience must be finite and >= 0"}
	}
	return nil
}

// Store is the interface implemented by the production Postgres store and
// narrow test doubles.
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
	// Returns ErrNotFound if the source or target record does not exist.
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

// IncomingRelationLookup is implemented by stores that can retrieve concrete
// edges targeting a record. Retrieval uses it opportunistically so graph
// expansion can cross edges even when a writer only stored one direction.
type IncomingRelationLookup interface {
	GetIncomingRelations(ctx context.Context, targetID string) ([]schema.GraphEdge, error)
}

// BoundedRelationLookup is implemented by stores that enforce an explicit row
// limit while retrieving outgoing graph relations. Implementations must return
// no more than limit rows in deterministic graph-priority order.
type BoundedRelationLookup interface {
	GetRelationsLimited(ctx context.Context, id string, limit int) ([]schema.Relation, error)
}

// BoundedIncomingRelationLookup is implemented by stores that enforce an
// explicit row limit while retrieving incoming graph relations. Implementations
// must return no more than limit rows in deterministic graph-priority order.
type BoundedIncomingRelationLookup interface {
	GetIncomingRelationsLimited(ctx context.Context, targetID string, limit int) ([]schema.GraphEdge, error)
}

// BoundedRelationResult reports outgoing rows hydrated under one aggregate
// byte budget. ProjectedBytes must include every returned relation.
type BoundedRelationResult struct {
	Relations               []schema.Relation
	ProjectedBytes          int64
	HydrationBytesTruncated bool
}

// BoundedIncomingRelationResult is the incoming-edge equivalent of
// BoundedRelationResult.
type BoundedIncomingRelationResult struct {
	Edges                   []schema.GraphEdge
	ProjectedBytes          int64
	HydrationBytesTruncated bool
}

// ByteBoundedRelationLookup prevents large relation strings from turning a
// finite row limit into unbounded hydration work. Implementations must cap
// limit and maxHydratedBytes before allocating or issuing the query.
type ByteBoundedRelationLookup interface {
	GetRelationsBounded(ctx context.Context, id string, limit int, maxHydratedBytes int64) (BoundedRelationResult, error)
}

// ByteBoundedIncomingRelationLookup applies the same contract to incoming
// concrete graph edges.
type ByteBoundedIncomingRelationLookup interface {
	GetIncomingRelationsBounded(ctx context.Context, targetID string, limit int, maxHydratedBytes int64) (BoundedIncomingRelationResult, error)
}

// GraphRecordLookup is a legacy projected-record compatibility interface.
// Bounded network graph expansion uses exact-ID BoundedListStore lookups so
// record bytes are capped before hydration; RetrieveByID remains the trusted
// complete-record API.
type GraphRecordLookup interface {
	GetGraphRecord(ctx context.Context, id string) (*schema.MemoryRecord, error)
}

// EntityLookup is implemented by stores that maintain first-class entity
// lookup indexes.
type EntityLookup interface {
	FindEntitiesByTerm(ctx context.Context, term, scope string, limit int) ([]*schema.MemoryRecord, error)
	FindEntityByIdentifier(ctx context.Context, namespace, value, scope string) (*schema.MemoryRecord, error)
}

// EntityLookupAllScopes is implemented by entity lookup stores that can search
// across every indexed scope. It is intended for unrestricted retrieval trust
// contexts; scoped callers should use EntityLookup so global fallback ordering
// stays anchored to the requested scope.
type EntityLookupAllScopes interface {
	FindEntitiesByTermAllScopes(ctx context.Context, term string, limit int) ([]*schema.MemoryRecord, error)
	FindEntityByIdentifierAllScopes(ctx context.Context, namespace, value string) (*schema.MemoryRecord, error)
}

// GraphEntityLookup is the graph-specific entity lookup variant. Returned
// records leave relation, audit, and provenance-source history unhydrated so
// graph callers can load relations with an explicit edge budget.
type GraphEntityLookup interface {
	FindGraphEntitiesByTerm(ctx context.Context, term, scope string, limit int) ([]*schema.MemoryRecord, error)
	FindGraphEntityByIdentifier(ctx context.Context, namespace, value, scope string) (*schema.MemoryRecord, error)
}

// BoundedGraphEntityResult reports graph entity candidates hydrated under an
// aggregate byte budget.
type BoundedGraphEntityResult struct {
	Records                 []*schema.MemoryRecord
	ProjectedBytes          int64
	HydrationBytesTruncated bool
}

// BoundedGraphEntityLookup is the byte-budgeted graph-specific entity lookup
// contract. Graph retrieval requires this interface and fails closed rather
// than invoking the legacy entity lookup methods.
type BoundedGraphEntityLookup interface {
	FindGraphEntitiesByTermBounded(ctx context.Context, term, scope string, limit int, maxHydratedBytes int64) (BoundedGraphEntityResult, error)
	FindGraphEntityByIdentifierBounded(ctx context.Context, namespace, value, scope string, maxHydratedBytes int64) (BoundedGraphEntityResult, error)
}

// GraphEntityLookupAllScopes is the unrestricted-scope graph entity lookup
// variant with the same bounded projection contract.
type GraphEntityLookupAllScopes interface {
	FindGraphEntitiesByTermAllScopes(ctx context.Context, term string, limit int) ([]*schema.MemoryRecord, error)
	FindGraphEntityByIdentifierAllScopes(ctx context.Context, namespace, value string) (*schema.MemoryRecord, error)
}

// BoundedGraphEntityLookupAllScopes is the unrestricted-scope byte-budgeted
// graph entity lookup contract.
type BoundedGraphEntityLookupAllScopes interface {
	FindGraphEntitiesByTermAllScopesBounded(ctx context.Context, term string, limit int, maxHydratedBytes int64) (BoundedGraphEntityResult, error)
	FindGraphEntityByIdentifierAllScopesBounded(ctx context.Context, namespace, value string, maxHydratedBytes int64) (BoundedGraphEntityResult, error)
}

// SemanticLookup is implemented by stores that can locate an exact semantic
// subject-predicate-object fact.
type SemanticLookup interface {
	FindSemanticExact(ctx context.Context, subject, predicate, object string) (*schema.MemoryRecord, error)
}

// SemanticLookupInScope is implemented by stores that can locate an exact
// semantic fact within one visibility scope. Scoped exact lookup prevents
// project-specific observations from reinforcing or linking to a semantic fact
// stored under another scope.
type SemanticLookupInScope interface {
	FindSemanticExactInScope(ctx context.Context, subject, predicate, object, scope string) (*schema.MemoryRecord, error)
}

// EmbeddingStats summarizes vector coverage for stores that persist record
// embeddings alongside memory records.
type EmbeddingStats struct {
	Model           string
	TotalRecords    int
	EmbeddedRecords int
}

// EmbeddingStatsProvider is implemented by stores that can report how many
// records have embeddings for the currently configured vector model.
type EmbeddingStatsProvider interface {
	EmbeddingStats(ctx context.Context) (EmbeddingStats, error)
}

// MetricsFilter constrains an aggregate metrics query to records visible to a
// server-derived read policy. Empty MaxSensitivity and Scopes are unrestricted.
type MetricsFilter struct {
	MaxSensitivity  schema.Sensitivity
	Scopes          []string
	IncludeUnscoped bool
}

// MetricsAggregate contains fixed-size database aggregates used to construct
// the public metrics snapshot without hydrating every record.
type MetricsAggregate struct {
	TotalRecords          int
	RecordsByType         map[string]int
	AvgSalience           float64
	AvgConfidence         float64
	SalienceDistribution  map[string]int
	ActiveRecords         int
	PinnedRecords         int
	TotalAuditEntries     int
	EmbeddingModel        string
	EmbeddedRecords       int
	EmbeddingCoverage     float64
	MemoryGrowthRate      float64
	RetrievalUsefulness   float64
	CompetenceSuccessRate float64
	PlanReuseFrequency    float64
	RevisionRate          float64
}

// MetricsAggregateProvider is implemented by production stores that can
// compute metrics through a fixed number of bounded aggregate queries.
type MetricsAggregateProvider interface {
	AggregateMetrics(ctx context.Context, filter MetricsFilter) (MetricsAggregate, error)
}

// ListOptions specifies filters for the List operation.
type ListOptions struct {
	// ID filters to one exact record identifier. Empty means no ID filter.
	ID string

	// Type filters records by memory type. Empty means no filter.
	Type schema.MemoryType

	// Types filters records to any of the supplied memory types when Type is
	// empty. An empty slice means no filter.
	Types []schema.MemoryType

	// Tags filters records that have ALL of the specified tags.
	Tags []string

	// Scope filters records by scope. Empty means no filter.
	Scope string

	// Scopes filters records to any of the supplied scopes when Scope is empty.
	// An empty slice means no filter.
	Scopes []string

	// IncludeUnscoped includes global records (NULL or empty scope) when Scopes
	// is set. It has no effect when there is no multi-scope filter.
	IncludeUnscoped bool

	// Sensitivity filters records by sensitivity level. Empty means no filter.
	Sensitivity schema.Sensitivity

	// MaxSensitivity includes records at or below this sensitivity when
	// Sensitivity is empty. Empty means no maximum filter.
	MaxSensitivity schema.Sensitivity

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

	// OmitRelations leaves relation metadata unhydrated. Callers that set this
	// must retrieve relations separately through an explicitly bounded lookup.
	OmitRelations bool

	// OmitHistory leaves append-only audit and provenance history unhydrated.
	// Bounded retrieval projections set this and use RetrieveByID when a caller
	// explicitly needs the complete history for one selected record.
	OmitHistory bool

	// MaxHydratedBytes caps aggregate payload and interpretation bytes before a
	// bounded store batch-hydrates candidate records. Zero preserves the full
	// List API. Bounded retrieval sets the repository hard ceiling.
	MaxHydratedBytes int64
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
