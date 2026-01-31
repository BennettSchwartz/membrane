---
title: Storage Layer
outline: [2, 3]
---

# Storage Layer

The storage layer is the persistence backbone of Membrane. It defines how memory records are written to disk, read back, queried, and managed within transactions. This page covers the interface-driven design, the SQLite reference implementation, schema layout, encryption, and what you need to know to implement a custom backend.

## Overview

Membrane separates storage concerns from business logic through a `Store` interface defined in `pkg/storage/store.go`. Every component that needs to persist or retrieve memory records depends on this interface rather than a concrete database driver. This design delivers several benefits:

- **Testability** -- Unit tests can use an in-memory SQLite database or a mock store without touching the filesystem.
- **Portability** -- Swapping from SQLite to PostgreSQL, DynamoDB, or any other backend requires implementing a single interface; no call sites change.
- **Transaction safety** -- The `Transaction` interface ensures that multi-step mutations are atomic, regardless of the underlying engine.
- **Encapsulation** -- Serialization details (JSON payloads, RFC 3339 timestamps, nullable columns) are hidden behind clean Go method signatures.

The reference implementation lives in `pkg/storage/sqlite/` and uses [SQLCipher](https://www.zetetic.net/sqlcipher/) (via `go-sqlcipher/v4`) for optional encryption at rest.

## Store Interface

The `Store` interface in `pkg/storage/store.go` is the contract every backend must fulfill. It exposes CRUD operations, specialized mutations, relation management, and transaction support.

```go
type Store interface {
    Create(ctx context.Context, record *schema.MemoryRecord) error
    Get(ctx context.Context, id string) (*schema.MemoryRecord, error)
    Update(ctx context.Context, record *schema.MemoryRecord) error
    Delete(ctx context.Context, id string) error
    List(ctx context.Context, opts ListOptions) ([]*schema.MemoryRecord, error)
    ListByType(ctx context.Context, memType schema.MemoryType) ([]*schema.MemoryRecord, error)
    UpdateSalience(ctx context.Context, id string, salience float64) error
    AddAuditEntry(ctx context.Context, id string, entry schema.AuditEntry) error
    AddRelation(ctx context.Context, sourceID string, rel schema.Relation) error
    GetRelations(ctx context.Context, id string) ([]schema.Relation, error)
    Begin(ctx context.Context) (Transaction, error)
    Close() error
}
```

### Method Reference

| Method | Description |
|---|---|
| `Create` | Persist a new `MemoryRecord`. Returns `ErrAlreadyExists` if the ID is already taken. The record is validated before insertion. |
| `Get` | Retrieve a single record by its unique ID. Returns `ErrNotFound` if no matching record exists. Hydrates all related data: tags, payload, decay profile, provenance, relations, and audit log. |
| `Update` | Replace an existing record in its entirety. Tags and provenance sources are deleted and re-inserted. Returns `ErrNotFound` if the record does not exist. |
| `Delete` | Remove a record by ID. All child rows (tags, payload, decay profile, relations, audit log, competence stats, provenance) are cascade-deleted. Returns `ErrNotFound` if missing. |
| `List` | Query records using `ListOptions` filters. Supports filtering by type, scope, sensitivity, salience range, tags (AND semantics), plus limit/offset pagination. Results are ordered by salience descending, then creation time descending. |
| `ListByType` | Convenience wrapper that calls `List` with a type filter. |
| `UpdateSalience` | Set the salience value for a single record and update its `updated_at` timestamp. Used by the decay scheduler. Returns `ErrNotFound` if missing. |
| `AddAuditEntry` | Append an audit log entry to an existing record. Verifies the record exists before inserting. |
| `AddRelation` | Create a directed graph edge from `sourceID` to `rel.TargetID`. Defaults weight to `1.0` if unset. |
| `GetRelations` | Return all outgoing relations for a given record ID. Returns an empty slice (not nil) when there are no relations. |
| `Begin` | Start a new transaction. Returns a `Transaction` that must be committed or rolled back. |
| `Close` | Release resources held by the store (database connections, file handles). |

### Sentinel Errors

The package defines three sentinel errors that all backends must return consistently:

```go
var (
    ErrNotFound      = errors.New("record not found")
    ErrAlreadyExists = errors.New("record already exists")
    ErrTxClosed      = errors.New("transaction already closed")
)
```

### ListOptions

`ListOptions` controls how `List` filters and paginates results:

```go
type ListOptions struct {
    Type        schema.MemoryType   // Filter by memory type (empty = no filter)
    Tags        []string            // Record must have ALL specified tags
    Scope       string              // Filter by scope (empty = no filter)
    Sensitivity schema.Sensitivity  // Filter by sensitivity level
    MinSalience float64             // salience >= value (0 = no filter)
    MaxSalience float64             // salience <= value (0 = no filter)
    Limit       int                 // Max records returned (0 = unlimited)
    Offset      int                 // Skip first N records
}
```

::: tip
Tag filtering uses AND semantics. If you specify `Tags: []string{"important", "reviewed"}`, only records that carry **both** tags are returned.
:::

## Transaction Support

### The Transaction Interface

The `Transaction` interface mirrors the `Store` interface for all read/write operations but adds `Commit` and `Rollback`:

```go
type Transaction interface {
    Create(ctx context.Context, record *schema.MemoryRecord) error
    Get(ctx context.Context, id string) (*schema.MemoryRecord, error)
    Update(ctx context.Context, record *schema.MemoryRecord) error
    Delete(ctx context.Context, id string) error
    List(ctx context.Context, opts ListOptions) ([]*schema.MemoryRecord, error)
    ListByType(ctx context.Context, memType schema.MemoryType) ([]*schema.MemoryRecord, error)
    UpdateSalience(ctx context.Context, id string, salience float64) error
    AddAuditEntry(ctx context.Context, id string, entry schema.AuditEntry) error
    AddRelation(ctx context.Context, sourceID string, rel schema.Relation) error
    GetRelations(ctx context.Context, id string) ([]schema.Relation, error)
    Commit() error
    Rollback() error
}
```

Once `Commit()` or `Rollback()` is called, any subsequent method call on the transaction returns `ErrTxClosed`.

### The `WithTransaction` Helper

The `pkg/storage/transaction.go` file provides a convenience function that manages the transaction lifecycle automatically:

```go
func WithTransaction(ctx context.Context, s Store, fn func(tx Transaction) error) error
```

This helper:

1. Calls `s.Begin(ctx)` to open a transaction.
2. Executes `fn(tx)`. If `fn` returns `nil`, the transaction is committed.
3. If `fn` returns an error **or** panics, the transaction is rolled back.
4. On panic, the rollback happens first, then the panic is re-raised.

**Example usage:**

```go
err := storage.WithTransaction(ctx, store, func(tx storage.Transaction) error {
    rec := schema.NewMemoryRecord("mem-001", schema.MemoryTypeSemantic,
        schema.SensitivityLow, payload)
    if err := tx.Create(ctx, rec); err != nil {
        return err
    }
    return tx.AddRelation(ctx, "mem-001", schema.Relation{
        Predicate: "derived_from",
        TargetID:  "mem-000",
    })
})
```

::: warning
Never ignore the error from `WithTransaction`. If commit fails, the returned error wraps the underlying commit error with context.
:::

### Atomicity Guarantees

The SQLite implementation opens transactions at `sql.LevelSerializable` isolation. This means:

- Reads within the transaction see a consistent snapshot.
- If two concurrent transactions conflict, one will fail with a busy error (the `_busy_timeout=5000` DSN parameter gives SQLite up to 5 seconds to retry).
- A rollback fully reverts all operations performed within the transaction, including creates that succeeded before a later step failed.

## SQLite Implementation

The reference backend lives in `pkg/storage/sqlite/sqlite.go`. It uses the `go-sqlcipher/v4` driver (a fork of `go-sqlite3` with SQLCipher support).

### Opening a Store

```go
store, err := sqlite.Open("/var/lib/membrane/data.db", encryptionKey)
```

The `Open` function:

1. Opens the database with pragmas: `_foreign_keys=on`, `_journal_mode=WAL`, `_busy_timeout=5000`.
2. If `encryptionKey` is non-empty, issues `PRAGMA key = ?` and verifies the key works by querying `sqlite_master`.
3. Applies the embedded DDL schema (`schema.sql`) using `CREATE TABLE IF NOT EXISTS` statements.
4. Returns a `*SQLiteStore` ready for use.

Pass `":memory:"` as the DSN for a fully in-memory database (useful in tests).

### The `queryable` Abstraction

Internally, the SQLite implementation defines a `queryable` interface:

```go
type queryable interface {
    ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
    QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
    QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}
```

Both `*sql.DB` and `*sql.Tx` satisfy this interface. Every CRUD helper function (`createRecord`, `getRecord`, `updateRecord`, etc.) accepts a `queryable` parameter, which means the same logic runs identically whether the caller is the store directly or a transaction. This avoids code duplication and guarantees behavioral parity.

### Schema Design

The database schema is embedded in the binary via `//go:embed schema.sql`. It consists of seven tables and thirteen indexes.

#### Tables

```sql
-- Core record table
CREATE TABLE memory_records (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL CHECK(type IN ('episodic','working','semantic',
                                       'competence','plan_graph')),
    sensitivity TEXT NOT NULL CHECK(sensitivity IN ('public','low',
                                                     'medium','high','hyper')),
    confidence REAL NOT NULL CHECK(confidence >= 0 AND confidence <= 1),
    salience REAL NOT NULL CHECK(salience >= 0),
    scope TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- One-to-one: decay and lifecycle metadata
CREATE TABLE decay_profiles (
    record_id TEXT PRIMARY KEY REFERENCES memory_records(id) ON DELETE CASCADE,
    curve TEXT NOT NULL CHECK(curve IN ('exponential','linear','custom')),
    half_life_seconds INTEGER NOT NULL CHECK(half_life_seconds > 0),
    min_salience REAL NOT NULL DEFAULT 0,
    max_age_seconds INTEGER,
    reinforcement_gain REAL NOT NULL DEFAULT 0.1,
    last_reinforced_at TEXT NOT NULL,
    pinned INTEGER NOT NULL DEFAULT 0,
    deletion_policy TEXT NOT NULL DEFAULT 'auto_prune'
);

-- One-to-one: type-specific payload stored as JSON
CREATE TABLE payloads (
    record_id TEXT PRIMARY KEY REFERENCES memory_records(id) ON DELETE CASCADE,
    payload_json TEXT NOT NULL
);

-- Many-to-one: free-form tags
CREATE TABLE tags (
    record_id TEXT NOT NULL REFERENCES memory_records(id) ON DELETE CASCADE,
    tag TEXT NOT NULL,
    PRIMARY KEY(record_id, tag)
);

-- Many-to-one: provenance source references
CREATE TABLE provenance_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    record_id TEXT NOT NULL REFERENCES memory_records(id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK(kind IN ('event','artifact','tool_call',
                                       'observation','outcome')),
    ref TEXT NOT NULL,
    hash TEXT,
    created_by TEXT,
    timestamp TEXT NOT NULL
);

-- Many-to-many: directed graph edges between records
CREATE TABLE relations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id TEXT NOT NULL REFERENCES memory_records(id) ON DELETE CASCADE,
    predicate TEXT NOT NULL,
    target_id TEXT NOT NULL REFERENCES memory_records(id) ON DELETE CASCADE,
    weight REAL DEFAULT 1.0 CHECK(weight >= 0 AND weight <= 1),
    created_at TEXT NOT NULL
);

-- Many-to-one: immutable audit trail
CREATE TABLE audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    record_id TEXT NOT NULL REFERENCES memory_records(id) ON DELETE CASCADE,
    action TEXT NOT NULL CHECK(action IN ('create','revise','fork',
                                          'merge','delete','reinforce','decay')),
    actor TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    rationale TEXT NOT NULL,
    previous_state_json TEXT
);

-- One-to-one: competence-type performance counters
CREATE TABLE competence_stats (
    record_id TEXT PRIMARY KEY REFERENCES memory_records(id) ON DELETE CASCADE,
    success_count INTEGER NOT NULL DEFAULT 0,
    failure_count INTEGER NOT NULL DEFAULT 0
);
```

#### Entity Relationship

```
memory_records (1) ---- (1) decay_profiles
memory_records (1) ---- (1) payloads
memory_records (1) ---- (N) tags
memory_records (1) ---- (N) provenance_sources
memory_records (1) ---- (N) audit_log
memory_records (1) ---- (1) competence_stats  [optional]
memory_records (N) ---- (N) relations          [directed graph]
```

All child tables use `ON DELETE CASCADE`, so deleting a `memory_records` row automatically removes all associated data.

## Encryption at Rest

Membrane uses [SQLCipher](https://www.zetetic.net/sqlcipher/) for transparent, page-level AES-256 encryption of the database file.

### How It Works

When `Open()` receives a non-empty `encryptionKey`:

```go
if encryptionKey != "" {
    db.Exec("PRAGMA key = ?", encryptionKey)
}
```

SQLCipher encrypts every database page before writing it to disk and decrypts on read. The key is never stored in the database itself.

After setting the key, the implementation verifies it by running a test query against `sqlite_master`. If the key is wrong (or the database was created without encryption), `Open` returns an error and closes the connection immediately.

### Configuration

In your Membrane configuration, set the encryption key:

```yaml
storage:
  driver: sqlite
  dsn: /var/lib/membrane/data.db
  encryption_key: "your-secret-key-here"
```

::: danger
Store the encryption key securely. If you lose it, the database is unrecoverable. Consider using a secrets manager or environment variable injection rather than hardcoding it in config files.
:::

::: tip
For development and testing, omit `encryption_key` (or set it to an empty string) to use an unencrypted database. The in-memory DSN `":memory:"` also works without encryption.
:::

## Record Storage

### How MemoryRecord Maps to Database Rows

A single `schema.MemoryRecord` is distributed across multiple tables during `Create`:

| Go Field | Table | Column(s) |
|---|---|---|
| `ID`, `Type`, `Sensitivity`, `Confidence`, `Salience`, `Scope`, `CreatedAt`, `UpdatedAt` | `memory_records` | Corresponding columns directly |
| `Lifecycle.Decay.*`, `Lifecycle.Pinned`, `Lifecycle.DeletionPolicy`, `Lifecycle.LastReinforcedAt` | `decay_profiles` | `curve`, `half_life_seconds`, `min_salience`, `max_age_seconds`, `reinforcement_gain`, `last_reinforced_at`, `pinned`, `deletion_policy` |
| `Payload` (any type) | `payloads` | `payload_json` (JSON-serialized) |
| `Tags` | `tags` | One row per tag |
| `Provenance.Sources` | `provenance_sources` | One row per source |
| `Relations` | `relations` | One row per relation edge |
| `AuditLog` | `audit_log` | One row per audit entry |
| `Payload.(*CompetencePayload).Performance` | `competence_stats` | `success_count`, `failure_count` |

### JSON Payload Serialization

The `Payload` field is a polymorphic interface (`schema.Payload`) that can be any of the five memory type payloads: `SemanticPayload`, `EpisodicPayload`, `CompetencePayload`, `WorkingPayload`, or `PlanGraphPayload`.

On write, the payload is marshaled to JSON using `json.Marshal(rec.Payload)` and stored as a TEXT blob in the `payloads` table.

On read, the JSON is deserialized through `schema.PayloadWrapper.UnmarshalJSON()`, which inspects a discriminator field (`kind`) in the JSON to determine the concrete type:

```go
var wrapper schema.PayloadWrapper
wrapper.UnmarshalJSON([]byte(payloadJSON))
rec.Payload = wrapper.Payload
```

### Timestamp Handling

All timestamps are stored as TEXT in RFC 3339 Nano format (`time.RFC3339Nano`). They are converted to UTC before storage and parsed back on retrieval:

```go
// Write
now := rec.UpdatedAt.UTC().Format(time.RFC3339Nano)

// Read
rec.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAt)
```

### Nullable Columns

Optional fields (`scope`, `hash`, `created_by`, `max_age_seconds`) are stored as SQL NULLs when empty. Helper functions handle the conversion:

```go
func nullableString(s string) any {
    if s == "" {
        return nil
    }
    return s
}
```

On read, `sql.NullString` and `sql.NullInt64` scanners handle the NULL-to-zero-value mapping.

## Querying

### How Filters Translate to SQL

The `List` method builds a dynamic SQL query starting from a base:

```sql
SELECT id FROM memory_records WHERE 1=1
```

Each non-zero field in `ListOptions` appends an `AND` clause:

| Filter | SQL Fragment |
|---|---|
| `Type` | `AND type = ?` |
| `Scope` | `AND scope = ?` |
| `Sensitivity` | `AND sensitivity = ?` |
| `MinSalience` | `AND salience >= ?` |
| `MaxSalience` | `AND salience <= ?` |
| `Limit` | `LIMIT ?` |
| `Offset` | `OFFSET ?` |

### Tag-Based Queries

Tags use correlated subqueries with AND semantics. For each tag in the filter, an `EXISTS` clause is added:

```sql
AND EXISTS (
    SELECT 1 FROM tags t0
    WHERE t0.record_id = memory_records.id AND t0.tag = ?
)
```

If you filter by three tags, there will be three `EXISTS` subqueries (aliased `t0`, `t1`, `t2`). A record must satisfy all of them to be included.

### Result Ordering

Results are always ordered by:

```sql
ORDER BY salience DESC, created_at DESC
```

This ensures the most salient (important) records appear first, with ties broken by recency.

### Two-Phase Loading

The `List` method uses a two-phase approach:

1. **ID query** -- Execute the filtered query to collect matching record IDs.
2. **Full hydration** -- For each ID, call `getRecord()` to load the complete record with all child data (tags, payload, decay profile, provenance, relations, audit log).

This design keeps the filter query simple and reuses the same hydration logic as `Get`.

## Audit Storage

Every `MemoryRecord` carries an `AuditLog` field -- an ordered list of `schema.AuditEntry` values. Each entry is stored as a row in the `audit_log` table:

```sql
INSERT INTO audit_log (record_id, action, actor, timestamp, rationale, previous_state_json)
VALUES (?, ?, ?, ?, ?, ?)
```

The `action` column is constrained to a fixed set of values:

| Action | Meaning |
|---|---|
| `create` | Record was created |
| `revise` | Record content was updated |
| `fork` | Record was forked into a new record |
| `merge` | Record was merged from multiple sources |
| `delete` | Record was marked for deletion |
| `reinforce` | Salience was boosted by reinforcement |
| `decay` | Salience was reduced by the decay scheduler |

The `previous_state_json` column is reserved for storing a snapshot of the record before mutation. The current implementation passes `nil` for this column, but the schema supports it for future use.

Audit entries are append-only. The `AddAuditEntry` method verifies the target record exists, then inserts a new row. Entries are retrieved in insertion order (`ORDER BY id`) when hydrating a record.

## Relations

Relations model directed graph edges between memory records. They are defined by the `schema.Relation` struct:

```go
type Relation struct {
    Predicate string    `json:"predicate"`    // e.g. "supports", "contradicts"
    TargetID  string    `json:"target_id"`    // ID of the target record
    Weight    float64   `json:"weight"`       // Strength [0, 1], default 1.0
    CreatedAt time.Time `json:"created_at"`   // When the relation was established
}
```

### Predicate Types

The predicate is a free-form string. Common conventions include:

- **`supports`** -- This record provides evidence for the target.
- **`contradicts`** -- This record conflicts with the target.
- **`derived_from`** -- This record was derived from the target.
- **`supersedes`** -- This record replaces the target.
- **`fork`** -- This record was forked from the target.

### Storage Details

Relations are stored in the `relations` table with foreign keys on both `source_id` and `target_id`. Both sides cascade on delete -- if either the source or target record is removed, the relation row is automatically deleted.

The `AddRelation` method defaults weight to `1.0` and `created_at` to `time.Now().UTC()` if they are zero-valued:

```go
w := rel.Weight
if w == 0 {
    w = 1.0
}
ca := rel.CreatedAt
if ca.IsZero() {
    ca = time.Now().UTC()
}
```

`GetRelations` returns all **outgoing** relations from a given record (where the record is the `source_id`). To find **incoming** relations (where a record is the `target_id`), you would need to query the `relations` table directly -- this is not exposed through the `Store` interface in the current implementation.

## Performance

### Indexing Strategy

The schema defines thirteen indexes to cover the primary query patterns:

```sql
-- Record lookups and filtering
CREATE INDEX idx_records_type        ON memory_records(type);
CREATE INDEX idx_records_salience    ON memory_records(salience DESC);
CREATE INDEX idx_records_sensitivity ON memory_records(sensitivity);
CREATE INDEX idx_records_scope       ON memory_records(scope);
CREATE INDEX idx_records_created     ON memory_records(created_at);

-- Relation traversal
CREATE INDEX idx_relations_source    ON relations(source_id);
CREATE INDEX idx_relations_target    ON relations(target_id);
CREATE INDEX idx_relations_predicate ON relations(predicate);

-- Audit log queries
CREATE INDEX idx_audit_record        ON audit_log(record_id);
CREATE INDEX idx_audit_timestamp     ON audit_log(timestamp);

-- Tag filtering
CREATE INDEX idx_tags_tag            ON tags(tag);

-- Provenance lookups
CREATE INDEX idx_provenance_record   ON provenance_sources(record_id);
```

The `idx_records_salience` index is declared `DESC` to optimize the default `ORDER BY salience DESC` used by `List`.

### WAL Mode

The database opens with `_journal_mode=WAL` (Write-Ahead Logging). WAL mode provides:

- **Concurrent readers** -- Multiple goroutines can read simultaneously without blocking each other.
- **Non-blocking writes** -- Readers do not block writers (and vice versa), except when a checkpoint occurs.
- **Better throughput** -- WAL generally outperforms the default rollback journal for write-heavy workloads.

The `_busy_timeout=5000` parameter tells SQLite to wait up to 5 seconds when the database is locked before returning `SQLITE_BUSY`.

### Synchronous Mode

The schema sets `PRAGMA synchronous = NORMAL`, which balances durability and performance. In WAL mode, `NORMAL` means transactions are durable after a WAL sync but checkpoints may not be fully synced. This is safe for most workloads -- data loss can only occur on power failure during a checkpoint, and only for already-committed transactions that have not yet been checkpointed.

### Foreign Keys

Foreign keys are enabled via `_foreign_keys=on` in the DSN. This enforces referential integrity and powers the `ON DELETE CASCADE` behavior. The trade-off is a small overhead on inserts and deletes for constraint checking.

### Batch Operations

The current implementation does not expose a dedicated batch API. However, you can achieve batch semantics using transactions:

```go
storage.WithTransaction(ctx, store, func(tx storage.Transaction) error {
    for _, rec := range records {
        if err := tx.Create(ctx, rec); err != nil {
            return err // triggers rollback
        }
    }
    return nil // triggers commit
})
```

All operations within the transaction share a single database transaction, so SQLite batches the writes into a single journal entry. This is significantly faster than individual auto-committed inserts.

## Implementing a Custom Store

To add a new storage backend (for example, PostgreSQL or an embedded key-value store), implement both the `Store` and `Transaction` interfaces.

### Step 1: Implement `Store`

Your type must satisfy every method in the `Store` interface. Use compile-time checks:

```go
var _ storage.Store = (*PostgresStore)(nil)
```

### Step 2: Implement `Transaction`

Your transaction type must wrap all read/write `Store` methods plus `Commit()` and `Rollback()`. After either is called, all subsequent operations must return `storage.ErrTxClosed`:

```go
var _ storage.Transaction = (*postgresTx)(nil)
```

### Step 3: Return Correct Sentinel Errors

Backends **must** return the package-level sentinel errors for consistent behavior:

- `storage.ErrNotFound` when a record ID does not exist (on `Get`, `Update`, `Delete`, `UpdateSalience`, `AddAuditEntry`, `AddRelation`).
- `storage.ErrAlreadyExists` when `Create` is called with a duplicate ID.
- `storage.ErrTxClosed` when a committed or rolled-back transaction is reused.

### Step 4: Validate Records

Always call `rec.Validate()` at the start of `Create` and `Update`. The `MemoryRecord.Validate()` method checks for required fields, valid confidence range `[0, 1]`, non-negative salience, and a non-nil payload.

### Step 5: Handle Payload Serialization

The `Payload` field is a polymorphic interface. You must serialize it (typically to JSON) and deserialize using `schema.PayloadWrapper`:

```go
// Serialize
data, err := json.Marshal(rec.Payload)

// Deserialize
var wrapper schema.PayloadWrapper
err := wrapper.UnmarshalJSON(data)
rec.Payload = wrapper.Payload
```

### Step 6: Cascade Deletes

When a record is deleted, all associated data must be removed: tags, payload, decay profile, provenance sources, relations (both as source and target), audit log entries, and competence stats. If your database supports foreign key cascades, leverage them. Otherwise, implement the cleanup manually.

### Step 7: Relation Defaults

When storing a relation, default `Weight` to `1.0` if it is zero and `CreatedAt` to `time.Now().UTC()` if it is the zero value. This matches the behavior callers expect from the reference implementation.

### Checklist

| Requirement | Notes |
|---|---|
| All `Store` methods implemented | 12 methods total |
| All `Transaction` methods implemented | 12 methods + `Commit` + `Rollback` |
| Sentinel errors returned correctly | `ErrNotFound`, `ErrAlreadyExists`, `ErrTxClosed` |
| `Validate()` called on create/update | Prevents invalid records from being persisted |
| Payload JSON round-trip works | Use `PayloadWrapper` for deserialization |
| Cascade delete behavior | Remove all child data when a record is deleted |
| Timestamps stored in UTC | Use `time.RFC3339Nano` format or native timestamp types |
| `GetRelations` returns empty slice, not nil | Callers depend on this for JSON serialization |
| `ErrTxClosed` after commit or rollback | Guard every transaction method |
| Compile-time interface assertions | `var _ storage.Store = (*YourStore)(nil)` |
