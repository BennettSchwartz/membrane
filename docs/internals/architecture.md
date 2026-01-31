---
title: Architecture Overview
outline: [2, 3]
---

# Architecture Overview

Membrane is a selective learning and memory substrate for agentic AI systems.
It provides a structured, trust-gated memory layer that agents use to persist
observations, tool outputs, events, working state, and derived knowledge --
then retrieve that knowledge later with layered filtering, salience ranking,
and sensitivity controls.

This page describes the internal architecture of the `membraned` server
process: how the major components are wired together, how requests flow through
the system, and the concurrency model that keeps background maintenance running
alongside request handling.

## High-Level Component Map

The system is organized into three tiers separated by clean interfaces:

```
 Clients (agents, tools, dashboards)
       |
       | gRPC / protobuf
       v
 +--------------------------+
 |   Transport Layer        |
 |   api/grpc/server.go     |   TLS, auth interceptor, rate limiter
 |   api/grpc/handlers.go   |   Input validation, proto <-> domain mapping
 +--------------------------+
       |
       | domain types (pkg/ingestion, pkg/retrieval, pkg/schema)
       v
 +--------------------------+
 |   Membrane Facade        |
 |   pkg/membrane/          |   Wires subsystems, exposes unified API
 |     membrane.go          |
 |     config.go            |
 +--------------------------+
       |
       | delegates to subsystem services
       v
 +-----------+----------+-----------+--------------+-----------+-----------+
 | Ingestion | Retrieval|  Revision | Consolidation|   Decay   |  Metrics  |
 | pkg/      | pkg/     | pkg/      | pkg/         | pkg/      | pkg/      |
 | ingestion | retrieval| revision  | consolidation| decay     | metrics   |
 +-----------+----------+-----------+--------------+-----------+-----------+
       |            |          |            |             |           |
       +------------+----------+------------+-------------+-----------+
                               |
                    storage.Store interface
                               |
                               v
                 +---------------------------+
                 |   Storage Backend         |
                 |   pkg/storage/sqlite/     |   SQLCipher-encrypted SQLite
                 +---------------------------+
```

Every vertical arrow crosses a well-defined interface boundary.  The transport
layer never touches the store directly; the subsystem services never know about
gRPC.  The `storage.Store` interface is the sole contract between business
logic and persistence.

## The Six Subsystems

| Subsystem | Package | Responsibility |
|---|---|---|
| **Ingestion** | `pkg/ingestion` | Classify incoming data, apply policy defaults (sensitivity, salience), produce `MemoryRecord` structs and persist them. |
| **Retrieval** | `pkg/retrieval` | Layered memory lookup with trust-context gating, salience filtering, and multi-solution selection for competence/plan_graph records. |
| **Revision** | `pkg/revision` | Atomic record mutations: supersede, fork, retract, merge, and contest. Every mutation produces an audit trail entry. |
| **Consolidation** | `pkg/consolidation` | Background compression of episodic records, extraction of semantic/competence/plan_graph knowledge, and duplicate resolution. |
| **Decay** | `pkg/decay` | Time-based salience degradation using configurable curves, plus pruning of records that fall below a salience floor. |
| **Metrics** | `pkg/metrics` | Point-in-time observability snapshots including record counts, salience distributions, and behavioral metrics per RFC 15.10. |

## Request Lifecycle: Ingestion

The following trace walks through what happens when an agent calls
`IngestEvent` over gRPC.

### 1. Transport: authentication and rate limiting

The gRPC unary interceptor chain runs before the handler method is invoked.
Two checks execute in sequence:

```go
// api/grpc/server.go -- chainInterceptors
if apiKey != "" {
    md, ok := metadata.FromIncomingContext(ctx)
    tokens := md.Get("authorization")
    // Must match "Bearer <key>"
}
if limiter != nil {
    if !limiter.allow() {
        // codes.ResourceExhausted
    }
}
```

If the API key is configured (via `MEMBRANE_API_KEY` or the YAML config), the
interceptor rejects any request that does not carry a valid `Bearer` token in
the `authorization` metadata header.  The token-bucket rate limiter then checks
whether the caller has remaining capacity.

::: tip
Authentication and rate limiting are both optional. When `APIKey` is empty,
the auth check is skipped entirely. When `RateLimitPerSecond` is zero, no
limiter is created.
:::

### 2. Handler: validation and mapping

`Handler.IngestEvent` in `api/grpc/handlers.go` performs input validation
before touching any business logic:

- String fields are checked against `maxStringLength` (100 KB).
- Tags are capped at 100 entries, each at most 256 bytes.
- JSON payloads are capped at 10 MB.
- Timestamps are parsed as RFC 3339 (or defaulted to zero for "use now").

After validation, the handler maps protobuf types to domain types and calls
the Membrane facade:

```go
rec, err := h.membrane.IngestEvent(ctx, ingestion.IngestEventRequest{
    Source:      req.Source,
    EventKind:   req.EventKind,
    Ref:         req.Ref,
    Summary:     req.Summary,
    Timestamp:   ts,
    Tags:        req.Tags,
    Scope:       req.Scope,
    Sensitivity: schema.Sensitivity(req.Sensitivity),
})
```

### 3. Facade: delegation

`Membrane.IngestEvent` in `pkg/membrane/membrane.go` is a thin delegate:

```go
func (m *Membrane) IngestEvent(ctx context.Context, req ingestion.IngestEventRequest) (*schema.MemoryRecord, error) {
    return m.ingestion.IngestEvent(ctx, req)
}
```

The facade exists so that the gRPC layer (and any future transport layers)
only depend on a single type.  All subsystem wiring is hidden inside `New()`.

### 4. Ingestion service: classify, apply policy, persist

Inside `pkg/ingestion`, the service:

1. Runs the **Classifier** to determine the appropriate memory type for the
   incoming data (episodic for events and tool outputs, semantic for
   observations, working for state snapshots).
2. Runs the **PolicyEngine** to fill in defaults -- if the caller did not
   specify a sensitivity level, the engine applies `DefaultSensitivity` from
   the config.
3. Constructs a `schema.MemoryRecord` with a generated UUID, initial salience
   of 1.0, and the current timestamp.
4. Calls `store.Create(ctx, record)` to persist the record.

### 5. Storage: SQLite write

The `sqlite.Store` implementation executes an `INSERT` statement into the
records table.  If an encryption key was provided at startup, the database is
opened with SQLCipher and all data at rest is encrypted.

### 6. Response: marshal and return

The handler serializes the returned `*schema.MemoryRecord` to JSON and wraps
it in an `IngestResponse` protobuf message, which is sent back over the wire.

## Request Lifecycle: Retrieval

Retrieval follows a layered strategy defined by RFC 15.8.  Here is the full
path for a `Retrieve` call.

### 1. Transport and validation

Same interceptor chain as ingestion.  The handler additionally validates:

- `trust` context is non-nil (always required).
- `min_salience` is non-negative and finite.
- `limit` is between 0 and 10,000.

### 2. Layered store queries

The retrieval service queries each memory type **in canonical order**:

```
working -> semantic -> competence -> plan_graph -> episodic
```

If the caller specifies `MemoryTypes`, only those layers are queried.  For
each layer, the service calls `store.ListByType(ctx, memType)`.

### 3. Trust filtering

Every record returned from the store passes through `FilterByTrust`, which
calls `TrustContext.Allows(record)`.  A record is allowed when:

- Its sensitivity level does not exceed the trust context's `MaxSensitivity`.
  Sensitivity is ordered: `public < low < medium < high < hyper`.
- Its scope matches one of the allowed scopes (or the record is unscoped, or
  the trust context has no scope restrictions).

::: warning
Records exactly one sensitivity level above the trust ceiling are eligible for
**redacted** access via `AllowsRedacted`, which exposes metadata but strips
sensitive content.  This implements graduated exposure per the RFC.
:::

### 4. Salience filtering

Records below `MinSalience` are dropped.  This prevents low-relevance memories
from cluttering the response.

### 5. Multi-solution selection

Competence and plan_graph records are collected into a separate candidate pool
and passed through the **Selector** (`pkg/retrieval/selector.go`).  The
selector evaluates candidates against a configurable confidence threshold
(`SelectionConfidenceThreshold`, default 0.7) and produces a `SelectionResult`
that ranks the best matches.

### 6. Sort, limit, respond

All surviving records are sorted by salience descending, truncated to the
requested limit, and returned alongside the optional `SelectionResult`.

## The Membrane Facade

`pkg/membrane/membrane.go` is the central orchestrator.  It owns:

| Field | Type | Purpose |
|---|---|---|
| `config` | `*Config` | Immutable configuration snapshot |
| `store` | `storage.Store` | The single storage backend instance |
| `ingestion` | `*ingestion.Service` | Ingestion business logic |
| `retrieval` | `*retrieval.Service` | Retrieval business logic |
| `decay` | `*decay.Service` | Decay curve application |
| `revision` | `*revision.Service` | Record mutation operations |
| `consolidation` | `*consolidation.Service` | Knowledge extraction |
| `metrics` | `*metrics.Collector` | Observability snapshots |
| `decayScheduler` | `*decay.Scheduler` | Background decay goroutine |
| `consolScheduler` | `*consolidation.Scheduler` | Background consolidation goroutine |

### Initialization (`New`)

`New(cfg)` performs all wiring in a deterministic sequence:

1. **Open storage** -- `sqlite.Open(cfg.DBPath, encKey)` initializes the
   SQLite/SQLCipher database.  The encryption key is read from the config or
   the `MEMBRANE_ENCRYPTION_KEY` environment variable.
2. **Build ingestion pipeline** -- Classifier, PolicyEngine (with
   `DefaultSensitivity` from config), and Service are constructed.
3. **Build retrieval pipeline** -- Selector (with
   `SelectionConfidenceThreshold`) and Service.
4. **Build decay** -- Service and Scheduler (with `DecayInterval`).
5. **Build revision** -- Service only (no background work).
6. **Build consolidation** -- Service and Scheduler (with
   `ConsolidationInterval`).
7. **Build metrics** -- Collector backed by the store.

Every subsystem service receives only the `storage.Store` interface.  None of
them hold references to each other or to the Membrane struct.

### Lifecycle methods

```go
func (m *Membrane) Start(ctx context.Context) error {
    m.decayScheduler.Start(ctx)
    m.consolScheduler.Start(ctx)
    return nil
}

func (m *Membrane) Stop() error {
    m.decayScheduler.Stop()
    m.consolScheduler.Stop()
    return m.store.Close()
}
```

`Start` launches background goroutines.  `Stop` signals them to terminate,
waits for completion, and closes the database connection.

### Delegate methods

The remaining methods on `Membrane` are one-line delegates that forward to the
appropriate subsystem service.  This keeps the facade thin and avoids
duplicating logic:

- **Ingestion**: `IngestEvent`, `IngestToolOutput`, `IngestObservation`, `IngestOutcome`, `IngestWorkingState`
- **Retrieval**: `Retrieve`, `RetrieveByID`
- **Revision**: `Supersede`, `Fork`, `Retract`, `Merge`, `Contest`
- **Decay**: `Reinforce`, `Penalize`
- **Metrics**: `GetMetrics`

## Configuration

Configuration flows through three layers, each overriding the previous:

```
DefaultConfig()  -->  YAML file (--config flag)  -->  CLI flag overrides
```

### The Config struct

Defined in `pkg/membrane/config.go`:

```go
type Config struct {
    DBPath                       string        `yaml:"db_path"`
    ListenAddr                   string        `yaml:"listen_addr"`
    DecayInterval                time.Duration `yaml:"decay_interval"`
    ConsolidationInterval        time.Duration `yaml:"consolidation_interval"`
    DefaultSensitivity           string        `yaml:"default_sensitivity"`
    SelectionConfidenceThreshold float64       `yaml:"selection_confidence_threshold"`
    EncryptionKey                string        `yaml:"encryption_key"`
    TLSCertFile                  string        `yaml:"tls_cert_file"`
    TLSKeyFile                   string        `yaml:"tls_key_file"`
    APIKey                       string        `yaml:"api_key"`
    RateLimitPerSecond           int           `yaml:"rate_limit_per_second"`
}
```

### Defaults

| Parameter | Default | Notes |
|---|---|---|
| `db_path` | `membrane.db` | Relative to working directory |
| `listen_addr` | `:9090` | All interfaces, port 9090 |
| `decay_interval` | `1h` | Hourly decay sweeps |
| `consolidation_interval` | `6h` | Consolidation every 6 hours |
| `default_sensitivity` | `low` | Applied when caller omits sensitivity |
| `selection_confidence_threshold` | `0.7` | Minimum confidence for selector candidates |
| `encryption_key` | _(empty)_ | No encryption unless set |
| `rate_limit_per_second` | `100` | Token-bucket capacity |

### Loading order in `main.go`

```go
// 1. Start with defaults
cfg = membrane.DefaultConfig()

// 2. Override with YAML file if --config is provided
cfg, err = membrane.LoadConfig(*configPath)

// 3. Override individual fields from CLI flags
if *dbPath != "" { cfg.DBPath = *dbPath }
if *addr != ""   { cfg.ListenAddr = *addr }

// 4. Read secrets from environment
if cfg.APIKey == "" { cfg.APIKey = os.Getenv("MEMBRANE_API_KEY") }
```

::: tip
Sensitive values like `EncryptionKey` and `APIKey` should be set via
environment variables (`MEMBRANE_ENCRYPTION_KEY`, `MEMBRANE_API_KEY`) rather
than written into config files.
:::

## Concurrency Model

Membrane uses a small number of long-lived goroutines with clean shutdown
semantics.  There is no goroutine-per-request model beyond what the gRPC
framework itself manages.

### Background schedulers

Both the decay and consolidation schedulers follow an identical pattern:

```go
type Scheduler struct {
    service    *Service
    interval   time.Duration
    stopCh     chan struct{}     // signal channel, closed to stop
    done       chan struct{}     // closed when goroutine exits
    started    sync.Once        // ensures Start runs exactly once
    wasStarted atomic.Bool      // tracks whether Start was ever called
}
```

Key properties:

- **`sync.Once` for Start**: calling `Start` multiple times is safe; only the
  first invocation launches the goroutine.
- **Dual stop signals**: the goroutine exits on either `ctx.Done()` (parent
  context cancelled) or `stopCh` closed (explicit `Stop()` call).
- **Panic recovery**: a deferred `recover()` prevents a panicking sweep from
  crashing the entire process.
- **Safe Stop**: `Stop()` is safe to call even if `Start` was never called.
  It checks `wasStarted` before waiting on the `done` channel, preventing a
  deadlock.

```
 main goroutine          decay goroutine         consolidation goroutine
       |                       |                          |
  Start(ctx) ---------> ticker loop               ticker loop
       |                  ApplyDecayAll()           RunAll()
       |                  Prune()                     |
       |                       |                      |
  signal received              |                      |
       |                       |                      |
  srv.Stop()                   |                      |
  cancel()  ------ctx.Done()-->|                      |
  m.Stop()                     |                      |
    close(stopCh) ------------>X                      |
    close(stopCh) ----------------------------------->X
    <-done                     |                      |
    <-done                                            |
    store.Close()
```

### Rate limiter mutex

The token-bucket rate limiter in `api/grpc/server.go` uses a `sync.Mutex` to
protect its token count and last-refill timestamp.  Because the mutex is held
only for the brief duration of a single arithmetic update, contention is
minimal even under high request rates.

```go
func (r *rateLimiter) allow() bool {
    r.mu.Lock()
    defer r.mu.Unlock()
    // refill tokens based on elapsed time, then decrement
}
```

### gRPC server goroutine

The gRPC server runs in its own goroutine launched from `main()`:

```go
go func() {
    errCh <- srv.Start()  // blocks until GracefulStop or error
}()
```

The main goroutine then selects on either a shutdown signal (`SIGINT`,
`SIGTERM`) or an error from the server.  On shutdown, it calls
`srv.Stop()` (which triggers `grpc.GracefulStop()`, finishing in-flight RPCs),
cancels the context, and calls `m.Stop()` to drain the schedulers and close
the database.

### Goroutine summary

| Goroutine | Launched by | Lifetime | Shutdown mechanism |
|---|---|---|---|
| gRPC server | `main()` | Process lifetime | `GracefulStop()` |
| Decay scheduler | `Membrane.Start()` | Until `Stop()` or ctx cancel | `stopCh` + `done` |
| Consolidation scheduler | `Membrane.Start()` | Until `Stop()` or ctx cancel | `stopCh` + `done` |
| Per-RPC handlers | gRPC framework | Single request | Context cancellation |

## Storage Interface

The `storage.Store` interface in `pkg/storage/store.go` is the system's
persistence contract.  Every subsystem interacts with storage exclusively
through this interface.

### Core operations

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

### Transactions

The `Transaction` interface mirrors `Store` but adds `Commit()` and
`Rollback()`.  Revision operations (supersede, fork, merge) use transactions
to ensure atomicity -- for example, a supersede reads the old record, creates
the new record, adds a relation, and writes an audit entry all within a single
transaction.

### Error conventions

Three sentinel errors provide a stable contract:

| Error | Meaning |
|---|---|
| `ErrNotFound` | The requested record ID does not exist |
| `ErrAlreadyExists` | A `Create` was attempted with a duplicate ID |
| `ErrTxClosed` | A committed or rolled-back transaction was reused |

### ListOptions

The `ListOptions` struct supports filtering by type, tags, scope, sensitivity,
salience range, and pagination (limit/offset).  This keeps complex query logic
inside the storage layer rather than in business logic code.

## Design Principles

### Separation of concerns

Each subsystem package is self-contained.  The ingestion package does not know
about retrieval.  The decay package does not know about consolidation.  They
communicate only through the shared `schema.MemoryRecord` type and the
`storage.Store` interface.  The `Membrane` facade is the only type that holds
references to all subsystems.

### Interface-driven storage

The `storage.Store` interface decouples all business logic from the concrete
database.  The current implementation uses SQLite with optional SQLCipher
encryption, but the interface is designed so that a PostgreSQL, DynamoDB, or
in-memory backend could be swapped in without modifying any subsystem code.

### Trust boundaries

Trust is enforced at retrieval time, not at storage time.  Every retrieval
operation requires a `TrustContext` that specifies the caller's maximum
sensitivity level and allowed scopes.  The system never returns a record
that exceeds the caller's trust ceiling.  Records one level above the ceiling
are available in redacted form, implementing graduated exposure.

::: warning
Trust filtering happens in application code, not in database queries.  This
means records of all sensitivity levels live in the same store.  The
`EncryptionKey` config option encrypts the entire database at rest to
compensate for this -- if an attacker obtains the database file, they cannot
read any records regardless of sensitivity.
:::

### Immutable audit trail

Every mutation operation (supersede, fork, retract, merge, contest) appends an
`AuditEntry` to the affected record.  Audit entries are append-only -- they
are never modified or deleted.  This provides a full lineage of how any given
memory record evolved over time, which is critical for debugging agent
behavior and for trust in the memory substrate.

### Thin facade, rich subsystems

The `Membrane` struct intentionally avoids business logic.  Its methods are
one-line delegates.  This ensures that each subsystem can be tested in
isolation with a mock store, and that the facade never becomes a "God object"
that accumulates unrelated responsibilities.

### Graceful degradation

Background schedulers (decay, consolidation) are designed to tolerate errors
without crashing.  A failed sweep logs the error and continues to the next
tick.  A panic in a scheduler goroutine is recovered and logged.  The gRPC
server continues serving requests even if a background sweep fails.
