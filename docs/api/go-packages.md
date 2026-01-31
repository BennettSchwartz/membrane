# Go Packages

Complete API reference for every public Go package in the Membrane memory substrate.
All import paths are rooted at `github.com/GustyCube/membrane/pkg`.

[[toc]]

---

## membrane

```
import "github.com/GustyCube/membrane/pkg/membrane"
```

The top-level package that wires together all subsystems -- ingestion, retrieval,
decay, revision, consolidation, and metrics -- and exposes the unified API surface.

### Config

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

| Field | Type | Default | Description |
|---|---|---|---|
| `DBPath` | `string` | `"membrane.db"` | SQLite database file path. |
| `ListenAddr` | `string` | `":9090"` | gRPC server listen address. |
| `DecayInterval` | `time.Duration` | `1h` | How often the decay scheduler runs. |
| `ConsolidationInterval` | `time.Duration` | `6h` | How often the consolidation scheduler runs. |
| `DefaultSensitivity` | `string` | `"low"` | Default sensitivity level for ingested records. |
| `SelectionConfidenceThreshold` | `float64` | `0.7` | Minimum confidence for the retrieval selector to accept a competence or plan\_graph candidate. |
| `EncryptionKey` | `string` | `""` | SQLCipher encryption key. Falls back to `MEMBRANE_ENCRYPTION_KEY` env var. |
| `TLSCertFile` | `string` | `""` | Path to TLS certificate PEM. Empty disables TLS. |
| `TLSKeyFile` | `string` | `""` | Path to TLS private key PEM. |
| `APIKey` | `string` | `""` | Shared secret for gRPC auth. Falls back to `MEMBRANE_API_KEY` env var. |
| `RateLimitPerSecond` | `int` | `100` | Max requests per second per client. `0` disables limiting. |

### DefaultConfig

```go
func DefaultConfig() *Config
```

Returns a `Config` populated with the default values listed above.

### LoadConfig

```go
func LoadConfig(path string) (*Config, error)
```

Reads a YAML configuration file and returns a `Config`. Fields not present in the
file retain their default values.

### Membrane

```go
type Membrane struct { /* unexported fields */ }
```

The central orchestrator. Create one with `New`, start background schedulers
with `Start`, and tear down with `Stop`.

### New

```go
func New(cfg *Config) (*Membrane, error)
```

Initialises every subsystem (store, ingestion, retrieval, decay, revision,
consolidation, metrics) from the provided `Config` and returns a ready-to-start
`Membrane` instance. Opens the SQLite database (optionally encrypted with
SQLCipher) at `cfg.DBPath`.

### Start

```go
func (m *Membrane) Start(ctx context.Context) error
```

Begins the background decay and consolidation schedulers. The schedulers run
until the context is cancelled or `Stop` is called.

### Stop

```go
func (m *Membrane) Stop() error
```

Gracefully shuts down schedulers and closes the underlying store.

### Ingestion Methods

#### IngestEvent

```go
func (m *Membrane) IngestEvent(ctx context.Context, req ingestion.IngestEventRequest) (*schema.MemoryRecord, error)
```

Creates an **episodic** memory record from an event (user input, error, system
event). The record is classified, policy is applied, and the result is persisted.

#### IngestToolOutput

```go
func (m *Membrane) IngestToolOutput(ctx context.Context, req ingestion.IngestToolOutputRequest) (*schema.MemoryRecord, error)
```

Creates an **episodic** memory record from a tool invocation, capturing the tool
graph (arguments, result, dependencies) for later consolidation.

#### IngestObservation

```go
func (m *Membrane) IngestObservation(ctx context.Context, req ingestion.IngestObservationRequest) (*schema.MemoryRecord, error)
```

Creates a **semantic** memory record from an observation, encoding a
subject-predicate-object triple with global validity.

#### IngestOutcome

```go
func (m *Membrane) IngestOutcome(ctx context.Context, req ingestion.IngestOutcomeRequest) (*schema.MemoryRecord, error)
```

Updates an existing **episodic** record with outcome data (`success`, `failure`,
or `partial`). Appends provenance and audit entries.

#### IngestWorkingState

```go
func (m *Membrane) IngestWorkingState(ctx context.Context, req ingestion.IngestWorkingStateRequest) (*schema.MemoryRecord, error)
```

Creates a **working** memory record from a task state snapshot, capturing
thread ID, task state, next actions, open questions, and active constraints.

### Retrieval Methods

#### Retrieve

```go
func (m *Membrane) Retrieve(ctx context.Context, req *retrieval.RetrieveRequest) (*retrieval.RetrieveResponse, error)
```

Performs layered retrieval per RFC 15.8: queries each memory type layer in
canonical order (working, semantic, competence, plan\_graph, episodic), applies
trust and salience filtering, runs competence/plan\_graph candidates through the
multi-solution selector, sorts by salience descending, and applies the limit.

#### RetrieveByID

```go
func (m *Membrane) RetrieveByID(ctx context.Context, id string, trust *retrieval.TrustContext) (*schema.MemoryRecord, error)
```

Fetches a single record by ID. Returns `retrieval.ErrAccessDenied` if the trust
context denies access.

### Revision Methods

#### Supersede

```go
func (m *Membrane) Supersede(ctx context.Context, oldID string, newRec *schema.MemoryRecord, actor, rationale string) (*schema.MemoryRecord, error)
```

Atomically replaces an old record with a new one. The old record is retracted
(salience zeroed, semantic status set to `retracted`) and the new record receives
a `supersedes` relation to the old. Episodic records cannot be superseded.

#### Fork

```go
func (m *Membrane) Fork(ctx context.Context, sourceID string, forkedRec *schema.MemoryRecord, actor, rationale string) (*schema.MemoryRecord, error)
```

Creates a new record derived from an existing source. Unlike `Supersede`, both
the source and the fork remain active -- intended for conditional variants.
A `derived_from` relation is added. Episodic records cannot be forked.

#### Retract

```go
func (m *Membrane) Retract(ctx context.Context, id, actor, rationale string) error
```

Marks a record as retracted without deleting it, preserving auditability.
Salience is set to 0. Semantic records get revision status `retracted`.
Episodic records cannot be retracted.

#### Merge

```go
func (m *Membrane) Merge(ctx context.Context, ids []string, mergedRec *schema.MemoryRecord, actor, rationale string) (*schema.MemoryRecord, error)
```

Atomically combines multiple source records into a single merged record. All
sources are retracted and the merged record gets `derived_from` relations to
each source. Episodic records cannot be merged.

#### Contest

```go
func (m *Membrane) Contest(ctx context.Context, id, contestingRef, actor, rationale string) error
```

Marks a record as **contested**, indicating conflicting evidence exists. Adds a
`contested_by` relation to the contesting reference and sets semantic revision
status to `contested`.

### Decay Methods

#### Reinforce

```go
func (m *Membrane) Reinforce(ctx context.Context, id, actor, rationale string) error
```

Boosts a record's salience by its `ReinforcementGain` and updates
`LastReinforcedAt`. Recorded in the audit log.

#### Penalize

```go
func (m *Membrane) Penalize(ctx context.Context, id string, amount float64, actor, rationale string) error
```

Reduces a record's salience by `amount`, floored at `MinSalience`. Recorded in
the audit log.

### Metrics

#### GetMetrics

```go
func (m *Membrane) GetMetrics(ctx context.Context) (*metrics.Snapshot, error)
```

Collects a point-in-time snapshot of substrate metrics including record counts,
salience distribution, and RFC 15.10 behavioral metrics (growth rate, retrieval
usefulness, competence success rate, plan reuse frequency, revision rate).

---

## schema

```
import "github.com/GustyCube/membrane/pkg/schema"
```

Defines the core data structures for the memory substrate as specified in the
RFC 15A Schema Appendix.

### MemoryRecord

```go
type MemoryRecord struct {
    ID          string        `json:"id"`
    Type        MemoryType    `json:"type"`
    Sensitivity Sensitivity   `json:"sensitivity"`
    Confidence  float64       `json:"confidence"`
    Salience    float64       `json:"salience"`
    Scope       string        `json:"scope,omitempty"`
    Tags        []string      `json:"tags,omitempty"`
    CreatedAt   time.Time     `json:"created_at"`
    UpdatedAt   time.Time     `json:"updated_at"`
    Lifecycle   Lifecycle     `json:"lifecycle"`
    Provenance  Provenance    `json:"provenance"`
    Relations   []Relation    `json:"relations,omitempty"`
    Payload     Payload       `json:"payload"`
    AuditLog    []AuditEntry  `json:"audit_log"`
}
```

The atomic unit of storage. Every stored memory item conforms to this shape.
Custom JSON marshaling dispatches `Payload` based on the `kind` discriminator.

#### NewMemoryRecord

```go
func NewMemoryRecord(id string, memType MemoryType, sensitivity Sensitivity, payload Payload) *MemoryRecord
```

Convenience constructor. Sets `Confidence` to `1.0`, `Salience` to `1.0`,
timestamps to `now`, decay curve to exponential with a 1-day half-life,
deletion policy to `auto_prune`, and creates an initial `create` audit entry.

#### Validate

```go
func (mr *MemoryRecord) Validate() error
```

Returns a `*ValidationError` if required fields are missing or out of range
(`id`, `type`, `sensitivity`, `confidence` in [0,1], `salience` >= 0, non-nil
`payload`).

### ValidationError

```go
type ValidationError struct {
    Field   string
    Message string
}
```

Returned by `Validate`. Implements `error`.

### MemoryType

```go
type MemoryType string
```

| Constant | Value | Description |
|---|---|---|
| `MemoryTypeEpisodic` | `"episodic"` | Raw experience: events, tool calls, errors. Short-lived. |
| `MemoryTypeWorking` | `"working"` | Current task state for cross-session resumption. |
| `MemoryTypeSemantic` | `"semantic"` | Stable knowledge: preferences, facts, relationships. Revisable. |
| `MemoryTypeCompetence` | `"competence"` | Procedural knowledge: how to achieve goals. |
| `MemoryTypePlanGraph` | `"plan_graph"` | Reusable solution structures as directed graphs. |

### Sensitivity

```go
type Sensitivity string
```

| Constant | Value | Description |
|---|---|---|
| `SensitivityPublic` | `"public"` | Freely shareable. |
| `SensitivityLow` | `"low"` | Minimal sensitivity. |
| `SensitivityMedium` | `"medium"` | Moderate sensitivity. |
| `SensitivityHigh` | `"high"` | Requires elevated trust. |
| `SensitivityHyper` | `"hyper"` | Maximum protection. |

### DecayCurve

```go
type DecayCurve string
```

| Constant | Value | Description |
|---|---|---|
| `DecayCurveExponential` | `"exponential"` | Exponential decay with half-life. |
| `DecayCurveLinear` | `"linear"` | Linear decay over time. |
| `DecayCurveCustom` | `"custom"` | Implementation-defined. |

### DeletionPolicy

```go
type DeletionPolicy string
```

| Constant | Value | Description |
|---|---|---|
| `DeletionPolicyAutoPrune` | `"auto_prune"` | Auto-delete when salience drops below threshold. |
| `DeletionPolicyManualOnly` | `"manual_only"` | Explicit user action required. |
| `DeletionPolicyNever` | `"never"` | Prevents deletion entirely. |

### RevisionStatus

```go
type RevisionStatus string
```

| Constant | Value | Description |
|---|---|---|
| `RevisionStatusActive` | `"active"` | Currently valid. |
| `RevisionStatusContested` | `"contested"` | Uncertain until resolved. |
| `RevisionStatusRetracted` | `"retracted"` | Withdrawn. |

### ValidityMode

```go
type ValidityMode string
```

| Constant | Value | Description |
|---|---|---|
| `ValidityModeGlobal` | `"global"` | Universally valid. |
| `ValidityModeConditional` | `"conditional"` | Valid under specific conditions. |
| `ValidityModeTimeboxed` | `"timeboxed"` | Valid within a time window. |

### TaskState

```go
type TaskState string
```

| Constant | Value | Description |
|---|---|---|
| `TaskStatePlanning` | `"planning"` | In planning phase. |
| `TaskStateExecuting` | `"executing"` | Actively executing. |
| `TaskStateBlocked` | `"blocked"` | Cannot proceed. |
| `TaskStateWaiting` | `"waiting"` | Awaiting external input. |
| `TaskStateDone` | `"done"` | Completed. |

### OutcomeStatus

```go
type OutcomeStatus string
```

| Constant | Value | Description |
|---|---|---|
| `OutcomeStatusSuccess` | `"success"` | Completed successfully. |
| `OutcomeStatusFailure` | `"failure"` | Ended in failure. |
| `OutcomeStatusPartial` | `"partial"` | Partial or incomplete. |

### AuditAction

```go
type AuditAction string
```

| Constant | Value | Description |
|---|---|---|
| `AuditActionCreate` | `"create"` | Record created. |
| `AuditActionRevise` | `"revise"` | Record revised. |
| `AuditActionFork` | `"fork"` | Record forked. |
| `AuditActionMerge` | `"merge"` | Records merged. |
| `AuditActionDelete` | `"delete"` | Record deleted. |
| `AuditActionReinforce` | `"reinforce"` | Salience reinforced. |
| `AuditActionDecay` | `"decay"` | Salience decayed. |

### ProvenanceKind

```go
type ProvenanceKind string
```

| Constant | Value | Description |
|---|---|---|
| `ProvenanceKindEvent` | `"event"` | Source is an event. |
| `ProvenanceKindArtifact` | `"artifact"` | Source is an artifact (log, file). |
| `ProvenanceKindToolCall` | `"tool_call"` | Source is a tool invocation. |
| `ProvenanceKindObservation` | `"observation"` | Source is an observation. |
| `ProvenanceKindOutcome` | `"outcome"` | Source is a task outcome. |

### EdgeKind

```go
type EdgeKind string
```

| Constant | Value |
|---|---|
| `EdgeKindData` | `"data"` |
| `EdgeKindControl` | `"control"` |

### Lifecycle

```go
type Lifecycle struct {
    Decay            DecayProfile   `json:"decay"`
    LastReinforcedAt time.Time      `json:"last_reinforced_at"`
    Pinned           bool           `json:"pinned,omitempty"`
    DeletionPolicy   DeletionPolicy `json:"deletion_policy,omitempty"`
}
```

Controls decay, reinforcement, pinning, and deletion behavior. When `Pinned` is
`true`, salience does not decrease over time.

### DecayProfile

```go
type DecayProfile struct {
    Curve             DecayCurve `json:"curve"`
    HalfLifeSeconds   int64      `json:"half_life_seconds"`
    MinSalience       float64    `json:"min_salience,omitempty"`
    MaxAgeSeconds     int64      `json:"max_age_seconds,omitempty"`
    ReinforcementGain float64    `json:"reinforcement_gain,omitempty"`
}
```

Defines how salience decreases over time. `HalfLifeSeconds` is the time for
salience to decay by half. `MinSalience` is the floor. `ReinforcementGain`
controls the boost applied on reinforcement.

### Provenance

```go
type Provenance struct {
    Sources   []ProvenanceSource `json:"sources"`
    CreatedBy string             `json:"created_by,omitempty"`
}
```

Links a record to its source events or artifacts. Every record should have at
least one provenance source.

### ProvenanceSource

```go
type ProvenanceSource struct {
    Kind      ProvenanceKind `json:"kind"`
    Ref       string         `json:"ref"`
    Hash      string         `json:"hash,omitempty"`
    CreatedBy string         `json:"created_by,omitempty"`
    Timestamp time.Time      `json:"timestamp,omitempty"`
}
```

A single source of evidence. `Ref` is an opaque reference into the host system.
`Hash` enables content-addressed immutability verification.

### ProvenanceRef

```go
type ProvenanceRef struct {
    SourceType string    `json:"source_type"`
    SourceID   string    `json:"source_id"`
    Timestamp  time.Time `json:"timestamp"`
}
```

A lightweight evidence reference used inside semantic payloads.

### AuditEntry

```go
type AuditEntry struct {
    Action    AuditAction `json:"action"`
    Actor     string      `json:"actor"`
    Timestamp time.Time   `json:"timestamp"`
    Rationale string      `json:"rationale"`
}
```

Records a single action in the audit log. Every revision must be auditable and
traceable to evidence (RFC 15A.8).

### Relation

```go
type Relation struct {
    Predicate string    `json:"predicate"`
    TargetID  string    `json:"target_id"`
    Weight    float64   `json:"weight,omitempty"`
    CreatedAt time.Time `json:"created_at,omitempty"`
}
```

A directed edge to another `MemoryRecord`. Common predicates include
`supports`, `contradicts`, `derived_from`, `supersedes`, and `contested_by`.

### Payload Interface

```go
type Payload interface {
    PayloadKind() string
}
```

All five payload types implement `Payload`. The `PayloadKind()` return value
matches the `"kind"` JSON discriminator field.

### EpisodicPayload

```go
type EpisodicPayload struct {
    Kind         string               `json:"kind"`          // "episodic"
    Timeline     []TimelineEvent      `json:"timeline"`
    ToolGraph    []ToolNode           `json:"tool_graph,omitempty"`
    Environment  *EnvironmentSnapshot `json:"environment,omitempty"`
    Outcome      OutcomeStatus        `json:"outcome,omitempty"`
    Artifacts    []string             `json:"artifacts,omitempty"`
    ToolGraphRef string               `json:"tool_graph_ref,omitempty"`
}
```

Captures raw experience as a time-ordered event sequence. Episodic payloads are
append-only; semantic correction is forbidden (RFC 15A.6).

**Supporting types:**

```go
type TimelineEvent struct {
    T         time.Time `json:"t"`
    EventKind string    `json:"event_kind"`
    Ref       string    `json:"ref"`
    Summary   string    `json:"summary,omitempty"`
}

type ToolNode struct {
    ID        string         `json:"id"`
    Tool      string         `json:"tool"`
    Args      map[string]any `json:"args,omitempty"`
    Result    any            `json:"result,omitempty"`
    Timestamp time.Time      `json:"timestamp,omitempty"`
    DependsOn []string       `json:"depends_on,omitempty"`
}

type EnvironmentSnapshot struct {
    OS               string            `json:"os,omitempty"`
    OSVersion        string            `json:"os_version,omitempty"`
    ToolVersions     map[string]string `json:"tool_versions,omitempty"`
    WorkingDirectory string            `json:"working_directory,omitempty"`
    Context          map[string]any    `json:"context,omitempty"`
}
```

### WorkingPayload

```go
type WorkingPayload struct {
    Kind              string       `json:"kind"`   // "working"
    ThreadID          string       `json:"thread_id"`
    State             TaskState    `json:"state"`
    ActiveConstraints []Constraint `json:"active_constraints,omitempty"`
    NextActions       []string     `json:"next_actions,omitempty"`
    OpenQuestions     []string     `json:"open_questions,omitempty"`
    ContextSummary    string       `json:"context_summary,omitempty"`
}
```

Captures current task state for cross-session resumption. Working memory may be
freely edited and discarded when the task ends.

```go
type Constraint struct {
    Type     string `json:"type"`
    Key      string `json:"key"`
    Value    any    `json:"value"`
    Required bool   `json:"required,omitempty"`
}
```

### SemanticPayload

```go
type SemanticPayload struct {
    Kind           string         `json:"kind"`   // "semantic"
    Subject        string         `json:"subject"`
    Predicate      string         `json:"predicate"`
    Object         any            `json:"object"`
    Validity       Validity       `json:"validity"`
    Evidence       []ProvenanceRef `json:"evidence,omitempty"`
    RevisionPolicy string         `json:"revision_policy,omitempty"`
    Revision       *RevisionState `json:"revision,omitempty"`
}
```

Stores revisable facts as subject-predicate-object triples. Supports coexistence
of multiple conditional truths (RFC 15A.8).

```go
type Validity struct {
    Mode       ValidityMode   `json:"mode"`
    Conditions map[string]any `json:"conditions,omitempty"`
    Start      *time.Time     `json:"start,omitempty"`
    End        *time.Time     `json:"end,omitempty"`
}

type RevisionState struct {
    Supersedes   string         `json:"supersedes,omitempty"`
    SupersededBy string         `json:"superseded_by,omitempty"`
    Status       RevisionStatus `json:"status,omitempty"`
}
```

### CompetencePayload

```go
type CompetencePayload struct {
    Kind          string           `json:"kind"`   // "competence"
    SkillName     string           `json:"skill_name"`
    Triggers      []Trigger        `json:"triggers"`
    Recipe        []RecipeStep     `json:"recipe"`
    RequiredTools []string         `json:"required_tools,omitempty"`
    FailureModes  []string         `json:"failure_modes,omitempty"`
    Fallbacks     []string         `json:"fallbacks,omitempty"`
    Performance   *PerformanceStats `json:"performance,omitempty"`
    Version       string           `json:"version,omitempty"`
}
```

Encodes procedural knowledge: "knowing how" rather than "knowing that".

```go
type Trigger struct {
    Signal     string         `json:"signal"`
    Conditions map[string]any `json:"conditions,omitempty"`
}

type RecipeStep struct {
    Step       string         `json:"step"`
    Tool       string         `json:"tool,omitempty"`
    ArgsSchema map[string]any `json:"args_schema,omitempty"`
    Validation string         `json:"validation,omitempty"`
}

type PerformanceStats struct {
    SuccessCount int64      `json:"success_count,omitempty"`
    FailureCount int64      `json:"failure_count,omitempty"`
    SuccessRate  float64    `json:"success_rate,omitempty"`
    AvgLatencyMs float64    `json:"avg_latency_ms,omitempty"`
    LastUsedAt   *time.Time `json:"last_used_at,omitempty"`
}
```

### PlanGraphPayload

```go
type PlanGraphPayload struct {
    Kind          string         `json:"kind"`   // "plan_graph"
    PlanID        string         `json:"plan_id"`
    Version       string         `json:"version"`
    Intent        string         `json:"intent,omitempty"`
    Constraints   map[string]any `json:"constraints,omitempty"`
    InputsSchema  map[string]any `json:"inputs_schema,omitempty"`
    OutputsSchema map[string]any `json:"outputs_schema,omitempty"`
    Nodes         []PlanNode     `json:"nodes"`
    Edges         []PlanEdge     `json:"edges"`
    Metrics       *PlanMetrics   `json:"metrics,omitempty"`
}
```

Stores reusable solution structures as directed action graphs. Plans are
versioned and selectable by constraint matching.

```go
type PlanNode struct {
    ID     string         `json:"id"`
    Op     string         `json:"op"`
    Params map[string]any `json:"params,omitempty"`
    Guards map[string]any `json:"guards,omitempty"`
}

type PlanEdge struct {
    From string   `json:"from"`
    To   string   `json:"to"`
    Kind EdgeKind `json:"kind"`
}

type PlanMetrics struct {
    AvgLatencyMs   float64    `json:"avg_latency_ms,omitempty"`
    FailureRate    float64    `json:"failure_rate,omitempty"`
    ExecutionCount int64      `json:"execution_count,omitempty"`
    LastExecutedAt *time.Time `json:"last_executed_at,omitempty"`
}
```

---

## ingestion

```
import "github.com/GustyCube/membrane/pkg/ingestion"
```

Provides the ingestion layer that classifies incoming data, applies lifecycle
policies, and persists memory records.

### Service

```go
type Service struct { /* unexported fields */ }

func NewService(store storage.Store, classifier *Classifier, policy *PolicyEngine) *Service
```

Orchestrates ingestion. Coordinates the classifier, policy engine, and store.

#### IngestEvent

```go
func (s *Service) IngestEvent(ctx context.Context, req IngestEventRequest) (*schema.MemoryRecord, error)
```

Creates an episodic record from an event.

#### IngestToolOutput

```go
func (s *Service) IngestToolOutput(ctx context.Context, req IngestToolOutputRequest) (*schema.MemoryRecord, error)
```

Creates an episodic record with tool graph data from a tool invocation.

#### IngestObservation

```go
func (s *Service) IngestObservation(ctx context.Context, req IngestObservationRequest) (*schema.MemoryRecord, error)
```

Creates a semantic record from a subject-predicate-object observation.

#### IngestOutcome

```go
func (s *Service) IngestOutcome(ctx context.Context, req IngestOutcomeRequest) (*schema.MemoryRecord, error)
```

Updates an existing episodic record with outcome data.

#### IngestWorkingState

```go
func (s *Service) IngestWorkingState(ctx context.Context, req IngestWorkingStateRequest) (*schema.MemoryRecord, error)
```

Creates a working memory record from a task state snapshot.

### Request Types

#### IngestEventRequest

```go
type IngestEventRequest struct {
    Source      string
    EventKind   string
    Ref         string
    Summary     string
    Timestamp   time.Time
    Tags        []string
    Scope       string
    Sensitivity schema.Sensitivity
}
```

| Field | Required | Description |
|---|---|---|
| `Source` | Yes | Actor or system that produced the event. |
| `EventKind` | Yes | Type of event (e.g. `"user_input"`, `"error"`, `"system"`). |
| `Ref` | Yes | Reference identifier for the source event. |
| `Summary` | No | Human-readable summary. |
| `Timestamp` | No | When the event occurred. Defaults to `time.Now()`. |
| `Tags` | No | Labels for categorization. |
| `Scope` | No | Visibility scope. |
| `Sensitivity` | No | Overrides default sensitivity if set. |

#### IngestToolOutputRequest

```go
type IngestToolOutputRequest struct {
    Source      string
    ToolName    string
    Args        map[string]any
    Result      any
    DependsOn   []string
    Timestamp   time.Time
    Tags        []string
    Scope       string
    Sensitivity schema.Sensitivity
}
```

#### IngestObservationRequest

```go
type IngestObservationRequest struct {
    Source      string
    Subject     string
    Predicate   string
    Object      any
    Timestamp   time.Time
    Tags        []string
    Scope       string
    Sensitivity schema.Sensitivity
}
```

#### IngestOutcomeRequest

```go
type IngestOutcomeRequest struct {
    Source         string
    TargetRecordID string
    OutcomeStatus  schema.OutcomeStatus
    Timestamp      time.Time
}
```

#### IngestWorkingStateRequest

```go
type IngestWorkingStateRequest struct {
    Source            string
    ThreadID          string
    State             schema.TaskState
    NextActions       []string
    OpenQuestions      []string
    ContextSummary    string
    ActiveConstraints []schema.Constraint
    Timestamp         time.Time
    Tags              []string
    Scope             string
    Sensitivity       schema.Sensitivity
}
```

### Classifier

```go
type Classifier struct{}

func NewClassifier() *Classifier
```

Determines the memory type for a candidate.

```go
func (c *Classifier) Classify(candidate *MemoryCandidate) schema.MemoryType
```

Classification rules:

- Events and tool outputs produce `episodic` memory.
- Observations produce `semantic` memory.
- Outcomes update existing `episodic` records.
- Working state changes produce `working` memory.

### PolicyEngine

```go
type PolicyEngine struct { /* unexported fields */ }

func NewPolicyEngine(defaults PolicyDefaults) *PolicyEngine
```

Assigns lifecycle metadata and validates candidates before persistence.

```go
func (pe *PolicyEngine) Apply(candidate *MemoryCandidate, memType schema.MemoryType) (*PolicyResult, error)
```

Validates the candidate and produces a `PolicyResult` with:

- **Sensitivity**: candidate override or default.
- **Confidence**: source-dependent (tool outputs: 0.9, events: 0.8, observations: 0.7, outcomes: 0.85, working: 1.0).
- **Decay profile**: type-specific half-lives (episodic: 1h, semantic: 30d, working: 1d).
- **Deletion policy**: configurable default.

### PolicyDefaults

```go
type PolicyDefaults struct {
    Sensitivity             schema.Sensitivity
    EpisodicHalfLifeSeconds int64
    SemanticHalfLifeSeconds int64
    WorkingHalfLifeSeconds  int64
    DefaultInitialSalience  float64
    DefaultDeletionPolicy   schema.DeletionPolicy
}

func DefaultPolicyDefaults() PolicyDefaults
```

Returns defaults: sensitivity `low`, episodic half-life 1h, semantic half-life
30d, working half-life 1d, initial salience 1.0, deletion policy `auto_prune`.

### PolicyResult

```go
type PolicyResult struct {
    Sensitivity    schema.Sensitivity
    Confidence     float64
    Salience       float64
    Lifecycle      schema.Lifecycle
    DeletionPolicy schema.DeletionPolicy
}
```

### MemoryCandidate

```go
type MemoryCandidate struct {
    Kind              CandidateKind
    Source            string
    Timestamp         time.Time
    Tags              []string
    Scope             string
    EventKind         string
    EventRef          string
    Summary           string
    ToolName          string
    ToolArgs          map[string]any
    ToolResult        any
    ToolDependsOn     []string
    Subject           string
    Predicate         string
    Object            any
    TargetRecordID    string
    OutcomeStatus     schema.OutcomeStatus
    ThreadID          string
    TaskState         schema.TaskState
    ContextSummary    string
    NextActions       []string
    OpenQuestions      []string
    Sensitivity       schema.Sensitivity
}
```

Intermediate representation used between request parsing, classification, and
policy application.

### CandidateKind

```go
type CandidateKind string
```

| Constant | Value |
|---|---|
| `CandidateKindEvent` | `"event"` |
| `CandidateKindToolOutput` | `"tool_output"` |
| `CandidateKindObservation` | `"observation"` |
| `CandidateKindOutcome` | `"outcome"` |
| `CandidateKindWorkingState` | `"working_state"` |

---

## retrieval

```
import "github.com/GustyCube/membrane/pkg/retrieval"
```

Implements layered memory retrieval per RFC 15.8 and multi-solution selection
per RFC 15A.11.

### Service

```go
type Service struct { /* unexported fields */ }

func NewService(store storage.Store, selector *Selector) *Service
```

#### Retrieve

```go
func (svc *Service) Retrieve(ctx context.Context, req *RetrieveRequest) (*RetrieveResponse, error)
```

Layered retrieval: queries each memory type in canonical order
(`working` > `semantic` > `competence` > `plan_graph` > `episodic`), applies
trust and salience filtering, runs competence/plan\_graph candidates through the
selector, sorts by salience descending, and applies the limit.

#### RetrieveByID

```go
func (svc *Service) RetrieveByID(ctx context.Context, id string, trust *TrustContext) (*schema.MemoryRecord, error)
```

Fetches a single record with trust-context gating.

#### RetrieveByType

```go
func (svc *Service) RetrieveByType(ctx context.Context, memType schema.MemoryType, trust *TrustContext) ([]*schema.MemoryRecord, error)
```

Fetches all records of a given type that pass the trust check, sorted by
salience descending.

### RetrieveRequest

```go
type RetrieveRequest struct {
    TaskDescriptor string
    Trust          *TrustContext
    MemoryTypes    []schema.MemoryType
    MinSalience    float64
    Limit          int
}
```

| Field | Description |
|---|---|
| `TaskDescriptor` | Describes the current task for contextual retrieval. |
| `Trust` | **Required.** Gates what records can be returned. |
| `MemoryTypes` | Restricts to specific types. Empty means all layers. |
| `MinSalience` | Filters records below this threshold. |
| `Limit` | Caps total results. `0` means no limit. |

### RetrieveResponse

```go
type RetrieveResponse struct {
    Records   []*schema.MemoryRecord
    Selection *SelectionResult
}
```

`Selection` is non-nil when competence or plan\_graph candidates were evaluated.

### TrustContext

```go
type TrustContext struct {
    MaxSensitivity schema.Sensitivity
    Authenticated  bool
    ActorID        string
    Scopes         []string
}

func NewTrustContext(maxSensitivity schema.Sensitivity, authenticated bool, actorID string, scopes []string) *TrustContext
```

Gates retrieval based on the requester's trust attributes.

#### Allows

```go
func (tc *TrustContext) Allows(record *schema.MemoryRecord) bool
```

Returns `true` if the record's sensitivity does not exceed `MaxSensitivity` and
its scope matches one of the allowed scopes (or the context has no scope
restrictions).

#### AllowsRedacted

```go
func (tc *TrustContext) AllowsRedacted(record *schema.MemoryRecord) bool
```

Returns `true` if the record is exactly one sensitivity level above the
maximum, allowing a redacted (metadata-only) view.

### Selector

```go
type Selector struct { /* unexported fields */ }

func NewSelector(confidenceThreshold float64) *Selector
```

Multi-solution selector for competence and plan\_graph records. Ranks candidates
using three equally-weighted signals:

1. **Applicability** -- trigger/constraint match (approximated by the `Confidence` field).
2. **Observed success rate** -- from `PerformanceStats` or `PlanMetrics`.
3. **Recency of reinforcement** -- exponential decay with a 30-day half-life.

#### Select

```go
func (s *Selector) Select(candidates []*schema.MemoryRecord) *SelectionResult
```

Ranks candidates and returns a `SelectionResult`. Confidence is computed as the
normalized score gap between the best and second-best candidate.

### SelectionResult

```go
type SelectionResult struct {
    Selected   []*schema.MemoryRecord
    Confidence float64
    NeedsMore  bool
}
```

| Field | Description |
|---|---|
| `Selected` | Ranked candidates in descending score order. |
| `Confidence` | Selection confidence in [0, 1]. |
| `NeedsMore` | `true` when confidence is below the threshold, indicating disambiguation is needed. |

### Filter Functions

```go
func FilterBySalience(records []*schema.MemoryRecord, minSalience float64) []*schema.MemoryRecord
```

Returns records with salience >= `minSalience`.

```go
func FilterBySensitivity(records []*schema.MemoryRecord, maxSensitivity schema.Sensitivity) []*schema.MemoryRecord
```

Returns records with sensitivity at or below `maxSensitivity`.

```go
func FilterByTrust(records []*schema.MemoryRecord, trust *TrustContext) []*schema.MemoryRecord
```

Returns records allowed by the trust context. Records one level above the
threshold are returned in **redacted** form (metadata only).

```go
func SortBySalience(records []*schema.MemoryRecord)
```

Sorts records by salience descending (highest first). In-place.

```go
func Redact(record *schema.MemoryRecord) *schema.MemoryRecord
```

Creates a redacted copy preserving ID, type, sensitivity, confidence, salience,
scope, tags, and timestamps. Clears payload, provenance, relations, and audit
log.

### Sentinel Errors

```go
var ErrAccessDenied = errors.New("access denied by trust context")
var ErrNilTrust     = errors.New("trust context is required")
```

---

## revision

```
import "github.com/GustyCube/membrane/pkg/revision"
```

Implements atomic revision operations for memory records. All operations run
within a single transaction so partial revisions are never externally visible
(RFC 15.7). Episodic records are immutable and cannot be revised.

### Service

```go
type Service struct { /* unexported fields */ }

func NewService(store storage.Store) *Service
```

#### Supersede

```go
func (s *Service) Supersede(ctx context.Context, oldID string, newRecord *schema.MemoryRecord, actor, rationale string) (*schema.MemoryRecord, error)
```

Atomically replaces an old record. The old record is retracted, the new record
gets a `supersedes` relation and provenance link. Semantic payloads get
`SupersededBy` / `Supersedes` cross-references.

#### Fork

```go
func (s *Service) Fork(ctx context.Context, sourceID string, forkedRecord *schema.MemoryRecord, actor, rationale string) (*schema.MemoryRecord, error)
```

Creates a derived record. Both source and fork remain active. A `derived_from`
relation is added. Intended for conditional variants.

#### Retract

```go
func (s *Service) Retract(ctx context.Context, id, actor, rationale string) error
```

Marks a record as retracted (salience zeroed, semantic status `retracted`)
without deleting it.

#### Merge

```go
func (s *Service) Merge(ctx context.Context, recordIDs []string, mergedRecord *schema.MemoryRecord, actor, rationale string) (*schema.MemoryRecord, error)
```

Combines multiple source records into one. All sources are retracted. The merged
record gets `derived_from` relations to each source.

#### Contest

```go
func (s *Service) Contest(ctx context.Context, id string, contestingRef string, actor, rationale string) error
```

Marks a record as contested. Adds a `contested_by` relation and sets semantic
revision status to `contested`.

### Sentinel Errors

```go
var ErrEpisodicImmutable = errors.New("episodic memory is immutable and cannot be revised")
var ErrRecordNotFound    = storage.ErrNotFound
```

### Evidence Validation

Semantic records created through revision operations **must** include at least
one evidence reference in their payload (`Evidence` field) or provenance
(`Sources` field). Operations fail with an error if this requirement is not met.

---

## decay

```
import "github.com/GustyCube/membrane/pkg/decay"
```

Implements salience decay, reinforcement, penalization, pruning, and background
scheduling as specified in RFC 15A.7.

### Service

```go
type Service struct { /* unexported fields */ }

func NewService(store storage.Store) *Service
```

#### ApplyDecay

```go
func (s *Service) ApplyDecay(ctx context.Context, id string) error
```

Calculates and applies decay to a single record's salience based on elapsed time
since `LastReinforcedAt`. Uses the record's configured decay curve. Records past
`MaxAgeSeconds` have salience zeroed. Runs within a transaction.

#### ApplyDecayAll

```go
func (s *Service) ApplyDecayAll(ctx context.Context) (int, error)
```

Applies decay to all non-pinned records. Returns the count of records processed.

#### Reinforce

```go
func (s *Service) Reinforce(ctx context.Context, id string, actor string, rationale string) error
```

Boosts salience by `ReinforcementGain`, updates `LastReinforcedAt`, and appends
an audit entry. Runs within a transaction.

#### Penalize

```go
func (s *Service) Penalize(ctx context.Context, id string, amount float64, actor string, rationale string) error
```

Reduces salience by `amount`, floored at `MinSalience`. Appends an audit entry.
Runs within a transaction.

#### Prune

```go
func (s *Service) Prune(ctx context.Context) (int, error)
```

Deletes records whose salience has dropped below 0.001 and whose deletion policy
is `auto_prune`. Pinned records are never pruned. Returns the count of pruned
records.

### DecayFunc

```go
type DecayFunc func(currentSalience float64, elapsedSeconds float64, profile schema.DecayProfile) float64
```

Signature for decay curve implementations.

### Built-in Curves

#### Exponential

```go
func Exponential(currentSalience, elapsedSeconds float64, profile schema.DecayProfile) float64
```

Computes `salience * 2^(-elapsed / halfLife)`, floored at `MinSalience`.

#### Linear

```go
func Linear(currentSalience, elapsedSeconds float64, profile schema.DecayProfile) float64
```

Computes `salience - (elapsed / halfLife) * salience`, floored at `MinSalience`.

#### GetDecayFunc

```go
func GetDecayFunc(curve schema.DecayCurve) DecayFunc
```

Returns the appropriate `DecayFunc` for a curve type. Falls back to
`Exponential` for unknown or custom curve types.

### Scheduler

```go
type Scheduler struct { /* unexported fields */ }

func NewScheduler(service *Service, interval time.Duration) *Scheduler
```

Runs periodic decay sweeps in a background goroutine.

#### Start

```go
func (s *Scheduler) Start(ctx context.Context)
```

Begins the periodic loop. Runs `ApplyDecayAll` followed by `Prune` at each
tick. Safe to call multiple times (only the first call starts the loop).

#### Stop

```go
func (s *Scheduler) Stop()
```

Gracefully shuts down the scheduler and waits for the goroutine to finish.
Safe to call even if `Start` was never called.

---

## consolidation

```
import "github.com/GustyCube/membrane/pkg/consolidation"
```

Analyzes episodic and working memory to extract durable knowledge. Consolidation
promotes raw experience into semantic facts, competence records, and plan graphs
(RFC Sections 10, 15.7). Consolidation is automatic and requires no user
approval. Promoted knowledge remains subject to decay and revision.

### Service

```go
type Service struct { /* unexported fields */ }

func NewService(store storage.Store) *Service
```

Creates a service that orchestrates four sub-consolidators.

#### RunAll

```go
func (s *Service) RunAll(ctx context.Context) (*ConsolidationResult, error)
```

Executes every consolidation pipeline in sequence and returns a combined result.
If any sub-consolidator fails, the run is aborted and partial results are
returned alongside the error.

### ConsolidationResult

```go
type ConsolidationResult struct {
    EpisodicCompressed  int
    SemanticExtracted   int
    CompetenceExtracted int
    PlanGraphsExtracted int
    DuplicatesResolved  int
}
```

| Field | Description |
|---|---|
| `EpisodicCompressed` | Episodic records whose salience was reduced past the age threshold. |
| `SemanticExtracted` | New semantic records created from episodic observations. |
| `CompetenceExtracted` | New competence records created from repeated successful patterns. |
| `PlanGraphsExtracted` | New plan graph records extracted from episodic tool graphs. |
| `DuplicatesResolved` | Duplicate records resolved by reinforcing existing records. |

### Sub-Consolidators

Each sub-consolidator can be used independently, though the `Service` orchestrates
them together.

#### EpisodicConsolidator

```go
type EpisodicConsolidator struct { /* unexported fields */ }

func NewEpisodicConsolidator(store storage.Store) *EpisodicConsolidator

func (c *EpisodicConsolidator) Consolidate(ctx context.Context) (int, error)
```

Compresses old episodic records by reducing their salience.

#### SemanticConsolidator

```go
type SemanticConsolidator struct { /* unexported fields */ }

func NewSemanticConsolidator(store storage.Store) *SemanticConsolidator

func (c *SemanticConsolidator) Consolidate(ctx context.Context) (int, int, error)
```

Extracts semantic facts from episodic observations. Returns `(created, reinforced, error)`.

#### CompetenceConsolidator

```go
type CompetenceConsolidator struct { /* unexported fields */ }

func NewCompetenceConsolidator(store storage.Store) *CompetenceConsolidator

func (c *CompetenceConsolidator) Consolidate(ctx context.Context) (int, int, error)
```

Extracts competence records from repeated successful episodic patterns. Returns
`(created, reinforced, error)`.

#### PlanGraphConsolidator

```go
type PlanGraphConsolidator struct { /* unexported fields */ }

func NewPlanGraphConsolidator(store storage.Store) *PlanGraphConsolidator

func (c *PlanGraphConsolidator) Consolidate(ctx context.Context) (int, error)
```

Extracts plan graph records from episodic tool graphs.

### Scheduler

```go
type Scheduler struct { /* unexported fields */ }

func NewScheduler(service *Service, interval time.Duration) *Scheduler
```

Runs periodic consolidation sweeps in a background goroutine.

#### Start

```go
func (s *Scheduler) Start(ctx context.Context)
```

Begins the periodic loop. Runs `RunAll` at each tick. Safe to call multiple
times.

#### Stop

```go
func (s *Scheduler) Stop()
```

Gracefully shuts down the scheduler. Safe to call even if `Start` was never
called.

---

## storage

```
import "github.com/GustyCube/membrane/pkg/storage"
```

Defines the `Store` and `Transaction` interfaces that all storage backends must
implement.

### Store

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

| Method | Description |
|---|---|
| `Create` | Persists a new record. Returns `ErrAlreadyExists` on duplicate ID. |
| `Get` | Retrieves by ID. Returns `ErrNotFound` if absent. |
| `Update` | Replaces an existing record. Returns `ErrNotFound` if absent. |
| `Delete` | Removes by ID. Returns `ErrNotFound` if absent. |
| `List` | Retrieves records matching filter options. |
| `ListByType` | Retrieves all records of a given memory type. |
| `UpdateSalience` | Sets salience for a specific record. |
| `AddAuditEntry` | Appends an audit log entry. |
| `AddRelation` | Adds a relation edge from a source record. |
| `GetRelations` | Retrieves all relations originating from a record. |
| `Begin` | Starts a new transaction. |
| `Close` | Releases resources (database connections, etc.). |

### Transaction

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

Wraps `Store` methods in an atomic transaction. Using a `Transaction` after
`Commit` or `Rollback` returns `ErrTxClosed`.

### WithTransaction

```go
func WithTransaction(ctx context.Context, s Store, fn func(tx Transaction) error) error
```

Executes `fn` within a transaction. Commits on success, rolls back on error or
panic (re-panics after rollback).

### ListOptions

```go
type ListOptions struct {
    Type        schema.MemoryType
    Tags        []string
    Scope       string
    Sensitivity schema.Sensitivity
    MinSalience float64
    MaxSalience float64
    Limit       int
    Offset      int
}
```

| Field | Description |
|---|---|
| `Type` | Filter by memory type. Empty means no filter. |
| `Tags` | Filter records that have ALL specified tags. |
| `Scope` | Filter by scope. Empty means no filter. |
| `Sensitivity` | Filter by sensitivity level. |
| `MinSalience` | Minimum salience threshold. `0` means no minimum. |
| `MaxSalience` | Maximum salience threshold. `0` means no maximum. |
| `Limit` | Max records returned. `0` means no limit. |
| `Offset` | Skip first N records for pagination. |

### Sentinel Errors

```go
var ErrNotFound      = errors.New("record not found")
var ErrAlreadyExists = errors.New("record already exists")
var ErrTxClosed      = errors.New("transaction already closed")
```

---

## metrics

```
import "github.com/GustyCube/membrane/pkg/metrics"
```

Collects observability metrics from the memory substrate.

### Collector

```go
type Collector struct { /* unexported fields */ }

func NewCollector(store storage.Store) *Collector
```

Gathers metrics by querying the underlying store.

#### Collect

```go
func (c *Collector) Collect(ctx context.Context) (*Snapshot, error)
```

Queries all records and computes a point-in-time `Snapshot`.

### Snapshot

```go
type Snapshot struct {
    TotalRecords          int            `json:"total_records"`
    RecordsByType         map[string]int `json:"records_by_type"`
    AvgSalience           float64        `json:"avg_salience"`
    AvgConfidence         float64        `json:"avg_confidence"`
    SalienceDistribution  map[string]int `json:"salience_distribution"`
    ActiveRecords         int            `json:"active_records"`
    PinnedRecords         int            `json:"pinned_records"`
    TotalAuditEntries     int            `json:"total_audit_entries"`
    MemoryGrowthRate      float64        `json:"memory_growth_rate"`
    RetrievalUsefulness   float64        `json:"retrieval_usefulness"`
    CompetenceSuccessRate float64        `json:"competence_success_rate"`
    PlanReuseFrequency    float64        `json:"plan_reuse_frequency"`
    RevisionRate          float64        `json:"revision_rate"`
}
```

| Field | Description |
|---|---|
| `TotalRecords` | Total number of memory records. |
| `RecordsByType` | Count per memory type (`episodic`, `semantic`, etc.). |
| `AvgSalience` | Mean salience across all records. |
| `AvgConfidence` | Mean confidence across all records. |
| `SalienceDistribution` | Histogram buckets: `0.0-0.2`, `0.2-0.4`, `0.4-0.6`, `0.6-0.8`, `0.8-1.0`. |
| `ActiveRecords` | Records with salience > 0. |
| `PinnedRecords` | Records with `Pinned = true`. |
| `TotalAuditEntries` | Sum of all audit log entries across all records. |
| `MemoryGrowthRate` | Fraction of records created in the last 24 hours (RFC 15.10). |
| `RetrievalUsefulness` | Ratio of reinforce actions to total audit actions (RFC 15.10). |
| `CompetenceSuccessRate` | Average success rate across competence records (RFC 15.10). |
| `PlanReuseFrequency` | Average execution count across plan graph records (RFC 15.10). |
| `RevisionRate` | Ratio of revision actions (revise, fork, merge) to total audit actions (RFC 15.10). |
