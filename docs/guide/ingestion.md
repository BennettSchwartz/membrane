---
outline: [2, 3]
---

# Ingestion

Ingestion is the process by which raw data enters Membrane and becomes structured, classified memory. Every piece of knowledge the system retains -- an event observed, a tool output captured, a fact learned, a task state snapshot -- begins as an ingestion request and exits the pipeline as a fully-formed `MemoryRecord` persisted to storage.

## Overview

The ingestion layer sits between external callers (agents, tools, human operators) and the memory store. It is responsible for three things:

1. **Classification** -- determining what _type_ of memory a piece of data should become.
2. **Policy application** -- assigning sensitivity, confidence, salience, and lifecycle metadata.
3. **Record construction** -- building a complete `MemoryRecord` with provenance, audit log, and type-specific payload, then persisting it.

```
Caller (gRPC / Go API)
        |
        v
  +-----------+      +---------------+      +----------------+
  | Classifier| ---> | Policy Engine | ---> | Record Builder |
  +-----------+      +---------------+      +----------------+
                                                    |
                                                    v
                                               [ Storage ]
```

The pipeline is synchronous. A single ingest call classifies, validates, builds, and stores the record before returning the fully-populated `MemoryRecord` to the caller.

## Memory Types

Membrane supports five memory types. The ingestion layer creates records for four of them directly and updates a fifth (outcome) on existing records.

| Type | Kind constant | Description | When to use |
|------|--------------|-------------|-------------|
| **Episodic** | `episodic` | Raw experience: events, tool calls, errors | Capturing what happened during agent execution |
| **Semantic** | `semantic` | Stable facts as subject-predicate-object triples | Recording learned knowledge, preferences, environment facts |
| **Working** | `working` | Current task state for resumption | Snapshotting in-flight task context across sessions |
| **Competence** | `competence` | Procedural knowledge: how to achieve goals | Encoding reusable skills with triggers and recipes |
| **Plan Graph** | `plan_graph` | Reusable solution structures as directed graphs | Storing multi-step plans with dependencies |

::: tip
Episodic memories are intentionally short-lived (default half-life of 1 hour). They serve as raw evidence that the consolidation layer later distills into longer-lived semantic or competence memories.
:::

## The Ingestion Pipeline

Every ingest call follows the same four-stage pipeline:

### 1. Candidate Construction

The caller's request is converted into a `MemoryCandidate` -- an intermediate representation that carries all fields needed by the classifier and policy engine. The candidate's `Kind` field identifies the source type:

| CandidateKind | Value | Produces |
|---------------|-------|----------|
| `CandidateKindEvent` | `"event"` | Episodic memory |
| `CandidateKindToolOutput` | `"tool_output"` | Episodic memory |
| `CandidateKindObservation` | `"observation"` | Semantic memory |
| `CandidateKindOutcome` | `"outcome"` | Updates existing episodic record |
| `CandidateKindWorkingState` | `"working_state"` | Working memory |

### 2. Classification

The `Classifier` examines the candidate's `Kind` and returns the appropriate `MemoryType`. Classification is deterministic -- the mapping is fixed:

```go
func (c *Classifier) Classify(candidate *MemoryCandidate) schema.MemoryType {
    switch candidate.Kind {
    case CandidateKindEvent:        return schema.MemoryTypeEpisodic
    case CandidateKindToolOutput:   return schema.MemoryTypeEpisodic
    case CandidateKindObservation:  return schema.MemoryTypeSemantic
    case CandidateKindOutcome:      return schema.MemoryTypeEpisodic
    case CandidateKindWorkingState: return schema.MemoryTypeWorking
    default:                        return schema.MemoryTypeEpisodic
    }
}
```

### 3. Policy Application

The `PolicyEngine` validates the candidate and produces a `PolicyResult` containing lifecycle metadata. This stage determines:

- **Sensitivity** -- uses the caller's override if provided, otherwise falls back to the configured default.
- **Confidence** -- assigned based on source type (see [Ingestion Policies](#ingestion-policies)).
- **Salience** -- set to the configured initial value (default `1.0`).
- **Decay profile** -- type-specific half-life with exponential decay curve.
- **Deletion policy** -- configurable default (typically `auto_prune`).

### 4. Record Construction and Storage

The `buildRecord` helper assembles the final `MemoryRecord`:

- Generates a UUID for the record ID.
- Sets `CreatedAt` and `UpdatedAt` to the current time.
- Attaches the lifecycle configuration from the policy result.
- Creates provenance with the source actor and timestamp.
- Writes an initial audit log entry (`create` action).
- Persists the record via the storage layer.

## Classification

The classifier is a stateless component that maps candidate kinds to memory types. It does not inspect payload content -- classification is based purely on the structural kind of the incoming data.

### Classification Rules

| Input Kind | Output MemoryType | Rationale |
|-----------|-------------------|-----------|
| Event | `episodic` | Events are temporal experiences |
| Tool Output | `episodic` | Tool calls are recorded as episodes with tool graph data |
| Observation | `semantic` | Observations encode factual knowledge (subject-predicate-object) |
| Outcome | `episodic` | Outcomes update existing episodic records |
| Working State | `working` | Task state snapshots are working memory |

::: info
Unknown candidate kinds default to `episodic`. This ensures the system degrades gracefully if new candidate types are introduced before the classifier is updated.
:::

## Ingestion Policies

The `PolicyEngine` enforces validation rules and assigns lifecycle metadata to every record before it reaches storage.

### Validation Rules

The policy engine validates that all required fields are present for each candidate kind. Validation failures return an error and prevent the record from being created.

**Universal requirements** (all candidate kinds):

| Field | Rule |
|-------|------|
| `kind` | Must not be empty |
| `source` | Must not be empty |
| `timestamp` | Must not be zero |

**Kind-specific requirements:**

| Kind | Required Fields |
|------|----------------|
| Event | `event_kind`, `event_ref` |
| Tool Output | `tool_name` |
| Observation | `subject`, `predicate` |
| Outcome | `target_record_id`, `outcome_status` |
| Working State | `thread_id`, `task_state` |

### Confidence Assignment

Confidence is an epistemic score in the range `[0, 1]` that reflects how reliable the source is considered to be:

| Candidate Kind | Confidence | Rationale |
|---------------|------------|-----------|
| Event | `0.8` | Events from the system are generally reliable |
| Tool Output | `0.9` | Tool outputs are deterministic and verifiable |
| Observation | `0.7` | Observations may be subjective or imprecise |
| Outcome | `0.85` | Outcomes are reported post-hoc with reasonable accuracy |
| Working State | `1.0` | Working state is the agent's own current context |

### Decay Half-Lives

Each memory type has a configured decay half-life that controls how quickly salience decreases over time:

| Memory Type | Default Half-Life | Duration |
|------------|-------------------|----------|
| Episodic | `3600` seconds | 1 hour |
| Semantic | `2592000` seconds | 30 days |
| Working | `86400` seconds | 1 day |
| Competence | `2592000` seconds | 30 days (same as semantic) |
| Plan Graph | `2592000` seconds | 30 days (same as semantic) |

All records use exponential decay by default. The decay curve, half-lives, and initial salience are configurable through `PolicyDefaults`:

```go
defaults := ingestion.PolicyDefaults{
    Sensitivity:             schema.SensitivityLow,
    EpisodicHalfLifeSeconds: 3600,
    SemanticHalfLifeSeconds: 2592000,
    WorkingHalfLifeSeconds:  86400,
    DefaultInitialSalience:  1.0,
    DefaultDeletionPolicy:   schema.DeletionPolicyAutoPrune,
}
engine := ingestion.NewPolicyEngine(defaults)
```

### Deletion Policies

Records are created with one of three deletion policies:

| Policy | Value | Behavior |
|--------|-------|----------|
| Auto-prune | `auto_prune` | Automatically deleted when salience drops below threshold |
| Manual only | `manual_only` | Requires explicit user action to delete |
| Never | `never` | Cannot be deleted |

The default is `auto_prune`.

## Ingestion via gRPC

Membrane exposes five ingest RPCs. Each accepts a structured request, runs it through the pipeline, and returns the created `MemoryRecord` as JSON bytes in the response.

### IngestEvent

Creates an episodic memory from a discrete event.

```protobuf
rpc IngestEvent(IngestEventRequest) returns (IngestResponse);
```

**Request fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source` | `string` | Yes | Actor or system that produced the event |
| `event_kind` | `string` | Yes | Type of event (e.g., `"user_input"`, `"error"`, `"system"`) |
| `ref` | `string` | Yes | Reference identifier for the source event |
| `summary` | `string` | No | Human-readable summary |
| `timestamp` | `string` | No | RFC 3339 timestamp; defaults to current time |
| `tags` | `string[]` | No | Labels for categorization |
| `scope` | `string` | No | Visibility scope (`"user"`, `"project"`, `"global"`) |
| `sensitivity` | `string` | No | Override sensitivity (`"public"`, `"low"`, `"medium"`, `"high"`, `"hyper"`) |

**Example (grpcurl):**

```bash
grpcurl -plaintext -d '{
  "source": "coding-agent",
  "event_kind": "user_input",
  "ref": "msg-001",
  "summary": "User asked to refactor auth module",
  "tags": ["refactor", "auth"],
  "scope": "project"
}' localhost:9820 membrane.v1.MembraneService/IngestEvent
```

### IngestToolOutput

Creates an episodic memory with tool graph data from a tool invocation.

```protobuf
rpc IngestToolOutput(IngestToolOutputRequest) returns (IngestResponse);
```

**Request fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source` | `string` | Yes | Actor or system that invoked the tool |
| `tool_name` | `string` | Yes | Name of the tool |
| `args` | `bytes` (JSON) | No | Arguments passed to the tool |
| `result` | `bytes` (JSON) | No | Output produced by the tool |
| `depends_on` | `string[]` | No | IDs of tool nodes this depends on |
| `timestamp` | `string` | No | RFC 3339 timestamp |
| `tags` | `string[]` | No | Labels for categorization |
| `scope` | `string` | No | Visibility scope |
| `sensitivity` | `string` | No | Override sensitivity |

**Example:**

```bash
grpcurl -plaintext -d '{
  "source": "coding-agent",
  "tool_name": "file_read",
  "args": "{\"path\": \"/src/auth.go\"}",
  "result": "{\"content\": \"package auth...\", \"lines\": 142}",
  "tags": ["tool", "file_read"]
}' localhost:9820 membrane.v1.MembraneService/IngestToolOutput
```

### IngestObservation

Creates a semantic memory from an observation, extracting subject-predicate-object structure.

```protobuf
rpc IngestObservation(IngestObservationRequest) returns (IngestResponse);
```

**Request fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source` | `string` | Yes | Actor that made the observation |
| `subject` | `string` | Yes | Entity the observation is about |
| `predicate` | `string` | Yes | Relationship or property observed |
| `object` | `bytes` (JSON) | No | Value or related entity |
| `timestamp` | `string` | No | RFC 3339 timestamp |
| `tags` | `string[]` | No | Labels for categorization |
| `scope` | `string` | No | Visibility scope |
| `sensitivity` | `string` | No | Override sensitivity |

**Example:**

```bash
grpcurl -plaintext -d '{
  "source": "coding-agent",
  "subject": "user",
  "predicate": "prefers_language",
  "object": "\"Go\"",
  "tags": ["preference"]
}' localhost:9820 membrane.v1.MembraneService/IngestObservation
```

### IngestOutcome

Updates an existing episodic record with outcome data. This does not create a new record -- it modifies the target record's payload, adds a provenance source, and appends an audit entry.

```protobuf
rpc IngestOutcome(IngestOutcomeRequest) returns (IngestResponse);
```

**Request fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source` | `string` | Yes | Actor reporting the outcome |
| `target_record_id` | `string` | Yes | ID of the existing episodic record |
| `outcome_status` | `string` | Yes | Result: `"success"`, `"failure"`, or `"partial"` |
| `timestamp` | `string` | No | RFC 3339 timestamp |

**Example:**

```bash
grpcurl -plaintext -d '{
  "source": "coding-agent",
  "target_record_id": "a1b2c3d4-...",
  "outcome_status": "success"
}' localhost:9820 membrane.v1.MembraneService/IngestOutcome
```

::: warning
`IngestOutcome` will return an error if the target record is not an episodic memory. Only episodic records have an `Outcome` field in their payload.
:::

### IngestWorkingState

Creates a working memory record from a task state snapshot.

```protobuf
rpc IngestWorkingState(IngestWorkingStateRequest) returns (IngestResponse);
```

**Request fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source` | `string` | Yes | Actor that produced the state |
| `thread_id` | `string` | Yes | Thread/session identifier |
| `state` | `string` | Yes | Task state: `"planning"`, `"executing"`, `"blocked"`, `"waiting"`, `"done"` |
| `next_actions` | `string[]` | No | Next planned actions |
| `open_questions` | `string[]` | No | Unresolved questions |
| `context_summary` | `string` | No | Summary of current context |
| `active_constraints` | `bytes` (JSON) | No | JSON array of constraint objects |
| `timestamp` | `string` | No | RFC 3339 timestamp |
| `tags` | `string[]` | No | Labels for categorization |
| `scope` | `string` | No | Visibility scope |
| `sensitivity` | `string` | No | Override sensitivity |

**Example:**

```bash
grpcurl -plaintext -d '{
  "source": "coding-agent",
  "thread_id": "session-42",
  "state": "executing",
  "next_actions": ["run tests", "commit changes"],
  "open_questions": ["Which test framework to use?"],
  "context_summary": "Refactoring auth module, tests passing",
  "tags": ["task-refactor"]
}' localhost:9820 membrane.v1.MembraneService/IngestWorkingState
```

### Response Format

All ingest RPCs return an `IngestResponse` containing the full `MemoryRecord` serialized as JSON bytes:

```protobuf
message IngestResponse {
  bytes record = 1; // JSON-encoded MemoryRecord
}
```

## Payload Schemas

Each memory type has a structured payload with a `kind` discriminator field. The ingestion layer constructs these payloads automatically from the request data.

### Episodic Payload

Created by `IngestEvent` and `IngestToolOutput`. Captures a timeline of events and optional tool graph.

```json
{
  "kind": "episodic",
  "timeline": [
    {
      "t": "2025-01-15T10:30:00Z",
      "event_kind": "user_input",
      "ref": "msg-001",
      "summary": "User asked to refactor auth module"
    }
  ],
  "tool_graph": [
    {
      "id": "node-uuid",
      "tool": "file_read",
      "args": { "path": "/src/auth.go" },
      "result": { "content": "..." },
      "timestamp": "2025-01-15T10:30:05Z",
      "depends_on": []
    }
  ],
  "outcome": "success",
  "environment": null,
  "artifacts": []
}
```

The `tool_graph` field is only populated by `IngestToolOutput`. Events produce a timeline-only payload.

### Semantic Payload

Created by `IngestObservation`. Stores revisable facts as subject-predicate-object triples.

```json
{
  "kind": "semantic",
  "subject": "user",
  "predicate": "prefers_language",
  "object": "Go",
  "validity": {
    "mode": "global"
  },
  "evidence": [
    {
      "source_type": "observation",
      "source_id": "coding-agent",
      "timestamp": "2025-01-15T10:30:00Z"
    }
  ],
  "revision_policy": "replace"
}
```

Observations are created with `global` validity mode and `replace` revision policy by default. The validity mode supports three values:

| Mode | Description |
|------|-------------|
| `global` | The fact is universally valid |
| `conditional` | The fact is valid under specific conditions |
| `timeboxed` | The fact is valid within a time window |

### Working Payload

Created by `IngestWorkingState`. Captures the current state of an in-flight task.

```json
{
  "kind": "working",
  "thread_id": "session-42",
  "state": "executing",
  "active_constraints": [
    {
      "type": "resource",
      "key": "max_file_edits",
      "value": 5,
      "required": true
    }
  ],
  "next_actions": ["run tests", "commit changes"],
  "open_questions": ["Which test framework to use?"],
  "context_summary": "Refactoring auth module, tests passing"
}
```

### Competence Payload

Competence and plan graph records are not created through dedicated ingest RPCs in the current version. They are typically produced by the consolidation layer or ingested as full `MemoryRecord` objects. Their payload schemas are included here for reference.

```json
{
  "kind": "competence",
  "skill_name": "fix_go_compilation_error",
  "triggers": [
    {
      "signal": "compilation_error",
      "conditions": { "language": "go" }
    }
  ],
  "recipe": [
    {
      "step": "Read the error message and identify the file",
      "tool": "file_read",
      "validation": "File contents loaded"
    },
    {
      "step": "Apply the fix",
      "tool": "file_edit",
      "validation": "File saved without errors"
    }
  ],
  "required_tools": ["file_read", "file_edit"],
  "failure_modes": ["File not found", "Permission denied"],
  "performance": {
    "success_count": 12,
    "failure_count": 2,
    "success_rate": 0.857
  }
}
```

### Plan Graph Payload

```json
{
  "kind": "plan_graph",
  "plan_id": "setup-go-project",
  "version": "1.0.0",
  "intent": "setup_project",
  "constraints": { "language": "go" },
  "nodes": [
    { "id": "n1", "op": "mkdir", "params": { "path": "cmd/" } },
    { "id": "n2", "op": "go_mod_init", "params": { "module": "example.com/app" } },
    { "id": "n3", "op": "write_main", "params": {} }
  ],
  "edges": [
    { "from": "n1", "to": "n3", "kind": "control" },
    { "from": "n2", "to": "n3", "kind": "data" }
  ]
}
```

## Tags and Metadata

### Tags

Tags are free-form string labels attached to memory records for categorization and retrieval filtering. They are set at ingestion time and passed through to the stored record.

```go
req := ingestion.IngestEventRequest{
    Source:    "coding-agent",
    EventKind: "error",
    Ref:       "err-042",
    Tags:      []string{"error", "compilation", "go"},
}
```

**Constraints enforced by the gRPC layer:**

| Constraint | Limit |
|-----------|-------|
| Maximum number of tags | 100 |
| Maximum tag length | 256 characters |

### Thread IDs

The `thread_id` field in working memory records identifies the thread or session a task belongs to. This enables retrieval of working memory scoped to a specific execution context.

### Source Tracking

Every record tracks its source through two mechanisms:

1. **`source` field on the request** -- identifies the actor or system component that produced the data.
2. **Provenance** -- the record's `provenance.sources` array contains a `ProvenanceSource` entry with the source actor, timestamp, and provenance kind.

The provenance kind is mapped automatically from the candidate kind:

| Candidate Kind | Provenance Kind |
|---------------|----------------|
| Event | `event` |
| Tool Output | `tool_call` |
| Observation | `observation` |
| Outcome | `outcome` |

## Trust Context

### Sensitivity Levels

Sensitivity controls access during retrieval. Records with higher sensitivity require elevated trust context to read. The five levels, from least to most restrictive:

| Level | Value | Description |
|-------|-------|-------------|
| Public | `public` | Freely shareable content |
| Low | `low` | Minimal sensitivity (default) |
| Medium | `medium` | Moderately sensitive |
| High | `high` | Requires elevated trust |
| Hyper | `hyper` | Maximum protection |

### Setting Sensitivity at Ingestion

Sensitivity can be set in two ways:

1. **Caller override** -- pass a `sensitivity` value in the ingest request. This takes precedence.
2. **Policy default** -- if no override is provided, the policy engine assigns the configured default (default: `low`).

```bash
# Override sensitivity to "high"
grpcurl -plaintext -d '{
  "source": "coding-agent",
  "subject": "user",
  "predicate": "api_key",
  "object": "\"sk-...\"",
  "sensitivity": "high"
}' localhost:9820 membrane.v1.MembraneService/IngestObservation
```

### Scopes

The `scope` field defines the visibility boundary for a record. Scopes are implementation-defined strings such as `"user"`, `"project"`, `"workspace"`, or `"global"`. During retrieval, the trust context specifies which scopes the caller is authorized to access.

::: tip
Use scopes to partition memory by project or user. For example, a multi-tenant agent can set `scope: "project:acme"` to isolate memories per customer project.
:::

## Error Handling

Errors can occur at multiple stages of the ingestion pipeline. The gRPC layer validates input constraints before the request reaches the ingestion service, and the policy engine validates structural requirements on the candidate.

### gRPC Input Validation Errors

These are returned as gRPC `INVALID_ARGUMENT` status codes:

| Error | Condition |
|-------|-----------|
| String exceeds maximum length | Any string field exceeds 100,000 characters |
| Too many tags | More than 100 tags provided |
| Tag exceeds maximum length | Any single tag exceeds 256 characters |
| Payload exceeds maximum size | JSON payload (`args`, `result`, `object`, `active_constraints`) exceeds 10 MB |
| Invalid timestamp | Timestamp string is not valid RFC 3339 |
| Invalid JSON | `args`, `result`, `object`, or `active_constraints` contain malformed JSON |

### Policy Validation Errors

These are returned when the candidate fails structural validation in the policy engine:

| Error | Condition |
|-------|-----------|
| `candidate kind is required` | `Kind` field is empty |
| `candidate source is required` | `Source` field is empty |
| `candidate timestamp is required` | `Timestamp` is zero |
| `event kind is required for event candidates` | Event candidate missing `EventKind` |
| `event ref is required for event candidates` | Event candidate missing `EventRef` |
| `tool name is required for tool output candidates` | Tool output missing `ToolName` |
| `subject is required for observation candidates` | Observation missing `Subject` |
| `predicate is required for observation candidates` | Observation missing `Predicate` |
| `target record ID is required for outcome candidates` | Outcome missing `TargetRecordID` |
| `outcome status is required for outcome candidates` | Outcome missing `OutcomeStatus` |
| `thread ID is required for working state candidates` | Working state missing `ThreadID` |
| `task state is required for working state candidates` | Working state missing `TaskState` |

### Record Validation Errors

The `MemoryRecord.Validate()` method checks the final record before storage:

| Field | Rule |
|-------|------|
| `id` | Must not be empty |
| `type` | Must not be empty |
| `sensitivity` | Must not be empty |
| `confidence` | Must be in range `[0, 1]` |
| `salience` | Must be >= 0 |
| `payload` | Must not be nil |

### Outcome-Specific Errors

`IngestOutcome` has additional failure modes:

- **Record not found** -- the `target_record_id` does not exist in storage.
- **Wrong record type** -- the target record is not an episodic memory. Only episodic payloads have an `Outcome` field.
