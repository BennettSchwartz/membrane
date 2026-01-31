# gRPC API

Membrane exposes a gRPC service at `membrane.v1.MembraneService` with 15 RPC methods covering ingestion, retrieval, revision, reinforcement, and metrics.

The server listens on the configured address (default: `:9090`). Request and response bodies use JSON encoding over gRPC.

## Ingestion Methods

### IngestEvent

Ingest a raw event into episodic memory.

**Request:**

```json
{
  "source": "my-agent",
  "event_kind": "user_input",
  "ref": "session-001/msg-1",
  "summary": "User asked about deployment options",
  "timestamp": "2025-01-15T10:30:00Z",
  "tags": ["deployment", "question"],
  "scope": "project-alpha",
  "sensitivity": "low"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source` | string | Yes | Identifier for the source system or agent |
| `event_kind` | string | Yes | Type of event (e.g., `user_input`, `error`, `observation`) |
| `ref` | string | Yes | Opaque reference into the host system |
| `summary` | string | No | Human-readable summary |
| `timestamp` | string | No | RFC 3339 timestamp; defaults to current time |
| `tags` | string[] | No | Free-form labels |
| `scope` | string | No | Visibility scope |
| `sensitivity` | string | No | Sensitivity level; defaults to server config |

**Response:** `IngestResponse` containing the full `MemoryRecord` as JSON.

---

### IngestToolOutput

Ingest a tool call and its result into episodic memory.

**Request:**

```json
{
  "source": "my-agent",
  "tool_name": "shell",
  "args": { "command": "go build ./..." },
  "result": { "exit_code": 0, "stdout": "OK" },
  "depends_on": ["tool-node-001"],
  "timestamp": "2025-01-15T10:31:00Z",
  "tags": ["build"],
  "scope": "project-alpha",
  "sensitivity": "low"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source` | string | Yes | Source identifier |
| `tool_name` | string | Yes | Name of the tool |
| `args` | object | No | Arguments passed to the tool |
| `result` | any | No | Tool output |
| `depends_on` | string[] | No | IDs of tool nodes this depends on |
| `timestamp` | string | No | RFC 3339 timestamp |
| `tags` | string[] | No | Free-form labels |
| `scope` | string | No | Visibility scope |
| `sensitivity` | string | No | Sensitivity level |

**Response:** `IngestResponse` containing the full `MemoryRecord` as JSON.

---

### IngestObservation

Ingest a factual observation (subject-predicate-object) into episodic memory. May later be consolidated into a semantic fact.

**Request:**

```json
{
  "source": "my-agent",
  "subject": "user",
  "predicate": "prefers_editor",
  "object": "vscode",
  "timestamp": "2025-01-15T10:32:00Z",
  "tags": ["preference"],
  "scope": "user",
  "sensitivity": "medium"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source` | string | Yes | Source identifier |
| `subject` | string | Yes | Entity the observation is about |
| `predicate` | string | Yes | Relationship or property |
| `object` | any | Yes | The observed value |
| `timestamp` | string | No | RFC 3339 timestamp |
| `tags` | string[] | No | Free-form labels |
| `scope` | string | No | Visibility scope |
| `sensitivity` | string | No | Sensitivity level |

**Response:** `IngestResponse` containing the full `MemoryRecord` as JSON.

---

### IngestOutcome

Record the outcome of a task or action, linking it to an existing record.

**Request:**

```json
{
  "source": "my-agent",
  "target_record_id": "550e8400-e29b-41d4-a716-446655440000",
  "outcome_status": "success",
  "timestamp": "2025-01-15T10:35:00Z"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source` | string | Yes | Source identifier |
| `target_record_id` | string | Yes | UUID of the record to attach the outcome to |
| `outcome_status` | string | Yes | `success`, `failure`, or `partial` |
| `timestamp` | string | No | RFC 3339 timestamp |

**Response:** `IngestResponse` containing the full `MemoryRecord` as JSON.

---

### IngestWorkingState

Ingest the current state of an ongoing task into working memory. Working memory supports task resumption across sessions.

**Request:**

```json
{
  "source": "my-agent",
  "thread_id": "session-001",
  "state": "executing",
  "next_actions": ["run tests", "deploy"],
  "open_questions": ["which test suite?"],
  "context_summary": "Backend initialized, frontend pending",
  "active_constraints": [{"type": "deadline", "value": "2025-02-01"}],
  "timestamp": "2025-01-15T10:33:00Z",
  "tags": ["setup"],
  "scope": "project-alpha",
  "sensitivity": "low"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source` | string | Yes | Source identifier |
| `thread_id` | string | Yes | Identifier for the task thread |
| `state` | string | Yes | Task state: `planning`, `executing`, `blocked`, `waiting`, or `done` |
| `next_actions` | string[] | No | Planned next steps |
| `open_questions` | string[] | No | Unresolved questions |
| `context_summary` | string | No | Human-readable summary of current context |
| `active_constraints` | object[] | No | JSON-encoded active constraints |
| `timestamp` | string | No | RFC 3339 timestamp; defaults to current time |
| `tags` | string[] | No | Free-form labels |
| `scope` | string | No | Visibility scope |
| `sensitivity` | string | No | Sensitivity level |

**Response:** `IngestResponse` containing the full `MemoryRecord` as JSON.

---

## Retrieval Methods

### Retrieve

Perform layered retrieval across all memory types.

**Request:**

```json
{
  "task_descriptor": "fix npm peer dependency error",
  "trust": {
    "max_sensitivity": "medium",
    "authenticated": true,
    "actor_id": "agent-1",
    "scopes": ["project-alpha"]
  },
  "memory_types": ["semantic", "competence"],
  "min_salience": 0.2,
  "limit": 10
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `task_descriptor` | string | No | Description of the current task for contextual retrieval |
| `trust` | object | Yes | Trust context (see [Security](/guide/security)) |
| `memory_types` | string[] | No | Restrict to specific types; empty means all |
| `min_salience` | float | No | Minimum salience threshold |
| `limit` | int | No | Maximum records to return; 0 means unlimited |

**Response:**

```json
{
  "records": [ /* array of MemoryRecord JSON objects */ ],
  "selection": {
    "selected": [ /* ranked candidates */ ],
    "confidence": 0.85,
    "needs_more": false
  }
}
```

The `selection` field is present only when competence or plan graph candidates were evaluated.

---

### RetrieveByID

Fetch a single record by its UUID.

**Request:**

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "trust": {
    "max_sensitivity": "high",
    "authenticated": true,
    "actor_id": "agent-1",
    "scopes": []
  }
}
```

**Response:** `MemoryRecordResponse` containing the full `MemoryRecord` as JSON. Returns an error if the record does not exist or the trust context denies access.

---

## Revision Methods

### Supersede

Replace a semantic fact with a corrected version. The old record is marked with `superseded_by` and the new record links back via `supersedes`.

**Request:**

```json
{
  "old_id": "550e8400-e29b-41d4-a716-446655440000",
  "new_record": { /* full MemoryRecord JSON */ },
  "actor": "agent-1",
  "rationale": "Updated preferred language from Python to Rust"
}
```

**Response:** `MemoryRecordResponse` containing the new record.

---

### Fork

Create a conditional variant of a record. Used when a fact is context-dependent and both versions should coexist.

**Request:**

```json
{
  "source_id": "550e8400-e29b-41d4-a716-446655440000",
  "forked_record": { /* full MemoryRecord JSON */ },
  "actor": "agent-1",
  "rationale": "Forked: user prefers Python for scripting, Rust for systems"
}
```

**Response:** `MemoryRecordResponse` containing the forked record.

---

### Retract

Mark a record as retracted without deleting it. Retracted records remain in storage but have their `revision.status` set to `retracted`.

**Request:**

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "actor": "agent-1",
  "rationale": "Observation was incorrect"
}
```

**Response:** Empty acknowledgement.

---

### Merge

Combine multiple related records into a single consolidated record. The source records are linked to the merged result.

**Request:**

```json
{
  "ids": [
    "550e8400-e29b-41d4-a716-446655440000",
    "660e8400-e29b-41d4-a716-446655440001"
  ],
  "merged_record": { /* full MemoryRecord JSON */ },
  "actor": "consolidator",
  "rationale": "Merged duplicate semantic facts about user editor preference"
}
```

**Response:** `MemoryRecordResponse` containing the merged record.

---

### Contest

Mark a record as contested when conflicting evidence appears. The contesting reference links to the record or evidence that contradicts this one.

**Request:**

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "contesting_ref": "770e8400-e29b-41d4-a716-446655440002",
  "actor": "agent-1",
  "rationale": "New observation contradicts this semantic fact"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | Yes | UUID of the record to contest |
| `contesting_ref` | string | Yes | Reference to the conflicting record or evidence |
| `actor` | string | Yes | Identity of the contesting agent |
| `rationale` | string | Yes | Explanation for the contestation |

**Response:** Empty acknowledgement.

---

## Reinforcement Methods

### Reinforce

Boost a record's salience and reset its decay clock. Used when a memory proves useful.

**Request:**

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "actor": "agent-1",
  "rationale": "Used this competence successfully to fix build"
}
```

**Response:** Empty acknowledgement.

---

### Penalize

Reduce a record's salience by a specified amount, floored at the record's `min_salience`.

**Request:**

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "amount": 0.3,
  "actor": "agent-1",
  "rationale": "Competence recipe failed on this attempt"
}
```

**Response:** Empty acknowledgement.

---

## Metrics

### GetMetrics

Retrieve a snapshot of system metrics.

**Request:** Empty object `{}`.

**Response:**

```json
{
  "snapshot": {
    "total_records": 1542,
    "records_by_type": {
      "episodic": 890,
      "working": 12,
      "semantic": 340,
      "competence": 78,
      "plan_graph": 22
    },
    "ingestions_total": 2150,
    "retrievals_total": 8430,
    "consolidations_total": 45,
    "decay_runs_total": 720
  }
}
```
