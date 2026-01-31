# Memory Types

Membrane defines five memory types, each with a dedicated payload schema. Every `MemoryRecord` has a `type` field that determines which payload structure it carries. See [Schemas](/reference/schemas) for the full `MemoryRecord` base type and related structures.

## Episodic Memory

**Type value:** `episodic`

Episodic memory captures raw experience as a time-ordered sequence of events. It is intentionally short-lived and provides evidence for later consolidation into durable knowledge.

::: tip Key Rule
Episodic payloads are **append-only**. Semantic correction of episodic memory is forbidden -- the raw experience must be preserved as-is.
:::

### Payload Structure

```json
{
  "kind": "episodic",
  "timeline": [
    {
      "t": "2025-01-15T10:30:00Z",
      "event_kind": "user_input",
      "ref": "session-001/msg-1",
      "summary": "User asked about deployment options"
    },
    {
      "t": "2025-01-15T10:30:05Z",
      "event_kind": "tool_call",
      "ref": "session-001/tool-1",
      "summary": "Called deploy-check tool"
    }
  ],
  "tool_graph": [
    {
      "id": "t1",
      "tool": "deploy-check",
      "args": { "env": "production" },
      "result": { "status": "ready" },
      "depends_on": []
    }
  ],
  "environment": {
    "os": "linux",
    "os_version": "6.1",
    "working_directory": "/home/user/project"
  },
  "outcome": "success",
  "artifacts": ["logs/deploy-check-001.log"]
}
```

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `kind` | string | Yes | Always `"episodic"` |
| `timeline` | array | Yes | Time-ordered sequence of `TimelineEvent` objects |
| `tool_graph` | array | No | Tool call nodes with dependency edges |
| `environment` | object | No | OS, versions, working directory snapshot |
| `outcome` | string | No | `success`, `failure`, or `partial` |
| `artifacts` | array | No | References to external logs, screenshots, files |

---

## Working Memory

**Type value:** `working`

Working memory captures the current state of an ongoing task. It enables resumption across sessions or devices and is freely editable. Working memory is discarded when the task ends.

### Payload Structure

```json
{
  "kind": "working",
  "thread_id": "thread-abc-123",
  "state": "executing",
  "active_constraints": [
    {
      "type": "budget",
      "key": "max_tokens",
      "value": 4096,
      "required": true
    }
  ],
  "next_actions": [
    "Run integration tests",
    "Update deployment manifest"
  ],
  "open_questions": [
    "Which region should we deploy to?"
  ],
  "context_summary": "Deploying v2.1 to production with zero-downtime strategy"
}
```

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `kind` | string | Yes | Always `"working"` |
| `thread_id` | string | Yes | Identifier for the current thread or session |
| `state` | string | Yes | `planning`, `executing`, `blocked`, `waiting`, or `done` |
| `active_constraints` | array | No | Constraints on task execution |
| `next_actions` | array | No | Planned next steps |
| `open_questions` | array | No | Unresolved questions |
| `context_summary` | string | No | Human-readable summary of current context |

---

## Semantic Memory

**Type value:** `semantic`

Semantic memory stores stable, revisable facts as subject-predicate-object triples. It supports coexistence of multiple conditional truths and tracks revision history.

### Payload Structure

```json
{
  "kind": "semantic",
  "subject": "user",
  "predicate": "prefers_language",
  "object": "Python",
  "validity": {
    "mode": "global"
  },
  "evidence": [
    {
      "source_type": "observation",
      "source_id": "obs-001",
      "timestamp": "2025-01-10T09:00:00Z"
    }
  ],
  "revision_policy": "replace",
  "revision": {
    "status": "active"
  }
}
```

### Validity Modes

Semantic facts support three validity modes:

- **`global`** -- the fact is universally valid
- **`conditional`** -- valid only under specific conditions (stored in `conditions` map)
- **`timeboxed`** -- valid within a time window (`start` and `end` timestamps)

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `kind` | string | Yes | Always `"semantic"` |
| `subject` | string | Yes | The entity the fact is about |
| `predicate` | string | Yes | The relationship or property |
| `object` | any | Yes | The value (string, number, boolean, object, or array) |
| `validity` | object | Yes | When this fact is valid |
| `evidence` | array | No | Provenance references supporting this fact |
| `revision_policy` | string | No | `replace`, `fork`, or `contest` |
| `revision` | object | No | Revision state: `supersedes`, `superseded_by`, `status` |

---

## Competence Memory

**Type value:** `competence`

Competence memory encodes procedural knowledge -- how to achieve goals reliably under specific conditions. It represents "knowing how" rather than "knowing that."

### Payload Structure

```json
{
  "kind": "competence",
  "skill_name": "fix-npm-peer-dependency",
  "triggers": [
    {
      "signal": "npm ERR! ERESOLVE",
      "conditions": { "package_manager": "npm" }
    }
  ],
  "recipe": [
    {
      "step": "Run npm with legacy peer deps flag",
      "tool": "shell",
      "args_schema": { "command": "string" },
      "validation": "Exit code 0 and no ERESOLVE errors in output"
    },
    {
      "step": "Verify lock file was updated",
      "tool": "file_read",
      "validation": "package-lock.json modified timestamp changed"
    }
  ],
  "required_tools": ["shell", "file_read"],
  "failure_modes": ["Flag not supported in older npm versions"],
  "fallbacks": ["Delete node_modules and reinstall"],
  "performance": {
    "success_count": 12,
    "failure_count": 2,
    "success_rate": 0.857,
    "avg_latency_ms": 4500
  },
  "version": "1.2"
}
```

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `kind` | string | Yes | Always `"competence"` |
| `skill_name` | string | Yes | Name of the skill or procedure |
| `triggers` | array | Yes | Conditions that activate this competence |
| `recipe` | array | Yes | Ordered steps to execute |
| `required_tools` | array | No | Tools needed for execution |
| `failure_modes` | array | No | Known failure cases |
| `fallbacks` | array | No | Alternative strategies |
| `performance` | object | No | Success/failure statistics |
| `version` | string | No | Version identifier |

---

## Plan Graph Memory

**Type value:** `plan_graph`

Plan graph memory stores reusable solution structures as directed graphs of actions. Plans are versioned, selectable by constraint matching, and track execution metrics.

### Payload Structure

```json
{
  "kind": "plan_graph",
  "plan_id": "plan-setup-go-project",
  "version": "2.0",
  "intent": "setup_go_project",
  "constraints": {
    "min_go_version": "1.21"
  },
  "nodes": [
    {
      "id": "n1",
      "op": "shell",
      "params": { "command": "go mod init {{module}}" }
    },
    {
      "id": "n2",
      "op": "file_write",
      "params": { "path": "main.go", "template": "hello_world" }
    },
    {
      "id": "n3",
      "op": "shell",
      "params": { "command": "go build ./..." },
      "guards": { "files_exist": ["go.mod", "main.go"] }
    }
  ],
  "edges": [
    { "from": "n1", "to": "n2", "kind": "control" },
    { "from": "n2", "to": "n3", "kind": "control" },
    { "from": "n1", "to": "n3", "kind": "data" }
  ],
  "metrics": {
    "avg_latency_ms": 2300,
    "failure_rate": 0.05,
    "execution_count": 40
  }
}
```

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `kind` | string | Yes | Always `"plan_graph"` |
| `plan_id` | string | Yes | Unique plan identifier |
| `version` | string | Yes | Version identifier |
| `intent` | string | No | High-level intent label for matching |
| `constraints` | object | No | Trust requirements, tool requirements, etc. |
| `inputs_schema` | object | No | Expected input parameters |
| `outputs_schema` | object | No | Expected output format |
| `nodes` | array | Yes | Action nodes with `id`, `op`, `params`, and optional `guards` |
| `edges` | array | Yes | Dependency edges (`data` or `control`) connecting nodes |
| `metrics` | object | No | Execution statistics |
