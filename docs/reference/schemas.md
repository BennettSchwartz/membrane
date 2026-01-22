# Schemas

This page documents all data structures used in Membrane's memory substrate.

## MemoryRecord

The atomic unit of storage. Every stored memory item conforms to this shape.

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "type": "semantic",
  "sensitivity": "low",
  "confidence": 0.95,
  "salience": 0.8,
  "scope": "project-alpha",
  "tags": ["preference", "editor"],
  "created_at": "2025-01-15T10:00:00Z",
  "updated_at": "2025-01-15T10:00:00Z",
  "lifecycle": { /* Lifecycle */ },
  "provenance": { /* Provenance */ },
  "relations": [ /* Relation[] */ ],
  "payload": { /* one of five payload types */ },
  "audit_log": [ /* AuditEntry[] */ ]
}
```

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | Yes | Globally unique identifier (UUID). Immutable once created. |
| `type` | string | Yes | Memory type: `episodic`, `working`, `semantic`, `competence`, or `plan_graph` |
| `sensitivity` | string | Yes | Access classification: `public`, `low`, `medium`, `high`, or `hyper` |
| `confidence` | float | Yes | Epistemic confidence in range [0, 1] |
| `salience` | float | Yes | Decay-weighted importance score, range [0, +inf) |
| `scope` | string | No | Visibility scope (e.g., `user`, `project`, `global`) |
| `tags` | string[] | No | Free-form labels for categorization |
| `created_at` | datetime | Yes | Creation timestamp (RFC 3339) |
| `updated_at` | datetime | Yes | Last update timestamp (RFC 3339) |
| `lifecycle` | Lifecycle | Yes | Decay, reinforcement, and deletion metadata |
| `provenance` | Provenance | Yes | Links to source events or artifacts |
| `relations` | Relation[] | No | Graph edges to other MemoryRecords |
| `payload` | Payload | Yes | Type-specific structured content |
| `audit_log` | AuditEntry[] | Yes | Chronological action log |

---

## Lifecycle

Controls decay, reinforcement, and deletion behavior.

```json
{
  "decay": {
    "curve": "exponential",
    "half_life_seconds": 86400,
    "min_salience": 0.01,
    "max_age_seconds": 2592000,
    "reinforcement_gain": 0.2
  },
  "last_reinforced_at": "2025-01-15T10:00:00Z",
  "pinned": false,
  "deletion_policy": "auto_prune"
}
```

### Lifecycle Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `decay` | DecayProfile | Yes | Decay configuration |
| `last_reinforced_at` | datetime | Yes | When salience was last reinforced |
| `pinned` | bool | No | If true, salience does not decay |
| `deletion_policy` | string | No | `auto_prune`, `manual_only`, or `never` |

### DecayProfile Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `curve` | string | Yes | `exponential`, `linear`, or `custom` |
| `half_life_seconds` | int | Yes | Time for salience to halve (minimum: 1) |
| `min_salience` | float | No | Floor value; salience will not decay below this |
| `max_age_seconds` | int | No | Maximum age before eligible for deletion |
| `reinforcement_gain` | float | No | Salience boost on reinforcement |

---

## Provenance

Links a record to its source events or artifacts.

```json
{
  "sources": [
    {
      "kind": "event",
      "ref": "session-001/msg-1",
      "hash": "sha256:abc123...",
      "created_by": "agent-1",
      "timestamp": "2025-01-15T10:00:00Z"
    }
  ],
  "created_by": "ingestion-classifier-v1"
}
```

### Provenance Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `sources` | ProvenanceSource[] | Yes | At least one source |
| `created_by` | string | No | Creator identifier (e.g., classifier version) |

### ProvenanceSource Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `kind` | string | Yes | `event`, `artifact`, `tool_call`, `observation`, or `outcome` |
| `ref` | string | Yes | Opaque reference into the host system |
| `hash` | string | No | Content hash for immutability verification |
| `created_by` | string | No | Actor that created this source |
| `timestamp` | datetime | No | When the source was created or observed |

### ProvenanceRef

Used by semantic payloads to reference evidence:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source_type` | string | Yes | `event`, `tool`, `observation`, or `human` |
| `source_id` | string | Yes | Unique source identifier |
| `timestamp` | datetime | Yes | When the evidence was created |

---

## Relation

A graph edge connecting two MemoryRecords.

```json
{
  "predicate": "supports",
  "target_id": "660e8400-e29b-41d4-a716-446655440001",
  "weight": 0.9,
  "created_at": "2025-01-15T10:00:00Z"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `predicate` | string | Yes | Relationship type: `supports`, `contradicts`, `derived_from`, `supersedes`, etc. |
| `target_id` | string | Yes | UUID of the related record |
| `weight` | float | No | Relationship strength in range [0, 1] |
| `created_at` | datetime | No | When the relation was established |

---

## AuditEntry

A single entry in a record's audit log.

```json
{
  "action": "revise",
  "actor": "agent-1",
  "timestamp": "2025-01-15T12:00:00Z",
  "rationale": "Updated preferred language based on new evidence"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `action` | string | Yes | `create`, `revise`, `fork`, `merge`, `delete`, `reinforce`, or `decay` |
| `actor` | string | Yes | Who or what performed the action |
| `timestamp` | datetime | Yes | When the action occurred |
| `rationale` | string | Yes | Why the action was taken |

---

## Payload Types

Each memory type has a dedicated payload schema. See [Memory Types](/guide/memory-types) for detailed field descriptions and examples.

| Payload | `kind` value | Key Fields |
|---------|-------------|------------|
| EpisodicPayload | `episodic` | `timeline`, `tool_graph`, `environment`, `outcome` |
| WorkingPayload | `working` | `thread_id`, `state`, `active_constraints`, `next_actions` |
| SemanticPayload | `semantic` | `subject`, `predicate`, `object`, `validity`, `revision` |
| CompetencePayload | `competence` | `skill_name`, `triggers`, `recipe`, `performance` |
| PlanGraphPayload | `plan_graph` | `plan_id`, `version`, `nodes`, `edges`, `metrics` |

---

## Enums Reference

### MemoryType

`episodic` | `working` | `semantic` | `competence` | `plan_graph`

### Sensitivity

`public` | `low` | `medium` | `high` | `hyper`

### DecayCurve

`exponential` | `linear` | `custom`

### DeletionPolicy

`auto_prune` | `manual_only` | `never`

### RevisionStatus

`active` | `contested` | `retracted`

### ValidityMode

`global` | `conditional` | `timeboxed`

### TaskState

`planning` | `executing` | `blocked` | `waiting` | `done`

### OutcomeStatus

`success` | `failure` | `partial`

### AuditAction

`create` | `revise` | `fork` | `merge` | `delete` | `reinforce` | `decay`

### ProvenanceKind

`event` | `artifact` | `tool_call` | `observation` | `outcome`

### EdgeKind

`data` | `control`
