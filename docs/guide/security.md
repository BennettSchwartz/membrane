# Security

Membrane implements trust-aware retrieval to ensure that sensitive memories are only accessible to authorized requesters.

## Trust Context

Every retrieval request must include a `TrustContext` with four fields:

| Field | Type | Description |
|-------|------|-------------|
| `max_sensitivity` | string | Maximum sensitivity level the requester can access |
| `authenticated` | bool | Whether the requester has been authenticated |
| `actor_id` | string | Identifier for the requesting actor |
| `scopes` | string[] | Scopes the requester is allowed to query |

::: warning
Retrieval requests without a trust context are rejected with an error. There is no anonymous access mode.
:::

## Sensitivity Levels

Every memory record has a `sensitivity` field set during ingestion. The five levels form a strict hierarchy:

| Level | Numeric | Description |
|-------|---------|-------------|
| `public` | 0 | Freely shareable content |
| `low` | 1 | Minimal sensitivity (default for ingested records) |
| `medium` | 2 | Moderately sensitive content |
| `high` | 3 | Highly sensitive, requires elevated trust |
| `hyper` | 4 | Maximum protection, strictest access |

During retrieval, a record is accessible only if its sensitivity level is **less than or equal to** the trust context's `max_sensitivity`.

### Example

A trust context with `max_sensitivity: "medium"` can access:
- `public` records (level 0)
- `low` records (level 1)
- `medium` records (level 2)

But **cannot** access:
- `high` records (level 3)
- `hyper` records (level 4)

## Scope-Based Access

Scopes provide a second dimension of access control. Common scope values include:

- `user` -- personal to a single user
- `device` -- specific to a device
- `project` -- shared within a project
- `workspace` -- shared within a workspace
- `global` -- available everywhere

### Scope Rules

1. If the trust context specifies **one or more scopes**, only records whose `scope` matches one of the allowed scopes are returned
2. **Unscoped records** (empty `scope` field) are always accessible, regardless of the trust context's scope list
3. If the trust context has an **empty scopes list**, all scopes are allowed

### Example

```json
{
  "trust": {
    "max_sensitivity": "high",
    "authenticated": true,
    "actor_id": "agent-1",
    "scopes": ["project-alpha", "global"]
  }
}
```

This trust context can access:
- Records with `scope: "project-alpha"`
- Records with `scope: "global"`
- Records with no scope (unscoped)

But **cannot** access:
- Records with `scope: "project-beta"`
- Records with `scope: "user-private"`

## Sensitivity at Ingestion

The sensitivity level is set when a memory is ingested. If not specified, the default sensitivity from the server configuration is used (default: `"low"`).

```json
{
  "source": "my-agent",
  "event_kind": "user_input",
  "ref": "session-001/msg-1",
  "sensitivity": "high"
}
```

::: tip
Set sensitivity at ingestion time based on the content. User credentials and secrets should be `hyper`. Personal preferences might be `medium`. Public documentation references can be `public`.
:::

## Audit Trail

All access and modifications to memory records are tracked in the audit log. Each audit entry records:

- **Action** -- what happened (`create`, `revise`, `fork`, `merge`, `delete`, `reinforce`, `decay`)
- **Actor** -- who or what performed the action
- **Timestamp** -- when it happened
- **Rationale** -- why it was done

This provides a full trace of who accessed or modified a memory, supporting compliance and debugging.
