---
outline: deep
---

# Revision Operations

Membrane treats knowledge as a living structure. Facts change, contexts shift, and agents learn from mistakes. The revision layer provides five atomic operations -- **Supersede**, **Fork**, **Retract**, **Merge**, and **Contest** -- that allow agents to evolve their memory safely, without losing the history of what was previously believed and why it changed.

## Why Revisable Knowledge Matters

Traditional databases overwrite values in place. For an agentic system, this is dangerous:

- An agent that silently overwrites a fact cannot explain *why* it changed its mind.
- A downstream agent relying on a retracted fact will never know it was wrong.
- Conflicting observations with no dispute mechanism lead to silent knowledge corruption.

Membrane solves this by making every change to semantic knowledge an explicit, audited revision operation. Old records are never physically deleted -- they are **retracted** (salience set to zero) so they fall out of retrieval results while remaining available for audit and forensic analysis.

::: tip Key Principle
Episodic memory is **append-only** and cannot be revised. Only semantic, competence, working, and plan-graph records support revision operations. Attempting to revise an episodic record returns `ErrEpisodicImmutable`.
:::

## Lifecycle States

Semantic records carry a `RevisionState` that tracks their position in the revision lifecycle. The three states form a simple state machine:

```
                  +-----------+
        create    |           |   supersede / retract
     ------------>|  active   |------------------------+
                  |           |                        |
                  +-----+-----+                        v
                        |                       +-----------+
                contest |                       |           |
                        +------>  contested     | retracted |
                        |       (needs review)  |           |
                        |                       +-----------+
                        |                              ^
                        +------------------------------+
                                resolve / retract
```

| Status | Value | Description |
|--------|-------|-------------|
| **Active** | `"active"` | The record is current and valid. It participates in retrieval normally. |
| **Contested** | `"contested"` | Conflicting evidence exists. The record is flagged for review but still retrievable. |
| **Retracted** | `"retracted"` | The record has been withdrawn. Its salience is set to `0`, effectively hiding it from retrieval. |

The `RevisionState` struct lives inside `SemanticPayload`:

```go
type RevisionState struct {
    Supersedes   string         `json:"supersedes,omitempty"`
    SupersededBy string         `json:"superseded_by,omitempty"`
    Status       RevisionStatus `json:"status,omitempty"`
}
```

The `Supersedes` and `SupersededBy` fields create a doubly-linked chain so you can walk the full revision history of any fact in either direction.

## Supersede

**Supersede** atomically replaces an existing record with a newer version. This is the primary mechanism for correcting or updating known facts.

### What Happens

1. The old record is **retracted**: salience set to `0`, revision status set to `"retracted"`, and `superseded_by` set to the new record's ID.
2. A new record is created with a `"supersedes"` relation pointing back to the old record.
3. The new record's provenance includes a source reference to the old record.
4. Audit entries are added to both old and new records.

All steps execute inside a single **transaction** -- partial revisions are never externally visible (RFC 15.7).

### Supersession Chains

When record B supersedes A, and later record C supersedes B, you get a chain:

```
A (retracted, superseded_by: B)
  -> B (retracted, superseded_by: C)
       -> C (active, supersedes: B)
```

You can walk this chain in either direction using the `supersedes` and `superseded_by` fields to reconstruct the full history of a fact.

### Go Example

```go
// Original fact: Go version is 1.21
original := ingestSemanticRecord("Go", "version", "1.21")

// Correction: Go version is now 1.22
newRec := schema.NewMemoryRecord("", schema.MemoryTypeSemantic, schema.SensitivityLow,
    &schema.SemanticPayload{
        Kind:      "semantic",
        Subject:   "Go",
        Predicate: "version",
        Object:    "1.22",
        Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
    },
)
// Semantic revisions require evidence
newRec.Provenance.Sources = append(newRec.Provenance.Sources, schema.ProvenanceSource{
    Kind: "observation",
    Ref:  "release-notes-1.22",
})

superseded, err := membrane.Supersede(ctx, original.ID, newRec, "updater-agent", "Go 1.22 released")
```

After this call:
- `original.Salience` is `0` and its status is `"retracted"`
- `superseded` is active with a `"supersedes"` relation to `original.ID`

::: warning Evidence Required
Semantic records created via supersession **must** include at least one evidence reference in their provenance sources or payload evidence list. The operation will fail if no evidence is provided.
:::

## Fork

**Fork** creates a conditional variant of an existing record. Unlike Supersede, **both the source and the forked record remain active**. This is useful when a fact is true in some contexts but not others.

### What Happens

1. The source record is verified as revisable (not episodic).
2. A new record is created with a `"derived_from"` relation pointing to the source.
3. Audit entries are added to both the source and the forked record.
4. The source record's salience is **not** changed -- both records are independently active.

### Use Cases

- **Environment-specific configuration**: "The database is PostgreSQL" is true in production, but in development it is SQLite.
- **Conditional preferences**: "The user prefers dark mode" except on mobile where they prefer light mode.
- **Temporal variants**: "The API endpoint is v2" but during migration some clients still use v1.

### Go Example

```go
source := ingestSemanticRecord("database", "type", "PostgreSQL")

forkedRec := schema.NewMemoryRecord("", schema.MemoryTypeSemantic, schema.SensitivityLow,
    &schema.SemanticPayload{
        Kind:      "semantic",
        Subject:   "database",
        Predicate: "type",
        Object:    "SQLite",
        Validity: schema.Validity{
            Mode:       schema.ValidityModeConditional,
            Conditions: map[string]any{"env": "development"},
        },
    },
)
forkedRec.Provenance.Sources = append(forkedRec.Provenance.Sources, schema.ProvenanceSource{
    Kind: "observation",
    Ref:  "dev-environment-config",
})

forked, err := membrane.Fork(ctx, source.ID, forkedRec, "config-agent", "conditional variant for dev")
```

After this call:
- `source` remains active with its original salience
- `forked` is active with a `"derived_from"` relation to `source.ID`

## Retract

**Retract** performs a soft-delete: the record is marked as withdrawn but not physically removed. This preserves the audit trail while ensuring the fact no longer influences agent behavior.

### What Happens

1. The record's **salience** is set to `0`.
2. For semantic records, the revision status is set to `"retracted"`.
3. An audit entry with action `"delete"` is recorded with the provided rationale.

### Go Example

```go
rec := ingestSemanticRecord("fact", "is", "true")

err := membrane.Retract(ctx, rec.ID, "cleanup-agent", "fact was determined to be incorrect")
```

After this call:
- `rec.Salience` is `0`
- `rec` will not appear in retrieval results (unless the caller explicitly queries for retracted records)
- The audit log records who retracted it and why

::: tip Soft Delete, Not Hard Delete
Retracted records are never physically deleted. They remain in storage for audit purposes. The zero salience ensures they are excluded from normal retrieval, but they can always be looked up by ID.
:::

## Merge

**Merge** combines multiple source records into a single consolidated record. All source records are retracted, and the merged record links back to each source via `"derived_from"` relations.

### What Happens

1. All source records are verified as revisable.
2. All source records are **retracted** (salience set to `0`, semantic status set to `"retracted"`).
3. A new merged record is created with `"derived_from"` relations to every source.
4. Audit entries with action `"merge"` are added to each source record.
5. A `"create"` audit entry is added to the merged record listing all source IDs.

### Use Cases

- **Deduplication**: Multiple observations describe the same fact with slight variations.
- **Consolidation**: Several partial observations are combined into one authoritative record.
- **Conflict resolution**: Two contested records are resolved by creating a single merged truth.

### Go Example

```go
rec1 := ingestSemanticRecord("tool", "uses", "vim")
rec2 := ingestSemanticRecord("tool", "uses", "neovim")
rec3 := ingestSemanticRecord("tool", "uses", "editor")

mergedRec := schema.NewMemoryRecord("", schema.MemoryTypeSemantic, schema.SensitivityLow,
    &schema.SemanticPayload{
        Kind:      "semantic",
        Subject:   "tool",
        Predicate: "uses",
        Object:    "neovim-based editor",
        Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
    },
)
mergedRec.Provenance.Sources = append(mergedRec.Provenance.Sources, schema.ProvenanceSource{
    Kind: "observation",
    Ref:  "consolidated-editor-preference",
})

merged, err := membrane.Merge(ctx,
    []string{rec1.ID, rec2.ID, rec3.ID},
    mergedRec,
    "consolidation-agent",
    "consolidating editor preferences",
)
```

After this call:
- `rec1`, `rec2`, and `rec3` all have salience `0`
- `merged` has three `"derived_from"` relations, one for each source

::: warning Minimum Sources
At least one source record ID must be provided. Calling `Merge` with an empty ID list returns an error.
:::

## Contest

**Contest** flags a record as disputed. This does not retract the record -- it marks it with status `"contested"` so that agents and reviewers know there is conflicting evidence.

### What Happens

1. The record is verified as revisable.
2. The semantic revision status is set to `"contested"`.
3. If a `contestingRef` is provided, a `"contested_by"` relation is added pointing to the conflicting record or evidence.
4. An audit entry with action `"revise"` is recorded.

### Contest Workflow

A typical contest lifecycle looks like this:

1. **Flag**: An agent or user calls `Contest` to mark a fact as disputed.
2. **Review**: A human or higher-authority agent examines the conflicting evidence.
3. **Resolve**: The reviewer either:
   - **Supersedes** the contested record with a corrected version, or
   - **Retracts** the contested record if it is simply wrong, or
   - Clears the contested status if the original fact is confirmed correct.

### Go Example

```go
// An agent discovers conflicting evidence about a fact
err := membrane.Contest(ctx,
    recordID,
    conflictingRecordID,   // optional: the record that contradicts this one
    "verification-agent",
    "conflicting observation found: user indicated different preference",
)
```

After this call:
- The record's revision status is `"contested"`
- A `"contested_by"` relation links to the conflicting record
- The record is still retrievable, but agents can check the status to weigh it appropriately

## Audit Trail

Every revision operation writes to an append-only audit log attached to each record. This provides full traceability for any change in the knowledge base.

### Audit Entry Structure

Each entry in the audit log contains four required fields (RFC 15A.8):

```go
type AuditEntry struct {
    Action    AuditAction `json:"action"`
    Actor     string      `json:"actor"`
    Timestamp time.Time   `json:"timestamp"`
    Rationale string      `json:"rationale"`
}
```

| Field | Description | Example |
|-------|-------------|---------|
| `action` | The type of operation performed | `"create"`, `"revise"`, `"fork"`, `"merge"`, `"delete"` |
| `actor` | Who or what performed the action | `"consolidation-agent"`, `"user:alice"` |
| `timestamp` | When the action occurred (UTC) | `"2025-03-15T10:30:00Z"` |
| `rationale` | Why the action was taken | `"superseded by rec-456: Go 1.22 released"` |

### Audit Actions

| Action | Constant | Recorded When |
|--------|----------|---------------|
| **create** | `AuditActionCreate` | A new record is created (including via supersede, fork, or merge) |
| **revise** | `AuditActionRevise` | A record is superseded or contested |
| **fork** | `AuditActionFork` | A record is forked (logged on the source record) |
| **merge** | `AuditActionMerge` | A record is merged into a new record (logged on each source) |
| **delete** | `AuditActionDelete` | A record is retracted |
| **reinforce** | `AuditActionReinforce` | A record's salience is boosted |
| **decay** | `AuditActionDecay` | A record's salience is decreased by the decay scheduler |

### Reading the Audit Log

Every `MemoryRecord` carries an `AuditLog` field (a slice of `AuditEntry`). When you retrieve a record by ID, the full audit history is included:

```go
rec, err := membrane.RetrieveByID(ctx, recordID, trust)
if err != nil {
    log.Fatal(err)
}

for _, entry := range rec.AuditLog {
    fmt.Printf("[%s] %s by %s: %s\n",
        entry.Timestamp.Format(time.RFC3339),
        entry.Action,
        entry.Actor,
        entry.Rationale,
    )
}
```

Example output:

```
[2025-03-15T08:00:00Z] create by ingestion-agent: initial observation
[2025-03-15T10:30:00Z] revise by updater-agent: superseded by rec-456: version updated
```

## gRPC API Reference

All revision operations are exposed via the Membrane gRPC service. Records are passed as JSON-encoded bytes.

### Supersede

```protobuf
message SupersedeRequest {
    string old_id = 1;
    bytes  new_record = 2;   // JSON-encoded MemoryRecord
    string actor = 3;
    string rationale = 4;
}
// Returns: MemoryRecordResponse { bytes record = 1; }
```

**Example request** (using `grpcurl`):

```bash
grpcurl -plaintext -d '{
  "old_id": "rec-123",
  "new_record": "{\"type\":\"semantic\",\"sensitivity\":\"low\",\"payload\":{\"kind\":\"semantic\",\"subject\":\"Go\",\"predicate\":\"version\",\"object\":\"1.22\",\"validity\":{\"mode\":\"global\"},\"evidence\":[{\"kind\":\"observation\",\"ref\":\"release-notes\"}]}}",
  "actor": "updater-agent",
  "rationale": "Go 1.22 released"
}' localhost:9090 membrane.v1.MembraneService/Supersede
```

### Fork

```protobuf
message ForkRequest {
    string source_id = 1;
    bytes  forked_record = 2;  // JSON-encoded MemoryRecord
    string actor = 3;
    string rationale = 4;
}
// Returns: MemoryRecordResponse { bytes record = 1; }
```

**Example request**:

```bash
grpcurl -plaintext -d '{
  "source_id": "rec-456",
  "forked_record": "{\"type\":\"semantic\",\"sensitivity\":\"low\",\"payload\":{\"kind\":\"semantic\",\"subject\":\"database\",\"predicate\":\"type\",\"object\":\"SQLite\",\"validity\":{\"mode\":\"conditional\",\"conditions\":{\"env\":\"development\"}}}}",
  "actor": "config-agent",
  "rationale": "conditional variant for dev environment"
}' localhost:9090 membrane.v1.MembraneService/Fork
```

### Retract

```protobuf
message RetractRequest {
    string id = 1;
    string actor = 2;
    string rationale = 3;
}
// Returns: RetractResponse {}
```

**Example request**:

```bash
grpcurl -plaintext -d '{
  "id": "rec-789",
  "actor": "cleanup-agent",
  "rationale": "fact was determined to be incorrect"
}' localhost:9090 membrane.v1.MembraneService/Retract
```

### Merge

```protobuf
message MergeRequest {
    repeated string ids = 1;
    bytes  merged_record = 2;  // JSON-encoded MemoryRecord
    string actor = 3;
    string rationale = 4;
}
// Returns: MemoryRecordResponse { bytes record = 1; }
```

**Example request**:

```bash
grpcurl -plaintext -d '{
  "ids": ["rec-100", "rec-101", "rec-102"],
  "merged_record": "{\"type\":\"semantic\",\"sensitivity\":\"low\",\"payload\":{\"kind\":\"semantic\",\"subject\":\"tool\",\"predicate\":\"uses\",\"object\":\"neovim\",\"validity\":{\"mode\":\"global\"}}}",
  "actor": "consolidation-agent",
  "rationale": "deduplicating editor preferences"
}' localhost:9090 membrane.v1.MembraneService/Merge
```

### Contest

```protobuf
message ContestRequest {
    string id = 1;
    string contesting_ref = 2;
    string actor = 3;
    string rationale = 4;
}
// Returns: ContestResponse {}
```

**Example request**:

```bash
grpcurl -plaintext -d '{
  "id": "rec-200",
  "contesting_ref": "rec-201",
  "actor": "verification-agent",
  "rationale": "conflicting user preference observed"
}' localhost:9090 membrane.v1.MembraneService/Contest
```

## Best Practices

### Choosing the Right Operation

| Scenario | Operation | Why |
|----------|-----------|-----|
| A fact has changed (e.g., version update) | **Supersede** | The old fact is no longer true; replace it |
| A fact is true only in certain contexts | **Fork** | Both variants are valid; keep both active |
| A fact was wrong from the start | **Retract** | Remove it from retrieval without a replacement |
| Multiple records say the same thing | **Merge** | Consolidate into one authoritative record |
| Evidence conflicts with an existing fact | **Contest** | Flag for review before deciding |

### Always Provide Evidence

Semantic revisions require at least one evidence reference. This is not just a validation rule -- it is fundamental to maintaining trust in the knowledge base. When superseding or forking, always include provenance sources that justify the change:

```go
newRec.Provenance.Sources = append(newRec.Provenance.Sources, schema.ProvenanceSource{
    Kind: "observation",
    Ref:  "source-of-evidence",
})
```

### Always Provide Meaningful Rationale

Every revision operation requires an `actor` and `rationale` string. These are recorded in the audit log and are essential for debugging and accountability. Write rationale strings that answer **why** the change was made, not just **what** changed:

```go
// Good rationale
"Go 1.22 released on 2024-02-06, updating version fact"

// Poor rationale
"updated"
```

### Prefer Supersede Over Retract-Then-Create

If you need to replace a fact, use `Supersede` rather than calling `Retract` followed by creating a new record. Supersede is atomic and creates the proper `supersedes`/`superseded_by` links automatically.

### Use Contest Before Retract When Uncertain

If conflicting evidence appears but you are not yet sure which version is correct, use `Contest` to flag the record rather than immediately retracting it. This preserves the record's availability while signaling that it needs review.

### Merge During Consolidation

The `Merge` operation is particularly useful during consolidation cycles. When the consolidation engine detects multiple episodic observations describing the same fact, it can merge them into a single semantic record with full provenance tracing back to all original observations.

### Transaction Safety

All revision operations are wrapped in storage transactions. You never need to worry about a partially applied revision leaving the knowledge base in an inconsistent state. If any step within a revision fails, the entire operation is rolled back.

::: info RFC Compliance
The revision layer implements RFC Section 5 (episodic immutability), RFC 15.7 (atomic revisions), and RFC 15A.8 (audit traceability). Every design decision traces back to the specification.
:::
