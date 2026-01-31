# Core Concepts

Membrane is a selective learning and memory substrate for agentic AI systems. Rather than treating all data as undifferentiated blobs in a key-value store, Membrane models knowledge the way cognitive systems do -- with distinct memory types, time-based decay, provenance tracking, and principled revision. This page explains every core concept in depth.

## The Five Memory Types

Membrane organizes all knowledge into five distinct types, each with its own semantics, lifecycle behavior, and payload structure.

| Type | Analogy | Purpose | Typical Half-Life |
|------|---------|---------|-------------------|
| **Episodic** | "What happened" | Raw experience: user inputs, tool calls, errors, observations | Hours to days |
| **Working** | "What I'm doing now" | Current task state for resumption across sessions | Task duration |
| **Semantic** | "What I know" | Stable facts: preferences, environment, relationships | Days to weeks |
| **Competence** | "How I do things" | Procedural knowledge: skill recipes with triggers and steps | Long-lived |
| **Plan Graph** | "Reusable playbooks" | Directed action graphs that solve recurring problems | Long-lived |

### Episodic Memory

Episodic records capture raw experience as it happens. Every tool call, user interaction, observation, and error can be ingested as an episodic record. These records are intentionally short-lived -- they provide the raw material from which higher-level knowledge is extracted during consolidation.

Episodic records carry an **outcome status** (`success`, `failure`, or `partial`) that is used downstream by the consolidation engine to decide which patterns are worth promoting to competence records.

### Working Memory

Working memory captures the state of an active task: what the agent is doing, what questions are open, what constraints are active, and what the next actions should be. Working memory enables an agent to resume a task after interruption or across sessions.

Key fields in a working memory payload include:

- **thread_id** -- identifies the task or conversation thread.
- **state** -- one of `planning`, `executing`, `blocked`, `waiting`, or `done`.
- **next_actions** -- a list of planned next steps.
- **open_questions** -- unresolved questions that block progress.
- **context_summary** -- a natural-language summary of the current context.
- **active_constraints** -- constraints that must be respected.

Working memory records are typically discarded or archived when the task reaches the `done` state.

### Semantic Memory

Semantic records represent stable, revisable knowledge. Observations that recur across multiple episodic experiences get promoted (via consolidation) into semantic facts. Semantic memory supports a rich revision model (see [Revision](#revision-operations) below).

Semantic records include:

- **subject / predicate / object** triples for structured knowledge.
- **validity_mode** -- whether the fact is `global`, `conditional`, or `timeboxed`.
- **revision_status** -- whether the fact is `active`, `contested`, or `retracted`.
- **evidence** -- provenance references linking back to the episodic sources.

### Competence Memory

Competence records encode procedural knowledge: how to accomplish a goal reliably under specific conditions. They are structured as recipes with triggers, preconditions, steps, and expected outcomes.

Competence records track **performance statistics** (success and failure counts) which feed back into the selection process during retrieval. Records with higher success rates and confidence are preferred.

### Plan Graph Memory

Plan graphs represent reusable solution structures as directed graphs of actions. Each node is an action (with a tool name, arguments template, and expected output), and edges represent data or control dependencies.

Plan graphs are versioned and can be synthesized automatically by the consolidation engine from repeated sequences of episodic tool calls.

## Layered Retrieval Model

When an agent retrieves memories, Membrane queries each layer in a fixed priority order:

```
Working --> Semantic --> Competence --> Plan Graph --> Episodic
```

This ordering ensures that the most actionable and stable knowledge surfaces first:

```
+--------------------------------------------------+
|                   RETRIEVAL                       |
|                                                   |
|   1. Working    -- immediate task context         |
|   2. Semantic   -- known facts                    |
|   3. Competence -- how-to knowledge               |
|   4. Plan Graph -- reusable playbooks             |
|   5. Episodic   -- raw supporting experience      |
|                                                   |
|   Each layer is filtered by:                      |
|     - Trust context (sensitivity + scopes)        |
|     - Minimum salience threshold                  |
|     - Memory type filter (if specified)           |
|     - Selection confidence threshold              |
+--------------------------------------------------+
```

The **selector** component evaluates competence and plan graph candidates against a configurable confidence threshold (default: `0.7`). Candidates below this threshold are excluded from the selection set.

## Memory Record Anatomy

Every piece of knowledge in Membrane is stored as a `MemoryRecord`. This is the atomic unit of storage, and all five memory types share this common envelope.

```
MemoryRecord
+-- id                  string        Globally unique identifier (UUID)
+-- type                MemoryType    episodic | working | semantic | competence | plan_graph
+-- sensitivity         Sensitivity   public | low | medium | high | hyper
+-- confidence          float64       Epistemic confidence [0, 1]
+-- salience            float64       Decay-weighted importance [0, +inf)
+-- scope               string        Visibility scope (optional)
+-- tags                []string      Free-form labels for categorization
+-- created_at          time.Time     Creation timestamp
+-- updated_at          time.Time     Last modification timestamp
+-- lifecycle           Lifecycle     Decay profile, pinning, deletion policy
+-- provenance          Provenance    Source evidence chain
+-- relations           []Relation    Graph edges to other records
+-- payload             Payload       Type-specific structured content
+-- audit_log           []AuditEntry  Complete action history
```

### Required Fields

- **id**: Immutable once created. Use a UUID v4 for uniqueness.
- **type**: Determines which payload shape is expected and how the record participates in retrieval and consolidation.
- **sensitivity**: Controls access. Must be one of the five sensitivity levels.
- **confidence**: A value between 0 and 1 representing how certain the system is about this record. Newly ingested records default to `1.0`.
- **salience**: Starts at `1.0` and decays over time. Determines retrieval priority.
- **payload**: The type-specific content. Must not be nil.

### Optional Fields

- **scope**: An arbitrary string that partitions the memory space. Examples: `"user:alice"`, `"project:alpha"`, `"global"`. Records are only visible to retrieval requests whose trust context includes the matching scope.
- **tags**: Up to 100 free-form labels (each max 256 characters). Used for filtering during retrieval. Tag filtering requires a record to have ALL specified tags.
- **relations**: Directed edges to other records with a predicate (e.g., `"supersedes"`, `"derived_from"`, `"depends_on"`) and a weight.

## Lifecycle: Decay, Reinforcement, and Deletion

Every memory record has a `Lifecycle` that controls how its salience changes over time.

### Decay Profile

The decay profile defines the mathematical function used to reduce salience:

```
DecayProfile
+-- curve                DecayCurve      exponential | linear | custom
+-- half_life_seconds    int64           Time for salience to halve (min: 1)
+-- min_salience         float64         Floor value [0, 1] -- salience cannot drop below this
+-- max_age_seconds      int64           Maximum record age before deletion eligibility
+-- reinforcement_gain   float64         Salience boost on reinforcement
```

**Exponential decay** (the default) uses the formula:

```
salience(t) = salience_0 * 0.5 ^ (elapsed / half_life)
```

**Linear decay** decreases salience at a constant rate per unit time.

The decay scheduler runs at a configurable interval (default: every 1 hour) and applies the decay formula to all non-pinned records.

### Reinforcement

Decay is reversible. When a record is accessed, explicitly reinforced, or otherwise deemed important, its `last_reinforced_at` timestamp is reset and its salience is boosted by `reinforcement_gain`. This models the cognitive concept of memory strengthening through use.

### Pinning

Setting `pinned: true` on a record exempts it from decay entirely. Pinned records retain their salience indefinitely. This is useful for critical configuration facts, user preferences, or other knowledge that should never be forgotten.

### Deletion Policies

| Policy | Behavior |
|--------|----------|
| `auto_prune` | Record is automatically deleted when salience drops below the minimum threshold (default behavior). |
| `manual_only` | Record persists at any salience level; deletion requires explicit action. |
| `never` | Record is never deleted, regardless of salience or age. |

### Lifecycle State Transitions

```
                     +--- Reinforce ---> salience boosted, clock reset
                     |
  [Created] ---> [Active / Decaying] --+--- Decay tick ---> salience reduced
                     |                  |
                     |                  +--- Pin ---> [Pinned] (no decay)
                     |                  |
                     |                  +--- salience < floor ---> [At Floor]
                     |                  |
                     |                  +--- age > max_age ---> [Eligible for Deletion]
                     |                  |
                     |                  +--- salience ~= 0 + auto_prune ---> [Deleted]
                     |
                     +--- Retract ---> [Retracted] (kept but excluded from retrieval)
                     |
                     +--- Supersede ---> [Superseded] (replaced by new record)
```

## Provenance Tracking

Every memory record carries a `Provenance` object that links it back to its sources. This creates an evidence chain that enables auditability, trust assessment, and conflict resolution.

### Provenance Structure

```
Provenance
+-- sources         []ProvenanceSource    List of evidence sources
+-- created_by      string                What created this record (optional)

ProvenanceSource
+-- kind            ProvenanceKind        event | artifact | tool_call | observation | outcome
+-- ref             string                Opaque reference into the host system
+-- hash            string                Content hash for immutability verification (optional)
+-- created_by      string                Actor or system that created this source (optional)
+-- timestamp       time.Time             When this source was created or observed
```

### Provenance Kinds

| Kind | Description | Example Ref |
|------|-------------|-------------|
| `event` | An event from the host system | `"event:user-message:abc123"` |
| `artifact` | A log, file, or other artifact | `"artifact:log:/var/log/app.log:line42"` |
| `tool_call` | A tool invocation and its result | `"tool:web-search:req-789"` |
| `observation` | A structured observation | `"obs:env:python-version"` |
| `outcome` | A task outcome (success/failure) | `"outcome:task-456"` |

Provenance is critical for **semantic revision**: when a fact is contested or superseded, the provenance chain shows what evidence supported the original claim and what new evidence triggered the revision.

## Trust Model

Membrane enforces access control at retrieval time through a **trust context** that accompanies every query.

### Sensitivity Levels

Records are classified into five sensitivity levels, forming a strict hierarchy:

```
public < low < medium < high < hyper
```

A retrieval request specifies the maximum sensitivity level the requester is allowed to access. Records above that level are filtered out before results are returned.

| Level | Intended Use |
|-------|-------------|
| `public` | Non-sensitive information; safe to share broadly |
| `low` | Default for most ingested records |
| `medium` | Contains user-specific or project-specific details |
| `high` | Contains credentials, PII, or sensitive business logic |
| `hyper` | Maximum protection; reserved for the most sensitive data |

### Scopes

Scopes partition the memory space into isolated namespaces. A record's `scope` field (e.g., `"project:alpha"`) determines who can see it. The trust context's `scopes` list specifies which scopes the requester is authorized to query. A record with no scope is visible to all authenticated requests.

### Trust Context Fields

Every retrieval request must include a `TrustContext` with:

| Field | Type | Description |
|-------|------|-------------|
| `max_sensitivity` | `Sensitivity` | Highest sensitivity level the requester can access |
| `authenticated` | `bool` | Whether the requester has been authenticated |
| `actor_id` | `string` | Identifier for the requesting actor |
| `scopes` | `[]string` | List of scopes the requester is authorized to query |

If authentication is enabled on the server (via `api_key`), the gRPC interceptor validates the API key before the trust context is evaluated.

## The Five Subsystems

Membrane is composed of five cooperating subsystems, plus a metrics collector. Understanding how they interact is key to understanding the system as a whole.

```
                        +------------------+
                        |    gRPC API      |
                        +--------+---------+
                                 |
                        +--------v---------+
                        |     Membrane     |  (orchestrator)
                        +--------+---------+
                                 |
         +-----------+-----------+-----------+-----------+
         |           |           |           |           |
   +-----v---+ +----v----+ +---v-----+ +---v-----+ +--v--------+
   |Ingestion| |Retrieval| |  Decay  | |Revision | |Consolidation|
   +---------+ +---------+ +---------+ +---------+ +-------------+
         |           |           |           |           |
         +-----------+-----------+-----------+-----------+
                                 |
                        +--------v---------+
                        |   Storage (SQLite)|
                        +------------------+
```

### 1. Ingestion

The ingestion subsystem is the entry point for all new knowledge. It accepts raw data (events, tool outputs, observations, outcomes, working state) and transforms it into typed `MemoryRecord` objects. The ingestion pipeline includes:

- **Classifier**: Determines the memory type and assigns initial metadata.
- **Policy Engine**: Applies default sensitivity, decay profiles, and other policies based on configuration.

### 2. Retrieval

The retrieval subsystem implements the layered query model. It queries the storage layer for records matching the request criteria, applies trust context filtering, and uses the **selector** to rank competence and plan graph candidates by confidence.

### 3. Decay

The decay subsystem manages salience over time. It provides:

- A **scheduler** that runs on a configurable interval (default: 1 hour) and applies the decay formula to all eligible records.
- **Reinforce** and **Penalize** operations for manual salience adjustment.
- Automatic pruning of records that have decayed below threshold (when using `auto_prune` deletion policy).

### 4. Revision

The revision subsystem handles knowledge updates. It implements five atomic operations, all of which are transactional and audited:

- **Supersede**: Replace an old record with a corrected version. The old record gets a `supersedes` relation.
- **Fork**: Create a conditional variant of an existing record (e.g., "this is true on macOS but not on Linux").
- **Retract**: Mark a record as withdrawn without deleting it. Retracted records are preserved for auditability.
- **Merge**: Combine multiple related records into a single consolidated record.
- **Contest**: Mark a record as disputed when conflicting evidence appears. Contested records remain accessible but are flagged.

### 5. Consolidation

The consolidation subsystem runs on a configurable schedule (default: every 6 hours) and distills raw episodic experience into durable knowledge. It performs four operations:

1. **Episodic compression**: Reduces salience of old episodic records.
2. **Semantic extraction**: Promotes repeated observations into semantic facts.
3. **Competence extraction**: Identifies successful tool-use patterns and creates competence records.
4. **Plan graph synthesis**: Extracts reusable action graphs from episodic tool sequences.

### Metrics

The metrics collector gathers a point-in-time snapshot of substrate health: record counts by type, salience distributions, decay statistics, and operational counters. It is exposed via the `GetMetrics` RPC.

## Revision Operations

Revision is central to how Membrane handles the inherently uncertain and evolving nature of knowledge.

### Supersede

Replace a fact with a corrected version. The old record is preserved, and a `supersedes` relation links the new record to the old one.

```
[Old Record] <--supersedes-- [New Record]
```

### Fork

Create a conditional variant when a fact turns out to be context-dependent.

```
[Original] <--derived_from-- [Fork A: "true on macOS"]
            <--derived_from-- [Fork B: "true on Linux"]
```

### Retract

Withdraw a fact without deleting it. The record's revision status changes to `retracted`.

### Merge

Combine multiple records into one when they represent the same underlying knowledge.

```
[Record A] --+
[Record B] --+--> [Merged Record]
[Record C] --+
```

### Contest

Flag a record as disputed when conflicting evidence appears. The record's revision status changes to `contested`, and the contesting evidence reference is recorded.

All revision operations produce audit log entries with actor, timestamp, and rationale.

## Audit Log

Every action performed on a record is tracked in its audit log. Each entry contains:

| Field | Type | Description |
|-------|------|-------------|
| `action` | `AuditAction` | `create`, `revise`, `fork`, `merge`, `delete`, `reinforce`, `decay` |
| `actor` | `string` | Who or what performed the action |
| `timestamp` | `time.Time` | When the action occurred |
| `rationale` | `string` | Why the action was taken |

The audit log is append-only and provides a complete history of a record's evolution.

## Relations

Records can be linked to other records via directed edges called **relations**. Each relation has:

- **predicate**: The relationship type (e.g., `"supersedes"`, `"derived_from"`, `"depends_on"`, `"related_to"`).
- **target_id**: The ID of the target record.
- **weight**: A numeric weight for the edge (default: `1.0`).
- **created_at**: When the relation was established.

Relations form a graph structure that enables traversal-based queries and supports the revision model (supersede, fork, merge all create relations).

## Glossary

| Term | Definition |
|------|-----------|
| **Salience** | A numeric score representing a record's current importance. Decays over time and can be reinforced. |
| **Sensitivity** | An access-control classification (`public`, `low`, `medium`, `high`, `hyper`) that determines who can read a record. |
| **Confidence** | An epistemic certainty score in `[0, 1]` indicating how trustworthy a record is. |
| **Scope** | A namespace string that partitions the memory space for access control. |
| **Decay** | The automatic reduction of salience over time, modeling forgetting. |
| **Reinforcement** | The act of boosting a record's salience, modeling memory strengthening through use. |
| **Pinning** | Protecting a record from decay so its salience remains constant. |
| **Consolidation** | The process of distilling raw episodic experience into durable semantic, competence, or plan graph knowledge. |
| **Provenance** | The chain of evidence linking a record back to its original sources. |
| **Trust Context** | A set of parameters (sensitivity ceiling, authentication status, actor ID, scopes) that gates retrieval access. |
| **Revision** | Any operation (supersede, fork, retract, merge, contest) that modifies or annotates existing knowledge. |
| **Audit Log** | An append-only record of every action taken on a memory record. |
| **Half-Life** | The time (in seconds) for a record's salience to decay to half its value under exponential decay. |
| **Auto-Prune** | A deletion policy that automatically removes records when salience drops below the minimum threshold. |
| **Selector** | The retrieval component that ranks competence and plan graph candidates by confidence. |
| **Policy Engine** | The ingestion component that assigns default sensitivity, decay profiles, and metadata to new records. |
| **Classifier** | The ingestion component that determines memory type and initial metadata. |
| **Working State** | A snapshot of an ongoing task's context, stored as working memory. |
| **Plan Graph** | A directed graph of actions (nodes) connected by data or control edges, representing a reusable solution. |
| **Competence** | Procedural knowledge encoding how to achieve a goal, tracked with performance statistics. |
