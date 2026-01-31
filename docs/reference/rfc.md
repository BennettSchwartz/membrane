# A General-Purpose Selective Learning & Memory Substrate for Agentic Systems

## Status

Draft

## Abstract

This document specifies a general-purpose, open-source learning and memory substrate for agentic systems. The system defined here enables agents to automatically learn from experience, remember procedures and preferences, revise or discard outdated knowledge, and reuse successful solution strategies. Unlike traditional retrieval-augmented generation (RAG) approaches that treat memory as an append-only text store, this specification defines typed memory classes, explicit lifecycle rules, evidence-based revisability, and structured representations of procedures and plans. The goal is to allow an agent to improve over time in a Jarvis-like manner—learning how to solve problems more effectively—while remaining predictable, auditable, and safe.

## 1. Motivation and Problem Statement

Most existing agent memory systems conflate learning with storage. They record past conversations or notes and retrieve them later via similarity search. This approach fails for advanced agents because it does not distinguish between different kinds of knowledge. A transient observation, a long-term preference, a learned troubleshooting procedure, and a reusable orchestration plan are all treated as the same kind of object. As a result, agents either forget useful skills or accumulate unstructured memory that becomes contradictory, stale, or misleading.

Additionally, most systems lack revisability. Once a memory is written, it is rarely modified or invalidated. This causes agents to repeat outdated behaviors, cling to incorrect assumptions, or fail to adapt when environments change. Finally, current systems do not store how a solution was reached, forcing agents to rediscover multi-step tool usage repeatedly.

This RFC addresses these failures by defining memory as a structured, typed, and revisable system with automatic learning, decay, and consolidation.

## 2. Design Goals

The system is designed around several non-negotiable goals. Learning must be automatic: the agent should not require explicit user approval to record experience or derive new knowledge. Memory must be selective and typed: different kinds of information must have different representations and lifecycles. Long-term memory must be revisable: facts, preferences, and procedures must be editable, replaceable, or retractable when new evidence appears. The system must support competence learning, meaning it learns procedures and heuristics, not just facts. Finally, all memory access must be auditable and compatible with trust- and sensitivity-based retrieval.

## 3. Conceptual Model of Memory

Memory in this specification is not a single store but a layered system. Each layer answers a different question.

Episodic memory answers: “What happened?” It captures raw experience.

Working memory answers: “What are we doing right now?” It supports task continuity.

Semantic memory answers: “What is true about the world, the system, or preferences?” It represents stable but revisable knowledge.

Competence memory answers: “How do I do this effectively?” It encodes procedures and troubleshooting knowledge.

Plan memory answers: “What sequence of actions solved this problem?” It stores reusable solution graphs.

Each layer has explicit rules for creation, update, decay, and deletion.

## 4. Memory Record Structure

All memory is stored as structured records. A record includes a unique identifier, a declared memory type, a sensitivity classification, confidence and salience scores, lifecycle metadata, and provenance links. Provenance links connect memory to the events, tool outputs, or observations that justify it.

Records are schema-driven. For example, a semantic preference record stores a subject, predicate, object, and evidence. A competence record stores triggers, steps, and failure modes. This structure allows targeted revision. Editing a preference does not require rewriting unrelated content, and contradictory facts can be compared explicitly.

**Normative requirement:** Conforming implementations MUST implement the canonical schemas defined in Section 15A (Schema Appendix) or provide a bijective, lossless mapping from their internal representation to those schemas.

All memory is stored as structured records. A record includes a unique identifier, a declared memory type, a sensitivity classification, confidence and salience scores, lifecycle metadata, and provenance links. Provenance links connect memory to the events, tool outputs, or observations that justify it.

Records are schema-driven. For example, a semantic preference record stores a subject, predicate, object, and evidence. A competence record stores triggers, steps, and failure modes. This structure allows targeted revision. Editing a preference does not require rewriting unrelated content, and contradictory facts can be compared explicitly.

## 5. Episodic Memory

Episodic memory captures raw experience and serves as evidentiary substrate for all higher-order learning. It MUST be high-fidelity, append-only, and aggressively decayed.

**Normative shape:** An episodic record MUST reference a time-ordered sequence of events (e.g., user inputs, tool calls, observations, errors) and MAY reference external artifacts such as logs, screenshots, or files. Episodic records MUST NOT be edited for semantic correctness; corrections occur only through higher-level memory.

**Example (troubleshooting):** When an agent encounters a build failure, episodic memory records the compiler error output, the commands executed, the environment details (OS, tool versions), and the eventual outcome. If the agent later learns a fix, that fix is *not* written into episodic memory; instead, episodic memory provides the evidence from which a competence record is derived.

Episodic memory records raw experience such as user inputs, tool calls, errors, and observations. For example, when an agent troubleshoots a build failure, episodic memory records the commands run, error messages observed, and fixes attempted.

Episodic memory is intentionally short-lived. Its purpose is to provide evidence for later learning, not to persist indefinitely. If a troubleshooting attempt repeatedly succeeds, consolidation may extract a competence record from multiple episodic traces. If nothing useful is learned, the episodic records decay and are removed.

## 6. Working Memory

Working memory captures the current state of an ongoing task. For example, if an agent is setting up a project, working memory may store that the backend is already initialized, the frontend choice is pending, and documentation remains to be written.

Working memory enables resumption across sessions or devices. It is updated frequently and discarded once the task concludes or becomes irrelevant.

## 7. Semantic Memory and Revisability

Semantic memory stores relatively stable facts, preferences, and relationships, but MUST support revision, replacement, conditionality, and deletion.

**Normative shape:** A semantic record MUST encode a subject–predicate–object triple (or equivalent structured form), evidence references, confidence, and validity conditions. Semantic records MUST be editable only through evidence-based updates.

**Revisability rules:** When new evidence conflicts with an existing semantic record, an implementation MUST choose one of the following actions: replacement (supersede the old record), conditional fork (maintain multiple records with disjoint applicability conditions), or contestation (mark uncertainty until resolved).

**Example (preferences):** An agent may store that a user prefers Go for backend services. Later evidence may show that for data-science projects, the user prefers Python. Both records may coexist with conditional applicability rather than one deleting the other.

Semantic memory stores stable knowledge such as preferences, environment facts, and relationships. For example, an agent may learn that a user prefers Go for backend services and a particular frontend framework for UI work.

Revisability is central. If later evidence suggests a change—for example, the user adopts a different frontend stack—the existing semantic record is not blindly overwritten. Instead, the system evaluates evidence, updates confidence, and either replaces the record, forks it into conditional variants, or marks it as contested.

Multiple entries for the same concept are allowed when context differs. For example, “project setup” may map to different stacks depending on project type. Semantic records therefore support conditional validity rather than a single global truth.

## 8. Competence Memory (Procedures and Troubleshooting)

Competence memory encodes procedural knowledge: how to achieve goals reliably under specific conditions.

**Normative shape:** A competence record MUST include trigger conditions, ordered or conditional steps, required tools, known failure modes, and fallback strategies. Competence records MUST be versionable and confidence-scored.

**Example (troubleshooting):** After repeatedly resolving a linker error by clearing a cache and rebuilding with specific flags, the system extracts a competence record describing that procedure. If an alternative fix later proves superior under different conditions, a second competence record is added rather than overwriting the first.

Competence memory stores learned procedures. A competence record represents a skill that reliably achieves a goal under certain conditions.

For example, after repeatedly fixing a specific build error, the system may extract a competence record describing how to diagnose and resolve that error. The record includes trigger conditions (error signature), procedural steps (commands to run), known failure modes, and fallback strategies.

Multiple competence records may exist for the same problem. Selection depends on context, such as operating system, tool versions, or constraints. Competence confidence is updated based on outcomes, allowing the system to prefer the most reliable procedure over time.

## 9. Plan Memory and Solution Graphs

Plan memory stores reusable solution structures as directed graphs of actions.

**Normative shape:** Plan graphs MUST represent actions as nodes and dependencies as edges. Plans MUST be versioned and MAY include conditional branches.

**Example (project setup):** A command such as "set up my project" may resolve to different plan graphs depending on semantic memory (preferred languages, frameworks) and context (target platform). Multiple plan graphs may exist for the same high-level intent, with selection driven by constraints and past success metrics.

Plan memory captures how complex solutions were achieved. Plans are represented as graphs where nodes correspond to actions or tool invocations and edges represent dependencies.

For example, “set up my project” may resolve to a plan graph that initializes a repository, configures a backend, sets up a frontend, and links CI tooling. The exact plan chosen depends on semantic preferences (language, frameworks) and environmental constraints.

Plans are versioned and revisable. If a plan becomes outdated or inefficient, a new version can supersede it without deleting historical data.

## 10. Automatic Learning and Consolidation

Learning occurs through consolidation. Periodically, the system analyzes episodic and working memory to extract durable knowledge. Consolidation may produce semantic updates, competence records, or plan graphs.

Consolidation is automatic. However, permanence is not guaranteed. All promoted knowledge remains subject to decay and revision.

## 11. Decay, Reinforcement, and Deletion

Each memory record has a salience score that decays over time. Salience increases when the record is successfully used and decreases when it is unused or leads to failure.

Deletion is a first-class operation. Records whose salience drops below a threshold may be removed. Deletion does not imply ignorance; it reflects that the knowledge is no longer useful or reliable.

## 12. Handling Conflicts and Multiple Solutions

The system explicitly supports multiple solutions to the same problem. Conflicts are resolved by context, evidence, and performance metrics rather than forced unification.

For example, two troubleshooting procedures may coexist, each tagged with applicable conditions. Retrieval selects the appropriate one based on current context.

## 13. Security and Trust-Aware Retrieval

Memory retrieval is conditioned on trust context. Sensitive records may be summarized or withheld depending on authentication state and environment. Trust gating affects retrieval only; it does not alter stored knowledge.

## 14. Evaluation and Metrics

The system exposes metrics such as retrieval usefulness, competence success rate, plan reuse frequency, contradiction rate, and memory decay behavior. These metrics allow operators to assess whether learning improves performance or degrades it.

## 15. Reference Implementation Architecture

This section defines a concrete, normative reference architecture for implementing the learning and memory substrate described in this RFC. The intent is not to mandate a single technology stack, but to constrain implementations such that the semantic guarantees of selective memory, revisability, decay, and auditability are preserved.

### 15.0 Storage Philosophy (Normative)

This specification explicitly distinguishes between *storage engines* and the *memory system*. Implementers MUST NOT attempt to encode learning semantics directly into a database alone, nor SHOULD they invent a custom database engine. Instead, learning semantics MUST be implemented in application logic layered atop proven storage systems.

The memory system defined in this RFC relies on strong invariants, atomic revision operations, and auditable state transitions. These requirements are most naturally satisfied by relational storage for authoritative metadata, supplemented by optional secondary stores for payloads and retrieval acceleration.

### 15.1 Authoritative Store (Source of Truth)

A conforming implementation MUST designate a single authoritative store for MemoryRecord metadata, lifecycle state, revision chains, salience, confidence, relations, and audit history.

A relational database (e.g., PostgreSQL or SQLite) is RECOMMENDED for this role.

The authoritative store MUST provide:

* Atomic transactions for revision operations (supersede, fork, retract)
* Enforceable invariants (e.g., uniqueness of active semantic facts per condition set)
* Durable audit trails
* Deterministic deletion and decay behavior

Implementations MUST NOT rely on document databases as the sole authoritative store, as such systems cannot reliably enforce the required invariants.

### 15.2 Payload Stores (Non-Authoritative)

Structured payloads (e.g., episodic timelines, competence recipes, plan graphs) MAY be stored outside the authoritative store to improve flexibility or performance.

Document-oriented databases (e.g., MongoDB) MAY be used as payload stores, provided that:

* They are not treated as the source of truth
* All payload records are referenced by immutable identifiers stored in the authoritative store
* Loss or corruption of payload data does not invalidate revision correctness

Payload stores MUST be considered replaceable caches from the perspective of the memory system.

### 15.3 Vector Indexes (Acceleration Only)

Vector similarity indexes MAY be used to accelerate retrieval. Such indexes MUST be treated as advisory and MUST NOT contain authoritative memory state.

Deletion, revision, or decay of memory records MUST be driven exclusively by the authoritative store, with vector indexes updated asynchronously.

### 15.4 Graph Representation

Relationships between MemoryRecords (e.g., derived_from, supersedes, contradicts) MUST be represented in the authoritative store. Graph databases MAY be introduced as secondary traversal accelerators but MUST NOT be the sole representation of relational state.

### 15.5 Process Model

The learning substrate SHALL run as a long-lived service or library embedded within an agent runtime. It MUST expose synchronous ingestion APIs and asynchronous consolidation jobs. Consolidation and decay processing SHOULD be performed periodically and MUST be interruptible.

The system is logically divided into three planes:

The ingestion plane receives events, tool outputs, and observations and converts them into candidate memory records.

The policy plane classifies candidates, assigns lifecycle metadata, and determines storage placement.

The storage and retrieval plane persists memory records, enforces decay, revision, and deletion, and serves context-aware retrieval queries.

These planes MAY be co-located in a single process or separated into services.

### 15.6 Canonical Data Structures

All implementations MUST support the canonical structures defined in the Schema Appendix. Memory types MUST be represented as distinct schemas. Implementations MUST NOT collapse all memory into free-form text.

### 15.7 Consolidation and Revision Guarantees

Revision operations (supersede, fork, retract, merge, delete) MUST be executed atomically against the authoritative store. Implementations MUST guarantee that partial revisions are not externally visible.

### 15.8 Retrieval Interface

The retrieval interface MUST accept a task descriptor and a trust context. Retrieval MUST proceed in layers (working, semantic, competence, plan, episodic as needed) and MUST return structured results with enforced redaction or summarization.

### 15.9 Security Model

Implementations MUST support encryption at rest and trust-aware retrieval gating. Sensitive payloads MAY be encrypted separately from metadata. Retrieval MUST NOT expose records exceeding the allowed sensitivity for the current trust context.

### 15.10 Metrics and Observability

A conforming implementation MUST expose metrics including memory growth rate, salience distribution, retrieval usefulness, competence success rates, plan reuse frequency, and contradiction/revision rates.

A conforming implementation SHALL include ingestion APIs, a policy engine, structured storage, consolidation jobs, and a retrieval interface. Memory types must be represented as distinct schemas. Decay, revision, and deletion must be supported.

The remainder of this section describes the required runtime components and the contract between them at the interface level.

### 15.1 Process Model

The learning substrate SHALL run as a long-lived service or library embedded within an agent runtime. It MUST expose synchronous ingestion APIs and asynchronous consolidation jobs. Consolidation and decay processing SHOULD be performed periodically and MUST be interruptible.

The system is logically divided into three planes:

The ingestion plane receives events, tool outputs, and observations and converts them into candidate memory records.

The policy plane classifies candidates, assigns lifecycle metadata, and determines storage placement.

The storage and retrieval plane persists memory records, enforces decay, and serves context-aware retrieval queries.

These planes MAY be co-located in a single process or separated into services.

### 15.2 Canonical Data Structures

All implementations MUST support the following canonical structures.

A MemoryRecord is the atomic unit of storage and MUST include:

* a globally unique identifier
* a declared memory type
* a sensitivity class
* a confidence score in the range [0,1]
* a salience score used for decay
* lifecycle metadata (creation time, last reinforcement, decay curve)
* provenance links to source events or artifacts
* a structured content payload conforming to the schema of its memory type

Memory types MUST be represented as distinct schemas. Implementations MUST NOT collapse all memory into free-form text.

### 15.3 Storage Layout

A reference implementation SHOULD include the following physical stores:

A metadata store for MemoryRecords and their lifecycle fields. A relational schema is RECOMMENDED.

A content store for structured payloads. JSON or binary-encoded structured formats are RECOMMENDED.

An optional vector index for semantic similarity search. The vector index MUST NOT be the source of truth.

An artifact store for large binary objects referenced by episodic memory. Artifacts MUST be content-addressed.

A relationship store representing graph edges between MemoryRecords.

### 15.4 Ingestion API

The system MUST expose ingestion functions for user inputs, tool outputs, observations, and outcomes. Each ingestion call MUST produce one or more MemoryCandidates placed into a staging area.

### 15.5 Classification and Policy Application

MemoryCandidates MUST be classified before persistence. Classification includes determining memory type, sensitivity, initial confidence, decay curve, scope, and visibility. Classification MAY use heuristic rules or models, but policy rules MUST be able to override classifier output.

### 15.6 Decay and Salience Mechanics

Each MemoryRecord SHALL maintain a salience score that decays over time according to its decay curve. Salience MUST be reinforced when a record is retrieved and contributes to a successful outcome, and penalized when it is retrieved but unused or associated with failure.

### 15.7 Consolidation Pipeline

Consolidation MUST support episodic compression, semantic update, competence extraction, plan graph extraction, and duplicate resolution.

### 15.8 Retrieval Interface

The retrieval interface MUST accept a task descriptor and a trust context and MUST return structured results, enforcing redaction/summarization decisions.

### 15.9 Security Model

Implementations MUST support encryption at rest and trust-aware retrieval gating. Retrieval MUST NOT expose records exceeding the allowed sensitivity for the current trust context.

### 15.10 Metrics and Observability

A conforming implementation MUST expose metrics including memory growth rate, salience distribution, retrieval usefulness, competence success rates, plan reuse frequency, and contradiction/revision rates.

## 15A. Schema Appendix (Normative)

This appendix defines the canonical record “shapes” (schemas). The shapes are expressed in a JSON-Schema-like form for clarity. Implementations MAY use alternative encodings (e.g., protobuf), but MUST preserve all fields and constraints.

### 15A.1 Common Enums

**MemoryType** MUST be one of:

* `episodic`
* `working`
* `semantic`
* `competence`
* `plan_graph`

**Sensitivity** MUST be one of:

* `public`
* `low`
* `medium`
* `high`
* `hyper`

### 15A.2 MemoryRecord Schema

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "mem://schemas/MemoryRecord",
  "type": "object",
  "required": [
    "id", "type", "sensitivity", "confidence", "salience",
    "created_at", "updated_at", "lifecycle", "provenance", "payload"
  ],
  "properties": {
    "id": {"type": "string", "description": "Globally unique identifier (UUID recommended)."},
    "type": {"type": "string", "enum": ["episodic","working","semantic","competence","plan_graph"]},
    "sensitivity": {"type": "string", "enum": ["public","low","medium","high","hyper"]},
    "confidence": {"type": "number", "minimum": 0, "maximum": 1},
    "salience": {"type": "number", "minimum": 0, "maximum": 1},
    "scope": {"type": "string", "description": "Implementation-defined; e.g., user/device/project/workspace/global."},
    "tags": {"type": "array", "items": {"type": "string"}},
    "created_at": {"type": "string", "format": "date-time"},
    "updated_at": {"type": "string", "format": "date-time"},
    "lifecycle": {"$ref": "mem://schemas/Lifecycle"},
    "provenance": {"$ref": "mem://schemas/Provenance"},
    "relations": {"type": "array", "items": {"$ref": "mem://schemas/Relation"}},
    "payload": {
      "oneOf": [
        {"$ref": "mem://schemas/EpisodicPayload"},
        {"$ref": "mem://schemas/WorkingPayload"},
        {"$ref": "mem://schemas/SemanticPayload"},
        {"$ref": "mem://schemas/CompetencePayload"},
        {"$ref": "mem://schemas/PlanGraphPayload"}
      ]
    }
  }
}
```

### 15A.3 Lifecycle Schema

```json
{
  "$id": "mem://schemas/Lifecycle",
  "type": "object",
  "required": ["decay", "last_reinforced_at"],
  "properties": {
    "decay": {
      "type": "object",
      "required": ["curve", "half_life_seconds"],
      "properties": {
        "curve": {"type": "string", "enum": ["exponential","linear","custom"]},
        "half_life_seconds": {"type": "integer", "minimum": 1},
        "min_salience": {"type": "number", "minimum": 0, "maximum": 1},
        "max_age_seconds": {"type": "integer", "minimum": 1}
      }
    },
    "last_reinforced_at": {"type": "string", "format": "date-time"},
    "pinned": {"type": "boolean"},
    "deletion_policy": {"type": "string", "enum": ["auto_prune","manual_only","never"]}
  }
}
```

### 15A.4 Provenance Schema

```json
{
  "$id": "mem://schemas/Provenance",
  "type": "object",
  "required": ["sources"],
  "properties": {
    "sources": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["kind", "ref"],
        "properties": {
          "kind": {"type": "string", "enum": ["event","artifact","tool_call","observation","outcome"]},
          "ref": {"type": "string", "description": "Opaque reference into the host system."},
          "hash": {"type": "string", "description": "Optional content hash for immutability."}
        }
      }
    },
    "created_by": {"type": "string", "description": "e.g., classifier/policy/consolidator version"}
  }
}
```

### 15A.5 Relation Schema

```json
{
  "$id": "mem://schemas/Relation",
  "type": "object",
  "required": ["predicate", "target_id"],
  "properties": {
    "predicate": {"type": "string", "description": "e.g., supports, contradicts, derived_from, supersedes"},
    "target_id": {"type": "string"},
    "weight": {"type": "number", "minimum": 0, "maximum": 1}
  }
}
```

### 15A.6 EpisodicPayload Schema

```json
{
  "$id": "mem://schemas/EpisodicPayload",
  "type": "object",
  "required": ["kind", "timeline"],
  "properties": {
    "kind": {"const": "episodic"},
    "timeline": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["t", "event_kind", "ref"],
        "properties": {
          "t": {"type": "string", "format": "date-time"},
          "event_kind": {"type": "string"},
          "ref": {"type": "string"},
          "summary": {"type": "string"}
        }
      }
    },
    "artifacts": {"type": "array", "items": {"type": "string"}},
    "tool_graph_ref": {"type": "string"}
  }
}
```

### 15A.7 WorkingPayload Schema

```json
{
  "$id": "mem://schemas/WorkingPayload",
  "type": "object",
  "required": ["kind", "thread_id", "state"],
  "properties": {
    "kind": {"const": "working"},
    "thread_id": {"type": "string"},
    "state": {"type": "string", "enum": ["planning","executing","blocked","waiting","done"]},
    "next_actions": {"type": "array", "items": {"type": "string"}},
    "open_questions": {"type": "array", "items": {"type": "string"}},
    "context_summary": {"type": "string"}
  }
}
```

### 15A.8 SemanticPayload Schema (Revisable Facts)

```json
{
  "$id": "mem://schemas/SemanticPayload",
  "type": "object",
  "required": ["kind", "subject", "predicate", "object", "validity"],
  "properties": {
    "kind": {"const": "semantic"},
    "subject": {"type": "string"},
    "predicate": {"type": "string"},
    "object": {"type": ["string","number","boolean","object","array"]},
    "validity": {
      "type": "object",
      "required": ["mode"],
      "properties": {
        "mode": {"type": "string", "enum": ["global","conditional","timeboxed"]},
        "conditions": {"type": "object", "description": "Implementation-defined conditional keys."},
        "start": {"type": "string", "format": "date-time"},
        "end": {"type": "string", "format": "date-time"}
      }
    },
    "revision": {
      "type": "object",
      "properties": {
        "supersedes": {"type": "string"},
        "superseded_by": {"type": "string"},
        "status": {"type": "string", "enum": ["active","contested","retracted"]}
      }
    }
  }
}
```

### 15A.9 CompetencePayload Schema (Procedures)

```json
{
  "$id": "mem://schemas/CompetencePayload",
  "type": "object",
  "required": ["kind", "skill_name", "triggers", "recipe"],
  "properties": {
    "kind": {"const": "competence"},
    "skill_name": {"type": "string"},
    "triggers": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["signal"],
        "properties": {
          "signal": {"type": "string", "description": "e.g., error signature, intent label"},
          "conditions": {"type": "object"}
        }
      }
    },
    "recipe": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["step"],
        "properties": {
          "step": {"type": "string"},
          "tool": {"type": "string"},
          "args_schema": {"type": "object"},
          "validation": {"type": "string"}
        }
      }
    },
    "failure_modes": {"type": "array", "items": {"type": "string"}},
    "fallbacks": {"type": "array", "items": {"type": "string"}},
    "version": {"type": "string"}
  }
}
```

### 15A.10 PlanGraphPayload Schema (Reusable Solution Graph)

```json
{
  "$id": "mem://schemas/PlanGraphPayload",
  "type": "object",
  "required": ["kind", "plan_id", "version", "nodes", "edges"],
  "properties": {
    "kind": {"const": "plan_graph"},
    "plan_id": {"type": "string"},
    "version": {"type": "string"},
    "intent": {"type": "string", "description": "High-level intent label (e.g., setup_project)."},
    "constraints": {"type": "object", "description": "e.g., trust requirements, sensitivity limits"},
    "inputs_schema": {"type": "object"},
    "outputs_schema": {"type": "object"},
    "nodes": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["id", "op"],
        "properties": {
          "id": {"type": "string"},
          "op": {"type": "string", "description": "action/tool identifier"},
          "params": {"type": "object"},
          "guards": {"type": "object", "description": "conditional execution"}
        }
      }
    },
    "edges": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["from", "to", "kind"],
        "properties": {
          "from": {"type": "string"},
          "to": {"type": "string"},
          "kind": {"type": "string", "enum": ["data","control"]}
        }
      }
    },
    "metrics": {
      "type": "object",
      "properties": {
        "avg_latency_ms": {"type": "number"},
        "failure_rate": {"type": "number", "minimum": 0, "maximum": 1}
      }
    }
  }
}
```

### 15A.11 Selection of Multiple Solutions (Normative)

When multiple competence records or plan graphs match a task, an implementation MUST perform selection using at least the following signals: applicability conditions, observed success rate, and recency of reinforcement. If selection confidence is below an implementation-defined threshold, the system MUST either (a) return multiple candidates ranked, or (b) request additional disambiguating context from the host agent.

### 15A.12 Example Records (Non-Normative)

The following examples illustrate compliant shapes.

**Example: Semantic preference with conditional validity**

```json
{
  "id": "9a2b...",
  "type": "semantic",
  "sensitivity": "low",
  "confidence": 0.9,
  "salience": 0.8,
  "created_at": "2026-01-28T00:00:00Z",
  "updated_at": "2026-01-28T00:00:00Z",
  "lifecycle": {"decay": {"curve": "exponential", "half_life_seconds": 7776000}, "last_reinforced_at": "2026-01-28T00:00:00Z"},
  "provenance": {"sources": [{"kind": "event", "ref": "evt://..."}]},
  "payload": {
    "kind": "semantic",
    "subject": "user",
    "predicate": "prefers_backend_language",
    "object": "go",
    "validity": {"mode": "conditional", "conditions": {"project_kind": "backend_service"}},
    "revision": {"status": "active"}
  }
}
```

**Example: Competence procedure for troubleshooting**

```json
{
  "id": "c01d...",
  "type": "competence",
  "sensitivity": "medium",
  "confidence": 0.7,
  "salience": 0.6,
  "created_at": "2026-01-28T00:00:00Z",
  "updated_at": "2026-01-28T00:00:00Z",
  "lifecycle": {"decay": {"curve": "exponential", "half_life_seconds": 2592000}, "last_reinforced_at": "2026-01-28T00:00:00Z"},
  "provenance": {"sources": [{"kind": "tool_call", "ref": "tool://build#123"}]},
  "payload": {
    "kind": "competence",
    "skill_name": "fix_linker_cache_error",
    "triggers": [{"signal": "ld: cache mismatch", "conditions": {"os": "macos"}}],
    "recipe": [
      {"step": "Clear build cache", "tool": "shell", "validation": "cache directory removed"},
      {"step": "Rebuild with clean flags", "tool": "build", "validation": "build succeeds"}
    ],
    "failure_modes": ["If cache directory is locked, retry with elevated permissions"],
    "fallbacks": ["Reinstall toolchain"],
    "version": "1.0"
  }
}
```

## 15A. Canonical Schemas (Normative)

This section defines the REQUIRED structural schemas ("shapes") for all conforming implementations. These schemas are normative. An implementation that does not preserve these shapes, semantics, and field meanings is non-compliant, even if functionally similar.

All schemas are presented in a JSON-like notation for clarity. Implementations MAY use other encodings (Protobuf, SQL, structs) provided the logical shape is preserved.

### 15A.1 MemoryRecord (Base Type)

Every stored memory item MUST conform to the MemoryRecord shape.

```
MemoryRecord {
  id: UUID,                         // globally unique, immutable
  type: MemoryType,                 // episodic | working | semantic | competence | plan
  sensitivity: SensitivityClass,    // public | low | medium | high | hyper
  confidence: Float [0,1],          // epistemic confidence
  salience: Float [0,∞),            // decay-weighted importance

  created_at: Timestamp,
  last_reinforced_at: Timestamp,

  decay_profile: DecayProfile,      // defines salience decay
  scope: Scope,                     // user | project | workspace | global

  provenance: ProvenanceRef[],      // evidence links
  relationships: RelationRef[],     // graph edges

  payload: StructuredObject,        // type-specific schema
  audit_log: AuditEntry[]           // revisions, merges, deletions
}
```

This base record MUST NOT be bypassed by any memory type.

---

### 15A.2 EpisodicRecord Payload

```
EpisodicPayload {
  timeline: EventRef[],             // ordered events
  tool_graph: ToolNode[],           // tool calls + data flow
  environment: EnvironmentSnapshot, // OS, versions, context
  outcome: OutcomeStatus,           // success | failure | partial
  artifact_refs: ArtifactRef[]      // logs, screenshots, files
}
```

Episodic payloads MUST be append-only. Semantic correction is forbidden at this layer.

---

### 15A.3 WorkingMemory Payload

```
WorkingPayload {
  task_id: Identifier,
  state: TaskState,                 // planning | executing | blocked | waiting
  active_constraints: Constraint[],
  next_actions: ActionHint[],
  open_questions: Question[],
  last_updated: Timestamp
}
```

Working memory MAY be freely edited and discarded when the task ends.

---

### 15A.4 SemanticMemory Payload

```
SemanticPayload {
  subject: EntityRef,
  predicate: NormalizedRelation,
  object: Value | EntityRef,

  validity: ValidityWindow,         // optional conditionality
  evidence: ProvenanceRef[],

  revision_policy: RevisionPolicy   // replace | fork | contest
}
```

Semantic payloads MUST support coexistence of multiple conditional truths.

---

### 15A.5 CompetenceMemory Payload

```
CompetencePayload {
  skill_name: String,

  triggers: Condition[],            // when this applies
  procedure: Step[],                // ordered or conditional

  required_tools: ToolRef[],
  failure_modes: FailureCase[],
  fallbacks: Step[],

  performance: PerformanceStats     // success / failure history
}
```

Competence payloads represent "knowing how" rather than "knowing that".

---

### 15A.6 PlanGraph Payload

```
PlanGraphPayload {
  intent: IntentLabel,

  nodes: PlanNode[],
  edges: PlanEdge[],

  input_schema: SchemaRef,
  output_schema: SchemaRef,

  constraints: Constraint[],        // trust, sensitivity, environment
  metrics: PerformanceStats,

  version: VersionTag
}
```

Plan graphs MUST be reusable, versioned, and selectable by constraint matching.

---

### 15A.7 DecayProfile

```
DecayProfile {
  function: DecayFunction,          // exponential | linear | custom
  half_life: Duration,
  floor: Float,
  reinforcement_gain: Float
}
```

Decay profiles MUST be monotonic and reversible via reinforcement.

---

### 15A.8 Provenance and Revision

```
ProvenanceRef {
  source_type: SourceType,           // event | tool | observation | human
  source_id: Identifier,
  timestamp: Timestamp
}

AuditEntry {
  action: AuditAction,               // create | revise | fork | merge | delete
  actor: ActorRef,
  timestamp: Timestamp,
  rationale: String
}
```

Every revision MUST be auditable and traceable to evidence.

---

## 15B. Behavioral Guarantees

An implementation conforming to this RFC MUST guarantee:

* Automatic memory creation without user approval
* Revisability of all non-episodic memory
* Support for multiple competing procedures and facts
* Context-sensitive selection rather than forced unification
* Safe deletion via decay rather than hard loss

---

## 15C. Relationship to Reasoning Systems

This specification intentionally does not mandate how reasoning systems (LLMs or otherwise) infer which memories to create or retrieve. It defines only the *storage, revision, and selection substrate*. Reasoning systems supply hypotheses; this system supplies structured, revisable knowledge.

---

## 15. Reference Implementation Architecture (continued)

A conforming implementation SHALL include ingestion APIs, a policy engine, structured storage, consolidation jobs, and a retrieval interface. Memory types must be represented as distinct schemas. Decay, revision, and deletion must be supported.

## 16. Compliance

An implementation is compliant if it supports typed memory, automatic learning with decay, revisable semantic memory, competence and plan learning, and trust-aware retrieval.

## 17. Conclusion

This RFC defines a selective, revisable learning substrate that enables agents to genuinely learn from experience. By storing procedures, preferences, and solution structures—and by allowing them to evolve over time—the system supports advanced, long-lived agents without sacrificing safety or predictability.
