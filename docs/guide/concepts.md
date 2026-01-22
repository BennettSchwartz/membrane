# Core Concepts

Membrane is built around a layered memory model inspired by cognitive science. Rather than treating all data as equal blobs in a database, Membrane distinguishes between different *kinds* of knowledge and manages each with purpose-built lifecycle rules.

## The Five Memory Types

| Type | Analogy | Purpose | Durability |
|------|---------|---------|------------|
| **Episodic** | "What happened" | Raw experience: user inputs, tool calls, errors, observations | Short-lived, decays quickly |
| **Working** | "What I'm doing now" | Current task state for resumption across sessions | Discarded when the task ends |
| **Semantic** | "What I know" | Stable facts: preferences, environment, relationships | Long-lived, revisable |
| **Competence** | "How I do things" | Procedural knowledge: skill recipes with triggers and steps | Long-lived, performance-tracked |
| **Plan Graph** | "Reusable playbooks" | Directed action graphs that solve recurring problems | Long-lived, versioned |

## Layered Memory Model

When an agent retrieves memories, Membrane queries each layer in a fixed order:

```
Working -> Semantic -> Competence -> Plan Graph -> Episodic
```

This ordering ensures that the most actionable and stable knowledge surfaces first:

1. **Working** memory provides immediate task context
2. **Semantic** memory provides known facts relevant to the task
3. **Competence** and **Plan Graph** memories offer procedural guidance
4. **Episodic** memory fills in recent experience as supporting evidence

## Decay

Every memory record has a **salience** score that decays over time. Membrane supports three decay curves:

- **Exponential** -- salience halves every `half_life_seconds` (default: 86400s / 1 day)
- **Linear** -- salience decreases at a constant rate
- **Custom** -- implementation-defined behavior

Decay is reversible through **reinforcement**: accessing or explicitly reinforcing a memory resets its decay clock and boosts its salience.

Records can be **pinned** to prevent decay entirely, or set to a **min_salience** floor below which they will not decay further.

## Consolidation

Consolidation is the process of distilling raw episodic experience into durable knowledge. It runs automatically on a configurable schedule (default: every 6 hours) and performs four operations:

1. **Episodic compression** -- reduces salience of old episodic records
2. **Semantic extraction** -- promotes repeated observations into semantic facts
3. **Competence extraction** -- identifies successful tool-use patterns and creates competence records
4. **Plan graph synthesis** -- extracts reusable action graphs from episodic tool graphs

Consolidation requires no user approval. Promoted knowledge remains subject to normal decay and revision.

## Revision

Semantic knowledge is inherently uncertain and changeable. Membrane supports four revision operations:

- **Supersede** -- replace a fact with a corrected version, preserving a `supersedes` link
- **Fork** -- create a conditional variant when a fact is context-dependent
- **Retract** -- mark a fact as withdrawn without deleting it
- **Merge** -- combine multiple related records into one

Every revision is recorded in the record's **audit log** with an actor, timestamp, and rationale.

## Trust and Security

Every retrieval request carries a **trust context** that specifies:

- The maximum **sensitivity level** the requester can access (public, low, medium, high, hyper)
- Whether the requester is **authenticated**
- The requester's **actor ID**
- The **scopes** the requester is allowed to query

Records whose sensitivity exceeds the trust context, or whose scope is not in the allowed list, are filtered out before results are returned. See [Security](/guide/security) for details.
