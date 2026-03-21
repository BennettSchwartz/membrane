# Membrane

[![CI](https://github.com/GustyCube/membrane/actions/workflows/ci.yml/badge.svg)](https://github.com/GustyCube/membrane/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/GustyCube/membrane)](https://goreportcard.com/report/github.com/GustyCube/membrane)
[![Go Reference](https://pkg.go.dev/badge/github.com/GustyCube/membrane.svg)](https://pkg.go.dev/github.com/GustyCube/membrane)
[![Go Version](https://img.shields.io/github/go-mod/go-version/GustyCube/membrane)](https://go.dev/)
[![License: MIT](https://img.shields.io/github/license/GustyCube/membrane)](LICENSE)
[![Release](https://img.shields.io/github/v/release/GustyCube/membrane?include_prereleases&sort=semver)](https://github.com/GustyCube/membrane/releases)

[![GitHub Stars](https://img.shields.io/github/stars/GustyCube/membrane?style=flat)](https://github.com/GustyCube/membrane/stargazers)
[![GitHub Forks](https://img.shields.io/github/forks/GustyCube/membrane?style=flat)](https://github.com/GustyCube/membrane/network/members)
[![GitHub Issues](https://img.shields.io/github/issues/GustyCube/membrane)](https://github.com/GustyCube/membrane/issues)
[![GitHub Pull Requests](https://img.shields.io/github/issues-pr/GustyCube/membrane)](https://github.com/GustyCube/membrane/pulls)
[![Last Commit](https://img.shields.io/github/last-commit/GustyCube/membrane)](https://github.com/GustyCube/membrane/commits)
[![Contributors](https://img.shields.io/github/contributors/GustyCube/membrane)](https://github.com/GustyCube/membrane/graphs/contributors)

**A general-purpose selective learning and memory substrate for LLM and agentic systems.**

Membrane gives long-lived LLM agents structured, revisable memory with built-in decay, trust-gated retrieval, and audit trails. Instead of an append-only context window or flat text log, agents get typed memory records that can be consolidated, revised, contested, and pruned over time.

---

## Table of Contents

- [Why Membrane](#why-membrane)
- [60-Second Mental Model](#60-second-mental-model)
- [Key Features](#key-features)
- [Memory Types](#memory-types)
- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Configuration](#configuration)
- [gRPC API](#grpc-api)
- [Revision Operations](#revision-operations)
- [Evaluation and Metrics](#evaluation-and-metrics)
- [Observability](#observability)
- [TypeScript Client](#typescript-client)
- [LLM Integration Pattern](#llm-integration-pattern)
- [Python Client](#python-client)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [Star History](#star-history)
- [License](#license)

---

## Why Membrane

Most LLM/agent "memory" is either ephemeral (context windows that reset each turn) or an append-only text log stuffed into a RAG pipeline. That gives you retrieval, but not learning: facts get stale, procedures drift, and the system cannot revise itself safely.

Membrane makes memory **selective** and **revisable**. It captures raw experience, promotes it into structured knowledge, and lets you supersede, fork, contest, or retract that knowledge with evidence. The result is an agent that can improve over time while remaining predictable, auditable, and safe.

## 60-Second Mental Model

1. **Ingest** events, tool outputs, observations, and working state.
2. **Consolidate** episodic traces into semantic facts, competence records, and plan graphs.
3. **Retrieve** in layers with trust gating and salience ranking.
4. **Revise** knowledge with explicit operations and audit trails.
5. **Decay** salience over time unless reinforced by success.

## Key Features

- **Typed Memory** -- Explicit schemas and lifecycles for each memory type, not a flat text store.
- **Revisable Knowledge** -- Supersede, fork, retract, merge, and contest records with full provenance tracking.
- **Competence Learning** -- Agents learn *how* to solve problems (procedures, success rates), not just *what* happened.
- **Decay and Consolidation** -- Time-based salience decay keeps memory useful; background consolidation extracts semantic facts, competence records, and plan graphs from episodic traces.
- **LLM-Based Semantic Extraction** -- On the Postgres + LLM tier, episodic records can be converted into typed semantic facts asynchronously through a structured extraction pipeline.
- **Trust-Aware Retrieval** -- Sensitivity levels (public, low, medium, high, hyper) with graduated access control and redacted responses for records above the caller's trust level.
- **Security and Operations** -- SQLCipher encryption at rest, optional TLS and API key authentication, configurable rate limiting, full audit logs.
- **Observability** -- Built-in metrics for retrieval usefulness, competence success rate, plan reuse frequency, memory growth, and revision rate.
- **gRPC API** -- 15-method gRPC service with TypeScript and Python client SDKs, or use Membrane as an embedded Go library.
- **Vector-Aware Retrieval** -- With the Postgres + pgvector backend enabled, competence and plan-graph applicability can be scored with embedding similarity instead of the confidence-only fallback.
- **LLM-Ready Context Retrieval** -- Retrieve trust-filtered, typed memory and inject it directly into LLM prompts for planning, execution, self-correction, and background learning loops.

## Memory Types

| Type | Purpose | Example |
|------|---------|---------|
| **Episodic** | Raw experience capture (immutable) | Tool calls, errors, observations from a debugging session |
| **Working** | Current task state | "Backend initialized, frontend pending, docs TODO" |
| **Semantic** | Stable facts and preferences | "User prefers Go for backend services" |
| **Competence** | Learned procedures with success tracking | "To fix linker cache error: clear cache, rebuild with flags" |
| **Plan Graph** | Reusable solution structures as directed graphs | Multi-step project setup workflow with dependencies and checkpoints |

Each memory type has its own schema, lifecycle rules, and consolidation behavior. Episodic records are immutable once ingested. Working memory tracks in-flight task state. Semantic, competence, and plan graph records are the durable output of consolidation and can be revised through explicit operations.

## Quick Start

### Prerequisites

- Go 1.22 or later
- Make
- Protocol Buffers compiler (`protoc` >= 3.20) for gRPC development
- Node.js 20+ for the TypeScript client SDK
- Python 3.10+ for the Python client SDK

### Build and Run

```bash
git clone https://github.com/GustyCube/membrane.git
cd membrane

# Build the daemon
make build

# Run tests
make test

# Start with default SQLite storage
./bin/membraned

# Start with Postgres + pgvector instead
./bin/membraned --postgres-dsn postgres://membrane:membrane@localhost:5432/membrane_test?sslmode=disable

# With custom configuration
./bin/membraned --config /path/to/config.yaml

# Override database path or listen address
./bin/membraned --db /path/to/membrane.db --addr :8080
```

### Using the Go Library

Membrane can be used as an embedded library without running the daemon:

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/GustyCube/membrane/pkg/ingestion"
    "github.com/GustyCube/membrane/pkg/membrane"
    "github.com/GustyCube/membrane/pkg/retrieval"
    "github.com/GustyCube/membrane/pkg/schema"
)

func main() {
    cfg := membrane.DefaultConfig()
    cfg.DBPath = "my-agent.db"

    m, err := membrane.New(cfg)
    if err != nil {
        log.Fatal(err)
    }
    defer m.Stop()

    ctx := context.Background()
    m.Start(ctx)

    // Ingest an episodic event (tool call observation)
    rec, _ := m.IngestEvent(ctx, ingestion.IngestEventRequest{
        Source:    "build-agent",
        EventKind: "tool_call",
        Ref:       "build#42",
        Summary:   "Executed go build, failed with linker error",
        Tags:      []string{"build", "error"},
    })
    fmt.Printf("Ingested episodic record: %s\n", rec.ID)

    // Ingest a semantic observation
    m.IngestObservation(ctx, ingestion.IngestObservationRequest{
        Source:    "build-agent",
        Subject:   "user",
        Predicate: "prefers_language",
        Object:    "go",
        Tags:      []string{"preferences"},
    })

    // Ingest working memory state
    m.IngestWorkingState(ctx, ingestion.IngestWorkingStateRequest{
        Source:     "build-agent",
        ThreadID:   "session-001",
        State:      schema.TaskStateExecuting,
        NextActions: []string{"run tests", "deploy"},
    })

    // Retrieve with trust context
    resp, _ := m.Retrieve(ctx, &retrieval.RetrieveRequest{
        TaskDescriptor: "fix build error",
        Trust: &retrieval.TrustContext{
            MaxSensitivity: schema.SensitivityMedium,
            Authenticated:  true,
        },
        MemoryTypes: []schema.MemoryType{
            schema.MemoryTypeCompetence,
            schema.MemoryTypeSemantic,
        },
    })

    for _, r := range resp.Records {
        fmt.Printf("Found: %s (type=%s, confidence=%.2f)\n", r.ID, r.Type, r.Confidence)
    }
}
```

## Architecture

Membrane runs as a long-lived daemon or embedded library. The architecture is organized into three logical planes:

```
+------------------+     +------------------+     +----------------------+
|  Ingestion Plane |---->|   Policy Plane   |---->| Storage & Retrieval  |
+------------------+     +------------------+     +----------------------+
        |                        |                         |
   Events, tool            Classification,            SQLCipher (encrypted),
   outputs, obs.,          sensitivity,               audit trails,
   working state           decay profiles             trust-gated access
```

### Storage Model

- **Authoritative Store** -- SQLite with SQLCipher remains the default embedded store; Postgres + pgvector is available as an opt-in backend for concurrent deployments and embedding-backed retrieval.
- **Structured Payloads** -- Type-specific schemas stored as JSON within the authoritative store.
- **Relationship Graph** -- Relations between records (supersedes, derived_from, contested_by, supports, contradicts) stored alongside the records they describe.

### Deployment Tiers

| Tier | Backend | Embedding | LLM | Behavior |
|------|---------|-----------|-----|----------|
| **1** | SQLite | - | - | Zero-infra default, confidence-based applicability fallback |
| **2** | Postgres | - | - | Concurrent writers, JSONB storage, same retrieval semantics as tier 1 |
| **3** | Postgres + pgvector | Yes | - | Recommended: hybrid vector+salience ranking for all record types |
| **4** | Postgres + pgvector | Yes | Yes | Full system with LLM-backed episodic to semantic extraction |

### Background Jobs

| Job | Default Interval | Purpose |
|-----|-----------------|---------|
| **Decay** | 1 hour | Applies time-based salience decay using an exponential curve |
| **Pruning** | With decay | Deletes records with `auto_prune` policy whose salience has reached 0 |
| **Consolidation** | 6 hours | Runs structural semantic consolidation, LLM-backed semantic extraction when configured, competence extraction, and plan-graph extraction |

### Security Model

- **Encryption at Rest** -- SQLCipher with `PRAGMA key` applied at database open.
- **TLS Transport** -- Optional TLS for gRPC connections.
- **Authentication** -- Bearer token API key via `authorization` metadata.
- **Rate Limiting** -- Token bucket limiter with configurable requests per second.
- **Trust-Aware Retrieval** -- Records filtered by sensitivity level. Records one level above the caller's threshold are returned in redacted form (metadata only, no payload).
- **Input Validation** -- Payload size limits, string length checks, tag count limits, NaN/Inf rejection.

## Configuration

Membrane is configured via a YAML file or command-line flags. Secrets should come from environment variables.

```yaml
backend: "sqlite"
db_path: "membrane.db"
# postgres_dsn: "postgres://membrane:membrane@localhost:5432/membrane?sslmode=disable"
listen_addr: ":9090"
decay_interval: "1h"
consolidation_interval: "6h"
default_sensitivity: "low"
selection_confidence_threshold: 0.7

# Optional embedding-backed retrieval (Postgres only)
# embedding_endpoint: "https://api.openai.com/v1/embeddings"
# embedding_model: "text-embedding-3-small"
# embedding_dimensions: 1536
# embedding_api_key: ""   # or set MEMBRANE_EMBEDDING_API_KEY

# Optional LLM-backed semantic extraction (Postgres only)
# llm_endpoint: "https://api.openai.com/v1/chat/completions"
# llm_model: "gpt-5-mini"
# llm_api_key: ""         # or set MEMBRANE_LLM_API_KEY

# Security (prefer environment variables for keys)
# encryption_key: ""       # or set MEMBRANE_ENCRYPTION_KEY
# api_key: ""              # or set MEMBRANE_API_KEY
# tls_cert_file: ""
# tls_key_file: ""
rate_limit_per_second: 100
```

| Variable | Purpose |
|----------|---------|
| `MEMBRANE_ENCRYPTION_KEY` | SQLCipher encryption key for the database |
| `MEMBRANE_POSTGRES_DSN` | PostgreSQL DSN used when `backend: postgres` |
| `MEMBRANE_EMBEDDING_API_KEY` | API key for the embedding endpoint |
| `MEMBRANE_LLM_API_KEY` | API key for the semantic extraction LLM endpoint |
| `MEMBRANE_API_KEY` | Bearer token for gRPC authentication |

## gRPC API

The gRPC API uses protoc-generated service stubs with JSON-encoded payloads over protobuf `bytes` fields.

| Method | Description |
|--------|-------------|
| `IngestEvent` | Create episodic record from an event |
| `IngestToolOutput` | Create episodic record from a tool invocation |
| `IngestObservation` | Create semantic record from an observation |
| `IngestOutcome` | Update episodic record with outcome data |
| `IngestWorkingState` | Create working memory record |
| `Retrieve` | Layered retrieval with trust context |
| `RetrieveByID` | Fetch single record by ID |
| `Supersede` | Replace a record with a new version |
| `Fork` | Create conditional variant of a record |
| `Retract` | Mark a record as retracted |
| `Merge` | Combine multiple records into one |
| `Contest` | Mark a record as contested by conflicting evidence |
| `Reinforce` | Boost a record's salience |
| `Penalize` | Reduce a record's salience |
| `GetMetrics` | Retrieve observability metrics snapshot |

## Revision Operations

Membrane provides five revision operations, each producing an audit trail and updating the record's revision status:

```go
// Supersede a semantic record with a new version
superseded, _ := m.Supersede(ctx, oldRecordID, newRec, "agent", "Go version updated")

// Fork a record for conditional validity
forked, _ := m.Fork(ctx, sourceID, conditionalRec, "agent", "different for dev environment")

// Contest a record when conflicting evidence appears
m.Contest(ctx, recordID, conflictingRecordID, "agent", "new evidence contradicts this")

// Retract a record that is no longer valid
m.Retract(ctx, recordID, "agent", "no longer accurate")

// Merge multiple records into one consolidated record
merged, _ := m.Merge(ctx, []string{id1, id2, id3}, mergedRec, "agent", "consolidating duplicates")
```

## Evaluation and Metrics

Membrane exposes behavioral metrics (retrieval usefulness, competence success rate, plan reuse frequency) via `GetMetrics`, and the eval suite compares Membrane's structured retrieval against pure RAG across all five memory types.

### Retrieval Eval (RAG vs Membrane)

Compares pure vector similarity search (RAG) against Membrane's full pipeline (vector ranking + salience + trust gating) using pgvector and OpenAI text-embedding-3-small embeddings. Tests all five memory types: episodic, working, semantic, competence, and plan graph.

```bash
# Requires Postgres + pgvector and an OpenRouter/OpenAI API key.
# Copy .env.example to .env and fill in values.
go run ./cmd/membrane-eval \
  -dataset tests/data/recall_dataset.jsonl \
  -postgres-dsn "$MEMBRANE_POSTGRES_DSN" \
  -embedding-endpoint "https://openrouter.ai/api/v1/embeddings" \
  -embedding-model "openai/text-embedding-3-small" \
  -embedding-api-key "$MEMBRANE_EMBEDDING_API_KEY" \
  -embedding-dimensions 1536
```

### Lifecycle Eval (Membrane vs RAG)

Tests four scenarios where Membrane's structured memory (retraction, reinforcement, supersession, decay) should outperform naive vector search:

| Scenario | What it tests |
|----------|---------------|
| **Retraction** | Wrong facts set to salience 0 and filtered out; RAG still returns them |
| **Reinforcement** | Proven-useful records ranked higher via hybrid vector+salience scoring |
| **Supersession** | Outdated facts replaced; old version filtered, new version returned |
| **Decay** | Stale records lose salience over time; fresh records promoted |

```bash
go run ./cmd/membrane-eval-lifecycle \
  -postgres-dsn "$MEMBRANE_POSTGRES_DSN" \
  -embedding-endpoint "https://openrouter.ai/api/v1/embeddings" \
  -embedding-model "openai/text-embedding-3-small" \
  -embedding-api-key "$MEMBRANE_EMBEDDING_API_KEY" \
  -embedding-dimensions 1536
```

### Unit and Integration Tests

```bash
make test                # All Go tests
make eval-all            # All targeted capability evals
```

Targeted evals:

```bash
make eval-typed          # Memory type handling
make eval-revision       # Revision semantics
make eval-decay          # Decay curves and pruning
make eval-trust          # Trust-gated retrieval
make eval-competence     # Competence learning
make eval-plan           # Plan graph operations
make eval-consolidation  # Episodic consolidation
make eval-metrics        # Observability metrics
make eval-invariants     # System invariants
make eval-grpc           # gRPC endpoint coverage
```

### Latest Results

**Retrieval Eval** (RAG vs Membrane, Mar 21, 2026):

59 records across 5 types, 25 queries.

| Metric | RAG | Membrane | Delta |
|--------|-----|----------|-------|
| recall@k | 0.959 | 0.959 | +0.000 |
| precision@k | 0.353 | 0.353 | +0.000 |
| MRR@k | 1.000 | 1.000 | +0.000 |
| NDCG@k | 0.956 | 0.956 | +0.000 |

Membrane matches RAG on pure retrieval quality while adding typed storage, trust gating, salience decay, revision tracking, and audit trails.

**Lifecycle Eval** (Mar 21, 2026):

| Scenario | RAG | Membrane | Winner |
|----------|-----|----------|--------|
| Retraction | 0 (returns wrong fact) | 1 (filters retracted record) | Membrane |
| Reinforcement | 0 (ranks bad procedure first) | 1 (ranks proven procedure first) | Membrane |
| Supersession | 0 (returns outdated value) | 1 (returns current value only) | Membrane |
| Decay | 0 (ranks stale record first) | 1 (ranks fresh record first) | Membrane |
| **Total** | **0/4** | **4/4** | **Membrane** |

<details>
<summary>What each scenario tests</summary>

- **Retraction**: A wrong fact (MySQL) is retracted. RAG still returns it via similarity; Membrane sets salience to 0 and filters it out.
- **Reinforcement**: Two debugging procedures exist. The proven one is reinforced 5x. RAG ranks by similarity alone (bad procedure first); Membrane's hybrid scoring promotes the reinforced one.
- **Supersession**: API rate limit changed from 50 to 200 rps. RAG returns both old and new; Membrane supersedes the old record (salience=0) and only returns the current value.
- **Decay**: Deployment target changed from Heroku to Kubernetes. The stale record is penalized. RAG ignores salience; Membrane's hybrid scoring demotes it.

</details>

Retrieval eval results depend on embedding quality, trust filters, and reinforcement behavior. Treat them as scenario-level regression guards. The lifecycle eval demonstrates Membrane's structural advantages over flat vector search. CI auto-updates this section when scores change.

## Observability

The `GetMetrics` endpoint returns a point-in-time snapshot:

```json
{
  "total_records": 142,
  "records_by_type": {
    "episodic": 80,
    "semantic": 35,
    "competence": 15,
    "plan_graph": 7,
    "working": 5
  },
  "avg_salience": 0.62,
  "avg_confidence": 0.78,
  "salience_distribution": {
    "0.0-0.2": 12,
    "0.2-0.4": 18,
    "0.4-0.6": 30,
    "0.6-0.8": 45,
    "0.8-1.0": 37
  },
  "active_records": 130,
  "pinned_records": 3,
  "total_audit_entries": 890,
  "memory_growth_rate": 0.15,
  "retrieval_usefulness": 0.42,
  "competence_success_rate": 0.85,
  "plan_reuse_frequency": 2.3,
  "revision_rate": 0.08
}
```

| Metric | Description |
|--------|-------------|
| `memory_growth_rate` | Fraction of records created in the last 24 hours |
| `retrieval_usefulness` | Ratio of reinforce actions to total audit entries |
| `competence_success_rate` | Average success rate across competence records |
| `plan_reuse_frequency` | Average execution count across plan graph records |
| `revision_rate` | Fraction of audit entries that are revisions (supersede, fork, merge) |

## TypeScript Client

Install the TypeScript client SDK:

```bash
npm install @gustycube/membrane
```

```ts
import { MembraneClient, Sensitivity } from "@gustycube/membrane";

const client = new MembraneClient("localhost:9090", { apiKey: "your-key" });

// Ingest an event
const record = await client.ingestEvent("tool_call", "task#1", {
  summary: "Ran database migration successfully",
  tags: ["db", "migration"]
});

// Retrieve with trust context
const results = await client.retrieve("database operations", {
  trust: {
    max_sensitivity: Sensitivity.MEDIUM,
    authenticated: true,
    actor_id: "ts-agent",
    scopes: []
  },
  memoryTypes: ["semantic", "competence"]
});

client.close();
```

See [clients/typescript/README.md](clients/typescript/README.md) for the full API reference.

## LLM Integration Pattern

Membrane is designed to sit between your orchestration layer and the model call. A common flow is:

1. Ingest tool/output observations during execution.
2. Retrieve relevant memory for the next task.
3. Build an LLM prompt using those retrieved records.
4. Use the model output to act, then ingest outcomes and reinforce useful records.

When Postgres plus an LLM endpoint are configured, Membrane can also run a background semantic extractor that turns episodic traces into typed semantic facts asynchronously during consolidation.

```ts
import OpenAI from "openai";
import { MembraneClient, Sensitivity } from "@gustycube/membrane";

const memory = new MembraneClient("localhost:9090", { apiKey: process.env.MEMBRANE_API_KEY });
const llm = new OpenAI({
  apiKey: process.env.LLM_API_KEY,
  // OpenAI-compatible providers are supported here, e.g. OpenRouter:
  // baseURL: "https://openrouter.ai/api/v1",
});

const records = await memory.retrieve("plan a safe migration", {
  trust: {
    max_sensitivity: Sensitivity.MEDIUM,
    authenticated: true,
    actor_id: "planner-agent",
    scopes: ["project-acme"],
  },
  memoryTypes: ["semantic", "competence", "working"],
  limit: 12,
});

const context = records.map((r) => JSON.stringify(r)).join("\n");

const completion = await llm.chat.completions.create({
  model: "gpt-5.2",
  messages: [
    { role: "system", content: "Use memory context as evidence. Cite record ids." },
    { role: "user", content: `Task: plan migration\n\nMemory:\n${context}` },
  ],
});

const answer = completion.choices[0]?.message?.content ?? "";
const planRecord = await memory.ingestEvent("llm_plan", "migration-task-42", {
  source: "planner-agent",
  summary: answer.slice(0, 500),
  tags: ["llm", "plan", "migration"],
  scope: "project-acme",
});
await memory.reinforce(planRecord.id, "planner-agent", "plan used successfully");

memory.close();
```

## Python Client

Install the Python client SDK:

```bash
pip install -e clients/python
```

For local client development and the same commands used in CI:

```bash
python -m pip install -e "clients/python[dev]"
python -m pytest clients/python/tests/
```

```python
from membrane import MembraneClient, Sensitivity, TrustContext

client = MembraneClient("localhost:9090", api_key="your-key")

# Ingest an event
record = client.ingest_event(
    source="my-agent",
    event_kind="tool_call",
    ref="task#1",
    summary="Ran database migration successfully",
    tags=["db", "migration"],
)

# Retrieve with trust context
results = client.retrieve(
    task_descriptor="database operations",
    trust=TrustContext(max_sensitivity=Sensitivity.MEDIUM, authenticated=True),
    memory_types=["semantic", "competence"],
)
```

See [clients/python/README.md](clients/python/README.md) for the full API reference.

## Documentation

Full documentation is available in the `docs/` directory, built with VitePress:

```bash
cd docs
npm install
npm run dev
```

Topics covered:

- Memory type schemas and lifecycle rules
- Revision semantics and conflict resolution
- Trust and sensitivity model
- API reference
- Deployment guide

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on code style, testing requirements, the pull request process, and SDK sync procedures.

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=GustyCube/membrane&type=Date)](https://star-history.com/#GustyCube/membrane&Date)

## License

Membrane is released under the [MIT License](LICENSE).

---

**Author:** Bennett Schwartz | **Repository:** [github.com/GustyCube/membrane](https://github.com/GustyCube/membrane)
