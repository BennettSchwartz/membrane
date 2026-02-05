# Membrane

[![Go Version](https://img.shields.io/badge/Go-1.24+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A general-purpose selective learning and memory substrate for agentic systems.

## Why Membrane

Membrane is a memory substrate for long-lived agents. Most agent "memory" is either ephemeral (context windows) or an append-only text log (RAG). That gives you retrieval, but not learning: facts get stale, procedures drift, and the system cannot revise itself safely.

Membrane makes memory *selective* and *revisable*. It captures raw experience, promotes it into structured knowledge, and lets you supersede, fork, contest, or retract that knowledge with evidence. The result is an agent that can improve over time while remaining predictable, auditable, and safe.

## 60-Second Mental Model

1. **Ingest** events, tool outputs, observations, and working state.
2. **Consolidate** episodic traces into semantic facts, competence records, and plan graphs.
3. **Retrieve** in layers with trust gating and salience ranking.
4. **Revise** knowledge with explicit operations and audit trails.
5. **Decay** salience over time unless reinforced by success.

## What You Get

- **Typed Memory** with explicit schemas and lifecycles (not a flat text store)
- **Revisable Knowledge** with provenance and conflict handling
- **Competence Learning** so agents learn *how* to solve problems, not just *what* happened
- **Decay + Consolidation** that keeps memory useful and prunes noise
- **Trust-Aware Retrieval** with sensitivity levels and graduated access
- **Security & Ops**: SQLCipher encryption, optional TLS + API keys, rate limiting, audit logs
- **Observability**: retrieval usefulness, competence success, plan reuse, revision rate
- **gRPC API** with embedded Go library support

## Memory Types

| Type | Purpose | Example |
|------|---------|---------|
| **Episodic** | Raw experience capture (immutable) | Tool calls, errors, observations from a debugging session |
| **Working** | Current task state | "Backend initialized, frontend pending, docs TODO" |
| **Semantic** | Stable facts and preferences | "User prefers Go for backend services" |
| **Competence** | Learned procedures | "To fix linker cache error: clear cache, rebuild with flags" |
| **Plan Graph** | Reusable solution structures | Multi-step project setup workflow as a directed graph |

## Evaluation & Metrics

Membrane exposes behavioral metrics (e.g., retrieval usefulness, competence success rate, plan reuse frequency) via `GetMetrics`, and the test suite covers ingestion, revision, selection, and retrieval ordering.

If you want **recall-style regression checks**, run the recall-focused retrieval test:

```bash
go test ./tests -run TestRetrievalRecallAtK
```

For **vector-aware end-to-end metrics** (optional; requires Python dependencies):

```bash
python3 -m pip install -r tools/eval/requirements.txt
make eval
```

Thresholds are enforced by default (override via env vars):

```bash
MEMBRANE_EVAL_MIN_RECALL=0.90
MEMBRANE_EVAL_MIN_PRECISION=0.20
MEMBRANE_EVAL_MIN_MRR=0.90
MEMBRANE_EVAL_MIN_NDCG=0.90
```

Latest eval results (local run on Feb 5, 2026):

- `go test ./tests -run TestEval -v`
  - 22 top-level eval tests + 7 subtests = 29 test cases
  - Failures: 0
  - Runtime: ~0.40s
- `make eval` (vector E2E)
  - Records: 35, Queries: 18
  - Mean recall@k: 1.000
  - Mean precision@k: 0.267
  - Mean MRR@k: 0.956
  - Mean NDCG@k: 0.955

For **targeted capability evals** (fast, isolated):

```bash
make eval-typed
make eval-revision
make eval-decay
make eval-trust
make eval-competence
make eval-plan
make eval-consolidation
make eval-metrics
make eval-invariants
make eval-grpc
```

To run everything at once:

```bash
make eval-all
```

Note: Membrane itself does not implement vector similarity search. End-to-end recall depends on the retrieval backend and the agent policy driving ingestion and reinforcement. Treat recall tests as scenario-level regression guards rather than universal benchmarks.

## Installation

### Building from Source

```bash
git clone https://github.com/GustyCube/membrane.git
cd membrane

# Build the daemon
make build

# Run tests
make test
```

### Running the Daemon

```bash
# Start with default SQLite storage (unencrypted)
./bin/membraned

# With custom configuration
./bin/membraned --config /path/to/config.yaml

# Override database path or listen address
./bin/membraned --db /path/to/membrane.db --addr :8080
```

## Configuration

Membrane is configured via a YAML file or command-line flags. Environment variables provide secrets:

```yaml
db_path: "membrane.db"
listen_addr: ":9090"
decay_interval: "1h"
consolidation_interval: "6h"
default_sensitivity: "low"
selection_confidence_threshold: 0.7

# Security (keys should come from environment variables)
# encryption_key: ""       # or set MEMBRANE_ENCRYPTION_KEY
# api_key: ""              # or set MEMBRANE_API_KEY
# tls_cert_file: ""
# tls_key_file: ""
rate_limit_per_second: 100
```

| Variable | Purpose |
|----------|---------|
| `MEMBRANE_ENCRYPTION_KEY` | SQLCipher encryption key for the database |
| `MEMBRANE_API_KEY` | Bearer token for gRPC authentication |

## Quick Start

### Using the Go Library Directly

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

### Revision Operations

```go
// Supersede a semantic record (requires evidence in the new record)
newRec := schema.NewMemoryRecord("", schema.MemoryTypeSemantic, schema.SensitivityLow,
    &schema.SemanticPayload{
        Kind:      "semantic",
        Subject:   "Go",
        Predicate: "version",
        Object:    "1.24",
        Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
    },
)
newRec.Provenance.Sources = append(newRec.Provenance.Sources, schema.ProvenanceSource{
    Kind: "observation",
    Ref:  "go-release-notes",
})
superseded, _ := m.Supersede(ctx, oldRecordID, newRec, "agent", "Go version updated")

// Fork a record for conditional validity
forked, _ := m.Fork(ctx, sourceID, conditionalRec, "agent", "different for dev environment")

// Contest a record when conflicting evidence appears
m.Contest(ctx, recordID, conflictingRecordID, "agent", "new evidence contradicts this")

// Retract a record
m.Retract(ctx, recordID, "agent", "no longer accurate")

// Merge multiple records into one
merged, _ := m.Merge(ctx, []string{id1, id2, id3}, mergedRec, "agent", "consolidating duplicates")
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

- **Authoritative Store** - SQLCipher-encrypted SQLite database for metadata, lifecycle state, revision chains, relations, and audit history
- **Structured Payloads** - Type-specific schemas stored as JSON within the authoritative store
- **Relationship Graph** - Relations between records (supersedes, derived_from, contested_by, supports, contradicts) stored in the authoritative store

### Background Jobs

| Job | Default Interval | Purpose |
|-----|-----------------|---------|
| **Decay** | 1 hour | Applies time-based salience decay (exponential/linear curves) |
| **Pruning** | With decay | Deletes records with `auto_prune` policy whose salience has reached 0 |
| **Consolidation** | 6 hours | Extracts semantic facts, competence records, and plan graphs from episodic memory |

### Security Model

- **Encryption at Rest** - SQLCipher with `PRAGMA key` applied at database open
- **TLS Transport** - Optional TLS for gRPC connections
- **Authentication** - Bearer token API key via `authorization` metadata
- **Rate Limiting** - Token bucket limiter (configurable requests/second)
- **Trust-Aware Retrieval** - Records filtered by sensitivity level; records one level above threshold are returned in redacted form (metadata only, no payload)
- **Input Validation** - Payload size limits, string length checks, tag count limits, NaN/Inf rejection

### gRPC API

The gRPC API uses protoc-generated service stubs with JSON-encoded payloads over protobuf `bytes` fields. Endpoints:

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
| `Reinforce` | Boost a record's salience |
| `Penalize` | Reduce a record's salience |
| `GetMetrics` | Retrieve observability metrics snapshot |
| `Contest` | Mark a record as contested by conflicting evidence |

## Observability

The `GetMetrics` endpoint returns a point-in-time snapshot:

```json
{
  "total_records": 142,
  "records_by_type": {"episodic": 80, "semantic": 35, "competence": 15, "plan_graph": 7, "working": 5},
  "avg_salience": 0.62,
  "avg_confidence": 0.78,
  "salience_distribution": {"0.0-0.2": 12, "0.2-0.4": 18, "0.4-0.6": 30, "0.6-0.8": 45, "0.8-1.0": 37},
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

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on:

- Code style and formatting
- Testing requirements
- Pull request process
- Issue reporting

## License

Membrane is released under the [MIT License](LICENSE).

---

**Author:** Bennett Schwartz

**Repository:** [github.com/GustyCube/membrane](https://github.com/GustyCube/membrane)
