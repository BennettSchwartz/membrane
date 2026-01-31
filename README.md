# Membrane

[![Go Version](https://img.shields.io/badge/Go-1.24+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A general-purpose selective learning and memory substrate for agentic systems.

## Overview

Membrane is an open-source memory system that enables AI agents to genuinely learn from experience. Unlike traditional retrieval-augmented generation (RAG) approaches that treat memory as an append-only text store, Membrane provides:

- **Typed Memory** - Five distinct memory classes with explicit schemas, lifecycles, and semantics
- **Revisable Knowledge** - Facts, preferences, and procedures can be updated, forked, contested, or retracted based on evidence
- **Competence Learning** - Agents learn *how* to solve problems, not just *what* happened
- **Decay and Consolidation** - Salience-based memory management that reinforces useful knowledge and prunes the irrelevant
- **Trust-Aware Retrieval** - Sensitivity-gated access with graduated redaction for sensitive records
- **Encryption at Rest** - SQLCipher-backed storage with optional TLS, authentication, and rate limiting

Membrane solves the fundamental problem of agent memory: enabling systems to improve over time in a Jarvis-like manner while remaining predictable, auditable, and safe.

## Features

### Five Memory Types

| Type | Purpose | Example |
|------|---------|---------|
| **Episodic** | Raw experience capture (immutable) | Tool calls, errors, observations from a debugging session |
| **Working** | Current task state | "Backend initialized, frontend pending, docs TODO" |
| **Semantic** | Stable facts and preferences | "User prefers Go for backend services" |
| **Competence** | Learned procedures | "To fix linker cache error: clear cache, rebuild with flags" |
| **Plan Graph** | Reusable solution structures | Multi-step project setup workflow as a directed graph |

### Core Capabilities

- **Automatic Learning** - Memory creation without explicit user approval
- **Consolidation Pipeline** - Episodic traces are analyzed and promoted to semantic facts, competence records, and plan graphs
- **Revision Semantics** - Supersede, fork, retract, merge, or contest conflicting knowledge
- **Evidence-Based Revisions** - Semantic revisions require provenance or evidence references
- **Salience Decay** - Exponential/linear decay with configurable half-lives and reinforcement on successful use
- **Auto-Pruning** - Records whose salience drops to the floor are automatically deleted based on deletion policy
- **Trust-Aware Retrieval** - Five sensitivity levels (public, low, medium, high, hyper) with graduated redaction
- **Encryption at Rest** - SQLCipher-encrypted database with key via config or `MEMBRANE_ENCRYPTION_KEY` env var
- **TLS and Authentication** - Optional TLS transport and Bearer token API key authentication
- **Rate Limiting** - Token bucket rate limiter for gRPC endpoints
- **Audit Logging** - Complete mutation history for all memory records
- **Observability** - Metrics for memory growth rate, salience distribution, retrieval usefulness, competence success rates, plan reuse frequency, and revision rates
- **gRPC API** - High-performance interface with protoc-generated service stubs and JSON-encoded payloads

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
