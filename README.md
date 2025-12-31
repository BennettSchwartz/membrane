# Membrane

[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A general-purpose selective learning and memory substrate for agentic systems.

## Overview

Membrane is an open-source memory system that enables AI agents to genuinely learn from experience. Unlike traditional retrieval-augmented generation (RAG) approaches that treat memory as an append-only text store, Membrane provides:

- **Typed Memory** - Five distinct memory classes with explicit schemas, lifecycles, and semantics
- **Revisable Knowledge** - Facts, preferences, and procedures can be updated, forked, or retracted based on new evidence
- **Competence Learning** - Agents learn *how* to solve problems, not just *what* happened
- **Decay and Consolidation** - Salience-based memory management that reinforces useful knowledge and prunes the irrelevant
- **Trust-Aware Retrieval** - Sensitivity-gated access that respects security contexts

Membrane solves the fundamental problem of agent memory: enabling systems to improve over time in a Jarvis-like manner while remaining predictable, auditable, and safe.

## Features

### Five Memory Types

| Type | Purpose | Example |
|------|---------|---------|
| **Episodic** | Raw experience capture | Tool calls, errors, observations from a debugging session |
| **Working** | Current task state | "Backend initialized, frontend pending, docs TODO" |
| **Semantic** | Stable facts and preferences | "User prefers Go for backend services" |
| **Competence** | Learned procedures | "To fix linker cache error: clear cache, rebuild with flags" |
| **Plan Graph** | Reusable solution structures | Multi-step project setup workflow |

### Core Capabilities

- **Automatic Learning** - Memory creation without explicit user approval
- **Consolidation Pipeline** - Episodic traces are analyzed and promoted to durable knowledge
- **Revision Semantics** - Supersede, fork, retract, or merge conflicting knowledge
- **Salience Decay** - Time-based decay with reinforcement on successful use
- **Trust-Aware Retrieval** - Sensitivity levels (public, low, medium, high, hyper) gate access
- **Audit Logging** - Complete mutation history for all memory records
- **gRPC API** - High-performance interface for agent integration
- **Python Client** - Native bindings for Python-based agents

## Installation

### Building from Source

```bash
# Clone the repository
git clone https://github.com/GustyCube/membrane.git
cd membrane

# Build the daemon
make build

# Run tests
make test
```

### Running the Daemon

```bash
# Start the membrane daemon
./bin/membraned

# With custom configuration
./bin/membraned --config /path/to/config.yaml
```

### Python Client

```bash
pip install membrane-client
```

## Quick Start

### Starting the Daemon

```bash
# Start with default SQLite storage
./bin/membraned

# The daemon listens on localhost:9090 by default
```

### Basic Ingestion via gRPC

```go
package main

import (
    "context"
    pb "github.com/GustyCube/membrane/proto"
    "google.golang.org/grpc"
)

func main() {
    conn, _ := grpc.Dial("localhost:9090", grpc.WithInsecure())
    defer conn.Close()

    client := pb.NewMembraneClient(conn)

    // Ingest an episodic event
    _, err := client.Ingest(context.Background(), &pb.IngestRequest{
        Type: pb.MemoryType_EPISODIC,
        Payload: &pb.Payload{
            Timeline: []*pb.Event{
                {Kind: "tool_call", Summary: "Executed build command"},
                {Kind: "observation", Summary: "Build failed with linker error"},
            },
        },
    })
}
```

### Retrieval with Trust Context

```go
// Retrieve relevant memories for a task
resp, _ := client.Retrieve(context.Background(), &pb.RetrieveRequest{
    TaskDescriptor: "fix build error",
    TrustContext: &pb.TrustContext{
        MaxSensitivity: pb.Sensitivity_MEDIUM,
        Authenticated:  true,
    },
    MemoryTypes: []pb.MemoryType{
        pb.MemoryType_COMPETENCE,
        pb.MemoryType_SEMANTIC,
    },
})

for _, record := range resp.Records {
    fmt.Printf("Found: %s (confidence: %.2f)\n", record.Id, record.Confidence)
}
```

### Python Client Usage

```python
from membrane import MembraneClient, MemoryType, Sensitivity

# Connect to the daemon
client = MembraneClient("localhost:9090")

# Store a semantic preference
client.ingest(
    memory_type=MemoryType.SEMANTIC,
    payload={
        "subject": "user",
        "predicate": "prefers_language",
        "object": "python",
        "validity": {"mode": "conditional", "conditions": {"project_type": "data_science"}}
    },
    sensitivity=Sensitivity.LOW
)

# Retrieve competence records for troubleshooting
records = client.retrieve(
    task="debug test failure",
    memory_types=[MemoryType.COMPETENCE],
    max_sensitivity=Sensitivity.MEDIUM
)

for record in records:
    print(f"Skill: {record.payload['skill_name']}")
    for step in record.payload['recipe']:
        print(f"  - {step['step']}")
```

## Architecture

Membrane runs as a long-lived daemon process that exposes a gRPC API. The architecture is organized into three logical planes:

```
+------------------+     +------------------+     +----------------------+
|  Ingestion Plane |---->|   Policy Plane   |---->| Storage & Retrieval  |
+------------------+     +------------------+     +----------------------+
        |                        |                         |
   Events, tool            Classification,            SQLite (auth),
   outputs, obs.           lifecycle rules            vector index,
                                                      artifacts
```

### Storage Model

- **Authoritative Store** - SQLite database for metadata, lifecycle state, revision chains, and audit history
- **Vector Index** - Optional acceleration for semantic similarity search (advisory only, not source of truth)
- **Artifact Store** - Content-addressed storage for large binary objects (logs, screenshots)

### Background Jobs

- **Consolidation** - Periodic analysis of episodic memory to extract semantic updates, competence records, and plan graphs
- **Decay Processing** - Salience score updates based on time and usage patterns
- **Pruning** - Removal of records below salience threshold

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
