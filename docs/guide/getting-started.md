# Getting Started

Membrane is a selective learning and memory substrate for agentic systems. This guide walks you through installation, running the daemon, and making your first API calls.

## Prerequisites

- **Go 1.24+** (for building from source)
- **SQLite 3** (bundled via `go-sqlite3`)

## Installation

### Build from Source

```bash
git clone https://github.com/GustyCube/membrane.git
cd membrane
make build
```

This produces the `bin/membraned` binary.

### Verify the Build

```bash
./bin/membraned -help
```

```
Usage of membraned:
  -addr string
        gRPC listen address (overrides config)
  -config string
        path to YAML config file
  -db string
        SQLite database path (overrides config)
```

## Starting the Daemon

Start `membraned` with default settings:

```bash
./bin/membraned
```

This starts the gRPC server on `:9090` with an SQLite database at `membrane.db`.

Override with flags:

```bash
./bin/membraned -db /var/lib/membrane/data.db -addr :8080
```

Or use a YAML config file:

```bash
./bin/membraned -config membrane.yaml
```

See [Configuration](/reference/configuration) for all available options.

## First Ingest

Use any gRPC client to call the `IngestEvent` method. Here is an example request body (JSON encoding):

```json
{
  "source": "my-agent",
  "event_kind": "user_input",
  "ref": "session-001/msg-1",
  "summary": "User asked about deployment options",
  "tags": ["deployment", "question"],
  "scope": "project-alpha",
  "sensitivity": "low"
}
```

The response contains the full `MemoryRecord` that was created, including a generated UUID, timestamps, lifecycle metadata, and audit log.

## First Retrieve

Query the memory substrate with a trust context:

```json
{
  "task_descriptor": "answer deployment question",
  "trust": {
    "max_sensitivity": "medium",
    "authenticated": true,
    "actor_id": "agent-1",
    "scopes": ["project-alpha"]
  },
  "min_salience": 0.1,
  "limit": 10
}
```

The response returns matching records sorted by salience, with an optional `selection` field when competence or plan graph candidates are ranked.

## Python Client

A Python client library is available in the `clients/python/` directory:

```python
from membrane import MembraneClient, Sensitivity, TrustContext

client = MembraneClient("localhost:9090")

# Ingest an event
record = client.ingest_event(
    event_kind="user_input",
    ref="session-001/msg-1",
    summary="User asked about deployment options",
    source="my-agent",
)

# Retrieve memories
trust = TrustContext(
    max_sensitivity=Sensitivity.MEDIUM,
    authenticated=True,
    actor_id="agent-1",
)
results = client.retrieve("answer deployment question", trust=trust)
```

## What Next?

- Learn the [Core Concepts](/guide/concepts) behind Membrane's layered memory model
- Explore the five [Memory Types](/guide/memory-types) in detail
- Browse the full [gRPC API](/api/grpc) reference
