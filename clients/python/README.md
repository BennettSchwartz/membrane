# Membrane Python Client

Python client library for the [Membrane](https://github.com/BennettSchwartz/membrane) memory substrate.

Communicates with the Membrane daemon over gRPC using the protobuf-defined
`membrane.v1.MembraneService` contract.

Several RPC fields still carry JSON-encoded payloads inside protobuf `bytes`
fields, but the client handles that encoding internally.

## Installation

```bash
pip install -e clients/python
```

Or install from the project root:

```bash
pip install -e clients/python[dev]   # includes pytest
```

## Quick Start

```python
from membrane import MembraneClient, Sensitivity, SourceKind, TrustContext

# Connect to the running Membrane daemon
client = MembraneClient("localhost:9090")

# Capture a rich memory candidate
capture = client.capture_memory(
    {"ref": "src/main.py", "text": "Refactored authentication module"},
    source_kind=SourceKind.EVENT,
    reason_to_remember="Keep the auth refactor available for future debugging",
    summary="Refactored authentication module",
    sensitivity=Sensitivity.LOW,
)
print(f"Created record: {capture.primary_record.id}")

# Retrieve memories relevant to a task
trust = TrustContext(
    max_sensitivity=Sensitivity.MEDIUM,
    authenticated=True,
    actor_id="agent-1",
)
graph = client.retrieve_graph("fix the login bug", trust=trust, root_limit=5)
for node in graph.nodes:
    print(f"  [{node.record.type.value}] {node.record.id} hop={node.hop}")

# Reinforce a useful memory
client.reinforce(capture.primary_record.id, actor="agent-1", rationale="Used successfully")

# Clean up
client.close()
```

### Context Manager

```python
with MembraneClient("localhost:9090") as client:
    capture = client.capture_memory(
        {"subject": "user", "predicate": "prefers", "object": {"language": "Python"}},
        source_kind=SourceKind.OBSERVATION,
        sensitivity=Sensitivity.LOW,
    )
```

## API Reference

### Capture

| Method | Description |
|--------|-------------|
| `capture_memory(content, ...)` | Capture a rich memory candidate for interpretation and graph linking |

### Retrieval

| Method | Description |
|--------|-------------|
| `retrieve_graph(task_descriptor, ...)` | Retrieve a rooted graph neighborhood relevant to a task |
| `retrieve_by_id(record_id, ...)` | Retrieve a single record by ID |

### Revision

| Method | Description |
|--------|-------------|
| `supersede(old_id, new_record, actor, rationale)` | Replace a record with a new version |
| `fork(source_id, forked_record, actor, rationale)` | Create a conditional variant |
| `retract(record_id, actor, rationale)` | Soft-delete a record |
| `merge(record_ids, merged_record, actor, rationale)` | Merge multiple records |
| `contest(record_id, contesting_ref, actor, rationale)` | Mark a record as contested |

### Reinforcement

| Method | Description |
|--------|-------------|
| `reinforce(record_id, actor, rationale)` | Boost a record's salience |
| `penalize(record_id, amount, actor, rationale)` | Reduce a record's salience |

### Metrics

| Method | Description |
|--------|-------------|
| `get_metrics()` | Get a snapshot of daemon metrics |

## TLS & Authentication

```python
client = MembraneClient(
    "membrane.example.com:443",
    tls=True,                        # use TLS transport
    tls_ca_cert="/path/to/ca.pem",   # optional custom CA
    api_key="your-api-key",          # Bearer token auth
    timeout=10.0,                    # default timeout in seconds
)
```

## Requirements

- Python >= 3.10
- `grpcio >= 1.78.0`
- `protobuf >= 6.31.1`
- A running Membrane daemon (default: `localhost:9090`)
