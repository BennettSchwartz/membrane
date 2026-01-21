# Membrane Python Client

Python client library for the [Membrane](https://github.com/GustyCube/membrane) memory substrate.

Communicates with the Membrane daemon over gRPC using JSON-encoded messages.

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
from membrane import MembraneClient, Sensitivity, TrustContext

# Connect to the running Membrane daemon
client = MembraneClient("localhost:9090")

# Ingest an event
record = client.ingest_event(
    event_kind="file_edit",
    ref="src/main.py",
    summary="Refactored authentication module",
    sensitivity=Sensitivity.LOW,
)
print(f"Created record: {record.id}")

# Retrieve memories relevant to a task
trust = TrustContext(
    max_sensitivity=Sensitivity.MEDIUM,
    authenticated=True,
    actor_id="agent-1",
)
records = client.retrieve("fix the login bug", trust=trust, limit=5)
for r in records:
    print(f"  [{r.type.value}] {r.id} (salience={r.salience:.2f})")

# Reinforce a useful memory
client.reinforce(record.id, actor="agent-1", rationale="Used successfully")

# Clean up
client.close()
```

### Context Manager

```python
with MembraneClient("localhost:9090") as client:
    record = client.ingest_observation(
        subject="user",
        predicate="prefers",
        obj={"language": "Python"},
        sensitivity=Sensitivity.LOW,
    )
```

## API Reference

### Ingestion

| Method | Description |
|--------|-------------|
| `ingest_event(event_kind, ref, ...)` | Ingest a raw event |
| `ingest_tool_output(tool_name, ...)` | Ingest tool invocation output |
| `ingest_observation(subject, predicate, obj, ...)` | Ingest a semantic triple |
| `ingest_outcome(target_record_id, outcome_status, ...)` | Attach an outcome to an existing record |

### Retrieval

| Method | Description |
|--------|-------------|
| `retrieve(task_descriptor, ...)` | Retrieve memories relevant to a task |
| `retrieve_by_id(record_id, ...)` | Retrieve a single record by ID |

### Revision

| Method | Description |
|--------|-------------|
| `supersede(old_id, new_record, actor, rationale)` | Replace a record with a new version |
| `fork(source_id, forked_record, actor, rationale)` | Create a conditional variant |
| `retract(record_id, actor, rationale)` | Soft-delete a record |
| `merge(record_ids, merged_record, actor, rationale)` | Merge multiple records |

### Reinforcement

| Method | Description |
|--------|-------------|
| `reinforce(record_id, actor, rationale)` | Boost a record's salience |
| `penalize(record_id, amount, actor, rationale)` | Reduce a record's salience |

### Metrics

| Method | Description |
|--------|-------------|
| `get_metrics()` | Get a snapshot of daemon metrics |

## Requirements

- Python >= 3.10
- `grpcio >= 1.60.0`
- A running Membrane daemon (default: `localhost:9090`)
