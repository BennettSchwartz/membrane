# Membrane Python Client

Python client library for the [Membrane](https://github.com/BennettSchwartz/membrane) memory substrate.

Communicates with the Membrane daemon over gRPC using the protobuf-defined
`membrane.v1.MembraneService` contract.

Arbitrary content fields use `google.protobuf.Value`; memory records, graph
nodes, graph edges, capture responses, graph retrieval responses, and metrics
envelopes are typed protobuf messages.

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

`retrieve_graph(..., query_embedding=[...])` accepts a precomputed query vector and validates it before the RPC. Non-empty vectors must be numeric, finite, and include at least one non-zero value; strings, booleans, `NaN`, and infinities are rejected. Graph budgets, metadata shapes, and enum fields are also validated client-side: `min_salience` must be finite and between `0` and `1`, `root_limit`/`node_limit`/`edge_limit` must be integers from `0` to `10000`, `max_hops` must be an integer no lower than `-1`, `root_only` must be boolean, `memory_types` must use public API memory type values, `trust.max_sensitivity` must use a public API sensitivity value, `trust.authenticated` must be boolean, `trust.actor_id` must be a string, and `trust.scopes` must be a sequence of strings. `retrieve_by_id()`, revision methods, `reinforce()`, and `penalize()` reject empty required record IDs before transport, while `contest()` still permits an empty `contesting_ref` for external evidence. Revision replacement records also validate semantic, entity, and relation invariants before transport; outbound semantic and entity records may use either SDK snake_case fields or Go-style JSON fields such as `Subject`, `Predicate`, `Object`, `Validity`, `CanonicalName`, and `Identifiers`. `capture_memory()` validates `source`, `source_kind`, `proposed_type`, `sensitivity`, `reason_to_remember`, `summary`, `timestamp`, `scope`, and `tags` before sending the RPC; plain strings are rejected for `tags` and `trust.scopes` to avoid accidental per-character lists.

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
| `get_metrics()` | Get a typed `MetricsSnapshot`, including pgvector coverage fields such as `embedded_records`, `missing_embeddings`, and `embedding_coverage` |

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

The SDK refuses to attach `api_key` metadata to plaintext gRPC connections
unless the address is loopback. For trusted local-network development without
TLS, set `allow_insecure_credentials=True` explicitly on both the client and
the Membrane server configuration. When the loopback plaintext exception is
used, the SDK disables gRPC HTTP proxying so proxy environment variables cannot
move the credential-bearing connection off-host. TLS and the explicit insecure
development override retain normal proxy behavior.

## Requirements

- Python >= 3.10
- `grpcio >= 1.80.0`
- `protobuf >= 6.31.1`
- A running Membrane daemon (default: `localhost:9090`)
- Inline type metadata via `py.typed` for type checkers

## Development

```bash
pip install -e clients/python[dev]
python clients/python/scripts/check_proto_sync.py
python clients/python/scripts/check_package.py
python -m pytest clients/python/tests
```
