# Membrane TypeScript Client

TypeScript/Node SDK for the [Membrane](https://github.com/GustyCube/membrane) memory substrate.

Communicates with the Membrane daemon over gRPC using the protobuf service contract.

## Installation

```bash
npm install @gustycube/membrane
```

## Quick Start

```ts
import { MembraneClient, Sensitivity } from "@gustycube/membrane";

const client = new MembraneClient("localhost:9090", {
  apiKey: "your-api-key"
});

const record = await client.ingestEvent("file_edit", "src/main.ts", {
  summary: "Refactored auth middleware",
  sensitivity: Sensitivity.LOW,
  tags: ["auth", "typescript"]
});

const records = await client.retrieve("debug auth", {
  trust: {
    max_sensitivity: Sensitivity.MEDIUM,
    authenticated: true,
    actor_id: "agent-1",
    scopes: []
  },
  limit: 10
});

console.log(record.id, records.length);
client.close();
```

## API Surface

The SDK mirrors the Python client behavior and defaults.

### Ingestion

- `ingestEvent(...)` / `ingest_event(...)`
- `ingestToolOutput(...)` / `ingest_tool_output(...)`
- `ingestObservation(...)` / `ingest_observation(...)`
- `ingestOutcome(...)` / `ingest_outcome(...)`
- `ingestWorkingState(...)` / `ingest_working_state(...)`

### Retrieval

- `retrieve(...)`
- `retrieveById(...)` / `retrieve_by_id(...)`

### Revision

- `supersede(...)`
- `fork(...)`
- `retract(...)`
- `merge(...)`
- `contest(...)`

### Reinforcement

- `reinforce(...)`
- `penalize(...)`

### Metrics

- `getMetrics()` / `get_metrics()`

## TLS and Authentication

```ts
const client = new MembraneClient("membrane.example.com:443", {
  tls: true,
  tlsCaCertPath: "/path/to/ca.pem",
  apiKey: "your-api-key",
  timeoutMs: 10_000
});
```

## Development

```bash
cd clients/typescript
npm install
npm run check:proto-sync
npm run typecheck
npm test
npm run build
```

### Proto Sync

The SDK keeps a local proto copy in `clients/typescript/proto/`.

```bash
npm run sync:proto
npm run check:proto-sync
```

## Requirements

- Node.js 20+
- A running Membrane daemon (default: `localhost:9090`)
