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

## LLM Integration Pattern

The common runtime pattern is: ingest execution traces, retrieve relevant memory, then pass that context into your model call.

```ts
import OpenAI from "openai";
import { MembraneClient, Sensitivity } from "@gustycube/membrane";

const memory = new MembraneClient("localhost:9090", {
  apiKey: process.env.MEMBRANE_API_KEY
});

const llm = new OpenAI({
  apiKey: process.env.LLM_API_KEY,
  // For OpenRouter or other OpenAI-compatible providers:
  // baseURL: "https://openrouter.ai/api/v1",
});

const records = await memory.retrieve("how should I handle this incident?", {
  trust: {
    max_sensitivity: Sensitivity.MEDIUM,
    authenticated: true,
    actor_id: "incident-agent",
    scopes: ["prod"],
  },
  memoryTypes: ["semantic", "competence", "working"],
  limit: 10,
});

const memoryContext = records.map((r) => JSON.stringify(r)).join("\n");

const completion = await llm.chat.completions.create({
  model: "gpt-5.2",
  messages: [
    { role: "system", content: "Use the memory context as evidence. Cite record ids." },
    { role: "user", content: `Incident task:\n...\n\nMemory:\n${memoryContext}` },
  ],
});

console.log(completion.choices[0]?.message?.content);
memory.close();
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
