# Membrane TypeScript Client

TypeScript/Node SDK for the [Membrane](https://github.com/GustyCube/membrane) memory substrate.

Communicates with the Membrane daemon over gRPC using the protobuf service contract.

## Installation

```bash
npm install @gustycube/membrane
```

## Quick Start

```ts
import { MembraneClient, Sensitivity, SourceKind } from "@gustycube/membrane";

const client = new MembraneClient("localhost:9090", {
  apiKey: "your-api-key"
});

const capture = await client.captureMemory(
  {
    ref: "src/main.ts",
    text: "Refactored auth middleware",
    file: "src/main.ts"
  },
  {
    sourceKind: SourceKind.EVENT,
    reasonToRemember: "Keep the auth refactor in long-term memory",
    summary: "Refactored auth middleware",
    sensitivity: Sensitivity.LOW,
    tags: ["auth", "typescript"]
  }
);

const graph = await client.retrieveGraph("debug auth", {
  trust: {
    max_sensitivity: Sensitivity.MEDIUM,
    authenticated: true,
    actor_id: "agent-1",
    scopes: []
  },
  rootLimit: 10,
  nodeLimit: 25,
  edgeLimit: 100,
  maxHops: 1
});

console.log(capture.primary_record.id, graph.nodes.length);
client.close();
```

## API Surface

The SDK mirrors the Python client behavior and defaults.

### Capture

- `captureMemory(...)` / `capture_memory(...)`

### Retrieval

- `retrieveGraph(...)` / `retrieve_graph(...)`
- `retrieveById(...)` / `retrieve_by_id(...)`

`retrieveGraph()` returns a rooted neighborhood of records plus graph edges and optional selector metadata.

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

## Error Handling

Failed RPCs reject with `MembraneError`. The error exposes a stable shape:

- `code`: numeric gRPC status code when available
- `codeName`: symbolic status name such as `UNAUTHENTICATED`
- `details`: gRPC details string when present
- `metadata`: response metadata normalized to string arrays

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

The common runtime pattern is: capture execution traces or observations, retrieve a graph neighborhood, then pass the rooted context into your model call.

```ts
import OpenAI from "openai";
import { MembraneClient, Sensitivity, SourceKind } from "@gustycube/membrane";

const memory = new MembraneClient("localhost:9090", {
  apiKey: process.env.MEMBRANE_API_KEY
});

const llm = new OpenAI({
  apiKey: process.env.LLM_API_KEY,
});

await memory.captureMemory(
  { text: "Observed a production auth regression", project: "auth" },
  {
    sourceKind: SourceKind.AGENT_TURN,
    reasonToRemember: "Incident handling should accumulate durable context",
    sensitivity: Sensitivity.MEDIUM,
    tags: ["incident", "auth"]
  }
);

const graph = await memory.retrieveGraph("how should I handle this auth incident?", {
  trust: {
    max_sensitivity: Sensitivity.MEDIUM,
    authenticated: true,
    actor_id: "incident-agent",
    scopes: ["prod"],
  },
  memoryTypes: ["semantic", "competence", "working", "entity"],
  rootLimit: 10,
  nodeLimit: 25,
  edgeLimit: 100,
  maxHops: 1,
});

const memoryContext = graph.nodes.map((node) => JSON.stringify(node)).join("\n");

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
