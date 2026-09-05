# Membrane TypeScript Client

TypeScript/Node SDK for the [Membrane](https://github.com/BennettSchwartz/membrane) memory substrate.

Communicates with the Membrane daemon over gRPC using the protobuf service contract.

## Installation

```bash
npm install @bennettschwartz/membrane
```

## Quick Start

```ts
import { MembraneClient, MemoryType, Sensitivity, SourceKind } from "@bennettschwartz/membrane";

const client = new MembraneClient("localhost:9090");

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
    scope: "default",
    tags: ["auth", "typescript"]
  }
);

const graph = await client.retrieveGraph("debug auth", {
  trust: {
    max_sensitivity: Sensitivity.LOW,
    authenticated: false,
    actor_id: "",
    scopes: ["default"]
  },
  memoryTypes: [MemoryType.EPISODIC, MemoryType.ENTITY],
  rootLimit: 10,
  nodeLimit: 25,
  edgeLimit: 100,
  rootOnly: false,
  maxHops: 1
});

if (graph.diagnostics?.length) {
  console.warn("Retrieval used fallback ranking", graph.diagnostics);
}

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

`retrieveGraph()` returns a rooted neighborhood of records plus graph edges, optional selector metadata, and optional non-fatal retrieval diagnostics. It accepts precomputed query vectors with `queryEmbedding` and roots-only retrieval with `rootOnly`. Supplied query vectors are validated before the RPC and must be finite with at least one non-zero value. Graph budgets, metadata shapes, and enum fields are also validated client-side: `minSalience` must be finite and between `0` and `1`, `rootLimit`/`nodeLimit`/`edgeLimit` must be integers from `0` to `10000`, `maxHops` must be an integer no lower than `-1`, `rootOnly` must be boolean, `memoryTypes` must use public API memory type values, `trust.max_sensitivity` must use a public API sensitivity value, `trust.authenticated` must be boolean, `trust.actor_id` must be a string, and `trust.scopes` must be an array of strings. `retrieveById()`, revision methods, `reinforce()`, and `penalize()` reject empty required record IDs before transport, while `contest()` still permits an empty `contestingRef` for external evidence. Revision replacement records also validate semantic, entity, and relation invariants before transport; outbound semantic and entity records may use either SDK snake_case fields or Go-style JSON fields such as `Subject`, `Predicate`, `Object`, `Validity`, `CanonicalName`, and `Identifiers`. `captureMemory()` validates `source`, `sourceKind`, `proposedType`, `sensitivity`, `reasonToRemember`, `summary`, `timestamp`, `scope`, and `tags` before sending the RPC.

Option objects accept camelCase names and protobuf-style snake_case aliases. For example, `retrieve_graph("debug auth", { root_only: true, query_embedding: [...] })` is equivalent to the camelCase form.

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

Returns a typed `MetricsSnapshot`, including pgvector coverage fields such as `embedded_records`, `missing_embeddings`, and `embedding_coverage`.

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

The SDK refuses to attach `apiKey` metadata to plaintext gRPC connections
unless the address is loopback. For trusted local-network development without
TLS, set `allowInsecureCredentials: true` explicitly. When the loopback
plaintext exception is used, the SDK disables gRPC HTTP proxying so proxy
environment variables cannot move the credential-bearing connection off-host.
TLS and the explicit insecure development override retain normal proxy behavior.

## LLM Integration Pattern

The common runtime pattern is: capture execution traces or observations, retrieve a graph neighborhood, then pass the rooted context into your model call.

```ts
import OpenAI from "openai";
import { MembraneClient, Sensitivity, SourceKind } from "@bennettschwartz/membrane";

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
  model: "gpt-5.5",
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

- Node.js 20.19+
- A running Membrane daemon (default: `localhost:9090`)
