# Membrane

[![CI](https://github.com/BennettSchwartz/membrane/actions/workflows/ci.yml/badge.svg)](https://github.com/BennettSchwartz/membrane/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/BennettSchwartz/membrane)](https://goreportcard.com/report/github.com/BennettSchwartz/membrane)
[![Go Reference](https://pkg.go.dev/badge/github.com/BennettSchwartz/membrane.svg)](https://pkg.go.dev/github.com/BennettSchwartz/membrane)
[![License: MIT](https://img.shields.io/github/license/BennettSchwartz/membrane)](LICENSE)

Membrane is a selective learning and memory substrate for LLM agents. It gives
agents typed, revisable memory instead of an append-only transcript or a flat
vector store.

Use it when an agent needs to remember facts, retrieve prior context, revise
stale knowledge, preserve task state, and cite the records that informed an
answer.

## What It Provides

- **Five memory layers plus entities**: episodic, working, semantic,
  competence, plan graph, and canonical entity nodes that connect them.
- **Entity-connected retrieval**: `RetrieveGraph` returns ranked records plus
  bounded graph neighborhoods, not only top-k chunks.
- **Capture-first ingestion**: `CaptureMemory` accepts events, observations,
  tool outputs, working state, and facts, then creates linked records and edges.
- **Revision operations**: supersede, fork, retract, merge, and contest records
  with audit trails.
- **Decay and reinforcement**: salience changes over time and can be reinforced
  or penalized by outcomes.
- **Trust-aware access**: retrieval filters or redacts records using sensitivity,
  authentication, and scope checks.
- **Multiple runtimes**: run `membraned` over gRPC, use the embedded Go API, or
  call it from TypeScript/Python SDKs.
- **Self-hosted docs**: docs are Docusaurus-based and deployable to Cloudflare
  Workers with Wrangler.

## Memory Model

| Layer | Purpose | Typical contents |
| --- | --- | --- |
| Episodic | Raw experience | Tool calls, incidents, observations, agent turns |
| Working | Active task state | Current goal, next actions, open questions |
| Semantic | Durable facts | Preferences, system facts, relationships |
| Competence | Learned procedures | Debugging playbooks, success rates, applicability |
| Plan graph | Reusable plans | Rollout DAGs, checkpoints, dependencies |
| Entity | Graph spine | Projects, services, tools, files, people, databases |

The entity layer is not a replacement for the five memory layers. It is the
shared graph layer that lets facts, episodes, procedures, plans, and task state
retrieve together.

## Quick Start

### Requirements

- Go 1.22+
- Make
- Node.js 20+ for the TypeScript SDK, docs, and examples
- Python 3.10+ for the Python SDK
- `protoc` only when regenerating protobuf code

### Build And Run

```bash
git clone https://github.com/BennettSchwartz/membrane.git
cd membrane

make build
./bin/membraned
```

By default, `membraned` uses local SQLite storage. To use Postgres:

```bash
./bin/membraned \
  --postgres-dsn "postgres://membrane:membrane@localhost:5432/membrane?sslmode=disable"
```

Useful commands:

```bash
make test                 # Go test suite
make proto                # Regenerate Go protobuf bindings
make ts-build             # Build the TypeScript SDK
make eval-all             # Targeted evaluation suite
```

## TypeScript SDK

```bash
npm --prefix clients/typescript install
npm --prefix clients/typescript run build
```

```ts
import {
  MembraneClient,
  MemoryType,
  Sensitivity,
  SourceKind,
} from "@bennettschwartz/membrane";

const client = new MembraneClient("localhost:9090", {
  apiKey: process.env.MEMBRANE_API_KEY,
});

const capture = await client.captureMemory(
  {
    subject: "auth-service",
    predicate: "uses_database",
    object: "PostgreSQL",
  },
  {
    source: "agent",
    sourceKind: SourceKind.OBSERVATION,
    summary: "auth-service uses PostgreSQL",
    tags: ["auth-service", "postgres"],
    scope: "project-orion",
    sensitivity: Sensitivity.LOW,
  },
);

const graph = await client.retrieveGraph("debug auth-service latency", {
  trust: {
    max_sensitivity: Sensitivity.MEDIUM,
    authenticated: true,
    actor_id: "debug-agent",
    scopes: ["project-orion"],
  },
  memoryTypes: [
    MemoryType.ENTITY,
    MemoryType.EPISODIC,
    MemoryType.WORKING,
    MemoryType.SEMANTIC,
    MemoryType.COMPETENCE,
    MemoryType.PLAN_GRAPH,
  ],
  rootLimit: 12,
  nodeLimit: 64,
  edgeLimit: 160,
  maxHops: 2,
});

console.log(capture.primary_record.id, graph.nodes.length);
client.close();
```

See [clients/typescript](clients/typescript) for the full SDK.

## Python SDK

```bash
python -m pip install -e "clients/python[dev]"
python -m pytest clients/python/tests
```

```python
from membrane import MembraneClient, MemoryType, Sensitivity, SourceKind

client = MembraneClient("localhost:9090")

capture = client.capture_memory(
    {
        "thread_id": "incident-42",
        "state": "investigating auth-service latency",
        "next_actions": ["check database wait", "hold canary"],
    },
    source="agent",
    source_kind=SourceKind.WORKING_STATE,
    summary="Incident state updated",
    tags=["auth-service", "incident"],
    sensitivity=Sensitivity.LOW,
)

graph = client.retrieve_graph(
    "auth-service canary incident",
    memory_types=[
        MemoryType.ENTITY,
        MemoryType.WORKING,
        MemoryType.SEMANTIC,
        MemoryType.COMPETENCE,
        MemoryType.PLAN_GRAPH,
    ],
)

print(capture.primary_record.id, len(graph.nodes))
client.close()
```

See [clients/python](clients/python) for package details.

## Go Library

Membrane can also run embedded in a Go process:

```go
package main

import (
	"context"
	"log"

	"github.com/BennettSchwartz/membrane/pkg/ingestion"
	"github.com/BennettSchwartz/membrane/pkg/membrane"
	"github.com/BennettSchwartz/membrane/pkg/retrieval"
	"github.com/BennettSchwartz/membrane/pkg/schema"
)

func main() {
	m, err := membrane.New(membrane.DefaultConfig())
	if err != nil {
		log.Fatal(err)
	}
	defer m.Stop()

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		log.Fatal(err)
	}

	capture, err := m.CaptureMemory(ctx, ingestion.CaptureMemoryRequest{
		Source:     "agent",
		SourceKind: "observation",
		Content: map[string]any{
			"subject":   "auth-service",
			"predicate": "uses_database",
			"object":    "PostgreSQL",
		},
		Summary: "auth-service uses PostgreSQL",
		Tags:    []string{"auth-service", "postgres"},
	})
	if err != nil {
		log.Fatal(err)
	}

	graph, err := m.RetrieveGraph(ctx, &retrieval.RetrieveGraphRequest{
		TaskDescriptor: "debug auth-service latency",
		Trust: &retrieval.TrustContext{
			MaxSensitivity: schema.SensitivityMedium,
			Authenticated:  true,
		},
		MemoryTypes: []schema.MemoryType{
			schema.MemoryTypeEntity,
			schema.MemoryTypeSemantic,
			schema.MemoryTypeCompetence,
		},
		RootLimit: 10,
		MaxHops:   2,
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("captured=%s graph_nodes=%d", capture.PrimaryRecord.ID, len(graph.Nodes))
}
```

## Agent Harness

The official harness in [examples/agent-harness](examples/agent-harness) tests
Membrane as optional OpenAI-compatible model tools.

It starts or connects to `membraned`, seeds a coherent incident scenario in
SQLite, verifies that all five memory layers are connected through entities, and
runs both deterministic and live LLM checks.

```bash
npm --prefix clients/typescript run build
cd examples/agent-harness
npm install
npm run test:deterministic

# Optional live run with OpenRouter or OpenAI credentials
OPENROUTER_API_KEY=... npm run test:llm
```

The live test uses natural-language prompts and `tool_choice: "auto"`; it
asserts that the model chooses retrieval and capture tools when those actions
are useful, instead of forcing tool calls in the harness.

## Documentation

The docs site lives in [docs](docs), is built with Docusaurus, and is deployed
to Cloudflare Workers at <https://membrane.gustycube.com>.

```bash
npm install
npm run docs:dev
npm run docs:build

# Deploy to Cloudflare Workers when Wrangler is configured
npm run docs:deploy
```

Cloudflare configuration is in [wrangler.jsonc](wrangler.jsonc). The GitHub
Actions deploy workflow runs when docs files change and deploys when
`CLOUDFLARE_API_TOKEN` is present as a repository secret. The
`membrane.gustycube.com` DNS record must be proxied through Cloudflare for the
Worker route to receive production traffic. Sidebar and theme configuration live
in [sidebars.js](sidebars.js) and [docusaurus.config.js](docusaurus.config.js).

## API Surface

The gRPC API is defined in
[api/proto/membrane/v1/membrane.proto](api/proto/membrane/v1/membrane.proto).
Core RPCs include:

- `CaptureMemory`
- `RetrieveGraph`
- `RetrieveByID`
- `Supersede`
- `Fork`
- `Retract`
- `Merge`
- `Contest`
- `Reinforce`
- `Penalize`
- `GetMetrics`

Generated Go, TypeScript, and Python bindings are committed so consumers do not
need protobuf tooling for normal development.

## Storage And Configuration

Membrane supports SQLite for local and embedded use, and Postgres for concurrent
deployments. Postgres can optionally use pgvector for embedding-backed ranking.

Common environment variables:

| Variable | Purpose |
| --- | --- |
| `MEMBRANE_API_KEY` | Bearer token for gRPC requests |
| `MEMBRANE_ENCRYPTION_KEY` | SQLCipher encryption key |
| `MEMBRANE_POSTGRES_DSN` | Postgres connection string |
| `MEMBRANE_EMBEDDING_API_KEY` | API key for embedding-backed retrieval |
| `MEMBRANE_LLM_API_KEY` | API key for background semantic extraction |
| `MEMBRANE_INGEST_LLM_API_KEY` | API key for ingest-time interpretation |

See [docs/guides/configuration.mdx](docs/guides/configuration.mdx) for full
configuration details.

## Project Layout

```text
api/                  Protobuf definitions and generated Go gRPC bindings
clients/typescript/   TypeScript SDK
clients/python/       Python SDK
cmd/membraned/        Daemon entrypoint
docs/                 Docusaurus docs content
examples/             Runnable examples, including the agent harness
pkg/                  Core Go packages
tests/                Evaluation and integration tests
```

## Development

Run the checks most often used before committing:

```bash
make build
make test
npm --prefix clients/typescript run build
npm --prefix examples/agent-harness run typecheck
npm --prefix examples/agent-harness run test:deterministic
npm run docs:build
```

Live harness testing needs an OpenRouter or OpenAI-compatible key:

```bash
OPENROUTER_API_KEY=... npm --prefix examples/agent-harness run test:llm
```

## Contributing

Issues and PRs are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for local
workflow and expectations.

## License

MIT. See [LICENSE](LICENSE).
