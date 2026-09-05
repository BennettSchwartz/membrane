<p align="center">
  <img src="static/img/logo-mark.svg" width="72" height="72" alt="Membrane logo">
</p>

<h1 align="center">Membrane</h1>

<p align="center">
  <strong>Memory that agents can connect, revise, and learn from.</strong>
</p>

<p align="center">
  <a href="https://membrane.gustycube.com">Documentation</a> ·
  <a href="https://membrane.gustycube.com/quickstart">Quickstart</a> ·
  <a href="https://membrane.gustycube.com/architecture">Architecture</a> ·
  <a href="https://membrane.gustycube.com/api/overview">API reference</a>
</p>

[![CI](https://github.com/BennettSchwartz/membrane/actions/workflows/ci.yml/badge.svg)](https://github.com/BennettSchwartz/membrane/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/BennettSchwartz/membrane.svg)](https://pkg.go.dev/github.com/BennettSchwartz/membrane)
[![License: MIT](https://img.shields.io/github/license/BennettSchwartz/membrane)](LICENSE)

Membrane gives LLM agents persistent, structured memory. An agent can remember
what happened, connect it to existing knowledge, retrieve the context a task
needs, and correct that knowledge when circumstances change.

Use it for agents that carry work across sessions, maintain changing facts,
reuse successful procedures, or need to explain which memories informed an answer.

## What changes when an agent has memory?

- **Context stays connected.** Facts, episodes, procedures, and plans link through
  shared entities. Retrieval returns a bounded graph of relevant records.
- **Knowledge can be corrected.** Supersede stale facts, contest uncertain claims,
  fork alternatives, or retract a record while retaining its audit history.
- **Experience influences future work.** Reinforcement, decay, and consolidation
  change what gets remembered and retrieved over time.
- **Access follows policy.** Scope and sensitivity rules govern reads and writes;
  restricted responses omit or redact information the caller cannot access.

## How it fits together

```mermaid
flowchart LR
    Experience["Events, observations<br/>and task state"] --> Memory["Typed memory<br/>+ entity links"]
    Memory --> Context["Relevant context<br/>within access limits"]
    Context --> Agent["Agent acts"]
    Agent -->|"New evidence and outcomes"| Memory
```

**Five memory layers, connected by entities:**

| Memory | The question it answers | Example |
| :--- | :--- | :--- |
| **Episodic** | What happened? | An incident, a tool result, an observation |
| **Working** | What are we doing now? | The current goal, next action, open questions |
| **Semantic** | What do we know? | A preference, a system fact, a relationship |
| **Competence** | What has worked before? | A debugging procedure and its success history |
| **Plan graph** | How should this work unfold? | A rollout with dependencies and checkpoints |
| **Entity** | What is this about? | The same service, project, file, or person across memories |

[Explore the memory model →](docs/concepts/memory-types.mdx)

## Choose your integration

| Use Membrane from… | Connection | Start here |
| :--- | :--- | :--- |
| **Go** | Embed the library in your process | [Go guide](docs/guides/go-library.mdx) · [Package reference](https://pkg.go.dev/github.com/BennettSchwartz/membrane/pkg/membrane) |
| **TypeScript / JavaScript** | Connect to the gRPC daemon | [SDK and examples](clients/typescript/README.md) |
| **Python** | Connect to the gRPC daemon | [SDK and examples](clients/python/README.md) |
| **OpenClaw** | Add memory tools and context through the plugin | [Plugin setup](clients/openclaw/README.md) |
| **Another language** | Use the protobuf service contract | [gRPC API](api/proto/membrane/v1/membrane.proto) |

For a complete agent loop, see the [agent harness](examples/agent-harness/README.md).
It exercises capture, graph retrieval, revisions, and access controls against a
running daemon, with both deterministic and optional live-model scenarios.

## Run it locally

> [!IMPORTANT]
> **PostgreSQL with pgvector is required**, whether you embed the Go library or
> run the daemon. Embedding and LLM providers are optional: structured capture,
> graph retrieval, revisions, and access controls work without a model API key.

<details>
<summary><strong>Build and start the daemon</strong> — Go, Docker Compose, and Make</summary>

Use the Go toolchain declared in [go.mod](go.mod). From a fresh checkout:

```bash
git clone https://github.com/BennettSchwartz/membrane.git
cd membrane
make build

export MEMBRANE_POSTGRES_PASSWORD="$(openssl rand -hex 24)"
export MEMBRANE_POSTGRES_DSN="postgres://membrane:${MEMBRANE_POSTGRES_PASSWORD}@127.0.0.1:5432/membrane_test?sslmode=disable"
docker compose up -d
until docker compose exec -T postgres pg_isready -U membrane -d membrane_test; do sleep 1; done
./bin/membraned --postgres-dsn "$MEMBRANE_POSTGRES_DSN"
```

The daemon applies its database schema on startup and listens on
`127.0.0.1:9090`. Its default policy permits the `default` scope at LOW sensitivity.
See [configuration](docs/guides/configuration.mdx) for scopes, provider settings,
TLS, and authentication.

The [TypeScript](clients/typescript/README.md#quick-start) and
[Python](clients/python/README.md#quick-start) quickstarts connect to this daemon.

</details>

### Add capabilities as you need them

| Setup | What it enables |
| :--- | :--- |
| **PostgreSQL + pgvector** | Typed records, entity links, bounded retrieval, revisions, and salience |
| **+ Embedding provider** | Automatic vector population and hybrid vector/salience ranking |
| **+ LLM provider** | Optional interpretation during capture and semantic extraction during consolidation |

[Full quickstart](docs/quickstart.mdx) · [Deployment guide](docs/guides/deployment.mdx) · [Security guide](docs/guides/security.mdx)

## Explore the project

| I want to… | Read |
| :--- | :--- |
| Understand the design | [Architecture](docs/architecture.mdx) · [Specification](rfc.md) |
| Configure a deployment | [Configuration](docs/guides/configuration.mdx) · [Deployment](docs/guides/deployment.mdx) |
| Understand retrieval and revision | [Retrieval](docs/api/retrieval.mdx) · [Revision](docs/api/revision.mdx) |
| Inspect behavior and limitations | [Observability](docs/guides/observability.mdx) · [Trust and sensitivity](docs/concepts/trust-and-sensitivity.mdx) |
| Change the code | [Contributing](CONTRIBUTING.md) · [Core packages](pkg) · [Integration and evaluation tests](tests) |

<details>
<summary><strong>Development and verification</strong></summary>

Go uses the toolchain in [go.mod](go.mod). The TypeScript SDK and examples need
Node.js 20.19+, the docs toolchain needs Node.js 22+, and the Python SDK needs
Python 3.10+.

| Command | What it checks |
| :--- | :--- |
| `make build` | Build the daemon |
| `make test` | Run Go tests |
| `make verify` | Check storage/protobuf/package contracts, SDKs, harness tools, and the docs build |
| `make lint` | Run Go vet and Staticcheck |
| `make agent-harness-deterministic` | Exercise an agent scenario against PostgreSQL |

Set disposable PostgreSQL DSNs to include database integration tests. Provider
credentials enable live embedding and model evaluations. See
[CONTRIBUTING.md](CONTRIBUTING.md) and the [harness guide](examples/agent-harness/README.md).

</details>

## Contributing

Bug reports, integration feedback, and contributions are welcome.
[Open an issue](https://github.com/BennettSchwartz/membrane/issues) or read the
[contribution guide](CONTRIBUTING.md).

**MIT licensed.** See [LICENSE](LICENSE).
