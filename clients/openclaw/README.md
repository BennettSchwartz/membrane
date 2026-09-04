# OpenClaw Membrane Plugin

[OpenClaw](https://github.com/openclaw/openclaw) plugin that bridges to [Membrane](https://github.com/BennettSchwartz/membrane) — giving your AI agents graph-aware memory.

## What it does

- **Ingests** agent events, tool outputs, and observations into Membrane
- **Searches** graph-aware memory via the `membrane_search` tool
- **Auto-injects** relevant context before each agent turn
- **Reports** connection status via the `/membrane` command

## Install

```bash
# In your OpenClaw extensions directory
npm install @vainplex/openclaw-membrane
```

Or with [Brainplex](https://www.npmjs.com/package/brainplex):

```bash
npx brainplex init  # Auto-detects and configures all plugins
```

## Prerequisites

- A running [Membrane](https://github.com/BennettSchwartz/membrane) instance (the `membraned` daemon)
- A Postgres database with the `pgvector` extension for Membrane storage
- OpenClaw v0.10+

## Configuration

In your OpenClaw config (`openclaw.yaml`), under `plugins.entries`:

```yaml
plugins:
  entries:
    openclaw-membrane:
      enabled: true
      config:
        grpc_endpoint: "localhost:9090"
        api_key: "your-api-key"
        tls: false
        timeout_ms: 10000
        scope: "project-openclaw"
        trust_scopes: ["project-openclaw"]
        default_sensitivity: "low"
        max_read_sensitivity: "low"
        auto_context: true
        context_limit: 5
        min_salience: 0.3
        context_types: ["entity", "episodic", "semantic", "competence"]
```

| Option | Default | Description |
|--------|---------|-------------|
| `grpc_endpoint` | `localhost:9090` | Membrane gRPC address |
| `api_key` | unset | Bearer token for Membrane gRPC requests |
| `tls` | `false` | Use TLS for the gRPC channel |
| `tls_ca_cert_path` | unset | Optional CA certificate path for TLS |
| `timeout_ms` | unset | Per-RPC timeout in milliseconds |
| `scope` | unset | Scope assigned to captured OpenClaw memories |
| `trust_scopes` | `[]` | Scopes allowed during retrieval. Defaults to `[scope]` when `scope` is set; retrieval is disabled when both are empty |
| `default_sensitivity` | `low` | Sensitivity for ingested events: `public`, `low`, `medium`, `high`, `hyper` |
| `max_read_sensitivity` | `low` | Maximum sensitivity requested during retrieval, separate from write sensitivity |
| `auto_context` | `true` | Auto-inject memories before each agent turn |
| `context_limit` | `5` | Max graph roots to retrieve for injected context, from `1` to `10000`; related neighbors may also be included |
| `min_salience` | `0.3` | Minimum salience score for retrieval, from `0` to `1` |
| `context_types` | `["entity", "episodic", "semantic", "competence"]` | Memory types: `episodic`, `working`, `entity`, `semantic`, `competence`, `plan_graph` |


## Usage

### membrane_search tool

Your agent can search graph-aware Membrane records, including entity-linked facts:

```javascript
membrane_search("what happened in yesterday's meeting", { limit: 10 })
```

When `scope` is configured, captured records are written with that Membrane scope and retrieval uses the same scope by default. Set `trust_scopes` to read from a different set of scopes, such as a project scope plus a shared workspace scope. If neither `scope` nor `trust_scopes` is configured, retrieval fails closed instead of requesting all scopes.

If Membrane falls back from embedding or vector ranking during retrieval, the plugin logs a warning such as `Retrieval degraded during search` while still returning salience-ranked results.
The `limit`, `min_salience`, and `memory_types` search options are validated before retrieval; invalid values fail the search call instead of being silently dropped from the filter. Expanded graph budgets are capped at Membrane SDK limits.
Search results preserve Membrane's root relevance order, then append graph neighbors in response order before applying the requested limit.

### Auto-context

When `auto_context: true`, the plugin injects relevant memories into the agent's context before each turn. This gives agents awareness of past interactions without explicit tool calls.
Non-fatal retrieval diagnostics, including an actually applied response-byte limit, are logged as warnings and are not injected into the agent prompt. Normal graph projection does not emit a warning.

Injected context keeps graph shape visible with roots, neighbor records, and relation edges:

```text
Membrane graph context:
Roots:
1. [entity] Orchid deploy target
Neighbors:
1. [hop=1] [episodic] Used Orchid during rollout verification
Relations:
1. Orchid deploy target --mentioned_in--> Used Orchid during rollout verification
```

### /membrane command

Check connection status:

```text
/membrane
→ Membrane: connected (localhost:9090) | 1,247 records | 3 memory types | embedding coverage 95%
```

When `GetMetrics` reports missing current-model embeddings, the status payload includes a warning so pgvector coverage gaps are visible from the agent surface.

## Architecture

```text
OpenClaw Agent
     │
     ├── after_agent_reply ──→ captureMemory()
     ├── after_tool_call ────→ captureMemory()
     ├── before_agent_start ─→ retrieveGraph() → inject context
     │
     └── membrane_search ───→ retrieveGraph() → return results
                                  │
                                  ▼
                          Membrane (gRPC)
                          ┌─────────────┐
                          │  membraned   │
                          │  Postgres    │
                          │  + pgvector  │
                          │  Embeddings  │
                          └─────────────┘
```

## Development

```bash
cd clients/openclaw
npm install
npm run build
npm test
```

## License

MIT — see [LICENSE](../../LICENSE)
