# OpenClaw Membrane Plugin

[OpenClaw](https://github.com/openclaw/openclaw) plugin that bridges to [Membrane](https://github.com/GustyCube/membrane) — giving your AI agents episodic memory.

## What it does

- **Ingests** agent events, tool outputs, and observations into Membrane
- **Searches** episodic memory via the `membrane_search` tool
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

- A running [Membrane](https://github.com/GustyCube/membrane) instance (the `membraned` daemon)
- OpenClaw v0.10+

## Configuration

In your OpenClaw config (`openclaw.yaml`), under `plugins.entries`:

```yaml
plugins:
  entries:
    openclaw-membrane:
      enabled: true
      config:
        grpc_endpoint: "localhost:4222"
        default_sensitivity: "low"
        auto_context: true
        context_limit: 5
        min_salience: 0.3
        context_types: ["episodic", "semantic", "competence"]
```

| Option | Default | Description |
|--------|---------|-------------|
| `grpc_endpoint` | `localhost:4222` | Membrane gRPC address |
| `default_sensitivity` | `low` | Sensitivity for ingested events: `public`, `low`, `medium`, `high`, `hyper` |
| `auto_context` | `true` | Auto-inject memories before each agent turn |
| `context_limit` | `5` | Max memories to inject |
| `min_salience` | `0.3` | Minimum salience score for retrieval |
| `context_types` | `["episodic", "semantic", "competence"]` | Memory types: `episodic`, `working`, `semantic`, `competence`, `plan_graph` |


## Usage

### membrane_search tool

Your agent can search episodic memory:

```javascript
membrane_search("what happened in yesterday's meeting", { limit: 10 })
```

### Auto-context

When `auto_context: true`, the plugin injects relevant memories into the agent's context before each turn. This gives agents awareness of past interactions without explicit tool calls.

### /membrane command

Check connection status:

```text
/membrane
→ Membrane: connected (localhost:4222) | 1,247 records | 3 memory types
```

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
                          │  SQLCipher   │
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
