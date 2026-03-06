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

`~/.openclaw/plugins/openclaw-membrane/config.json`:

```json
{
  "grpc_endpoint": "localhost:4222",
  "default_sensitivity": "low",
  "auto_context": true,
  "context_limit": 5,
  "min_salience": 0.3,
  "context_types": ["event", "tool_output", "observation"]
}
```

| Option | Default | Description |
|--------|---------|-------------|
| `grpc_endpoint` | `localhost:4222` | Membrane gRPC address |
| `default_sensitivity` | `low` | Sensitivity for ingested events: `public`, `low`, `medium`, `high`, `hyper` |
| `auto_context` | `true` | Auto-inject memories before each agent turn |
| `context_limit` | `5` | Max memories to inject |
| `min_salience` | `0.3` | Minimum salience score for retrieval |
| `context_types` | `["event", "tool_output", "observation"]` | Memory types to include |


## Usage

### membrane_search tool

Your agent can search episodic memory:

```
membrane_search("what happened in yesterday's meeting", { limit: 10 })
```

### Auto-context

When `auto_context: true`, the plugin injects relevant memories into the agent's context before each turn. This gives agents awareness of past interactions without explicit tool calls.

### /membrane command

Check connection status:

```
/membrane
→ Membrane: connected (localhost:4222) | 1,247 records | 3 memory types
```

## Architecture

```
OpenClaw Agent
     │
     ├── after_agent_reply ──→ ingestEvent()
     ├── after_tool_call ────→ ingestToolOutput()
     ├── before_agent_start ─→ retrieve() → inject context
     │
     └── membrane_search ───→ retrieve() → return results
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
