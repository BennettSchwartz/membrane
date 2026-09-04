# Membrane Agent Harness

This example wires Membrane into an LLM-style agent loop. It seeds a Postgres +
pgvector-backed Membrane daemon with all six memory types, links them through
entity records, exposes Membrane as model tools, and verifies graph retrieval.

## Run

Build the local SDK first:

```bash
npm --prefix ../../clients/typescript run build
npm install
```

For live LLM runs, create a local env file and add either `OPENROUTER_API_KEY`
or `OPENAI_API_KEY`:

```bash
cp .env.example .env
```

Run the deterministic graph check:

```bash
npm run test:deterministic
```

Run the fast harness unit checks:

```bash
npm test
```

Run a live LLM tool-loop check:

```bash
npm run test:llm
```

The harness starts `membraned` automatically when `MEMBRANE_ADDR` is unset. Set
`MEMBRANE_POSTGRES_DSN` for that auto-start path. To connect to an existing
daemon instead, set `MEMBRANE_ADDR` and optionally `MEMBRANE_API_KEY`.
The temporary daemon permits only the harness scope at LOW sensitivity. The
deterministic check explicitly adds its second scope and MEDIUM sensitivity to
exercise access filtering; writes outside that policy must still fail. An
external daemon needs the corresponding policy configured by its owner.
Set `AGENT_HARNESS_SCOPE` to isolate repeated runs in a shared test database;
the default is `project:agent-harness-orion`, and the deterministic check uses
`<scope>:other` as its second scope.

LLM configuration is OpenAI-compatible:

- `OPENROUTER_API_KEY` uses `https://openrouter.ai/api/v1` by default.
- `OPENAI_API_KEY` uses the default OpenAI endpoint.
- `AGENT_HARNESS_BASE_URL` overrides the endpoint.
- `AGENT_HARNESS_MODEL` overrides the model. OpenRouter defaults to
  `openai/gpt-5.5`.
- `AGENT_HARNESS_MAX_TOKENS` bounds each LLM response.
- `AGENT_HARNESS_REQUEST_TIMEOUT_MS` overrides the LLM request timeout.
- `AGENT_HARNESS_MAX_RETRIES` overrides OpenAI client retry count.

`test:llm` prints per-turn LLM latency and Membrane tool timings so the live
agent loop can be checked for both accuracy and performance regressions. It also
uses ordinary user-language requests with optional model tools
(`tool_choice: "auto"`). The live test asserts that the model chooses graph
retrieval, fact capture, working-state capture, episode capture, and exact
record retrieval when the prompt naturally asks for those behaviors. Graph
retrieval tool output includes non-fatal retrieval diagnostics, so degraded
embedding or vector ranking remains visible in traces.
