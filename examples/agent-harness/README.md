# Membrane Agent Harness

This example wires Membrane into an LLM-style agent loop. It seeds a local SQLite
database with all five memory types, links them through entity records, exposes
Membrane as model tools, and verifies graph retrieval.

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

Run a live LLM tool-loop check:

```bash
npm run test:llm
```

The harness starts `membraned` automatically when `MEMBRANE_ADDR` is unset. To
connect to an existing daemon instead, set `MEMBRANE_ADDR` and optionally
`MEMBRANE_API_KEY`.

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
record retrieval when the prompt naturally asks for those behaviors.
