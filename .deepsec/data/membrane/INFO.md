# membrane

## What this codebase does

Membrane is a Go/Postgres memory substrate for LLM agents.
It runs as `membraned` over gRPC or as an embedded Go library, with
TypeScript, Python, and OpenClaw clients.
It stores typed, revisable records across episodic, working, semantic,
competence, plan-graph, and entity layers.
Retrieval can return ranked records plus bounded graph neighborhoods.
Postgres with pgvector is durable storage; optional HTTP embedding,
semantic LLM, and ingest-LLM clients can receive record content when enabled.

## Auth shape

- `chainInterceptors` applies optional bearer-token auth and per-client
  rate limiting to unary gRPC calls.
- `cfg.APIKey` / `MEMBRANE_API_KEY` being empty intentionally disables
  transport auth for local/dev use.
- `TrustContext`, `Allows`, `AllowsRedacted`, and `FilterByTrust` are
  the data-visibility gate for retrieval.
- `validateTrustContext` and `toTrustContext` translate client-supplied
  gRPC trust; transport auth does not verify `actor_id` or `scopes`.
- TypeScript, Python, and OpenClaw clients inject API keys as
  `authorization: Bearer <key>` metadata but accept caller trust contexts.

## Threat model

Clients with network access to gRPC can capture memory, retrieve graph
neighborhoods, or invoke revision operations if `api_key` is empty or leaked.
Callers that can choose `high`/`hyper`, empty scopes, or
`authenticated: true` in `TrustContext` can overstate record visibility.
Configured embedding/LLM endpoints may receive captured content,
summaries, context, and interpretation material.
Direct Postgres access bypasses the application trust model entirely.

## Project-specific patterns to flag

- New gRPC methods should follow `CaptureMemory`, `RetrieveGraph`, and
  revision handlers: required IDs, bounded strings/tags, finite protobuf
  values, graph limits, and service-error mapping.
- Treat use of client-provided `TrustContext.Authenticated`, `ActorID`,
  or empty `Scopes` as sensitive; empty scopes mean all scopes, and
  unscoped records are globally visible.
- `Redact` clears payload/provenance/audit but keeps ID, type,
  sensitivity, salience, scope, and tags; sensitive facts must not live
  in tags/scopes/summaries.
- `CaptureMemory`, `Interpreter`, and `CandidateResolver` can materialize
  entities, relation edges, and provenance from untrusted content; check
  for cross-scope linking, semantic reuse, or LLM egress surprises.
- `Supersede`, `Fork`, `Retract`, `Merge`, and `Contest` should stay
  transactional, preserve auditability, reject episodic revision, and
  keep semantic evidence/provenance.

## Known false-positives

- `docs/**`, `README.md`, and SDK READMEs include localhost DSNs,
  `your-key`, grpcurl examples, and plaintext local-development commands.
- `tests/**` and `*_test.go` intentionally use fake tokens, test TLS
  files, loopback endpoints, invalid enums, and synthetic recall data.
- `examples/agent-harness/**` is an opt-in demo harness; the fallback
  `agent-harness-secret` and temporary local daemon startup are fixtures.
- Generated protobuf outputs (`api/grpc/gen/**`,
  `clients/python/membrane/v1/**`, TypeScript proto copies) are not
  hand-authored security logic.
- `.deepsec/**`, coverage files, package locks, Docusaurus assets, and
  docs deployment config are scanner/tooling or build artifacts.
