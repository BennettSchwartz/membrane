#!/usr/bin/env sh
set -eu

# Source .env if present (gitignored).
if [ -f .env ]; then
  set -a
  . ./.env
  set +a
fi

if [ -x ".venv/bin/python" ]; then
  PATH="$(pwd)/.venv/bin:$PATH"
  export PATH
fi

MIN_RECALL="${MEMBRANE_EVAL_MIN_RECALL:-0.90}"
MIN_PRECISION="${MEMBRANE_EVAL_MIN_PRECISION:-0.20}"
MIN_MRR="${MEMBRANE_EVAL_MIN_MRR:-0.90}"
MIN_NDCG="${MEMBRANE_EVAL_MIN_NDCG:-0.90}"

# Eval runs against the same required Postgres + pgvector substrate as the
# daemon. Supplying embeddings through the API keeps recall metrics comparable
# with runtime retrieval.
EMBEDDING_API_KEY="${MEMBRANE_EMBEDDING_API_KEY:-}"
POSTGRES_DSN="${MEMBRANE_POSTGRES_DSN:-}"
EMBEDDING_ENDPOINT="${MEMBRANE_EMBEDDING_ENDPOINT:-https://openrouter.ai/api/v1/embeddings}"
EMBEDDING_MODEL="${MEMBRANE_EMBEDDING_MODEL:-openai/text-embedding-3-small}"
EMBEDDING_DIMS="${MEMBRANE_EMBEDDING_DIMENSIONS:-1536}"

if [ -z "$POSTGRES_DSN" ] || [ -z "$EMBEDDING_API_KEY" ]; then
  echo "MEMBRANE_POSTGRES_DSN and MEMBRANE_EMBEDDING_API_KEY are required for Postgres + pgvector evals." >&2
  exit 2
fi

echo "Running eval in API mode (pgvector + ${EMBEDDING_MODEL})"
exec go run ./cmd/membrane-eval \
  -dataset tests/data/recall_dataset.jsonl \
  -postgres-dsn "${POSTGRES_DSN}" \
  -embedding-endpoint "${EMBEDDING_ENDPOINT}" \
  -embedding-model "${EMBEDDING_MODEL}" \
  -embedding-api-key "${EMBEDDING_API_KEY}" \
  -embedding-dimensions "${EMBEDDING_DIMS}" \
  -min-recall "${MIN_RECALL}" \
  -min-precision "${MIN_PRECISION}" \
  -min-mrr "${MIN_MRR}" \
  -min-ndcg "${MIN_NDCG}" \
  -verbose
