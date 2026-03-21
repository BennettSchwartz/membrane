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

# When an embedding API key and Postgres DSN are available, run in API mode
# using pgvector + OpenRouter embeddings. Otherwise fall back to local
# sentence-transformers via Python.
EMBEDDING_API_KEY="${MEMBRANE_EMBEDDING_API_KEY:-}"
POSTGRES_DSN="${MEMBRANE_POSTGRES_DSN:-}"
EMBEDDING_ENDPOINT="${MEMBRANE_EMBEDDING_ENDPOINT:-https://openrouter.ai/api/v1/embeddings}"
EMBEDDING_MODEL="${MEMBRANE_EMBEDDING_MODEL:-openai/text-embedding-3-small}"
EMBEDDING_DIMS="${MEMBRANE_EMBEDDING_DIMENSIONS:-1536}"

if [ -n "$EMBEDDING_API_KEY" ] && [ -n "$POSTGRES_DSN" ]; then
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
fi

# Fallback: local sentence-transformers mode.
if ! command -v python3 >/dev/null 2>&1; then
  echo "Skipping eval: python3 not available."
  exit 0
fi

if python3 - <<'PY'
try:
    import sentence_transformers  # noqa: F401
    import numpy  # noqa: F401
except Exception:
    raise SystemExit(1)
PY
then
  :
else
  echo "Skipping eval: missing Python deps. Install with: python3 -m pip install -r tools/eval/requirements.txt"
  exit 0
fi

exec go run ./cmd/membrane-eval \
  -dataset tests/data/recall_dataset.jsonl \
  -min-recall "${MIN_RECALL}" \
  -min-precision "${MIN_PRECISION}" \
  -min-mrr "${MIN_MRR}" \
  -min-ndcg "${MIN_NDCG}"
