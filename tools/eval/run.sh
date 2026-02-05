#!/usr/bin/env sh
set -eu

if [ -x ".venv/bin/python" ]; then
  PATH="$(pwd)/.venv/bin:$PATH"
  export PATH
fi

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

MIN_RECALL="${MEMBRANE_EVAL_MIN_RECALL:-0.90}"
MIN_PRECISION="${MEMBRANE_EVAL_MIN_PRECISION:-0.20}"
MIN_MRR="${MEMBRANE_EVAL_MIN_MRR:-0.90}"
MIN_NDCG="${MEMBRANE_EVAL_MIN_NDCG:-0.90}"

exec go run ./cmd/membrane-eval \
  -dataset tests/data/recall_dataset.jsonl \
  -min-recall "${MIN_RECALL}" \
  -min-precision "${MIN_PRECISION}" \
  -min-mrr "${MIN_MRR}" \
  -min-ndcg "${MIN_NDCG}"
