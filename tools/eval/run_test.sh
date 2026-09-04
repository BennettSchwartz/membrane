#!/usr/bin/env sh
set -eu

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

output="$TMPDIR/eval-run.out"
if (cd "$TMPDIR" && env -i PATH="$PATH" sh "$ROOT/tools/eval/run.sh") >"$output" 2>&1; then
  echo "tools/eval/run.sh should require Postgres and embedding API configuration" >&2
  exit 1
fi

if ! grep -q "MEMBRANE_POSTGRES_DSN and MEMBRANE_EMBEDDING_API_KEY are required" "$output"; then
  echo "tools/eval/run.sh should explain the required Postgres + embedding configuration" >&2
  cat "$output" >&2
  exit 1
fi

if grep -qi "sentence-transformers" "$output"; then
  echo "tools/eval/run.sh should not fall back to local sentence-transformers" >&2
  cat "$output" >&2
  exit 1
fi
