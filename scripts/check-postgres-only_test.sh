#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

cp "$ROOT/scripts/check-postgres-only.sh" "$TMPDIR/check-postgres-only.sh"
chmod +x "$TMPDIR/check-postgres-only.sh"

pushd "$TMPDIR" >/dev/null
git init -q
mkdir -p pkg/storage/postgres docs
cat > README.md <<'DOC'
# Fixture
DOC
git add README.md
git -c user.name="Postgres Only Test" -c user.email="postgres-only@example.invalid" commit -qm init

run_reject_fixture() {
  local name="$1"
  local text="$2"
  local fixture="examples/${name}.md"

  mkdir -p examples
  printf '%s\n' "$text" >"$fixture"

  output="$TMPDIR/check-postgres-only-${name}.out"
  if ./check-postgres-only.sh >"$output" 2>&1; then
    cat "$output" >&2
    echo "check-postgres-only should reject ${name}" >&2
    exit 1
  fi

  if ! grep -q "SQLite runtime references" "$output"; then
    cat "$output" >&2
    echo "check-postgres-only should explain the ${name} violation" >&2
    exit 1
  fi

  fallback_output="$TMPDIR/check-postgres-only-${name}-fallback.out"
  if env PATH="/usr/bin:/bin" ./check-postgres-only.sh >"$fallback_output" 2>&1; then
    cat "$fallback_output" >&2
    echo "check-postgres-only fallback should reject ${name} without rg" >&2
    exit 1
  fi

  if ! grep -q "SQLite runtime references" "$fallback_output"; then
    cat "$fallback_output" >&2
    echo "check-postgres-only fallback should explain the ${name} violation" >&2
    exit 1
  fi

  rm -f "$fixture" "$output" "$fallback_output"
}

run_reject_fixture "runtime-import" "This untracked runtime note accidentally tells users to import storage/sqlite."
run_reject_fixture "sqlite3-driver" "This stale implementation note imports github.com/mattn/go-sqlite3 for local storage."
run_reject_fixture "modernc-driver" "This stale implementation note imports modernc.org/sqlite for local storage."
run_reject_fixture "sqlite-db-file" "This stale quickstart writes agent-memory.sqlite on first run."
run_reject_fixture "file-backed-db-doc" "This stale quickstart says Membrane creates my-agent.db on first run."
run_reject_fixture "stale-vector-rfc" "This stale architecture note says there is an optional vector index for semantic similarity search."
run_reject_fixture "stale-graph-store-rfc" "This stale architecture note says Graph databases MAY be introduced as secondary traversal accelerators."
run_reject_fixture "stale-rfc-continued" "This stale RFC fragment says ## 15. Reference Implementation Architecture (continued)."
run_reject_fixture "stale-rfc-memory-types" "This stale schema comment says episodic | working | semantic | competence | plan."

popd >/dev/null
