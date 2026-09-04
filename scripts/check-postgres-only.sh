#!/usr/bin/env bash
set -euo pipefail

fail=0

if [ -d pkg/storage/sqlite ] && find pkg/storage/sqlite -type f | grep -q .; then
  echo "SQLite storage artifacts remain under pkg/storage/sqlite:" >&2
  find pkg/storage/sqlite -type f | sort >&2
  fail=1
fi

forbidden='github\.com/BennettSchwartz/membrane/pkg/storage/sqlite|storage/sqlite|SQLiteStore|SQLCipher|modernc\.org/sqlite|github\.com/mattn/go-sqlite3|sqlite3|\.sqlite3?\b|my-agent\.db|creates .*\.db on first run|optional secondary stores|optional vector index|MAY be stored outside the authoritative store|Graph databases MAY|Reference Implementation Architecture \(continued\)|## 15A\. Canonical Schemas|episodic \| working \| semantic \| competence \| plan'

if command -v rg >/dev/null 2>&1; then
  matches="$(
    rg -n --hidden --glob '!.git/**' \
      --glob '!api/grpc/gen/**' \
      --glob '!build/**' \
      --glob '!**/node_modules/**' \
      --glob '!clients/typescript/dist/**' \
      --glob '!scripts/check-postgres-only.sh' \
      --glob '!scripts/check-postgres-only_test.sh' \
      --glob '!check-postgres-only.sh' \
      "$forbidden" . || true
  )"
else
  matches="$(
    while IFS= read -r -d '' file; do
      grep -nE -I "$forbidden" "$file" | sed "s#^#${file}:#" || true
    done < <(
      find . \
        \( -path './.git' \
        -o -path './api/grpc/gen' \
        -o -path './build' \
        -o -path './clients/typescript/dist' \
        -o -path '*/node_modules' \) -prune \
        -o -type f \
        ! -path './scripts/check-postgres-only.sh' \
        ! -path './scripts/check-postgres-only_test.sh' \
        ! -path './check-postgres-only.sh' \
        -print0
    )
  )"
fi

if [ -n "$matches" ]; then
  echo "$matches"
  echo "SQLite runtime references must be removed; Membrane is Postgres + pgvector only." >&2
  fail=1
fi

exit "$fail"
