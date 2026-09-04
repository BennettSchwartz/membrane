#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
proto_dir="$repo_root/api/proto/membrane/v1"
generated_dir="$repo_root/api/grpc/gen/membranev1"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

mkdir -p "$tmp/api/grpc/gen/membranev1"

"$repo_root/scripts/protoc-go.sh" \
  --go_out="$tmp/api/grpc/gen/membranev1" --go_opt=paths=source_relative \
  --go-grpc_out="$tmp/api/grpc/gen/membranev1" --go-grpc_opt=paths=source_relative \
  -I "$proto_dir" \
  "$proto_dir"/*.proto

normalize_compiler_version() {
  # Both Go generators put the protoc version on line 4. Only that numeric
  # metadata varies with the system compiler; keep generator versions and every
  # byte of the generated implementation in the comparison.
  sed -E '4s@^(//[[:space:]]+(-[[:space:]]+)?protoc[[:space:]]+)v[0-9]+(\.[0-9]+)*$@\1VERSION@' "$1"
}

for generated in membrane.pb.go membrane_grpc.pb.go; do
  normalize_compiler_version "$generated_dir/$generated" >"$tmp/checked-in-$generated"
  normalize_compiler_version "$tmp/api/grpc/gen/membranev1/$generated" >"$tmp/regenerated-$generated"
  if ! diff -u -L "checked-in/$generated" -L "regenerated/$generated" \
    "$tmp/checked-in-$generated" "$tmp/regenerated-$generated"; then
    echo "Go protobuf stubs are stale. Regenerate with: make proto" >&2
    exit 1
  fi
done

echo "Go protobuf stubs are in sync."
