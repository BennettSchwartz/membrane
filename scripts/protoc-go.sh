#!/usr/bin/env bash
set -euo pipefail

if ! command -v protoc >/dev/null 2>&1; then
  echo "protoc is required. Install protobuf-compiler before regenerating Go stubs." >&2
  exit 1
fi

tmp_bin="$(mktemp -d)"
trap 'rm -rf "$tmp_bin"' EXIT

if command -v protoc-gen-go >/dev/null 2>&1; then
  ln -s "$(command -v protoc-gen-go)" "$tmp_bin/protoc-gen-go"
else
  cat >"$tmp_bin/protoc-gen-go" <<'WRAPPER'
#!/usr/bin/env bash
set -euo pipefail
exec go run google.golang.org/protobuf/cmd/protoc-gen-go "$@"
WRAPPER
  chmod +x "$tmp_bin/protoc-gen-go"
fi

if command -v protoc-gen-go-grpc >/dev/null 2>&1; then
  ln -s "$(command -v protoc-gen-go-grpc)" "$tmp_bin/protoc-gen-go-grpc"
else
  cat >"$tmp_bin/protoc-gen-go-grpc" <<'WRAPPER'
#!/usr/bin/env bash
set -euo pipefail
exec go run google.golang.org/grpc/cmd/protoc-gen-go-grpc "$@"
WRAPPER
  chmod +x "$tmp_bin/protoc-gen-go-grpc"
fi

PATH="$tmp_bin:$PATH" protoc "$@"
