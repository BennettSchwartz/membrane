#!/usr/bin/env python3
"""Verify checked-in Python protobuf stubs match the canonical API proto."""

from __future__ import annotations

import difflib
import subprocess
import sys
import tempfile
from pathlib import Path


def unified_diff(expected: Path, actual: Path, repo_root: Path) -> str:
    expected_text = expected.read_text(encoding="utf-8").splitlines(keepends=True)
    actual_text = actual.read_text(encoding="utf-8").splitlines(keepends=True)
    return "".join(
        difflib.unified_diff(
            expected_text,
            actual_text,
            fromfile=str(expected.relative_to(repo_root)),
            tofile=f"regenerated/{actual.relative_to(actual.parents[2])}",
        )
    )


def main() -> int:
    script = Path(__file__).resolve()
    repo_root = script.parents[3]
    proto_root = repo_root / "api" / "proto"
    proto_file = proto_root / "membrane" / "v1" / "membrane.proto"
    checked_in = repo_root / "clients" / "python" / "membrane" / "v1"

    with tempfile.TemporaryDirectory(prefix="membrane-python-proto-") as temp:
        generated_root = Path(temp)
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "grpc_tools.protoc",
                "-I",
                str(proto_root),
                f"--python_out={generated_root}",
                f"--grpc_python_out={generated_root}",
                str(proto_file),
            ],
            cwd=repo_root,
            text=True,
            capture_output=True,
        )
        if result.returncode != 0:
            sys.stderr.write(result.stdout)
            sys.stderr.write(result.stderr)
            return result.returncode

        diffs: list[str] = []
        for name in ("membrane_pb2.py", "membrane_pb2_grpc.py"):
            expected = checked_in / name
            actual = generated_root / "membrane" / "v1" / name
            if expected.read_text(encoding="utf-8") != actual.read_text(encoding="utf-8"):
                diffs.append(unified_diff(expected, actual, repo_root))

    if diffs:
        sys.stderr.write("Python protobuf stubs are stale. Regenerate with grpc_tools.protoc.\n")
        sys.stderr.write("\n".join(diffs))
        return 1

    print("Python protobuf stubs are in sync.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
