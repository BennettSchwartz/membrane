#!/usr/bin/env python3
"""Build the Python client wheel and verify package contents."""

from __future__ import annotations

import subprocess
import sys
import tempfile
import zipfile
from email.parser import Parser
from pathlib import Path


REQUIRED_WHEEL_FILES = {
    "membrane/__init__.py",
    "membrane/client.py",
    "membrane/types.py",
    "membrane/py.typed",
    "membrane/v1/__init__.py",
    "membrane/v1/membrane_pb2.py",
    "membrane/v1/membrane_pb2_grpc.py",
}


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def main() -> int:
    root = repo_root()
    package_dir = root / "clients" / "python"

    with tempfile.TemporaryDirectory(prefix="membrane-python-wheel-") as temp:
        wheel_dir = Path(temp)
        subprocess.run(
            [
                sys.executable,
                "-m",
                "pip",
                "wheel",
                "--no-deps",
                str(package_dir),
                "-w",
                str(wheel_dir),
            ],
            check=True,
            stdout=subprocess.DEVNULL,
        )
        wheels = sorted(wheel_dir.glob("membrane_client-*.whl"))
        if len(wheels) != 1:
            sys.stderr.write(f"Expected one membrane_client wheel, found {len(wheels)}\n")
            return 1

        with zipfile.ZipFile(wheels[0]) as wheel:
            names = set(wheel.namelist())
            metadata_name = next(
                (name for name in names if name.endswith(".dist-info/METADATA")),
                "",
            )
            metadata = Parser().parsestr(wheel.read(metadata_name).decode("utf-8")) if metadata_name else None

    missing = sorted(REQUIRED_WHEEL_FILES - names)
    if missing:
        sys.stderr.write("Python wheel is missing required files:\n")
        for name in missing:
            sys.stderr.write(f"  - {name}\n")
        return 1

    if metadata is None:
        sys.stderr.write("Python wheel is missing package metadata.\n")
        return 1
    if metadata.get("Description-Content-Type") != "text/markdown":
        sys.stderr.write("Python wheel metadata must use README.md as text/markdown.\n")
        return 1
    if "Membrane Python Client" not in metadata.get_payload():
        sys.stderr.write("Python wheel metadata must include the client README content.\n")
        return 1

    print("Python package wheel contains required client files.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
