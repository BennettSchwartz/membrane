"""Integration tests for the Python client against a live membraned instance."""

from __future__ import annotations

import os
import socket
import subprocess
import tempfile
import time
from pathlib import Path

import grpc
import pytest

from membrane import MembraneClient, Sensitivity, SourceKind, TrustContext


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _stop_process(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def _wait_for_ready(
    proc: subprocess.Popen[str], addr: str, api_key: str, timeout_seconds: float = 10.0
) -> None:
    deadline = time.time() + timeout_seconds
    last_error: Exception | None = None

    while time.time() < deadline:
        if proc.poll() is not None:
            break

        try:
            with MembraneClient(addr, api_key=api_key, timeout=1.0) as client:
                client.get_metrics()
            return
        except Exception as err:  # pragma: no cover - exercised only while polling
            last_error = err
            time.sleep(0.25)

    _stop_process(proc)
    logs = ""
    if proc.stdout is not None:
        logs = proc.stdout.read()
    raise AssertionError(
        f"membraned did not become ready at {addr}: {last_error}\nLogs:\n{logs}"
    )


@pytest.fixture
def daemon_binary(tmp_path_factory: pytest.TempPathFactory) -> str:
    repo_root = Path(__file__).resolve().parents[3]
    build_dir = tmp_path_factory.mktemp("membraned-bin")
    binary_path = build_dir / "membraned"

    subprocess.run(
        ["go", "build", "-o", str(binary_path), "./cmd/membraned"],
        cwd=repo_root,
        check=True,
    )

    return str(binary_path)


@pytest.fixture
def daemon_env(daemon_binary: str):
    repo_root = Path(__file__).resolve().parents[3]
    api_key = "python-integration-secret"
    addr = f"127.0.0.1:{_free_port()}"

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "membrane.db")
        env = os.environ.copy()
        env["MEMBRANE_API_KEY"] = api_key

        proc = subprocess.Popen(
            [daemon_binary, "-addr", addr, "-db", db_path],
            cwd=repo_root,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        try:
            _wait_for_ready(proc, addr, api_key)
            yield {"addr": addr, "api_key": api_key}
        finally:
            _stop_process(proc)


def test_auth_and_happy_path(daemon_env) -> None:
    addr = daemon_env["addr"]
    api_key = daemon_env["api_key"]

    with pytest.raises(grpc.RpcError) as excinfo:
        with MembraneClient(addr, timeout=1.0) as client:
            client.get_metrics()
    assert excinfo.value.code() == grpc.StatusCode.UNAUTHENTICATED

    trust = TrustContext(
        max_sensitivity=Sensitivity.MEDIUM,
        authenticated=True,
        actor_id="py-test",
        scopes=["project:alpha"],
    )

    with MembraneClient(addr, api_key=api_key, timeout=2.0) as client:
        capture = client.capture_memory(
            {
                "text": "Remember Orchid as the staging deploy target.",
                "project": "Orchid",
            },
            reason_to_remember="Niche deploy vocabulary should be recoverable",
            scope="project:alpha",
            tags=["integration", "python"],
            sensitivity=Sensitivity.LOW,
        )

        record = capture.primary_record
        assert record.id
        first_entity = next(
            created for created in capture.created_records if created.type is not None and created.type.value == "entity"
        )

        second_capture = client.capture_memory(
            {
                "text": "Use Orchid for rollout verification before production.",
                "project": "Orchid",
            },
            reason_to_remember="Repeated mention should link to same entity",
            scope="project:alpha",
            tags=["integration", "python", "orchid"],
            sensitivity=Sensitivity.LOW,
        )

        mention_edge = next(
            edge
            for edge in second_capture.edges
            if edge.predicate == "mentions_entity"
            and edge.source_id == second_capture.primary_record.id
        )
        assert mention_edge.target_id == first_entity.id

        graph = client.retrieve_graph(
            "Orchid deploy target",
            trust=trust,
            root_limit=10,
            node_limit=20,
            edge_limit=20,
            max_hops=1,
        )
        assert any(node.record.id == record.id for node in graph.nodes)
        assert any(node.record.type.value == "entity" for node in graph.nodes)
        assert any(edge.predicate == "mentions_entity" for edge in graph.edges)

        by_id = client.retrieve_by_id(record.id, trust=trust)
        assert by_id.id == record.id

        metrics = client.get_metrics()
        assert metrics["total_records"] >= 1


def test_retract_episodic_failure(daemon_env) -> None:
    addr = daemon_env["addr"]
    api_key = daemon_env["api_key"]

    with MembraneClient(addr, api_key=api_key, timeout=2.0) as client:
        capture = client.capture_memory(
            {"ref": "evt-1", "text": "Created for immutable retraction check"},
            source_kind=SourceKind.EVENT,
            summary="Created for immutable retraction check",
        )
        record = capture.primary_record

        with pytest.raises(grpc.RpcError) as excinfo:
            client.retract(record.id, actor="py-test", rationale="should fail")

    assert excinfo.value.code() == grpc.StatusCode.FAILED_PRECONDITION
