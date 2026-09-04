"""Unit tests for membrane.client."""

from __future__ import annotations

import math
import socket
import threading

import grpc
import pytest
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Value

from membrane import MembraneClient, MemoryType, Sensitivity, TrustContext
from membrane.v1 import membrane_pb2


def _value(value) -> Value:
    msg = Value()
    json_format.ParseDict(value, msg)
    return msg


def _record_msg(record_id: str, memory_type: str = "competence") -> membrane_pb2.MemoryRecord:
    msg = membrane_pb2.MemoryRecord()
    json_format.ParseDict(
        {
            "id": record_id,
            "type": memory_type,
            "sensitivity": "low",
            "confidence": 0.9,
            "salience": 0.8,
        },
        msg,
    )
    return msg


def _record_dict(record_id: str = "replacement") -> dict:
    return {
        "id": record_id,
        "type": "semantic",
        "sensitivity": "low",
        "confidence": 1.0,
        "salience": 1.0,
        "payload": {
            "kind": "semantic",
            "subject": "entity-orchid",
            "predicate": "uses",
            "object": "Postgres",
            "validity": {"mode": "global"},
        },
    }


class _FakeStub:
    def __init__(self, response):
        self.response = response

    def __getattr__(self, name):
        def _method(request, **kwargs):
            self.method = name
            self.request = request
            self.kwargs = kwargs
            return self.response

        return _method


def test_client_refuses_api_key_over_non_local_plaintext_by_default():
    with pytest.raises(ValueError, match="plaintext gRPC"):
        MembraneClient("membrane.example.com:9090", api_key="secret")


@pytest.mark.parametrize("addr", ("localhost:9090", "127.0.0.1:9090", "[::1]:9090"))
def test_client_allows_api_key_over_loopback_plaintext(addr):
    client = MembraneClient(addr, api_key="secret")
    client.close()


def test_client_allows_api_key_over_non_local_tls():
    client = MembraneClient("membrane.example.com:443", tls=True, api_key="secret")
    client.close()


def test_client_allows_api_key_over_non_local_plaintext_with_explicit_override():
    client = MembraneClient(
        "membrane.example.com:9090",
        api_key="secret",
        allow_insecure_credentials=True,
    )
    client.close()


def test_client_rejects_non_boolean_insecure_credentials_override():
    with pytest.raises(ValueError, match="allow_insecure_credentials"):
        MembraneClient(
            "membrane.example.com:9090",
            api_key="secret",
            allow_insecure_credentials="true",  # type: ignore[arg-type]
        )


def test_loopback_plaintext_api_key_does_not_use_grpc_proxy(monkeypatch):
    target_listener = socket.socket()
    target_listener.bind(("127.0.0.1", 0))
    target_listener.listen(1)
    target_port = target_listener.getsockname()[1]

    proxy_listener = socket.socket()
    proxy_listener.bind(("127.0.0.1", 0))
    proxy_listener.listen(1)
    proxy_port = proxy_listener.getsockname()[1]

    target_connected = threading.Event()
    proxy_connected = threading.Event()

    def accept_one(listener, connected):
        try:
            connection, _ = listener.accept()
            connected.set()
            connection.close()
        except OSError:
            pass

    target_thread = threading.Thread(
        target=accept_one,
        args=(target_listener, target_connected),
        daemon=True,
    )
    proxy_thread = threading.Thread(
        target=accept_one,
        args=(proxy_listener, proxy_connected),
        daemon=True,
    )
    target_thread.start()
    proxy_thread.start()

    monkeypatch.setenv("grpc_proxy", f"http://127.0.0.1:{proxy_port}")
    monkeypatch.setenv("no_grpc_proxy", "")
    monkeypatch.setenv("no_proxy", "")
    monkeypatch.setenv("NO_PROXY", "")

    client = MembraneClient(
        f"127.0.0.1:{target_port}",
        api_key="security-test-secret",
        timeout=0.3,
    )
    try:
        with pytest.raises(grpc.RpcError):
            client.get_metrics()
        assert target_connected.wait(1), "loopback target did not receive the direct connection"
        assert not proxy_connected.wait(0.2), "loopback credential transport connected to grpc_proxy"
    finally:
        client.close()
        target_listener.close()
        proxy_listener.close()
        target_thread.join(timeout=1)
        proxy_thread.join(timeout=1)


def test_capture_memory_parses_created_records_and_edges():
    response = membrane_pb2.CaptureMemoryResponse()
    response.primary_record.CopyFrom(_record_msg("source-1"))
    response.created_records.append(_record_msg("entity-1", "entity"))
    response.edges.append(
        membrane_pb2.GraphEdge(
            source_id="source-1",
            predicate="mentions_entity",
            target_id="entity-1",
            weight=1.0,
        )
    )

    client = MembraneClient("localhost:0")
    client._stub = _FakeStub(response)  # type: ignore[method-assign]

    result = client.capture_memory(
        {"text": "Remember Orchid", "project": "Orchid"},
        context={"thread_id": "thread-1"},
        reason_to_remember="important niche term",
    )

    assert client._stub.method == "CaptureMemory"  # type: ignore[attr-defined]
    assert json_format.MessageToDict(
        client._stub.request.content,  # type: ignore[attr-defined]
        preserving_proto_field_name=True,
    ) == {"text": "Remember Orchid", "project": "Orchid"}
    assert json_format.MessageToDict(
        client._stub.request.context,  # type: ignore[attr-defined]
        preserving_proto_field_name=True,
    ) == {"thread_id": "thread-1"}
    assert client._stub.request.source_kind == "event"  # type: ignore[attr-defined]
    assert result.primary_record.id == "source-1"
    assert [record.id for record in result.created_records] == ["entity-1"]
    assert result.edges[0].predicate == "mentions_entity"

    client.close()


def test_retrieve_graph_parses_nodes_edges_and_selection():
    response = membrane_pb2.RetrieveGraphResponse(root_ids=["root-1"])
    response.nodes.append(
        membrane_pb2.GraphNode(record=_record_msg("root-1", "entity"), root=True, hop=0)
    )
    response.edges.append(
        membrane_pb2.GraphEdge(
            source_id="root-1",
            predicate="mentioned_in",
            target_id="rec-1",
            weight=1.0,
        )
    )
    response.selection.selected.append(_record_msg("root-1", "entity"))
    response.selection.confidence = 0.9
    response.selection.needs_more = False
    response.selection.scores["root-1"] = 0.91
    response.diagnostics.append(
        membrane_pb2.RetrievalDiagnostic(
            code="vector_rank_failed",
            message="vector index unavailable",
        )
    )
    response.projection.relations_truncated = True
    response.projection.history_omitted = True
    response.projection.records_truncated = True

    client = MembraneClient("localhost:0")
    client._stub = _FakeStub(response)  # type: ignore[method-assign]

    result = client.retrieve_graph(
        "orchid",
        max_hops=2,
        root_limit=3,
        query_embedding=[0.25, 0.5, 0.75],
    )

    assert client._stub.method == "RetrieveGraph"  # type: ignore[attr-defined]
    assert client._stub.request.max_hops == 2  # type: ignore[attr-defined]
    assert client._stub.request.root_limit == 3  # type: ignore[attr-defined]
    assert list(client._stub.request.query_embedding) == [0.25, 0.5, 0.75]  # type: ignore[attr-defined]
    assert result.nodes[0].record.id == "root-1"
    assert result.edges[0].target_id == "rec-1"
    assert result.root_ids == ["root-1"]
    assert result.selection is not None
    assert result.selection.confidence == 0.9
    assert result.selection.scores["root-1"] == 0.91
    assert result.diagnostics[0].code == "vector_rank_failed"
    assert "vector index" in result.diagnostics[0].message
    assert result.projection is not None
    assert result.projection.relations_truncated is True
    assert result.projection.history_omitted is True
    assert result.projection.records_truncated is True

    client.close()


def test_retrieve_graph_canonicalizes_duplicate_memory_types_before_rpc():
    client = MembraneClient("localhost:0")
    client._stub = _FakeStub(membrane_pb2.RetrieveGraphResponse())  # type: ignore[method-assign]

    client.retrieve_graph(
        "canonical memory types",
        memory_types=[MemoryType.EPISODIC, "semantic", MemoryType.ENTITY, "semantic"],
    )

    assert list(client._stub.request.memory_types) == ["entity", "semantic", "episodic"]  # type: ignore[attr-defined]
    client.close()


def test_retrieve_graph_root_only_uses_daemon_sentinel():
    client = MembraneClient("localhost:0")
    client._stub = _FakeStub(membrane_pb2.RetrieveGraphResponse())  # type: ignore[method-assign]

    client.retrieve_graph("roots only", root_only=True, max_hops=2)

    assert client._stub.request.max_hops == -1  # type: ignore[attr-defined]

    client.close()


def test_retrieve_graph_rejects_non_boolean_root_only_before_rpc():
    client = MembraneClient("localhost:0")
    client._stub = _FakeStub(membrane_pb2.RetrieveGraphResponse())  # type: ignore[method-assign]

    with pytest.raises(ValueError, match="root_only"):
        client.retrieve_graph("invalid roots-only flag", root_only="true")  # type: ignore[arg-type]

    assert "method" not in client._stub.__dict__  # type: ignore[attr-defined]

    client.close()


@pytest.mark.parametrize(
    "query_embedding",
    ([0.0, 0.0], [0.1, math.nan], [math.inf], "123", [0.1, "bad"], [True, 0.2]),
)
def test_retrieve_graph_rejects_invalid_query_embeddings_before_rpc(query_embedding):
    client = MembraneClient("localhost:0")
    client._stub = _FakeStub(membrane_pb2.RetrieveGraphResponse())  # type: ignore[method-assign]

    with pytest.raises(ValueError, match="query_embedding"):
        client.retrieve_graph("invalid vector", query_embedding=query_embedding)

    assert "method" not in client._stub.__dict__  # type: ignore[attr-defined]

    client.close()


def test_retrieve_graph_rejects_invalid_negative_max_hops_before_rpc():
    client = MembraneClient("localhost:0")
    client._stub = _FakeStub(membrane_pb2.RetrieveGraphResponse())  # type: ignore[method-assign]

    with pytest.raises(ValueError, match="max_hops"):
        client.retrieve_graph("invalid graph depth", max_hops=-2)

    assert "method" not in client._stub.__dict__  # type: ignore[attr-defined]

    client.close()


def test_retrieve_graph_rejects_non_integer_max_hops_before_rpc():
    client = MembraneClient("localhost:0")
    client._stub = _FakeStub(membrane_pb2.RetrieveGraphResponse())  # type: ignore[method-assign]

    with pytest.raises(ValueError, match="max_hops"):
        client.retrieve_graph("invalid graph depth", max_hops=1.5)  # type: ignore[arg-type]

    assert "method" not in client._stub.__dict__  # type: ignore[attr-defined]

    client.close()


@pytest.mark.parametrize(
    ("kwargs", "match"),
    (
        ({"root_limit": -1}, "root_limit"),
        ({"node_limit": 1.5}, "node_limit"),
        ({"edge_limit": 10001}, "edge_limit"),
        ({"min_salience": math.nan}, "min_salience"),
        ({"min_salience": -0.1}, "min_salience"),
        ({"min_salience": 1.1}, "min_salience"),
    ),
)
def test_retrieve_graph_rejects_invalid_graph_budgets_before_rpc(kwargs, match):
    client = MembraneClient("localhost:0")
    client._stub = _FakeStub(membrane_pb2.RetrieveGraphResponse())  # type: ignore[method-assign]

    with pytest.raises(ValueError, match=match):
        client.retrieve_graph("invalid graph budget", **kwargs)

    assert "method" not in client._stub.__dict__  # type: ignore[attr-defined]

    client.close()


def test_retrieve_graph_rejects_non_string_task_descriptor_before_rpc():
    client = MembraneClient("localhost:0")
    client._stub = _FakeStub(membrane_pb2.RetrieveGraphResponse())  # type: ignore[method-assign]

    with pytest.raises(ValueError, match="task_descriptor"):
        client.retrieve_graph(42)  # type: ignore[arg-type]

    assert "method" not in client._stub.__dict__  # type: ignore[attr-defined]

    client.close()


@pytest.mark.parametrize(
    ("kwargs", "match"),
    (
        ({"source_kind": "unsupported"}, "source_kind"),
        ({"proposed_type": "unsupported"}, "proposed_type"),
        ({"sensitivity": "classified"}, "sensitivity"),
    ),
)
def test_capture_memory_rejects_invalid_enum_options_before_rpc(kwargs, match):
    client = MembraneClient("localhost:0")
    client._stub = _FakeStub(membrane_pb2.CaptureMemoryResponse())  # type: ignore[method-assign]

    with pytest.raises(ValueError, match=match):
        client.capture_memory({"text": "invalid enum"}, **kwargs)

    assert "method" not in client._stub.__dict__  # type: ignore[attr-defined]

    client.close()


@pytest.mark.parametrize(
    ("kwargs", "match"),
    (
        ({"source": ""}, "source"),
        ({"source": 7}, "source"),
        ({"reason_to_remember": 7}, "reason_to_remember"),
        ({"summary": False}, "summary"),
        ({"timestamp": 42}, "timestamp"),
        ({"tags": "sdk"}, "tags"),
        ({"tags": ["sdk", 7]}, "tags"),
        ({"scope": 7}, "scope"),
    ),
)
def test_capture_memory_rejects_invalid_tag_and_scope_shapes_before_rpc(kwargs, match):
    client = MembraneClient("localhost:0")
    client._stub = _FakeStub(membrane_pb2.CaptureMemoryResponse())  # type: ignore[method-assign]

    with pytest.raises(ValueError, match=match):
        client.capture_memory({"text": "invalid metadata"}, **kwargs)  # type: ignore[arg-type]

    assert "method" not in client._stub.__dict__  # type: ignore[attr-defined]

    client.close()


@pytest.mark.parametrize(
    ("content", "kwargs", "match"),
    (
        ({"score": math.nan}, {}, r"content\.score"),
        ({"scores": [1.0, math.inf]}, {}, r"content\.scores\[1\]"),
        ({"ok": True}, {"context": {"score": -math.inf}}, r"context\.score"),
        ({1: "bad-key"}, {}, "content object keys"),
    ),
)
def test_capture_memory_rejects_non_json_numeric_values_before_rpc(content, kwargs, match):
    client = MembraneClient("localhost:0")
    client._stub = _FakeStub(membrane_pb2.CaptureMemoryResponse())  # type: ignore[method-assign]

    with pytest.raises(ValueError, match=match):
        client.capture_memory(content, **kwargs)

    assert "method" not in client._stub.__dict__  # type: ignore[attr-defined]

    client.close()


@pytest.mark.parametrize(
    ("record", "match"),
    (
        (
            {
                **_record_dict("semantic-missing-validity"),
                "payload": {
                    "kind": "semantic",
                    "subject": "Orchid",
                    "predicate": "uses",
                    "object": "Postgres",
                },
            },
            "payload.validity.mode",
        ),
        (
            {
                "id": "entity-missing-name",
                "type": "entity",
                "sensitivity": "low",
                "confidence": 1.0,
                "salience": 1.0,
                "payload": {"kind": "entity", "primary_type": "Project"},
            },
            "payload.canonical_name",
        ),
        (
            {
                "id": "entity-missing-identifier-namespace",
                "type": "entity",
                "sensitivity": "low",
                "confidence": 1.0,
                "salience": 1.0,
                "payload": {
                    "kind": "entity",
                    "canonical_name": "Orchid",
                    "identifiers": [{"value": "orchid"}],
                },
            },
            r"payload.identifiers\[0\].namespace",
        ),
        (
            {
                "id": "entity-missing-identifier-value",
                "type": "entity",
                "sensitivity": "low",
                "confidence": 1.0,
                "salience": 1.0,
                "payload": {
                    "kind": "entity",
                    "canonical_name": "Orchid",
                    "identifiers": [{"namespace": "slug", "value": " "}],
                },
            },
            r"payload.identifiers\[0\].value",
        ),
        (
            {
                **_record_dict("relation-missing-predicate"),
                "relations": [{"target_id": "target-1", "weight": 0.5}],
            },
            r"relations\[0\].predicate",
        ),
        (
            {
                **_record_dict("relation-missing-target"),
                "relations": [{"predicate": "supports", "target_id": " "}],
            },
            r"relations\[0\].target_id",
        ),
        (
            {
                **_record_dict("relation-invalid-weight"),
                "relations": [{"predicate": "supports", "target_id": "target-1", "weight": float("inf")}],
            },
            r"relations\[0\].weight",
        ),
    ),
)
def test_revision_rejects_malformed_replacement_records_before_rpc(record, match):
    client = MembraneClient("localhost:0")
    client._stub = _FakeStub(membrane_pb2.MemoryRecordResponse())  # type: ignore[method-assign]

    with pytest.raises(ValueError, match=match):
        client.supersede("old-1", record, actor="py-test", rationale="replace")

    assert "method" not in client._stub.__dict__  # type: ignore[attr-defined]
    client.close()


@pytest.mark.parametrize(
    ("kwargs", "match"),
    (
        ({"memory_types": ["entity", "unsupported"]}, "memory_types"),
        ({"trust": {"max_sensitivity": "classified", "authenticated": True, "actor_id": "tester", "scopes": []}}, "trust.max_sensitivity"),
    ),
)
def test_retrieve_graph_rejects_invalid_enum_options_before_rpc(kwargs, match):
    client = MembraneClient("localhost:0")
    client._stub = _FakeStub(membrane_pb2.RetrieveGraphResponse())  # type: ignore[method-assign]

    with pytest.raises(ValueError, match=match):
        client.retrieve_graph("invalid retrieval enum", **kwargs)

    assert "method" not in client._stub.__dict__  # type: ignore[attr-defined]

    client.close()


@pytest.mark.parametrize(
    "trust",
    (
        {"max_sensitivity": "low", "authenticated": "true", "actor_id": "tester", "scopes": []},
        {"max_sensitivity": "low", "authenticated": True, "actor_id": 7, "scopes": []},
        {"max_sensitivity": "low", "authenticated": True, "actor_id": "tester", "scopes": "prod"},
        {"max_sensitivity": "low", "authenticated": True, "actor_id": "tester", "scopes": ["prod", 7]},
        {"max_sensitivity": "low", "authenticated": True, "actor_id": "tester", "scopes": ["prod", " \t"]},
        TrustContext(max_sensitivity=Sensitivity.LOW, authenticated=True, actor_id="tester", scopes=["prod", 7]),  # type: ignore[list-item]
    ),
)
def test_retrieve_graph_rejects_invalid_trust_scope_shapes_before_rpc(trust):
    client = MembraneClient("localhost:0")
    client._stub = _FakeStub(membrane_pb2.RetrieveGraphResponse())  # type: ignore[method-assign]

    with pytest.raises(ValueError, match=r"trust\.(authenticated|actor_id|scopes)"):
        client.retrieve_graph("invalid trust scopes", trust=trust)  # type: ignore[arg-type]

    assert "method" not in client._stub.__dict__  # type: ignore[attr-defined]

    client.close()


def test_retrieve_by_id_rejects_invalid_trust_sensitivity_before_rpc():
    client = MembraneClient("localhost:0")
    client._stub = _FakeStub(membrane_pb2.MemoryRecordResponse())  # type: ignore[method-assign]

    with pytest.raises(ValueError, match="trust.max_sensitivity"):
        client.retrieve_by_id(
            "record-1",
            trust={
                "max_sensitivity": "classified",
                "authenticated": True,
                "actor_id": "tester",
                "scopes": [],
            },  # type: ignore[arg-type]
        )

    assert "method" not in client._stub.__dict__  # type: ignore[attr-defined]

    client.close()


def test_retrieve_by_id_rejects_invalid_trust_scope_shapes_before_rpc():
    client = MembraneClient("localhost:0")
    client._stub = _FakeStub(membrane_pb2.MemoryRecordResponse())  # type: ignore[method-assign]

    with pytest.raises(ValueError, match="trust.scopes"):
        client.retrieve_by_id(
            "record-1",
            trust={
                "max_sensitivity": "low",
                "authenticated": True,
                "actor_id": "tester",
                "scopes": "prod",
            },  # type: ignore[arg-type]
        )

    assert "method" not in client._stub.__dict__  # type: ignore[attr-defined]

    client.close()


@pytest.mark.parametrize(
    ("name", "invoke", "match"),
    (
        ("retrieve_by_id", lambda client: client.retrieve_by_id(""), "record_id"),
        ("retrieve_by_id_blank", lambda client: client.retrieve_by_id(" \t"), "record_id"),
        ("supersede", lambda client: client.supersede("", _record_dict(), "py-test", "replace"), "old_id"),
        ("fork", lambda client: client.fork("", _record_dict(), "py-test", "branch"), "source_id"),
        ("retract", lambda client: client.retract("", "py-test", "obsolete"), "record_id"),
        ("merge_empty_ids", lambda client: client.merge([], _record_dict(), "py-test", "merge"), "record_ids"),
        (
            "merge_blank_id",
            lambda client: client.merge(["source-1", " "], _record_dict(), "py-test", "merge"),
            r"record_ids\[1\]",
        ),
        (
            "merge_duplicate_id",
            lambda client: client.merge(["source-1", "source-1"], _record_dict(), "py-test", "merge"),
            r"record_ids\[1\].*duplicates",
        ),
        ("contest", lambda client: client.contest("", "", "py-test", "conflict"), "record_id"),
        ("reinforce", lambda client: client.reinforce("", "py-test", "useful"), "record_id"),
        ("penalize", lambda client: client.penalize("", 0.25, "py-test", "stale"), "record_id"),
    ),
)
def test_required_record_ids_reject_empty_values_before_rpc(name, invoke, match):
    client = MembraneClient("localhost:0")
    client._stub = _FakeStub(membrane_pb2.MemoryRecordResponse())  # type: ignore[method-assign]

    with pytest.raises(ValueError, match=match):
        invoke(client)

    assert "method" not in client._stub.__dict__, name  # type: ignore[attr-defined]

    client.close()


def test_revision_requests_send_typed_memory_records():
    response = membrane_pb2.MemoryRecordResponse()
    response.record.CopyFrom(_record_msg("new-1", "semantic"))

    client = MembraneClient("localhost:0")
    client._stub = _FakeStub(response)  # type: ignore[method-assign]

    result = client.supersede(
        "old-1",
        {
            "id": "new-1",
            "type": "semantic",
            "sensitivity": "low",
            "confidence": 1.0,
            "salience": 1.0,
            "payload": {
                "kind": "semantic",
                "subject": "entity-orchid",
                "predicate": "uses",
                "object": "Postgres",
                "validity": {"mode": "global"},
            },
        },
        actor="py-test",
        rationale="replace stale fact",
    )

    assert result.id == "new-1"
    assert client._stub.request.new_record.payload.WhichOneof("kind") == "semantic"  # type: ignore[attr-defined]
    assert client._stub.request.new_record.payload.semantic.object.string_value == "Postgres"  # type: ignore[attr-defined]

    client.close()


def test_revision_requests_normalize_go_json_semantic_payload_fields():
    response = membrane_pb2.MemoryRecordResponse()
    response.record.CopyFrom(_record_msg("new-1", "semantic"))

    client = MembraneClient("localhost:0")
    client._stub = _FakeStub(response)  # type: ignore[method-assign]

    client.supersede(
        "old-1",
        {
            "id": "new-1",
            "Type": "semantic",
            "sensitivity": "low",
            "confidence": 1.0,
            "salience": 1.0,
            "payload": {
                "Kind": "semantic",
                "Subject": "entity-orchid",
                "Predicate": "uses",
                "Object": "Postgres",
                "Validity": {
                    "Mode": "conditional",
                    "Conditions": {"environment": "prod"},
                },
            },
        },
        actor="py-test",
        rationale="replace stale fact",
    )

    payload = client._stub.request.new_record.payload  # type: ignore[attr-defined]
    assert payload.WhichOneof("kind") == "semantic"
    assert payload.semantic.subject == "entity-orchid"
    assert payload.semantic.object.string_value == "Postgres"
    assert payload.semantic.validity.mode == "conditional"
    assert payload.semantic.validity.conditions["environment"].string_value == "prod"

    client.close()


def test_revision_requests_normalize_go_json_entity_payload_fields():
    response = membrane_pb2.MemoryRecordResponse()
    response.record.CopyFrom(_record_msg("entity-new", "entity"))

    client = MembraneClient("localhost:0")
    client._stub = _FakeStub(response)  # type: ignore[method-assign]

    client.fork(
        "source-1",
        {
            "id": "entity-new",
            "Type": "entity",
            "sensitivity": "low",
            "confidence": 1.0,
            "salience": 1.0,
            "payload": {
                "Kind": "entity",
                "CanonicalName": "Project Orchid",
                "PrimaryType": "Project",
                "Types": ["Project", "Repository"],
                "Aliases": [{"Value": "orchid", "Kind": "nickname", "Locale": "en"}],
                "Identifiers": [{"Namespace": "github", "Value": "BennettSchwartz/orchid"}],
                "Summary": "Deployment project",
            },
        },
        actor="py-test",
        rationale="branch entity",
    )

    payload = client._stub.request.forked_record.payload  # type: ignore[attr-defined]
    assert payload.WhichOneof("kind") == "entity"
    assert payload.entity.canonical_name == "Project Orchid"
    assert payload.entity.primary_type == "Project"
    assert list(payload.entity.types) == ["Project", "Repository"]
    assert payload.entity.aliases[0].value == "orchid"
    assert payload.entity.identifiers[0].namespace == "github"
    assert payload.entity.identifiers[0].value == "BennettSchwartz/orchid"

    client.close()


@pytest.mark.parametrize("amount", (-0.1, math.nan, math.inf, True, "0.2"))
def test_penalize_rejects_invalid_amount_before_rpc(amount):
    client = MembraneClient("localhost:0")
    client._stub = _FakeStub(membrane_pb2.PenalizeResponse())  # type: ignore[method-assign]

    with pytest.raises(ValueError, match="amount"):
        client.penalize("record-1", amount, "py-test", "not useful")  # type: ignore[arg-type]

    assert "method" not in client._stub.__dict__  # type: ignore[attr-defined]

    client.close()


def test_penalize_sends_valid_amount():
    client = MembraneClient("localhost:0")
    client._stub = _FakeStub(membrane_pb2.PenalizeResponse())  # type: ignore[method-assign]

    client.penalize("record-1", 0.25, "py-test", "not useful")

    assert client._stub.method == "Penalize"  # type: ignore[attr-defined]
    assert client._stub.request.amount == 0.25  # type: ignore[attr-defined]

    client.close()


def test_get_metrics_parses_value_snapshot():
    client = MembraneClient("localhost:0")
    client._stub = _FakeStub(  # type: ignore[method-assign]
        membrane_pb2.MetricsResponse(
            snapshot=_value(
                {
                    "total_records": 42,
                    "embedded_records": 40,
                    "missing_embeddings": 2,
                    "embedding_coverage": 0.95,
                }
            )
        )
    )

    snapshot = client.get_metrics()
    assert snapshot["total_records"] == 42
    assert snapshot["embedding_coverage"] == 0.95

    client.close()
