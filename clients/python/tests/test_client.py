"""Unit tests for membrane.client."""

from __future__ import annotations

from google.protobuf import json_format
from google.protobuf.struct_pb2 import Value

from membrane import MembraneClient
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

    client = MembraneClient("localhost:0")
    client._stub = _FakeStub(response)  # type: ignore[method-assign]

    result = client.retrieve_graph("orchid", max_hops=2, root_limit=3)

    assert client._stub.method == "RetrieveGraph"  # type: ignore[attr-defined]
    assert client._stub.request.max_hops == 2  # type: ignore[attr-defined]
    assert client._stub.request.root_limit == 3  # type: ignore[attr-defined]
    assert result.nodes[0].record.id == "root-1"
    assert result.edges[0].target_id == "rec-1"
    assert result.root_ids == ["root-1"]
    assert result.selection is not None
    assert result.selection.confidence == 0.9
    assert result.selection.scores["root-1"] == 0.91

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
            },
        },
        actor="py-test",
        rationale="replace stale fact",
    )

    assert result.id == "new-1"
    assert client._stub.request.new_record.payload.WhichOneof("kind") == "semantic"  # type: ignore[attr-defined]
    assert client._stub.request.new_record.payload.semantic.object.string_value == "Postgres"  # type: ignore[attr-defined]

    client.close()


def test_get_metrics_parses_value_snapshot():
    client = MembraneClient("localhost:0")
    client._stub = _FakeStub(  # type: ignore[method-assign]
        membrane_pb2.MetricsResponse(snapshot=_value({"total_records": 42}))
    )

    assert client.get_metrics()["total_records"] == 42

    client.close()
