"""Unit tests for membrane.client."""

from __future__ import annotations

import json
from types import SimpleNamespace

from membrane import MembraneClient


def _record_bytes(record_id: str) -> bytes:
    return json.dumps(
        {
            "id": record_id,
            "type": "competence",
            "sensitivity": "low",
            "confidence": 0.9,
            "salience": 0.8,
        }
    ).encode("utf-8")


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
    client = MembraneClient("localhost:0")
    client._stub = _FakeStub(  # type: ignore[method-assign]
        SimpleNamespace(
            primary_record=_record_bytes("source-1"),
            created_records=[_record_bytes("entity-1")],
            edges=json.dumps(
                [
                    {
                        "source_id": "source-1",
                        "predicate": "mentions_entity",
                        "target_id": "entity-1",
                    }
                ]
            ).encode("utf-8"),
        )
    )

    result = client.capture_memory(
        {"text": "Remember Orchid", "project": "Orchid"},
        context={"thread_id": "thread-1"},
        reason_to_remember="important niche term",
    )

    assert client._stub.method == "CaptureMemory"  # type: ignore[attr-defined]
    assert result.primary_record.id == "source-1"
    assert [record.id for record in result.created_records] == ["entity-1"]
    assert result.edges[0].predicate == "mentions_entity"

    client.close()


def test_retrieve_graph_parses_nodes_edges_and_selection():
    client = MembraneClient("localhost:0")
    client._stub = _FakeStub(  # type: ignore[method-assign]
        SimpleNamespace(
            nodes=json.dumps(
                [
                    {
                        "record": {
                            "id": "root-1",
                            "type": "entity",
                            "sensitivity": "low",
                            "confidence": 1.0,
                            "salience": 1.0,
                        },
                        "root": True,
                        "hop": 0,
                    }
                ]
            ).encode("utf-8"),
            edges=json.dumps(
                [
                    {
                        "source_id": "root-1",
                        "predicate": "mentioned_in",
                        "target_id": "rec-1",
                    }
                ]
            ).encode("utf-8"),
            root_ids=["root-1"],
            selection=json.dumps(
                {
                    "selected": [
                        {
                            "id": "root-1",
                            "type": "entity",
                            "sensitivity": "low",
                            "confidence": 1.0,
                            "salience": 1.0,
                        }
                    ],
                    "confidence": 0.9,
                    "needs_more": False,
                }
            ).encode("utf-8"),
        )
    )

    result = client.retrieve_graph("orchid", max_hops=2, root_limit=3)

    assert client._stub.method == "RetrieveGraph"  # type: ignore[attr-defined]
    assert client._stub.request.max_hops == 2  # type: ignore[attr-defined]
    assert client._stub.request.root_limit == 3  # type: ignore[attr-defined]
    assert result.nodes[0].record.id == "root-1"
    assert result.edges[0].target_id == "rec-1"
    assert result.root_ids == ["root-1"]
    assert result.selection is not None
    assert result.selection.confidence == 0.9

    client.close()
