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

    def Retrieve(self, request, **kwargs):
        self.request = request
        self.kwargs = kwargs
        return self.response


def test_retrieve_with_selection_parses_selection_metadata():
    client = MembraneClient("localhost:0")
    client._stub = _FakeStub(  # type: ignore[method-assign]
        SimpleNamespace(
            records=[_record_bytes("rec-1")],
            selection=json.dumps(
                {
                    "Selected": [
                        {
                            "id": "rec-2",
                            "type": "competence",
                            "sensitivity": "low",
                            "confidence": 0.8,
                            "salience": 0.7,
                        }
                    ],
                    "Confidence": 0.33,
                    "NeedsMore": True,
                }
            ).encode("utf-8"),
        )
    )

    result = client.retrieve_with_selection("debug task")

    assert len(result.records) == 1
    assert result.records[0].id == "rec-1"
    assert result.selection is not None
    assert result.selection.confidence == 0.33
    assert result.selection.needs_more is True
    assert result.selection.selected[0].id == "rec-2"

    client.close()


def test_retrieve_with_selection_handles_absent_selection():
    client = MembraneClient("localhost:0")
    client._stub = _FakeStub(  # type: ignore[method-assign]
        SimpleNamespace(records=[_record_bytes("rec-1")], selection=b"")
    )

    result = client.retrieve_with_selection("debug task")

    assert len(result.records) == 1
    assert result.selection is None
    assert [record.id for record in client.retrieve("debug task")] == ["rec-1"]

    client.close()
