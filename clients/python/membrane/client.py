"""Membrane gRPC client.

Communicates with the Membrane daemon over gRPC using JSON-encoded
messages. The Go server uses a hand-written gRPC service descriptor
(no protobuf), so this client sends and receives raw JSON bytes with
a custom JSON codec registered on the gRPC channel.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional, Sequence

import grpc

from membrane.types import (
    MemoryRecord,
    MemoryType,
    Sensitivity,
    TrustContext,
)

# ---------------------------------------------------------------------------
# JSON codec for gRPC
# ---------------------------------------------------------------------------
#
# The Go server uses a hand-written gRPC service descriptor with plain
# Go structs that carry ``json:"..."`` tags.  The Python client bypasses
# protobuf entirely by supplying custom ``request_serializer`` and
# ``response_deserializer`` callbacks to ``channel.unary_unary``.  These
# callbacks convert dicts to/from JSON bytes, which is the wire format
# understood by the Go server's codec.

_SERVICE = "membrane.v1.MembraneService"


def _method(name: str) -> str:
    """Return the full gRPC method path for *name*."""
    return f"/{_SERVICE}/{name}"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now_rfc3339() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_record_from_response(response: dict[str, Any]) -> MemoryRecord:
    """Extract and parse a MemoryRecord from a response dict.

    Both ``IngestResponse`` and ``MemoryRecordResponse`` wrap the record
    under a ``"record"`` key whose value is a JSON-encoded string *or*
    an already-decoded dict (depending on codec behaviour).
    """
    raw = response.get("record", response)
    if isinstance(raw, str):
        raw = json.loads(raw)
    if isinstance(raw, bytes):
        raw = json.loads(raw)
    return MemoryRecord.from_dict(raw)


def _parse_records_from_response(
    response: dict[str, Any],
) -> list[MemoryRecord]:
    """Parse a list of records from a RetrieveResponse."""
    raw_records = response.get("records", [])
    results: list[MemoryRecord] = []
    for raw in raw_records:
        if isinstance(raw, str):
            raw = json.loads(raw)
        if isinstance(raw, bytes):
            raw = json.loads(raw)
        results.append(MemoryRecord.from_dict(raw))
    return results


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class MembraneClient:
    """Python client for the Membrane memory substrate.

    Connects to the Membrane daemon over gRPC and exposes methods for
    ingestion, retrieval, revision, reinforcement, and metrics.

    Example::

        from membrane import MembraneClient, Sensitivity, TrustContext

        client = MembraneClient("localhost:9090")

        # Ingest an event
        record = client.ingest_event(
            event_kind="file_edit",
            ref="src/main.py",
            summary="Edited main entry point",
            sensitivity=Sensitivity.LOW,
        )

        # Retrieve relevant memories
        trust = TrustContext(
            max_sensitivity=Sensitivity.MEDIUM,
            authenticated=True,
            actor_id="agent-1",
        )
        records = client.retrieve("fix the login bug", trust=trust, limit=5)

        client.close()

    The client also supports the context-manager protocol::

        with MembraneClient("localhost:9090") as client:
            record = client.ingest_event(...)
    """

    def __init__(self, addr: str = "localhost:9090") -> None:
        self._addr = addr
        self._channel: grpc.Channel = grpc.insecure_channel(addr)

        # Pre-build callable stubs for each RPC method.  We use
        # ``channel.unary_unary`` with the full method path and custom
        # request/response serializers that speak JSON.
        self._stubs: dict[str, grpc.UnaryUnaryMultiCallable] = {}
        for method_name in (
            "IngestEvent",
            "IngestToolOutput",
            "IngestObservation",
            "IngestOutcome",
            "Retrieve",
            "RetrieveByID",
            "Supersede",
            "Fork",
            "Retract",
            "Merge",
            "Reinforce",
            "Penalize",
            "GetMetrics",
        ):
            self._stubs[method_name] = self._channel.unary_unary(
                _method(method_name),
                request_serializer=self._serialize,
                response_deserializer=self._deserialize,
            )

    # -- Serialization helpers -----------------------------------------------

    @staticmethod
    def _serialize(request: dict[str, Any]) -> bytes:
        """Serialize a request dict to JSON bytes for the wire."""
        return json.dumps(request, separators=(",", ":")).encode("utf-8")

    @staticmethod
    def _deserialize(data: bytes) -> dict[str, Any]:
        """Deserialize a JSON response from the wire."""
        return json.loads(data)  # type: ignore[no-any-return]

    # -- Context manager -----------------------------------------------------

    def __enter__(self) -> MembraneClient:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # -- Ingestion -----------------------------------------------------------

    def ingest_event(
        self,
        event_kind: str,
        ref: str,
        *,
        summary: str = "",
        sensitivity: Sensitivity | str = Sensitivity.LOW,
        source: str = "python-client",
        tags: Sequence[str] | None = None,
        scope: str = "",
        timestamp: str | None = None,
    ) -> MemoryRecord:
        """Ingest an event into the memory substrate.

        Args:
            event_kind: Kind of event (e.g. ``"file_edit"``, ``"error"``).
            ref: Reference identifier for the event source.
            summary: Optional human-readable summary.
            sensitivity: Sensitivity classification.
            source: Source identifier for provenance.
            tags: Optional tags for categorization.
            scope: Visibility scope.
            timestamp: RFC 3339 timestamp; defaults to now.

        Returns:
            The created ``MemoryRecord``.
        """
        req = {
            "source": source,
            "event_kind": event_kind,
            "ref": ref,
            "summary": summary,
            "timestamp": timestamp or _now_rfc3339(),
            "tags": list(tags) if tags else [],
            "scope": scope,
            "sensitivity": (
                sensitivity.value
                if isinstance(sensitivity, Sensitivity)
                else sensitivity
            ),
        }
        resp = self._stubs["IngestEvent"](req)
        return _parse_record_from_response(resp)

    def ingest_tool_output(
        self,
        tool_name: str,
        *,
        args: dict[str, Any] | None = None,
        result: Any = None,
        sensitivity: Sensitivity | str = Sensitivity.LOW,
        source: str = "python-client",
        depends_on: Sequence[str] | None = None,
        tags: Sequence[str] | None = None,
        scope: str = "",
        timestamp: str | None = None,
    ) -> MemoryRecord:
        """Ingest tool output into the memory substrate.

        Args:
            tool_name: Name of the tool that produced output.
            args: Arguments passed to the tool.
            result: Result produced by the tool.
            sensitivity: Sensitivity classification.
            source: Source identifier for provenance.
            depends_on: IDs of records this output depends on.
            tags: Optional tags for categorization.
            scope: Visibility scope.
            timestamp: RFC 3339 timestamp; defaults to now.

        Returns:
            The created ``MemoryRecord``.
        """
        req: dict[str, Any] = {
            "source": source,
            "tool_name": tool_name,
            "timestamp": timestamp or _now_rfc3339(),
            "tags": list(tags) if tags else [],
            "scope": scope,
            "depends_on": list(depends_on) if depends_on else [],
            "sensitivity": (
                sensitivity.value
                if isinstance(sensitivity, Sensitivity)
                else sensitivity
            ),
        }
        if args is not None:
            req["args"] = args
        if result is not None:
            req["result"] = result
        resp = self._stubs["IngestToolOutput"](req)
        return _parse_record_from_response(resp)

    def ingest_observation(
        self,
        subject: str,
        predicate: str,
        obj: Any,
        *,
        sensitivity: Sensitivity | str = Sensitivity.LOW,
        source: str = "python-client",
        tags: Sequence[str] | None = None,
        scope: str = "",
        timestamp: str | None = None,
    ) -> MemoryRecord:
        """Ingest an observation (subject-predicate-object triple).

        Args:
            subject: The subject of the observation.
            predicate: The predicate relating subject to object.
            obj: The object of the observation (any JSON-serializable value).
            sensitivity: Sensitivity classification.
            source: Source identifier for provenance.
            tags: Optional tags for categorization.
            scope: Visibility scope.
            timestamp: RFC 3339 timestamp; defaults to now.

        Returns:
            The created ``MemoryRecord``.
        """
        req: dict[str, Any] = {
            "source": source,
            "subject": subject,
            "predicate": predicate,
            "object": obj,
            "timestamp": timestamp or _now_rfc3339(),
            "tags": list(tags) if tags else [],
            "scope": scope,
            "sensitivity": (
                sensitivity.value
                if isinstance(sensitivity, Sensitivity)
                else sensitivity
            ),
        }
        resp = self._stubs["IngestObservation"](req)
        return _parse_record_from_response(resp)

    def ingest_outcome(
        self,
        target_record_id: str,
        outcome_status: OutcomeStatus | str,
        *,
        source: str = "python-client",
        timestamp: str | None = None,
    ) -> MemoryRecord:
        """Record an outcome for a previously ingested event.

        Args:
            target_record_id: ID of the record to attach the outcome to.
            outcome_status: One of ``"success"``, ``"failure"``, ``"partial"``.
            source: Source identifier for provenance.
            timestamp: RFC 3339 timestamp; defaults to now.

        Returns:
            The created ``MemoryRecord``.
        """
        req = {
            "source": source,
            "target_record_id": target_record_id,
            "outcome_status": (
                outcome_status.value
                if isinstance(outcome_status, OutcomeStatus)
                else outcome_status
            ),
            "timestamp": timestamp or _now_rfc3339(),
        }
        resp = self._stubs["IngestOutcome"](req)
        return _parse_record_from_response(resp)

    # -- Retrieval -----------------------------------------------------------

    def retrieve(
        self,
        task_descriptor: str,
        *,
        trust: TrustContext | None = None,
        memory_types: Sequence[MemoryType | str] | None = None,
        min_salience: float = 0.0,
        limit: int = 10,
    ) -> list[MemoryRecord]:
        """Retrieve memories relevant to a task descriptor.

        Args:
            task_descriptor: Natural-language description of the current task.
            trust: Trust context controlling access. Defaults to a minimal
                context with ``Sensitivity.LOW``.
            memory_types: Optional filter for specific memory types.
            min_salience: Minimum salience threshold (default ``0.0``).
            limit: Maximum number of records to return (default ``10``).

        Returns:
            List of matching ``MemoryRecord`` instances.
        """
        if trust is None:
            trust = TrustContext()

        types_list: list[str] = []
        if memory_types:
            for mt in memory_types:
                types_list.append(
                    mt.value if isinstance(mt, MemoryType) else mt
                )

        req = {
            "task_descriptor": task_descriptor,
            "trust": trust.to_dict(),
            "memory_types": types_list,
            "min_salience": min_salience,
            "limit": limit,
        }
        resp = self._stubs["Retrieve"](req)
        return _parse_records_from_response(resp)

    def retrieve_by_id(
        self,
        record_id: str,
        *,
        trust: TrustContext | None = None,
    ) -> MemoryRecord:
        """Retrieve a single memory record by its ID.

        Args:
            record_id: The UUID of the record.
            trust: Trust context controlling access. Defaults to a minimal
                context with ``Sensitivity.LOW``.

        Returns:
            The matching ``MemoryRecord``.
        """
        if trust is None:
            trust = TrustContext()

        req = {
            "id": record_id,
            "trust": trust.to_dict(),
        }
        resp = self._stubs["RetrieveByID"](req)
        return _parse_record_from_response(resp)

    # -- Revision ------------------------------------------------------------

    def supersede(
        self,
        old_id: str,
        new_record: dict[str, Any] | MemoryRecord,
        actor: str,
        rationale: str,
    ) -> MemoryRecord:
        """Supersede an existing record with a new version.

        Args:
            old_id: ID of the record to supersede.
            new_record: The replacement record (dict or ``MemoryRecord``).
            actor: Identifier of the actor performing the revision.
            rationale: Human-readable reason for the supersession.

        Returns:
            The newly created ``MemoryRecord``.
        """
        if isinstance(new_record, MemoryRecord):
            new_record = new_record.to_dict()

        req = {
            "old_id": old_id,
            "new_record": new_record,
            "actor": actor,
            "rationale": rationale,
        }
        resp = self._stubs["Supersede"](req)
        return _parse_record_from_response(resp)

    def fork(
        self,
        source_id: str,
        forked_record: dict[str, Any] | MemoryRecord,
        actor: str,
        rationale: str,
    ) -> MemoryRecord:
        """Fork a record into a conditional variant.

        Args:
            source_id: ID of the record to fork from.
            forked_record: The forked variant (dict or ``MemoryRecord``).
            actor: Identifier of the actor performing the fork.
            rationale: Human-readable reason for the fork.

        Returns:
            The newly created ``MemoryRecord``.
        """
        if isinstance(forked_record, MemoryRecord):
            forked_record = forked_record.to_dict()

        req = {
            "source_id": source_id,
            "forked_record": forked_record,
            "actor": actor,
            "rationale": rationale,
        }
        resp = self._stubs["Fork"](req)
        return _parse_record_from_response(resp)

    def retract(
        self,
        record_id: str,
        actor: str,
        rationale: str,
    ) -> None:
        """Retract (soft-delete) a record.

        Args:
            record_id: ID of the record to retract.
            actor: Identifier of the actor performing the retraction.
            rationale: Human-readable reason for the retraction.
        """
        req = {
            "id": record_id,
            "actor": actor,
            "rationale": rationale,
        }
        self._stubs["Retract"](req)

    def merge(
        self,
        record_ids: Sequence[str],
        merged_record: dict[str, Any] | MemoryRecord,
        actor: str,
        rationale: str,
    ) -> MemoryRecord:
        """Merge multiple records into a single record.

        Args:
            record_ids: IDs of the records to merge.
            merged_record: The merged result (dict or ``MemoryRecord``).
            actor: Identifier of the actor performing the merge.
            rationale: Human-readable reason for the merge.

        Returns:
            The newly created ``MemoryRecord``.
        """
        if isinstance(merged_record, MemoryRecord):
            merged_record = merged_record.to_dict()

        req = {
            "ids": list(record_ids),
            "merged_record": merged_record,
            "actor": actor,
            "rationale": rationale,
        }
        resp = self._stubs["Merge"](req)
        return _parse_record_from_response(resp)

    # -- Reinforcement / Penalization ----------------------------------------

    def reinforce(
        self,
        record_id: str,
        actor: str,
        rationale: str,
    ) -> None:
        """Reinforce a record, boosting its salience.

        Args:
            record_id: ID of the record to reinforce.
            actor: Identifier of the actor performing the reinforcement.
            rationale: Human-readable reason for the reinforcement.
        """
        req = {
            "id": record_id,
            "actor": actor,
            "rationale": rationale,
        }
        self._stubs["Reinforce"](req)

    def penalize(
        self,
        record_id: str,
        amount: float,
        actor: str,
        rationale: str,
    ) -> None:
        """Penalize a record, reducing its salience.

        Args:
            record_id: ID of the record to penalize.
            amount: Penalty amount to subtract from salience.
            actor: Identifier of the actor applying the penalty.
            rationale: Human-readable reason for the penalty.
        """
        req = {
            "id": record_id,
            "amount": amount,
            "actor": actor,
            "rationale": rationale,
        }
        self._stubs["Penalize"](req)

    # -- Metrics -------------------------------------------------------------

    def get_metrics(self) -> dict[str, Any]:
        """Retrieve current metrics from the Membrane daemon.

        Returns:
            A dictionary containing the metrics snapshot.
        """
        resp = self._stubs["GetMetrics"]({})
        snapshot = resp.get("snapshot", resp)
        if isinstance(snapshot, str):
            snapshot = json.loads(snapshot)
        if isinstance(snapshot, bytes):
            snapshot = json.loads(snapshot)
        return snapshot  # type: ignore[no-any-return]

    # -- Lifecycle -----------------------------------------------------------

    def close(self) -> None:
        """Close the underlying gRPC channel."""
        if self._channel is not None:
            self._channel.close()
            self._channel = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Convenience import so ``from membrane.client import OutcomeStatus`` works
# when the ingest_outcome method signature references it.
# ---------------------------------------------------------------------------
from membrane.types import OutcomeStatus  # noqa: E402, F811
