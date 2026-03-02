"""Membrane gRPC client.

Communicates with the Membrane daemon over gRPC using the protobuf-defined
``membrane.v1.MembraneService`` contract.

Several request/response fields still carry JSON-encoded ``bytes`` payloads
inside the protobuf messages. This client hides that encoding detail behind
Python-native helper methods.
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
from membrane.v1 import membrane_pb2, membrane_pb2_grpc

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now_rfc3339() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode("utf-8")


def _parse_json_bytes(data: bytes) -> Any:
    return json.loads(data.decode("utf-8"))  # type: ignore[no-any-return]


def _parse_record_from_response(record: bytes) -> MemoryRecord:
    """Parse a JSON-encoded MemoryRecord from a protobuf bytes field.

    Both ``IngestResponse`` and ``MemoryRecordResponse`` expose the record
    bytes directly on their ``record`` field.
    """
    return MemoryRecord.from_dict(_parse_json_bytes(record))


def _parse_records_from_response(
    response: membrane_pb2.RetrieveResponse,
) -> list[MemoryRecord]:
    """Parse a list of records from a RetrieveResponse."""
    return [_parse_record_from_response(raw) for raw in response.records]


def _sensitivity_value(value: Sensitivity | str) -> str:
    return value.value if isinstance(value, Sensitivity) else value


def _trust_context_message(trust: TrustContext) -> membrane_pb2.TrustContext:
    max_sensitivity = trust.max_sensitivity
    if isinstance(max_sensitivity, Sensitivity):
        max_sensitivity = max_sensitivity.value
    return membrane_pb2.TrustContext(
        max_sensitivity=max_sensitivity,
        authenticated=trust.authenticated,
        actor_id=trust.actor_id,
        scopes=trust.scopes,
    )


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

    For secured deployments, pass ``tls=True`` and/or ``api_key``::

        client = MembraneClient(
            "membrane.example.com:443",
            tls=True,
            api_key="your-api-key",
            timeout=10.0,
        )
    """

    def __init__(
        self,
        addr: str = "localhost:9090",
        *,
        tls: bool = False,
        tls_ca_cert: str | None = None,
        api_key: str | None = None,
        timeout: float | None = None,
    ) -> None:
        """Create a new client.

        Args:
            addr: gRPC server address (``host:port``).
            tls: Enable TLS transport. When *True* and *tls_ca_cert* is
                not provided, the system root certificates are used.
            tls_ca_cert: Path to a PEM-encoded CA certificate file for
                server verification.  Implies ``tls=True``.
            api_key: Optional Bearer token for server authentication.
            timeout: Default timeout in seconds for all RPC calls.
                ``None`` means no timeout.
        """
        self._addr = addr
        self._api_key = api_key
        self._timeout = timeout

        if tls or tls_ca_cert:
            if tls_ca_cert:
                with open(tls_ca_cert, "rb") as f:
                    root_certs = f.read()
                creds = grpc.ssl_channel_credentials(root_certificates=root_certs)
            else:
                creds = grpc.ssl_channel_credentials()
            self._channel: grpc.Channel = grpc.secure_channel(addr, creds)
        else:
            self._channel = grpc.insecure_channel(addr)

        self._stub = membrane_pb2_grpc.MembraneServiceStub(self._channel)

    def _call_kwargs(self) -> dict[str, Any]:
        """Return common keyword arguments for gRPC calls."""
        kwargs: dict[str, Any] = {}
        if self._timeout is not None:
            kwargs["timeout"] = self._timeout
        if self._api_key is not None:
            kwargs["metadata"] = [("authorization", f"Bearer {self._api_key}")]
        return kwargs

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
        req = membrane_pb2.IngestEventRequest(
            source=source,
            event_kind=event_kind,
            ref=ref,
            summary=summary,
            timestamp=timestamp or _now_rfc3339(),
            tags=list(tags) if tags else [],
            scope=scope,
            sensitivity=_sensitivity_value(sensitivity),
        )
        resp = self._stub.IngestEvent(req, **self._call_kwargs())
        return _parse_record_from_response(resp.record)

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
        req = membrane_pb2.IngestToolOutputRequest(
            source=source,
            tool_name=tool_name,
            depends_on=list(depends_on) if depends_on else [],
            timestamp=timestamp or _now_rfc3339(),
            tags=list(tags) if tags else [],
            scope=scope,
            sensitivity=_sensitivity_value(sensitivity),
        )
        if args is not None:
            req.args = _json_bytes(args)
        if result is not None:
            req.result = _json_bytes(result)
        resp = self._stub.IngestToolOutput(req, **self._call_kwargs())
        return _parse_record_from_response(resp.record)

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
        req = membrane_pb2.IngestObservationRequest(
            source=source,
            subject=subject,
            predicate=predicate,
            object=_json_bytes(obj),
            timestamp=timestamp or _now_rfc3339(),
            tags=list(tags) if tags else [],
            scope=scope,
            sensitivity=_sensitivity_value(sensitivity),
        )
        resp = self._stub.IngestObservation(req, **self._call_kwargs())
        return _parse_record_from_response(resp.record)

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
        req = membrane_pb2.IngestOutcomeRequest(
            source=source,
            target_record_id=target_record_id,
            outcome_status=(
                outcome_status.value
                if isinstance(outcome_status, OutcomeStatus)
                else outcome_status
            ),
            timestamp=timestamp or _now_rfc3339(),
        )
        resp = self._stub.IngestOutcome(req, **self._call_kwargs())
        return _parse_record_from_response(resp.record)

    def ingest_working_state(
        self,
        thread_id: str,
        state: str,
        *,
        next_actions: Sequence[str] | None = None,
        open_questions: Sequence[str] | None = None,
        context_summary: str = "",
        active_constraints: Sequence[dict[str, Any]] | None = None,
        sensitivity: Sensitivity | str = Sensitivity.LOW,
        source: str = "python-client",
        tags: Sequence[str] | None = None,
        scope: str = "",
        timestamp: str | None = None,
    ) -> MemoryRecord:
        """Ingest a working memory state snapshot.

        Args:
            thread_id: Identifier for the task thread.
            state: Current task state (e.g. ``"planning"``, ``"executing"``).
            next_actions: Planned next steps.
            open_questions: Unresolved questions.
            context_summary: Human-readable summary of current context.
            active_constraints: Active constraints as JSON-serializable dicts.
            sensitivity: Sensitivity classification.
            source: Source identifier for provenance.
            tags: Optional tags for categorization.
            scope: Visibility scope.
            timestamp: RFC 3339 timestamp; defaults to now.

        Returns:
            The created ``MemoryRecord``.
        """
        req = membrane_pb2.IngestWorkingStateRequest(
            source=source,
            thread_id=thread_id,
            state=state,
            next_actions=list(next_actions) if next_actions else [],
            open_questions=list(open_questions) if open_questions else [],
            context_summary=context_summary,
            timestamp=timestamp or _now_rfc3339(),
            tags=list(tags) if tags else [],
            scope=scope,
            sensitivity=_sensitivity_value(sensitivity),
        )
        if active_constraints is not None:
            req.active_constraints = _json_bytes(list(active_constraints))
        resp = self._stub.IngestWorkingState(req, **self._call_kwargs())
        return _parse_record_from_response(resp.record)

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

        req = membrane_pb2.RetrieveRequest(
            task_descriptor=task_descriptor,
            trust=_trust_context_message(trust),
            memory_types=types_list,
            min_salience=min_salience,
            limit=limit,
        )
        resp = self._stub.Retrieve(req, **self._call_kwargs())
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

        req = membrane_pb2.RetrieveByIDRequest(
            id=record_id,
            trust=_trust_context_message(trust),
        )
        resp = self._stub.RetrieveByID(req, **self._call_kwargs())
        return _parse_record_from_response(resp.record)

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

        req = membrane_pb2.SupersedeRequest(
            old_id=old_id,
            new_record=_json_bytes(new_record),
            actor=actor,
            rationale=rationale,
        )
        resp = self._stub.Supersede(req, **self._call_kwargs())
        return _parse_record_from_response(resp.record)

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

        req = membrane_pb2.ForkRequest(
            source_id=source_id,
            forked_record=_json_bytes(forked_record),
            actor=actor,
            rationale=rationale,
        )
        resp = self._stub.Fork(req, **self._call_kwargs())
        return _parse_record_from_response(resp.record)

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
        req = membrane_pb2.RetractRequest(
            id=record_id,
            actor=actor,
            rationale=rationale,
        )
        self._stub.Retract(req, **self._call_kwargs())

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

        req = membrane_pb2.MergeRequest(
            ids=list(record_ids),
            merged_record=_json_bytes(merged_record),
            actor=actor,
            rationale=rationale,
        )
        resp = self._stub.Merge(req, **self._call_kwargs())
        return _parse_record_from_response(resp.record)

    def contest(
        self,
        record_id: str,
        contesting_ref: str,
        actor: str,
        rationale: str,
    ) -> None:
        """Mark a record as contested due to conflicting evidence.

        Args:
            record_id: ID of the record to contest.
            contesting_ref: Reference to the conflicting evidence.
            actor: Identifier of the actor contesting the record.
            rationale: Human-readable reason for contesting.
        """
        req = membrane_pb2.ContestRequest(
            id=record_id,
            contesting_ref=contesting_ref,
            actor=actor,
            rationale=rationale,
        )
        self._stub.Contest(req, **self._call_kwargs())

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
        req = membrane_pb2.ReinforceRequest(
            id=record_id,
            actor=actor,
            rationale=rationale,
        )
        self._stub.Reinforce(req, **self._call_kwargs())

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
        req = membrane_pb2.PenalizeRequest(
            id=record_id,
            amount=amount,
            actor=actor,
            rationale=rationale,
        )
        self._stub.Penalize(req, **self._call_kwargs())

    # -- Metrics -------------------------------------------------------------

    def get_metrics(self) -> dict[str, Any]:
        """Retrieve current metrics from the Membrane daemon.

        Returns:
            A dictionary containing the metrics snapshot.
        """
        resp = self._stub.GetMetrics(
            membrane_pb2.GetMetricsRequest(),
            **self._call_kwargs(),
        )
        return _parse_json_bytes(resp.snapshot)

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
