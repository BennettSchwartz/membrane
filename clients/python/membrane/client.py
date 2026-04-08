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
    CaptureMemoryResult,
    GraphEdge,
    GraphNode,
    MemoryRecord,
    MemoryType,
    RetrieveGraphResult,
    SelectionResult,
    Sensitivity,
    SourceKind,
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

    ``MemoryRecordResponse`` exposes the record bytes directly on its
    ``record`` field.
    """
    return MemoryRecord.from_dict(_parse_json_bytes(record))


def _parse_selection_from_response(data: bytes) -> SelectionResult | None:
    if not data:
        return None

    raw = _parse_json_bytes(data)
    if not isinstance(raw, dict):
        raise TypeError("Expected selection metadata object")
    return SelectionResult.from_dict(raw)


def _parse_capture_memory_response(
    response: membrane_pb2.CaptureMemoryResponse,
) -> CaptureMemoryResult:
    raw_edges = _parse_json_bytes(response.edges) if response.edges else []
    return CaptureMemoryResult(
        primary_record=_parse_record_from_response(response.primary_record),
        created_records=[
            _parse_record_from_response(raw) for raw in response.created_records
        ],
        edges=[GraphEdge.from_dict(edge) for edge in raw_edges],
    )


def _parse_retrieve_graph_response(
    response: membrane_pb2.RetrieveGraphResponse,
) -> RetrieveGraphResult:
    raw_nodes = _parse_json_bytes(response.nodes) if response.nodes else []
    raw_edges = _parse_json_bytes(response.edges) if response.edges else []
    return RetrieveGraphResult(
        nodes=[GraphNode.from_dict(node) for node in raw_nodes],
        edges=[GraphEdge.from_dict(edge) for edge in raw_edges],
        root_ids=list(response.root_ids),
        selection=_parse_selection_from_response(response.selection),
    )


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
    graph-aware capture, retrieval, revision, reinforcement, and metrics.

    Example::

        from membrane import MembraneClient, Sensitivity, SourceKind, TrustContext

        client = MembraneClient("localhost:9090")

        capture = client.capture_memory(
            {"text": "Remember Orchid as the deploy target", "project": "Orchid"},
            source_kind=SourceKind.EVENT,
            sensitivity=Sensitivity.LOW,
        )

        trust = TrustContext(
            max_sensitivity=Sensitivity.MEDIUM,
            authenticated=True,
            actor_id="agent-1",
        )
        graph = client.retrieve_graph("deploy target", trust=trust, root_limit=5)

        client.close()

    The client also supports the context-manager protocol::

        with MembraneClient("localhost:9090") as client:
            capture = client.capture_memory(...)

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

    # -- Capture -------------------------------------------------------------

    def capture_memory(
        self,
        content: Any,
        *,
        source_kind: SourceKind | str = SourceKind.AGENT_TURN,
        context: Any = None,
        reason_to_remember: str = "",
        proposed_type: MemoryType | str | None = None,
        summary: str = "",
        sensitivity: Sensitivity | str = Sensitivity.LOW,
        source: str = "python-client",
        tags: Sequence[str] | None = None,
        scope: str = "",
        timestamp: str | None = None,
    ) -> CaptureMemoryResult:
        """Capture a rich memory candidate for interpretation and linking."""
        req = membrane_pb2.CaptureMemoryRequest(
            source=source,
            source_kind=(
                source_kind.value if isinstance(source_kind, SourceKind) else source_kind
            ),
            content=_json_bytes(content),
            reason_to_remember=reason_to_remember,
            proposed_type=(
                proposed_type.value
                if isinstance(proposed_type, MemoryType)
                else (proposed_type or "")
            ),
            summary=summary,
            tags=list(tags) if tags else [],
            scope=scope,
            sensitivity=_sensitivity_value(sensitivity),
            timestamp=timestamp or _now_rfc3339(),
        )
        if context is not None:
            req.context = _json_bytes(context)
        resp = self._stub.CaptureMemory(req, **self._call_kwargs())
        return _parse_capture_memory_response(resp)

    # -- Retrieval -----------------------------------------------------------

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

    def retrieve_graph(
        self,
        task_descriptor: str,
        *,
        trust: TrustContext | None = None,
        memory_types: Sequence[MemoryType | str] | None = None,
        min_salience: float = 0.0,
        root_limit: int = 10,
        node_limit: int = 25,
        edge_limit: int = 100,
        max_hops: int = 1,
    ) -> RetrieveGraphResult:
        """Retrieve graph-connected memories rooted in task-relevant matches."""
        if trust is None:
            trust = TrustContext()

        types_list: list[str] = []
        if memory_types:
            for mt in memory_types:
                types_list.append(
                    mt.value if isinstance(mt, MemoryType) else mt
                )

        req = membrane_pb2.RetrieveGraphRequest(
            task_descriptor=task_descriptor,
            trust=_trust_context_message(trust),
            memory_types=types_list,
            min_salience=min_salience,
            root_limit=root_limit,
            node_limit=node_limit,
            edge_limit=edge_limit,
            max_hops=max_hops,
        )
        resp = self._stub.RetrieveGraph(req, **self._call_kwargs())
        return _parse_retrieve_graph_response(resp)

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
