"""Membrane gRPC client.

Communicates with the Membrane daemon over gRPC using the protobuf-defined
``membrane.v1.MembraneService`` contract.

Arbitrary content fields use ``google.protobuf.Value`` while records, graph
nodes, graph edges, and response envelopes are typed protobuf messages.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
import math
import re
from typing import Any, Sequence, cast

import grpc
from google.protobuf import json_format
from google.protobuf.message import Message
from google.protobuf.struct_pb2 import Value

from membrane.types import (
    CaptureMemoryResult,
    GraphEdge,
    GraphNode,
    MemoryRecord,
    MetricsSnapshot,
    MemoryType,
    RecordProjection,
    RetrievalDiagnostic,
    RetrieveGraphResult,
    SelectionResult,
    Sensitivity,
    SourceKind,
    TrustContext,
    normalize_graph_predicate,
)
from membrane.v1 import membrane_pb2, membrane_pb2_grpc

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now_rfc3339() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


_PAYLOAD_ONEOF_KEYS = {
    "episodic",
    "working",
    "semantic",
    "competence",
    "plan_graph",
    "entity",
}

_MAX_GRAPH_LIMIT = 10_000
_VALID_MEMORY_TYPES = {item.value for item in MemoryType}
_VALID_SENSITIVITIES = {item.value for item in Sensitivity}
_VALID_SOURCE_KINDS = {item.value for item in SourceKind}
_CANONICAL_MEMORY_TYPE_ORDER = [
    MemoryType.WORKING.value,
    MemoryType.ENTITY.value,
    MemoryType.SEMANTIC.value,
    MemoryType.COMPETENCE.value,
    MemoryType.PLAN_GRAPH.value,
    MemoryType.EPISODIC.value,
]
_VALID_VALIDITY_MODES = {"global", "conditional", "timeboxed"}


def _validate_json_value(name: str, value: Any) -> None:
    if value is None or isinstance(value, (str, bool)):
        return
    if isinstance(value, (int, float)):
        if not math.isfinite(float(value)):
            raise ValueError(f"{name} must be finite")
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{name} object keys must be strings")
            _validate_json_value(f"{name}.{key}", item)
        return
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for idx, item in enumerate(value):
            _validate_json_value(f"{name}[{idx}]", item)


def _value_message(value: Any, name: str = "value") -> Value:
    _validate_json_value(name, value)
    msg = Value()
    json_format.ParseDict(value, msg)
    return msg


def _value_to_python(value: Value) -> Any:
    return json_format.MessageToDict(value, preserving_proto_field_name=True)


def _message_to_dict(message: Message) -> dict[str, Any]:
    raw = json_format.MessageToDict(message, preserving_proto_field_name=True)
    if not isinstance(raw, dict):
        raise TypeError(f"Expected {message.DESCRIPTOR.full_name} object")
    return raw


def _parse_record_from_response(record: membrane_pb2.MemoryRecord) -> MemoryRecord:
    return MemoryRecord.from_dict(_message_to_dict(record))


def _parse_selection_from_response(
    selection: membrane_pb2.SelectionResult,
) -> SelectionResult | None:
    if selection.ByteSize() == 0:
        return None
    return SelectionResult.from_dict(_message_to_dict(selection))


def _graph_edge_from_response(edge: membrane_pb2.GraphEdge) -> GraphEdge:
    return GraphEdge.from_dict(_message_to_dict(edge))


def _graph_node_from_response(node: membrane_pb2.GraphNode) -> GraphNode:
    return GraphNode.from_dict(_message_to_dict(node))


def _parse_capture_memory_response(
    response: membrane_pb2.CaptureMemoryResponse,
) -> CaptureMemoryResult:
    return CaptureMemoryResult(
        primary_record=_parse_record_from_response(response.primary_record),
        created_records=[
            _parse_record_from_response(record) for record in response.created_records
        ],
        edges=[_graph_edge_from_response(edge) for edge in response.edges],
    )


def _parse_retrieve_graph_response(
    response: membrane_pb2.RetrieveGraphResponse,
) -> RetrieveGraphResult:
    return RetrieveGraphResult(
        nodes=[_graph_node_from_response(node) for node in response.nodes],
        edges=[_graph_edge_from_response(edge) for edge in response.edges],
        root_ids=list(response.root_ids),
        selection=_parse_selection_from_response(response.selection),
        diagnostics=[
            RetrievalDiagnostic.from_dict(_message_to_dict(diagnostic))
            for diagnostic in response.diagnostics
        ],
        projection=(
            RecordProjection.from_dict(_message_to_dict(response.projection))
            if response.HasField("projection")
            else None
        ),
    )


def _sensitivity_value(value: Sensitivity | str) -> str:
    return _validated_sensitivity("sensitivity", value)


def _validated_enum(name: str, value: Any, valid: set[str], *, allow_empty: bool = False) -> str:
    if isinstance(value, (MemoryType, Sensitivity, SourceKind)):
        value = value.value
    if allow_empty and value in (None, ""):
        return ""
    if not isinstance(value, str) or value not in valid:
        raise ValueError(f"{name} must be one of: {', '.join(sorted(valid))}")
    return value


def _validated_memory_type(name: str, value: MemoryType | str | None, *, allow_empty: bool = False) -> str:
    return _validated_enum(name, value, _VALID_MEMORY_TYPES, allow_empty=allow_empty)


def _validated_memory_types(values: Sequence[MemoryType | str] | None) -> list[str]:
    if not values:
        return []
    seen = {
        _validated_memory_type(f"memory_types[{idx}]", value)
        for idx, value in enumerate(values)
    }
    return [value for value in _CANONICAL_MEMORY_TYPE_ORDER if value in seen]


def _validated_sensitivity(name: str, value: Sensitivity | str) -> str:
    return _validated_enum(name, value, _VALID_SENSITIVITIES)


def _validated_source_kind(value: SourceKind | str) -> str:
    return _validated_enum("source_kind", value, _VALID_SOURCE_KINDS)


def _validated_string(name: str, value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    return value


def _validated_required_string(name: str, value: Any) -> str:
    text = _validated_string(name, value)
    if not text.strip():
        raise ValueError(f"{name} is required")
    return text


def _validated_bool(name: str, value: Any) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be a boolean")
    return value


def _validated_string_sequence(name: str, value: Sequence[str] | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (str, bytes)):
        raise ValueError(f"{name} must be a sequence of strings")
    values = list(value)
    for idx, item in enumerate(values):
        if not isinstance(item, str):
            raise ValueError(f"{name}[{idx}] must be a string")
    return values


def _validated_non_empty_string_sequence(name: str, value: Sequence[str] | None) -> list[str]:
    values = _validated_string_sequence(name, value)
    for idx, item in enumerate(values):
        if not item.strip():
            raise ValueError(f"{name}[{idx}] must be non-empty")
    return values


def _trust_context_message(trust: TrustContext | Mapping[str, Any]) -> membrane_pb2.TrustContext:
    if isinstance(trust, Mapping):
        max_sensitivity = trust.get("max_sensitivity", Sensitivity.LOW)
        authenticated = trust.get("authenticated", False)
        actor_id = trust.get("actor_id", "")
        raw_scopes = trust.get("scopes", [])
    else:
        max_sensitivity = trust.max_sensitivity
        authenticated = trust.authenticated
        actor_id = trust.actor_id
        raw_scopes = trust.scopes

    return membrane_pb2.TrustContext(
        max_sensitivity=_validated_sensitivity("trust.max_sensitivity", max_sensitivity),
        authenticated=_validated_bool("trust.authenticated", authenticated),
        actor_id=_validated_string("trust.actor_id", actor_id),
        scopes=_validated_non_empty_string_sequence("trust.scopes", raw_scopes),
    )


def _validated_query_embedding(query_embedding: Sequence[float] | None) -> list[float]:
    if query_embedding is None:
        return []
    if isinstance(query_embedding, (str, bytes)):
        raise ValueError("query_embedding must be a sequence of numbers")
    values = list(query_embedding)
    if not values:
        return []
    normalized: list[float] = []
    non_zero = False
    for idx, value in enumerate(values):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"query_embedding[{idx}] must be a number")
        numeric = float(value)
        if not math.isfinite(numeric):
            raise ValueError(f"query_embedding[{idx}] must be finite")
        if numeric != 0:
            non_zero = True
        normalized.append(numeric)
    if not non_zero:
        raise ValueError("query_embedding must contain at least one non-zero value")
    return normalized


def _validated_max_hops(max_hops: int) -> int:
    if isinstance(max_hops, bool) or not isinstance(max_hops, int):
        raise ValueError("max_hops must be an integer")
    if max_hops < -1:
        raise ValueError("max_hops must be -1 or non-negative")
    return max_hops


def _validated_graph_limit(name: str, value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    if value < 0 or value > _MAX_GRAPH_LIMIT:
        raise ValueError(f"{name} must be between 0 and {_MAX_GRAPH_LIMIT}")
    return value


def _validated_min_salience(min_salience: float) -> float:
    if isinstance(min_salience, bool) or not isinstance(min_salience, (int, float)):
        raise ValueError("min_salience must be a number")
    normalized = float(min_salience)
    if not math.isfinite(normalized) or normalized < 0 or normalized > 1:
        raise ValueError("min_salience must be finite and between 0 and 1")
    return normalized


def _validated_penalty_amount(amount: float) -> float:
    if isinstance(amount, bool) or not isinstance(amount, (int, float)):
        raise ValueError("amount must be a number")
    if not math.isfinite(float(amount)) or amount < 0:
        raise ValueError("amount must be non-negative and finite")
    return float(amount)


def _record_dict_for_proto(record: dict[str, Any] | MemoryRecord) -> dict[str, Any]:
    data = _memory_record_for_proto(record.to_dict() if isinstance(record, MemoryRecord) else record)
    _validate_memory_record_payload(data)
    payload = data.get("payload")
    if not isinstance(payload, dict) or not payload:
        return data

    oneof = _payload_oneof(payload)
    if oneof is not None:
        kind, value = oneof
        data["payload"] = {kind: _payload_for_proto(kind, value)}
        return data

    payload_kind = _mapping_field(payload, "kind", "Kind") or _mapping_field(data, "type", "Type")
    if isinstance(payload_kind, MemoryType):
        payload_kind = payload_kind.value
    if payload_kind in _PAYLOAD_ONEOF_KEYS:
        kind = str(payload_kind)
        data["payload"] = {kind: _payload_for_proto(kind, payload)}
    return data


def _memory_record_for_proto(record: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(record)
    field_map = {
        "id": ("id", "ID"),
        "type": ("type", "Type"),
        "sensitivity": ("sensitivity", "Sensitivity"),
        "confidence": ("confidence", "Confidence"),
        "salience": ("salience", "Salience"),
        "scope": ("scope", "Scope"),
        "tags": ("tags", "Tags"),
        "created_at": ("created_at", "CreatedAt", "createdAt"),
        "updated_at": ("updated_at", "UpdatedAt", "updatedAt"),
        "lifecycle": ("lifecycle", "Lifecycle"),
        "provenance": ("provenance", "Provenance"),
        "relations": ("relations", "Relations"),
        "payload": ("payload", "Payload"),
        "interpretation": ("interpretation", "Interpretation"),
        "audit_log": ("audit_log", "AuditLog", "auditLog"),
    }
    for target, names in field_map.items():
        value = _mapping_field(record, *names)
        if value is not None:
            out[target] = value
        for name in names:
            if name != target:
                out.pop(name, None)
    relations = _mapping_field(out, "relations")
    if isinstance(relations, Sequence) and not isinstance(relations, (str, bytes)):
        out["relations"] = [_relation_for_proto(relation) for relation in relations]
    return out


def _relation_for_proto(relation: Any) -> Any:
    if not isinstance(relation, Mapping):
        return relation
    out = dict(relation)
    field_map = {
        "predicate": ("predicate", "Predicate"),
        "target_id": ("target_id", "TargetID", "targetId"),
        "weight": ("weight", "Weight"),
        "created_at": ("created_at", "CreatedAt", "createdAt"),
    }
    for target, names in field_map.items():
        value = _mapping_field(relation, *names)
        if value is not None:
            out[target] = value
        for name in names:
            if name != target:
                out.pop(name, None)
    return out


def _mapping_field(data: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        if name in data:
            return data[name]
    return None


def _payload_oneof(payload: Mapping[str, Any]) -> tuple[str, Mapping[str, Any]] | None:
    for key in _PAYLOAD_ONEOF_KEYS:
        value = _mapping_field(payload, key, _proto_payload_json_name(key), _proto_payload_go_name(key))
        if isinstance(value, Mapping):
            return key, value
    return None


def _proto_payload_json_name(kind: str) -> str:
    if kind == "plan_graph":
        return "planGraph"
    return kind


def _proto_payload_go_name(kind: str) -> str:
    return "".join(part.capitalize() for part in kind.split("_"))


def _payload_for_proto(kind: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    if kind == MemoryType.SEMANTIC.value:
        return _semantic_payload_for_proto(payload)
    if kind == MemoryType.ENTITY.value:
        return _entity_payload_for_proto(payload)
    return dict(payload)


def _semantic_payload_for_proto(payload: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(payload)
    field_map = {
        "kind": ("kind", "Kind"),
        "subject": ("subject", "Subject"),
        "predicate": ("predicate", "Predicate"),
        "object": ("object", "Object"),
    }
    for target, names in field_map.items():
        value = _mapping_field(payload, *names)
        if value is not None:
            out[target] = value
        for name in names:
            if name != target:
                out.pop(name, None)
    validity = _mapping_field(payload, "validity", "Validity")
    if isinstance(validity, Mapping):
        out["validity"] = _validity_payload_for_proto(validity)
    out.pop("Validity", None)
    return out


def _validity_payload_for_proto(validity: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(validity)
    mode = _mapping_field(validity, "mode", "Mode")
    if mode is not None:
        out["mode"] = mode
    out.pop("Mode", None)
    conditions = _mapping_field(validity, "conditions", "Conditions")
    if conditions is not None:
        out["conditions"] = conditions
    out.pop("Conditions", None)
    return out


def _entity_payload_for_proto(payload: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(payload)
    field_map = {
        "kind": ("kind", "Kind"),
        "canonical_name": ("canonical_name", "CanonicalName", "canonicalName"),
        "primary_type": ("primary_type", "PrimaryType", "primaryType"),
        "types": ("types", "Types"),
        "summary": ("summary", "Summary"),
    }
    for target, names in field_map.items():
        value = _mapping_field(payload, *names)
        if value is not None:
            out[target] = value
        for name in names:
            if name != target:
                out.pop(name, None)
    aliases = _mapping_field(payload, "aliases", "Aliases")
    if isinstance(aliases, Sequence) and not isinstance(aliases, (str, bytes)):
        out["aliases"] = [_entity_alias_for_proto(alias) for alias in aliases]
    out.pop("Aliases", None)
    identifiers = _mapping_field(payload, "identifiers", "Identifiers")
    if isinstance(identifiers, Sequence) and not isinstance(identifiers, (str, bytes)):
        out["identifiers"] = [
            _entity_identifier_for_proto(identifier) for identifier in identifiers
        ]
    out.pop("Identifiers", None)
    return out


def _entity_alias_for_proto(alias: Any) -> Any:
    if isinstance(alias, str) or not isinstance(alias, Mapping):
        return alias
    return {
        "value": _mapping_field(alias, "value", "Value") or "",
        "kind": _mapping_field(alias, "kind", "Kind") or "",
        "locale": _mapping_field(alias, "locale", "Locale") or "",
    }


def _entity_identifier_for_proto(identifier: Any) -> Any:
    if not isinstance(identifier, Mapping):
        return identifier
    return {
        "namespace": _mapping_field(identifier, "namespace", "Namespace") or "",
        "value": _mapping_field(identifier, "value", "Value") or "",
    }


def _payload_for_kind(payload: Mapping[str, Any], record_type: Any, kind: str) -> Mapping[str, Any] | None:
    oneof = _payload_oneof(payload)
    if oneof is not None and oneof[0] == kind:
        return oneof[1]
    payload_kind = _mapping_field(payload, "kind", "Kind")
    if isinstance(payload_kind, MemoryType):
        payload_kind = payload_kind.value
    if isinstance(record_type, MemoryType):
        record_type = record_type.value
    if payload_kind == kind or (payload_kind is None and record_type == kind):
        return payload
    return None


def _validate_memory_record_payload(data: Mapping[str, Any]) -> None:
    _validate_memory_record_relations(data)

    payload = data.get("payload")
    if not isinstance(payload, Mapping):
        return
    record_type = data.get("type")
    semantic = _payload_for_kind(payload, record_type, MemoryType.SEMANTIC.value)
    if semantic is not None:
        _validate_semantic_payload(semantic)
    entity = _payload_for_kind(payload, record_type, MemoryType.ENTITY.value)
    if entity is not None:
        _validate_entity_payload(entity)


def _validate_memory_record_relations(data: Mapping[str, Any]) -> None:
    relations = _mapping_field(data, "relations", "Relations")
    if relations is None:
        return
    if isinstance(relations, (str, bytes)) or not isinstance(relations, Sequence):
        raise ValueError("relations must be a sequence")
    for idx, relation in enumerate(relations):
        if not isinstance(relation, Mapping):
            raise ValueError(f"relations[{idx}] must be a mapping")
        predicate = _mapping_field(relation, "predicate", "Predicate")
        if predicate is None:
            predicate = _mapping_field(relation, "kind", "Kind")
        if not isinstance(predicate, str) or not normalize_graph_predicate(predicate):
            raise ValueError(f"relations[{idx}].predicate is required")
        target_id = _mapping_field(relation, "target_id", "TargetID", "targetId")
        if not isinstance(target_id, str) or not target_id.strip():
            raise ValueError(f"relations[{idx}].target_id is required")
        weight = _mapping_field(relation, "weight", "Weight")
        if weight is None:
            continue
        if isinstance(weight, bool) or not isinstance(weight, (int, float)):
            raise ValueError(f"relations[{idx}].weight must be a number")
        numeric = float(weight)
        if not math.isfinite(numeric) or numeric < 0 or numeric > 1:
            raise ValueError(f"relations[{idx}].weight must be finite and between 0 and 1")


def _validate_semantic_payload(payload: Mapping[str, Any]) -> None:
    subject = _mapping_field(payload, "subject", "Subject")
    if not isinstance(subject, str) or not subject.strip():
        raise ValueError("payload.subject is required for semantic records")
    predicate = _mapping_field(payload, "predicate", "Predicate")
    if not isinstance(predicate, str) or not predicate.strip():
        raise ValueError("payload.predicate is required for semantic records")
    if _mapping_field(payload, "object", "Object") is None:
        raise ValueError("payload.object is required for semantic records")
    validity = _mapping_field(payload, "validity", "Validity")
    mode = _mapping_field(validity, "mode", "Mode") if isinstance(validity, Mapping) else None
    if not isinstance(mode, str) or mode not in _VALID_VALIDITY_MODES:
        raise ValueError("payload.validity.mode must be one of: global, conditional, timeboxed")


def _validate_entity_payload(payload: Mapping[str, Any]) -> None:
    canonical_name = _mapping_field(payload, "canonical_name", "CanonicalName", "canonicalName")
    if not isinstance(canonical_name, str) or not canonical_name.strip():
        raise ValueError("payload.canonical_name is required for entity records")
    identifiers = _mapping_field(payload, "identifiers", "Identifiers")
    if identifiers is None:
        return
    if isinstance(identifiers, (str, bytes)) or not isinstance(identifiers, Sequence):
        raise ValueError("payload.identifiers must be a sequence")
    for idx, identifier in enumerate(identifiers):
        if not isinstance(identifier, Mapping):
            raise ValueError(f"payload.identifiers[{idx}] must be a mapping")
        namespace = _mapping_field(identifier, "namespace", "Namespace")
        if not isinstance(namespace, str) or not namespace.strip():
            raise ValueError(f"payload.identifiers[{idx}].namespace is required")
        value = _mapping_field(identifier, "value", "Value")
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"payload.identifiers[{idx}].value is required")


def _record_message(record: dict[str, Any] | MemoryRecord) -> membrane_pb2.MemoryRecord:
    msg = membrane_pb2.MemoryRecord()
    json_format.ParseDict(_record_dict_for_proto(record), msg)
    return msg


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


def _host_from_grpc_address(addr: str) -> str:
    bracketed = re.fullmatch(r"\[([^\]]+)\](?::\d+)?", addr)
    if bracketed:
        return bracketed.group(1)
    if addr.startswith(("unix:", "unix-abstract:")):
        return "localhost"
    if addr.count(":") == 1:
        return addr.rsplit(":", 1)[0]
    return addr


def _is_loopback_address(addr: str) -> bool:
    return _host_from_grpc_address(addr).lower() in {"", "localhost", "127.0.0.1", "::1"}


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
        allow_insecure_credentials: bool = False,
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
            allow_insecure_credentials: Permit sending ``api_key`` over
                plaintext gRPC to a non-loopback address. Use only on trusted
                development networks.
            timeout: Default timeout in seconds for all RPC calls.
                ``None`` means no timeout.
        """

        if not isinstance(allow_insecure_credentials, bool):
            raise ValueError("allow_insecure_credentials must be a boolean")
        if (
            api_key is not None
            and not (tls or tls_ca_cert or allow_insecure_credentials)
            and not _is_loopback_address(addr)
        ):
            raise ValueError(
                "Refusing to send api_key over plaintext gRPC to a non-loopback address. "
                "Enable TLS or set allow_insecure_credentials=True for a trusted development network."
            )

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
        elif (
            api_key is not None
            and not allow_insecure_credentials
            and _is_loopback_address(addr)
        ):
            # The plaintext credential exception is safe only while the
            # connection stays local. gRPC otherwise honors proxy environment
            # variables, which can route a localhost target through a remote
            # HTTP CONNECT proxy and disclose the bearer token.
            self._channel = grpc.insecure_channel(
                addr,
                options=(("grpc.enable_http_proxy", 0),),
            )
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
        source_kind: SourceKind | str = SourceKind.EVENT,
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
            source=_validated_required_string("source", source),
            source_kind=_validated_source_kind(source_kind),
            content=_value_message(content, "content"),
            reason_to_remember=_validated_string("reason_to_remember", reason_to_remember),
            proposed_type=_validated_memory_type("proposed_type", proposed_type, allow_empty=True),
            summary=_validated_string("summary", summary),
            tags=_validated_string_sequence("tags", tags),
            scope=_validated_string("scope", scope),
            sensitivity=_sensitivity_value(sensitivity),
            timestamp=_now_rfc3339() if timestamp is None else _validated_string("timestamp", timestamp),
        )
        if context is not None:
            req.context.CopyFrom(_value_message(context, "context"))
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
            id=_validated_required_string("record_id", record_id),
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
        query_embedding: Sequence[float] | None = None,
        root_only: bool = False,
        min_salience: float = 0.0,
        root_limit: int = 10,
        node_limit: int = 25,
        edge_limit: int = 100,
        max_hops: int = 1,
    ) -> RetrieveGraphResult:
        """Retrieve graph-connected memories rooted in task-relevant matches."""
        if trust is None:
            trust = TrustContext()

        descriptor = _validated_string("task_descriptor", task_descriptor)
        types_list = _validated_memory_types(memory_types)

        req = membrane_pb2.RetrieveGraphRequest(
            task_descriptor=descriptor,
            trust=_trust_context_message(trust),
            memory_types=types_list,
            min_salience=_validated_min_salience(min_salience),
            root_limit=_validated_graph_limit("root_limit", root_limit),
            node_limit=_validated_graph_limit("node_limit", node_limit),
            edge_limit=_validated_graph_limit("edge_limit", edge_limit),
            max_hops=-1 if _validated_bool("root_only", root_only) else _validated_max_hops(max_hops),
            query_embedding=_validated_query_embedding(query_embedding),
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
        req = membrane_pb2.SupersedeRequest(
            old_id=_validated_required_string("old_id", old_id),
            new_record=_record_message(new_record),
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
        req = membrane_pb2.ForkRequest(
            source_id=_validated_required_string("source_id", source_id),
            forked_record=_record_message(forked_record),
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
            id=_validated_required_string("record_id", record_id),
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
        if isinstance(record_ids, (str, bytes)):
            raise ValueError("record_ids must be a sequence of strings")
        ids = list(record_ids)
        if not ids:
            raise ValueError("record_ids must contain at least one record ID")
        validated_ids = [
            _validated_required_string(f"record_ids[{idx}]", record_id)
            for idx, record_id in enumerate(ids)
        ]
        seen_ids: set[str] = set()
        for idx, record_id in enumerate(validated_ids):
            if record_id in seen_ids:
                raise ValueError(
                    f"record_ids[{idx}] duplicates an earlier merge source ID"
                )
            seen_ids.add(record_id)
        req = membrane_pb2.MergeRequest(
            ids=validated_ids,
            merged_record=_record_message(merged_record),
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
            id=_validated_required_string("record_id", record_id),
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
            id=_validated_required_string("record_id", record_id),
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
            id=_validated_required_string("record_id", record_id),
            amount=_validated_penalty_amount(amount),
            actor=actor,
            rationale=rationale,
        )
        self._stub.Penalize(req, **self._call_kwargs())

    # -- Metrics -------------------------------------------------------------

    def get_metrics(self) -> MetricsSnapshot:
        """Retrieve current metrics from the Membrane daemon.

        Returns:
            A dictionary containing the metrics snapshot.
        """
        resp = self._stub.GetMetrics(
            membrane_pb2.GetMetricsRequest(),
            **self._call_kwargs(),
        )
        snapshot = _value_to_python(resp.snapshot)
        if not isinstance(snapshot, dict):
            raise TypeError("Expected metrics snapshot object")
        return cast(MetricsSnapshot, snapshot)

    # -- Lifecycle -----------------------------------------------------------

    def close(self) -> None:
        """Close the underlying gRPC channel."""
        if self._channel is not None:
            self._channel.close()
            self._channel = None  # type: ignore[assignment]
