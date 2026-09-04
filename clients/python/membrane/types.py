"""Core types for the Membrane Python client.

Defines enums and dataclasses that mirror the Go schema package,
providing type-safe representations for memory records, sensitivity
levels, trust contexts, and related structures.
"""

from __future__ import annotations

import dataclasses
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional, TypedDict


# ---------------------------------------------------------------------------
# Enums (mirror pkg/schema/enums.go)
# ---------------------------------------------------------------------------


class MemoryType(str, Enum):
    """Category of memory record (RFC 15A.1)."""

    EPISODIC = "episodic"
    WORKING = "working"
    SEMANTIC = "semantic"
    COMPETENCE = "competence"
    PLAN_GRAPH = "plan_graph"
    ENTITY = "entity"


class Sensitivity(str, Enum):
    """Sensitivity classification for access control (RFC 15A.1)."""

    PUBLIC = "public"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    HYPER = "hyper"


class OutcomeStatus(str, Enum):
    """Result of an episodic experience (RFC 15A.6)."""

    SUCCESS = "success"
    FAILURE = "failure"
    PARTIAL = "partial"


class DecayCurve(str, Enum):
    """Mathematical function used for salience decay (RFC 15A.3)."""

    EXPONENTIAL = "exponential"


class DeletionPolicy(str, Enum):
    """How memory records may be deleted (RFC 15A.3)."""

    AUTO_PRUNE = "auto_prune"
    MANUAL_ONLY = "manual_only"
    NEVER = "never"


class RevisionStatus(str, Enum):
    """Current state of a semantic memory revision (RFC 15A.8)."""

    ACTIVE = "active"
    CONTESTED = "contested"
    RETRACTED = "retracted"


class ValidityMode(str, Enum):
    """How a semantic fact's validity is scoped (RFC 15A.8)."""

    GLOBAL = "global"
    CONDITIONAL = "conditional"
    TIMEBOXED = "timeboxed"


class TaskState(str, Enum):
    """Current state of a working memory task (RFC 15A.7)."""

    PLANNING = "planning"
    EXECUTING = "executing"
    BLOCKED = "blocked"
    WAITING = "waiting"
    DONE = "done"


class AuditAction(str, Enum):
    """Type of action recorded in an audit entry (RFC 15A.8)."""

    CREATE = "create"
    REVISE = "revise"
    FORK = "fork"
    MERGE = "merge"
    DELETE = "delete"
    REINFORCE = "reinforce"
    DECAY = "decay"


class ProvenanceKind(str, Enum):
    """Type of source in provenance tracking (RFC 15A.4)."""

    EVENT = "event"
    ARTIFACT = "artifact"
    TOOL_CALL = "tool_call"
    OBSERVATION = "observation"
    OUTCOME = "outcome"


class EdgeKind(str, Enum):
    """Type of edge in a plan graph (RFC 15A.11)."""

    DATA = "data"
    CONTROL = "control"


class EntityKind(str, Enum):
    """Canonical entity categories for graph-linked memory."""

    PERSON = "person"
    TOOL = "tool"
    PROJECT = "project"
    FILE = "file"
    CONCEPT = "concept"
    OTHER = "other"


class EntityType:
    """Built-in ontology type names for entity records.

    The ontology is open: callers may use these constants or provide any
    project-specific string type.
    """

    PERSON = "Person"
    ORGANIZATION = "Organization"
    TEAM = "Team"
    AGENT = "Agent"
    PROJECT = "Project"
    REPOSITORY = "Repository"
    FILE = "File"
    DIRECTORY = "Directory"
    SYMBOL = "Symbol"
    API = "API"
    SERVICE = "Service"
    DATABASE = "Database"
    PACKAGE = "Package"
    DEPENDENCY = "Dependency"
    TOOL = "Tool"
    COMMAND = "Command"
    RUNTIME = "Runtime"
    ENVIRONMENT = "Environment"
    TASK = "Task"
    ISSUE = "Issue"
    PULL_REQUEST = "PullRequest"
    DECISION = "Decision"
    REQUIREMENT = "Requirement"
    INCIDENT = "Incident"
    DOCUMENT = "Document"
    URL = "URL"
    DATASET = "Dataset"
    METRIC = "Metric"
    CONCEPT = "Concept"
    EVENT = "Event"
    OTHER = "Other"


BUILTIN_ENTITY_TYPES: tuple[str, ...] = (
    EntityType.PERSON,
    EntityType.ORGANIZATION,
    EntityType.TEAM,
    EntityType.AGENT,
    EntityType.PROJECT,
    EntityType.REPOSITORY,
    EntityType.FILE,
    EntityType.DIRECTORY,
    EntityType.SYMBOL,
    EntityType.API,
    EntityType.SERVICE,
    EntityType.DATABASE,
    EntityType.PACKAGE,
    EntityType.DEPENDENCY,
    EntityType.TOOL,
    EntityType.COMMAND,
    EntityType.RUNTIME,
    EntityType.ENVIRONMENT,
    EntityType.TASK,
    EntityType.ISSUE,
    EntityType.PULL_REQUEST,
    EntityType.DECISION,
    EntityType.REQUIREMENT,
    EntityType.INCIDENT,
    EntityType.DOCUMENT,
    EntityType.URL,
    EntityType.DATASET,
    EntityType.METRIC,
    EntityType.CONCEPT,
    EntityType.EVENT,
    EntityType.OTHER,
)


class GraphPredicate:
    """Structural predicates used by graph/entity memory."""

    MENTIONS_ENTITY = "mentions_entity"
    MENTIONED_IN = "mentioned_in"
    SUBJECT_ENTITY = "subject_entity"
    FACT_SUBJECT_OF = "fact_subject_of"
    OBJECT_ENTITY = "object_entity"
    FACT_OBJECT_OF = "fact_object_of"
    DERIVED_FROM = "derived_from"
    DERIVED_SEMANTIC = "derived_semantic"
    REFERENCES_RECORD = "references_record"
    REFERENCED_BY = "referenced_by"
    DEPENDS_ON = "depends_on"
    DEPENDENCY_OF = "dependency_of"
    USES = "uses"
    USED_BY = "used_by"
    CAUSED_BY = "caused_by"
    CAUSES = "causes"
    SUPPORTS = "supports"
    SUPPORTED_BY = "supported_by"
    CONTRADICTS = "contradicts"
    CONTRADICTED_BY = "contradicted_by"
    SUPERSEDES = "supersedes"
    SUPERSEDED_BY = "superseded_by"
    CONTESTED_BY = "contested_by"
    CONTESTS = "contests"


_INVERSE_GRAPH_PREDICATES = {
    GraphPredicate.MENTIONS_ENTITY: GraphPredicate.MENTIONED_IN,
    GraphPredicate.MENTIONED_IN: GraphPredicate.MENTIONS_ENTITY,
    GraphPredicate.SUBJECT_ENTITY: GraphPredicate.FACT_SUBJECT_OF,
    GraphPredicate.FACT_SUBJECT_OF: GraphPredicate.SUBJECT_ENTITY,
    GraphPredicate.OBJECT_ENTITY: GraphPredicate.FACT_OBJECT_OF,
    GraphPredicate.FACT_OBJECT_OF: GraphPredicate.OBJECT_ENTITY,
    GraphPredicate.DERIVED_FROM: GraphPredicate.DERIVED_SEMANTIC,
    GraphPredicate.DERIVED_SEMANTIC: GraphPredicate.DERIVED_FROM,
    GraphPredicate.REFERENCES_RECORD: GraphPredicate.REFERENCED_BY,
    GraphPredicate.REFERENCED_BY: GraphPredicate.REFERENCES_RECORD,
    GraphPredicate.DEPENDS_ON: GraphPredicate.DEPENDENCY_OF,
    GraphPredicate.DEPENDENCY_OF: GraphPredicate.DEPENDS_ON,
    GraphPredicate.USES: GraphPredicate.USED_BY,
    GraphPredicate.USED_BY: GraphPredicate.USES,
    GraphPredicate.CAUSED_BY: GraphPredicate.CAUSES,
    GraphPredicate.CAUSES: GraphPredicate.CAUSED_BY,
    GraphPredicate.SUPPORTS: GraphPredicate.SUPPORTED_BY,
    GraphPredicate.SUPPORTED_BY: GraphPredicate.SUPPORTS,
    GraphPredicate.CONTRADICTS: GraphPredicate.CONTRADICTED_BY,
    GraphPredicate.CONTRADICTED_BY: GraphPredicate.CONTRADICTS,
    GraphPredicate.SUPERSEDES: GraphPredicate.SUPERSEDED_BY,
    GraphPredicate.SUPERSEDED_BY: GraphPredicate.SUPERSEDES,
    GraphPredicate.CONTESTED_BY: GraphPredicate.CONTESTS,
    GraphPredicate.CONTESTS: GraphPredicate.CONTESTED_BY,
}


def normalize_graph_predicate(predicate: str) -> str:
    """Return the canonical storage spelling for graph predicates."""
    value = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", str(predicate).strip())
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    return "_".join(
        part for part in re.split(r"[\W_]+", value.lower()) if part
    )


def normalize_semantic_predicate(predicate: str) -> str:
    """Return the canonical storage spelling for semantic fact predicates."""
    return normalize_graph_predicate(predicate)


def inverse_graph_predicate(predicate: str) -> str:
    """Return the reverse-edge predicate for a graph predicate."""
    normalized = normalize_graph_predicate(predicate)
    return _INVERSE_GRAPH_PREDICATES.get(normalized, f"inverse_of_{normalized}")


class InterpretationStatus(str, Enum):
    """State of interpretation metadata attached to a record."""

    TENTATIVE = "tentative"
    RESOLVED = "resolved"


class SourceKind(str, Enum):
    """Source kinds for rich capture ingestion."""

    EVENT = "event"
    TOOL_OUTPUT = "tool_output"
    OBSERVATION = "observation"
    WORKING_STATE = "working_state"
    AGENT_TURN = "agent_turn"


_PAYLOAD_ONEOF_KEYS = {
    "episodic",
    "working",
    "semantic",
    "competence",
    "plan_graph",
    "entity",
}

_PROTO_VALUE_KINDS = {
    "nullValue",
    "numberValue",
    "stringValue",
    "boolValue",
    "structValue",
    "listValue",
    "null_value",
    "number_value",
    "string_value",
    "bool_value",
    "struct_value",
    "list_value",
}

_LEGACY_ENTITY_TYPE_MAP = {
    "person": EntityType.PERSON,
    "tool": EntityType.TOOL,
    "project": EntityType.PROJECT,
    "file": EntityType.FILE,
    "concept": EntityType.CONCEPT,
    "other": EntityType.OTHER,
}


def _as_enum_value(value: Any) -> Any:
    return value.value if isinstance(value, Enum) else value


def _plain(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if dataclasses.is_dataclass(value):
        out: dict[str, Any] = {}
        for item in dataclasses.fields(value):
            key = "from" if item.name == "from_" else item.name
            out[key] = _plain(getattr(value, item.name))
        return out
    if isinstance(value, list):
        return [_plain(item) for item in value]
    if isinstance(value, tuple):
        return [_plain(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _plain(item) for key, item in value.items()}
    return value


def _get_field(data: dict[str, Any], *names: str, default: Any = None) -> Any:
    for name in names:
        if name in data:
            return data[name]
    return default


def _proto_value_kind(value: dict[str, Any]) -> str | None:
    kind = value.get("kind")
    if isinstance(kind, str) and kind in _PROTO_VALUE_KINDS:
        return kind
    for candidate in (
        "stringValue",
        "numberValue",
        "boolValue",
        "structValue",
        "listValue",
        "nullValue",
        "string_value",
        "number_value",
        "bool_value",
        "struct_value",
        "list_value",
        "null_value",
    ):
        if candidate in value:
            return candidate
    return None


def _from_proto_value(value: Any) -> Any:
    if not isinstance(value, dict):
        return value
    kind = _proto_value_kind(value)
    if kind in ("nullValue", "null_value"):
        return None
    if kind in ("numberValue", "number_value"):
        return _get_field(value, "numberValue", "number_value", default=0)
    if kind in ("stringValue", "string_value"):
        return str(_get_field(value, "stringValue", "string_value", default=""))
    if kind in ("boolValue", "bool_value"):
        return bool(_get_field(value, "boolValue", "bool_value", default=False))
    if kind in ("structValue", "struct_value"):
        struct_value = _get_field(value, "structValue", "struct_value", default={})
        fields = struct_value.get("fields", {}) if isinstance(struct_value, dict) else {}
        return _from_proto_value_map(fields) or {}
    if kind in ("listValue", "list_value"):
        list_value = _get_field(value, "listValue", "list_value", default={})
        values = list_value.get("values", []) if isinstance(list_value, dict) else []
        return [_from_proto_value(item) for item in values]
    return value


def _from_proto_value_map(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    return {str(key): _from_proto_value(item) for key, item in value.items()}


def _normalize_plan_node_payload(data: dict[str, Any]) -> dict[str, Any]:
    out = dict(data)
    params = _from_proto_value_map(_get_field(data, "params", "Params"))
    guards = _from_proto_value_map(_get_field(data, "guards", "Guards"))
    if params is not None:
        out["params"] = params
    if guards is not None:
        out["guards"] = guards
    return out


def _normalize_plan_metrics_payload(data: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for target, names in {
        "avg_latency_ms": ("avg_latency_ms", "AvgLatencyMs", "avgLatencyMs"),
        "failure_rate": ("failure_rate", "FailureRate", "failureRate"),
        "execution_count": ("execution_count", "ExecutionCount", "executionCount"),
        "last_executed_at": ("last_executed_at", "LastExecutedAt", "lastExecutedAt"),
    }.items():
        value = _get_field(data, *names)
        if value is not None:
            out[target] = value
    return out


def _normalize_plan_graph_payload(data: dict[str, Any]) -> dict[str, Any]:
    out = dict(data)
    for target, names in {
        "plan_id": ("plan_id", "PlanID", "planId"),
        "version": ("version", "Version"),
        "intent": ("intent", "Intent"),
    }.items():
        value = _get_field(data, *names)
        if value is not None:
            out[target] = value
    for target, names in {
        "constraints": ("constraints", "Constraints"),
        "inputs_schema": ("inputs_schema", "InputsSchema", "inputsSchema"),
        "outputs_schema": ("outputs_schema", "OutputsSchema", "outputsSchema"),
    }.items():
        value = _from_proto_value_map(_get_field(data, *names))
        if value is not None:
            out[target] = value
    nodes = _get_field(data, "nodes", "Nodes")
    if isinstance(nodes, list):
        out["nodes"] = [
            _normalize_plan_node_payload(item) if isinstance(item, dict) else item
            for item in nodes
        ]
    edges = _get_field(data, "edges", "Edges")
    if isinstance(edges, list):
        out["edges"] = edges
    metrics = _get_field(data, "metrics", "Metrics")
    if isinstance(metrics, dict):
        out["metrics"] = _normalize_plan_metrics_payload(metrics)
    return out


def _normalize_entity_payload(data: dict[str, Any]) -> dict[str, Any]:
    out = dict(data)
    for target, names in {
        "kind": ("kind", "Kind"),
        "canonical_name": ("canonical_name", "CanonicalName", "canonicalName"),
        "primary_type": ("primary_type", "PrimaryType", "primaryType"),
        "summary": ("summary", "Summary"),
    }.items():
        value = _get_field(data, *names)
        if value is not None:
            out[target] = value

    types = _get_field(data, "types", "Types")
    if isinstance(types, list):
        out["types"] = [str(item) for item in types]

    aliases = _get_field(data, "aliases", "Aliases")
    if isinstance(aliases, list):
        normalized_aliases: list[Any] = []
        for alias in aliases:
            if isinstance(alias, str):
                normalized_aliases.append(alias)
            elif isinstance(alias, dict):
                normalized_aliases.append(
                    {
                        "value": _get_field(alias, "value", "Value", default=""),
                        "kind": _get_field(alias, "kind", "Kind", default=""),
                        "locale": _get_field(alias, "locale", "Locale", default=""),
                    }
                )
            else:
                normalized_aliases.append(alias)
        out["aliases"] = normalized_aliases

    identifiers = _get_field(data, "identifiers", "Identifiers")
    if isinstance(identifiers, list):
        normalized_identifiers: list[Any] = []
        for identifier in identifiers:
            if isinstance(identifier, dict):
                normalized_identifiers.append(
                    {
                        "namespace": _get_field(
                            identifier, "namespace", "Namespace", default=""
                        ),
                        "value": _get_field(identifier, "value", "Value", default=""),
                    }
                )
            else:
                normalized_identifiers.append(identifier)
        out["identifiers"] = normalized_identifiers

    return out


def _unwrap_payload(payload: Any) -> Any:
    if isinstance(payload, dict):
        for key in _PAYLOAD_ONEOF_KEYS:
            if key in payload:
                value = payload[key]
                if key == "plan_graph" and isinstance(value, dict):
                    return _normalize_plan_graph_payload(value)
                if key == "entity" and isinstance(value, dict):
                    return _normalize_entity_payload(value)
                return value
        if "planGraph" in payload:
            value = payload["planGraph"]
            if isinstance(value, dict):
                return _normalize_plan_graph_payload(value)
            return value
        kind = _get_field(payload, "kind", "Kind")
        if kind == "entity":
            return _normalize_entity_payload(payload)
    return payload


def _legacy_entity_type(value: Any) -> str:
    raw = _as_enum_value(value)
    if raw is None:
        return EntityType.CONCEPT
    return _LEGACY_ENTITY_TYPE_MAP.get(str(raw), str(raw))


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class TrustContext:
    """Trust context for retrieval operations.

    Controls which sensitivity levels the caller is allowed to access.
    Mirrors the Go ``retrieval.TrustContext``.
    """

    max_sensitivity: Sensitivity = Sensitivity.LOW
    authenticated: bool = False
    actor_id: str = ""
    scopes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "max_sensitivity": _as_enum_value(self.max_sensitivity),
            "authenticated": self.authenticated,
            "actor_id": self.actor_id,
            "scopes": self.scopes,
        }


@dataclass
class DecayProfile:
    """Decay configuration for a memory record."""

    curve: DecayCurve = DecayCurve.EXPONENTIAL
    half_life_seconds: int = 86400
    min_salience: Optional[float] = None
    max_age_seconds: Optional[int] = None
    reinforcement_gain: Optional[float] = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DecayProfile:
        return cls(
            curve=DecayCurve(data.get("curve", "exponential")),
            half_life_seconds=data.get("half_life_seconds", 86400),
            min_salience=data.get("min_salience"),
            max_age_seconds=data.get("max_age_seconds"),
            reinforcement_gain=data.get("reinforcement_gain"),
        )


@dataclass
class Lifecycle:
    """Lifecycle metadata for a memory record."""

    decay: DecayProfile = field(default_factory=DecayProfile)
    last_reinforced_at: str = ""
    pinned: bool = False
    deletion_policy: DeletionPolicy = DeletionPolicy.AUTO_PRUNE

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Lifecycle:
        return cls(
            decay=DecayProfile.from_dict(data.get("decay", {})),
            last_reinforced_at=data.get("last_reinforced_at", ""),
            pinned=data.get("pinned", False),
            deletion_policy=DeletionPolicy(
                data.get("deletion_policy", "auto_prune")
            ),
        )


@dataclass
class ProvenanceSource:
    """A single provenance source link."""

    kind: str = ""
    ref: str = ""
    timestamp: str = ""
    hash: str = ""
    created_by: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ProvenanceSource:
        return cls(
            kind=data.get("kind", ""),
            ref=data.get("ref", ""),
            timestamp=data.get("timestamp", ""),
            hash=data.get("hash", ""),
            created_by=data.get("created_by", ""),
        )


@dataclass
class Provenance:
    """Provenance tracking for a memory record."""

    sources: list[ProvenanceSource] = field(default_factory=list)
    created_by: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Provenance:
        sources = [
            ProvenanceSource.from_dict(s) for s in data.get("sources", [])
        ]
        return cls(sources=sources, created_by=data.get("created_by", ""))


@dataclass
class Relation:
    """A graph edge to another MemoryRecord."""

    target_id: str = ""
    predicate: str = ""
    weight: float = 1.0
    created_at: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Relation:
        return cls(
            target_id=_get_field(data, "target_id", "TargetID", "targetId", default=""),
            # "predicate" is the canonical field; fall back to legacy "kind"
            predicate=_get_field(data, "predicate", "Predicate", "kind", "Kind", default=""),
            weight=_get_field(data, "weight", "Weight", default=1.0),
            created_at=_get_field(data, "created_at", "CreatedAt", "createdAt", default=""),
        )


@dataclass
class GraphEdge:
    """A concrete graph edge returned by capture and graph retrieval."""

    source_id: str = ""
    predicate: str = ""
    target_id: str = ""
    weight: float = 1.0
    created_at: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> GraphEdge:
        return cls(
            source_id=_get_field(data, "source_id", "SourceID", "sourceId", default=""),
            predicate=_get_field(data, "predicate", "Predicate", default=""),
            target_id=_get_field(data, "target_id", "TargetID", "targetId", default=""),
            weight=_get_field(data, "weight", "Weight", default=1.0),
            created_at=_get_field(data, "created_at", "CreatedAt", "createdAt", default=""),
        )


@dataclass
class AuditEntry:
    """An audit log entry for a memory record."""

    action: str = ""
    actor: str = ""
    timestamp: str = ""
    rationale: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AuditEntry:
        return cls(
            action=_get_field(data, "action", "Action", default=""),
            actor=_get_field(data, "actor", "Actor", default=""),
            timestamp=_get_field(data, "timestamp", "Timestamp", default=""),
            rationale=_get_field(data, "rationale", "Rationale", default=""),
        )


@dataclass
class Mention:
    """Surface-form entity mention extracted during capture."""

    surface: str = ""
    entity_kind: Optional[EntityKind | str] = None
    canonical_entity_id: str = ""
    confidence: float = 0.0
    aliases: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Mention:
        raw_kind = _get_field(data, "entity_kind", "EntityKind", "entityKind")
        entity_kind: EntityKind | str | None = None
        if raw_kind:
            try:
                entity_kind = EntityKind(raw_kind)
            except ValueError:
                entity_kind = str(raw_kind)
        return cls(
            surface=_get_field(data, "surface", "Surface", default=""),
            entity_kind=entity_kind,
            canonical_entity_id=_get_field(
                data,
                "canonical_entity_id",
                "CanonicalEntityID",
                "canonicalEntityId",
                default="",
            ),
            confidence=float(_get_field(data, "confidence", "Confidence", default=0.0)),
            aliases=_get_field(data, "aliases", "Aliases", default=[]) or [],
        )


@dataclass
class RelationCandidate:
    """Tentative relation extracted during capture interpretation."""

    predicate: str = ""
    target_record_id: str = ""
    target_entity_id: str = ""
    confidence: float = 0.0
    resolved: bool = False

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RelationCandidate:
        return cls(
            predicate=_get_field(data, "predicate", "Predicate", default=""),
            target_record_id=_get_field(
                data,
                "target_record_id",
                "TargetRecordID",
                "targetRecordId",
                default="",
            ),
            target_entity_id=_get_field(
                data,
                "target_entity_id",
                "TargetEntityID",
                "targetEntityId",
                default="",
            ),
            confidence=float(_get_field(data, "confidence", "Confidence", default=0.0)),
            resolved=bool(_get_field(data, "resolved", "Resolved", default=False)),
        )


@dataclass
class ReferenceCandidate:
    """Tentative record or entity reference extracted during capture."""

    ref: str = ""
    target_record_id: str = ""
    target_entity_id: str = ""
    confidence: float = 0.0
    resolved: bool = False

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ReferenceCandidate:
        return cls(
            ref=_get_field(data, "ref", "Ref", default=""),
            target_record_id=_get_field(
                data,
                "target_record_id",
                "TargetRecordID",
                "targetRecordId",
                default="",
            ),
            target_entity_id=_get_field(
                data,
                "target_entity_id",
                "TargetEntityID",
                "targetEntityId",
                default="",
            ),
            confidence=float(_get_field(data, "confidence", "Confidence", default=0.0)),
            resolved=bool(_get_field(data, "resolved", "Resolved", default=False)),
        )


@dataclass
class Interpretation:
    """Tentative or resolved interpretation metadata on a MemoryRecord."""

    status: InterpretationStatus | str = InterpretationStatus.TENTATIVE
    summary: str = ""
    proposed_type: Optional[MemoryType | str] = None
    topical_labels: list[str] = field(default_factory=list)
    mentions: list[Mention] = field(default_factory=list)
    relation_candidates: list[RelationCandidate] = field(default_factory=list)
    reference_candidates: list[ReferenceCandidate] = field(default_factory=list)
    extraction_confidence: float = 0.0

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Interpretation:
        raw_status = _get_field(
            data, "status", "Status", default=InterpretationStatus.TENTATIVE.value
        )
        raw_type = _get_field(data, "proposed_type", "ProposedType", "proposedType")
        return cls(
            status=InterpretationStatus(raw_status) if raw_status else InterpretationStatus.TENTATIVE,
            summary=_get_field(data, "summary", "Summary", default=""),
            proposed_type=MemoryType(raw_type) if raw_type else None,
            topical_labels=_get_field(
                data, "topical_labels", "TopicalLabels", "topicalLabels", default=[]
            )
            or [],
            mentions=[
                Mention.from_dict(item)
                for item in _get_field(data, "mentions", "Mentions", default=[])
            ],
            relation_candidates=[
                RelationCandidate.from_dict(item)
                for item in _get_field(
                    data,
                    "relation_candidates",
                    "RelationCandidates",
                    "relationCandidates",
                    default=[],
                )
            ],
            reference_candidates=[
                ReferenceCandidate.from_dict(item)
                for item in _get_field(
                    data,
                    "reference_candidates",
                    "ReferenceCandidates",
                    "referenceCandidates",
                    default=[],
                )
            ],
            extraction_confidence=float(
                _get_field(
                    data,
                    "extraction_confidence",
                    "ExtractionConfidence",
                    "extractionConfidence",
                    default=0.0,
                )
            ),
        )


@dataclass
class MemoryRecord:
    """The atomic unit of storage in the Membrane memory substrate.

    Mirrors the Go ``schema.MemoryRecord`` structure. All fields use
    snake_case names that match the JSON wire format.
    """

    id: str = ""
    type: MemoryType = MemoryType.EPISODIC
    sensitivity: Sensitivity = Sensitivity.LOW
    confidence: float = 1.0
    salience: float = 1.0
    scope: str = ""
    tags: list[str] = field(default_factory=list)
    created_at: str = ""
    updated_at: str = ""
    lifecycle: Optional[Lifecycle] = None
    provenance: Optional[Provenance] = None
    relations: list[Relation] = field(default_factory=list)
    interpretation: Optional[Interpretation] = None
    payload: Any = field(default_factory=dict)
    audit_log: list[AuditEntry] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> MemoryRecord:
        """Construct a MemoryRecord from a JSON-decoded dictionary."""
        lifecycle = None
        lifecycle_data = _get_field(data, "lifecycle", "Lifecycle")
        if lifecycle_data is not None:
            lifecycle = Lifecycle.from_dict(lifecycle_data)

        provenance = None
        provenance_data = _get_field(data, "provenance", "Provenance")
        if provenance_data is not None:
            provenance = Provenance.from_dict(provenance_data)

        relations = [
            Relation.from_dict(r)
            for r in _get_field(data, "relations", "Relations", default=[]) or []
        ]
        interpretation = None
        interpretation_data = _get_field(data, "interpretation", "Interpretation")
        if interpretation_data is not None:
            interpretation = Interpretation.from_dict(interpretation_data)
        audit_log = [
            AuditEntry.from_dict(a)
            for a in _get_field(data, "audit_log", "AuditLog", "auditLog", default=[]) or []
        ]

        mem_type = _get_field(data, "type", "Type", default="episodic")
        sensitivity = _get_field(data, "sensitivity", "Sensitivity", default="low")

        return cls(
            id=_get_field(data, "id", "ID", default=""),
            type=MemoryType(mem_type) if mem_type else MemoryType.EPISODIC,
            sensitivity=(
                Sensitivity(sensitivity) if sensitivity else Sensitivity.LOW
            ),
            confidence=_get_field(data, "confidence", "Confidence", default=1.0),
            salience=_get_field(data, "salience", "Salience", default=1.0),
            scope=_get_field(data, "scope", "Scope", default=""),
            tags=_get_field(data, "tags", "Tags", default=[]) or [],
            created_at=_get_field(data, "created_at", "CreatedAt", "createdAt", default=""),
            updated_at=_get_field(data, "updated_at", "UpdatedAt", "updatedAt", default=""),
            lifecycle=lifecycle,
            provenance=provenance,
            relations=relations,
            interpretation=interpretation,
            payload=_unwrap_payload(_get_field(data, "payload", "Payload", default={})),
            audit_log=audit_log,
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a dictionary matching the JSON wire format."""
        d: dict[str, Any] = {
            "id": self.id,
            "type": _as_enum_value(self.type),
            "sensitivity": _as_enum_value(self.sensitivity),
            "confidence": self.confidence,
            "salience": self.salience,
        }
        if self.scope:
            d["scope"] = self.scope
        if self.tags:
            d["tags"] = self.tags
        if self.created_at:
            d["created_at"] = self.created_at
        if self.updated_at:
            d["updated_at"] = self.updated_at
        if self.lifecycle is not None:
            d["lifecycle"] = _plain(self.lifecycle)
        if self.provenance is not None:
            d["provenance"] = _plain(self.provenance)
        if self.relations:
            d["relations"] = _plain(self.relations)
        if self.interpretation is not None:
            d["interpretation"] = _plain(self.interpretation)
        if self.payload:
            d["payload"] = _plain(self.payload)
        if self.audit_log:
            d["audit_log"] = _plain(self.audit_log)
        return d


@dataclass
class SelectionResult:
    """Selector metadata returned alongside retrieval results."""

    selected: list[MemoryRecord] = field(default_factory=list)
    confidence: float = 0.0
    needs_more: bool = False
    scores: dict[str, float] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SelectionResult:
        selected = data.get("selected", data.get("Selected", []))
        confidence = data.get("confidence", data.get("Confidence", 0.0))
        needs_more = data.get(
            "needs_more", data.get("NeedsMore", data.get("needsMore", False))
        )
        scores = data.get("scores", data.get("Scores", {})) or {}
        return cls(
            selected=[MemoryRecord.from_dict(item) for item in selected or []],
            confidence=float(confidence),
            needs_more=bool(needs_more),
            scores={str(key): float(value) for key, value in scores.items()},
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "selected": [item.to_dict() for item in self.selected],
            "confidence": self.confidence,
            "needs_more": self.needs_more,
            "scores": self.scores,
        }


@dataclass
class GraphNode:
    """A graph node returned by graph-aware retrieval."""

    record: MemoryRecord = field(default_factory=MemoryRecord)
    root: bool = False
    hop: int = 0

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> GraphNode:
        return cls(
            record=MemoryRecord.from_dict(
                _get_field(data, "record", "Record", default={})
            ),
            root=bool(_get_field(data, "root", "Root", default=False)),
            hop=int(_get_field(data, "hop", "Hop", default=0)),
        )


@dataclass
class RecordProjection:
    """Fields or records omitted from a bounded retrieval response."""

    relations_omitted: bool = False
    relations_truncated: bool = False
    history_omitted: bool = False
    records_truncated: bool = False

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RecordProjection:
        return cls(
            relations_omitted=bool(
                _get_field(data, "relations_omitted", "RelationsOmitted", "relationsOmitted", default=False)
            ),
            relations_truncated=bool(
                _get_field(data, "relations_truncated", "RelationsTruncated", "relationsTruncated", default=False)
            ),
            history_omitted=bool(
                _get_field(data, "history_omitted", "HistoryOmitted", "historyOmitted", default=False)
            ),
            records_truncated=bool(
                _get_field(data, "records_truncated", "RecordsTruncated", "recordsTruncated", default=False)
            ),
        )


@dataclass
class RetrievalDiagnostic:
    """Non-fatal retrieval fallback diagnostic."""

    code: str = ""
    message: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RetrievalDiagnostic:
        return cls(
            code=str(_get_field(data, "code", "Code", default="")),
            message=str(_get_field(data, "message", "Message", default="")),
        )


@dataclass
class RetrieveGraphResult:
    """Graph-aware retrieval result with nodes, edges, and roots."""

    nodes: list[GraphNode] = field(default_factory=list)
    edges: list[GraphEdge] = field(default_factory=list)
    root_ids: list[str] = field(default_factory=list)
    selection: Optional[SelectionResult] = None
    diagnostics: list[RetrievalDiagnostic] = field(default_factory=list)
    projection: Optional[RecordProjection] = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RetrieveGraphResult:
        selection = _get_field(data, "selection", "Selection")
        projection = _get_field(data, "projection", "Projection")
        return cls(
            nodes=[
                GraphNode.from_dict(item)
                for item in _get_field(data, "nodes", "Nodes", default=[]) or []
            ],
            edges=[
                GraphEdge.from_dict(item)
                for item in _get_field(data, "edges", "Edges", default=[]) or []
            ],
            root_ids=[
                str(item)
                for item in _get_field(
                    data, "root_ids", "RootIDs", "rootIds", default=[]
                )
                or []
            ],
            selection=(
                SelectionResult.from_dict(selection)
                if isinstance(selection, dict)
                else None
            ),
            diagnostics=[
                RetrievalDiagnostic.from_dict(item)
                for item in _get_field(
                    data, "diagnostics", "Diagnostics", default=[]
                )
                or []
            ],
            projection=(
                RecordProjection.from_dict(projection)
                if isinstance(projection, dict)
                else None
            ),
        )


class MetricsSnapshot(TypedDict, total=False):
    """Point-in-time metrics snapshot returned by get_metrics."""

    collected_at: str
    total_records: int
    records_by_type: dict[str, int]
    avg_salience: float
    avg_confidence: float
    salience_distribution: dict[str, int]
    active_records: int
    pinned_records: int
    total_audit_entries: int
    embedding_model: str
    embedded_records: int
    missing_embeddings: int
    embedding_coverage: float
    memory_growth_rate: float
    retrieval_usefulness: float
    competence_success_rate: float
    plan_reuse_frequency: float
    revision_rate: float


@dataclass
class CaptureMemoryResult:
    """Rich capture response including created records and graph edges."""

    primary_record: MemoryRecord = field(default_factory=MemoryRecord)
    created_records: list[MemoryRecord] = field(default_factory=list)
    edges: list[GraphEdge] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CaptureMemoryResult:
        return cls(
            primary_record=MemoryRecord.from_dict(
                _get_field(
                    data, "primary_record", "PrimaryRecord", "primaryRecord", default={}
                )
            ),
            created_records=[
                MemoryRecord.from_dict(item)
                for item in _get_field(
                    data,
                    "created_records",
                    "CreatedRecords",
                    "createdRecords",
                    default=[],
                )
                or []
            ],
            edges=[
                GraphEdge.from_dict(item)
                for item in _get_field(data, "edges", "Edges", default=[]) or []
            ],
        )


# ---------------------------------------------------------------------------
# Constraint (RFC 15A.3, 15A.6)
# ---------------------------------------------------------------------------


@dataclass
class Constraint:
    """A constraint on task execution or plan selection."""

    type: str = ""
    key: str = ""
    value: Any = None
    required: bool = False

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Constraint:
        return cls(
            type=data.get("type", ""),
            key=data.get("key", ""),
            value=data.get("value"),
            required=data.get("required", False),
        )


# ---------------------------------------------------------------------------
# Provenance reference (RFC 15A.8)
# ---------------------------------------------------------------------------


@dataclass
class ProvenanceRef:
    """A reference to evidence supporting a semantic memory record."""

    source_type: str = ""
    source_id: str = ""
    timestamp: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ProvenanceRef:
        return cls(
            source_type=data.get("source_type", ""),
            source_id=data.get("source_id", ""),
            timestamp=data.get("timestamp", ""),
        )


# ---------------------------------------------------------------------------
# Revision state (RFC 15A.8)
# ---------------------------------------------------------------------------


@dataclass
class RevisionState:
    """Revision tracking metadata for a semantic memory record."""

    supersedes: str = ""
    superseded_by: str = ""
    status: Optional[RevisionStatus] = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RevisionState:
        raw_status = _get_field(data, "status", "Status")
        return cls(
            supersedes=_get_field(data, "supersedes", "Supersedes", default=""),
            superseded_by=_get_field(
                data, "superseded_by", "SupersededBy", "supersededBy", default=""
            ),
            status=RevisionStatus(raw_status) if raw_status else None,
        )


# ---------------------------------------------------------------------------
# Validity (RFC 15A.8)
# ---------------------------------------------------------------------------


@dataclass
class Validity:
    """Temporal and conditional validity scope for a semantic fact."""

    mode: ValidityMode = ValidityMode.GLOBAL
    conditions: dict[str, Any] = field(default_factory=dict)
    start: Optional[str] = None
    end: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Validity:
        return cls(
            mode=ValidityMode(_get_field(data, "mode", "Mode", default="global")),
            conditions=_from_proto_value_map(
                _get_field(data, "conditions", "Conditions", default={})
            )
            or {},
            start=_get_field(data, "start", "Start"),
            end=_get_field(data, "end", "End"),
        )


# ---------------------------------------------------------------------------
# Episodic payload helpers (RFC 15A.6, 15A.2)
# ---------------------------------------------------------------------------


@dataclass
class TimelineEvent:
    """A single event in an episodic memory timeline."""

    t: str = ""
    event_kind: str = ""
    ref: str = ""
    summary: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TimelineEvent:
        return cls(
            t=data.get("t", ""),
            event_kind=data.get("event_kind", ""),
            ref=data.get("ref", ""),
            summary=data.get("summary", ""),
        )


@dataclass
class ToolNode:
    """A tool call node in an episodic tool graph."""

    id: str = ""
    tool: str = ""
    args: dict[str, Any] = field(default_factory=dict)
    result: Any = None
    timestamp: str = ""
    depends_on: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ToolNode:
        return cls(
            id=data.get("id", ""),
            tool=data.get("tool", ""),
            args=data.get("args", {}),
            result=data.get("result"),
            timestamp=data.get("timestamp", ""),
            depends_on=data.get("depends_on", []),
        )


@dataclass
class EnvironmentSnapshot:
    """Environment context captured during an episodic memory."""

    os: str = ""
    os_version: str = ""
    tool_versions: dict[str, str] = field(default_factory=dict)
    working_directory: str = ""
    context: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> EnvironmentSnapshot:
        return cls(
            os=data.get("os", ""),
            os_version=data.get("os_version", ""),
            tool_versions=data.get("tool_versions", {}),
            working_directory=data.get("working_directory", ""),
            context=data.get("context", {}),
        )


# ---------------------------------------------------------------------------
# Payload types (RFC 15A.2, 15A.6 – 15A.11)
# ---------------------------------------------------------------------------


@dataclass
class EpisodicPayload:
    """Typed payload for episodic memory records (RFC 15A.6)."""

    kind: str = "episodic"
    timeline: list[TimelineEvent] = field(default_factory=list)
    tool_graph: list[ToolNode] = field(default_factory=list)
    environment: Optional[EnvironmentSnapshot] = None
    outcome: str = ""
    artifacts: list[str] = field(default_factory=list)
    tool_graph_ref: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> EpisodicPayload:
        env_data = data.get("environment")
        return cls(
            kind=data.get("kind", "episodic"),
            timeline=[TimelineEvent.from_dict(e) for e in data.get("timeline", [])],
            tool_graph=[ToolNode.from_dict(n) for n in data.get("tool_graph", [])],
            environment=EnvironmentSnapshot.from_dict(env_data) if env_data else None,
            outcome=data.get("outcome", ""),
            artifacts=data.get("artifacts", []),
            tool_graph_ref=data.get("tool_graph_ref", ""),
        )


@dataclass
class WorkingPayload:
    """Typed payload for working memory records (RFC 15A.7)."""

    kind: str = "working"
    thread_id: str = ""
    state: str = ""
    active_constraints: list[Constraint] = field(default_factory=list)
    next_actions: list[str] = field(default_factory=list)
    open_questions: list[str] = field(default_factory=list)
    context_summary: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WorkingPayload:
        return cls(
            kind=data.get("kind", "working"),
            thread_id=data.get("thread_id", ""),
            state=data.get("state", ""),
            active_constraints=[
                Constraint.from_dict(c)
                for c in data.get("active_constraints", [])
            ],
            next_actions=data.get("next_actions", []),
            open_questions=data.get("open_questions", []),
            context_summary=data.get("context_summary", ""),
        )


@dataclass
class SemanticPayload:
    """Typed payload for semantic memory records (RFC 15A.8)."""

    kind: str = "semantic"
    subject: str = ""
    predicate: str = ""
    object: Any = None
    validity: Optional[Validity] = None
    evidence: list[ProvenanceRef] = field(default_factory=list)
    revision_policy: str = ""
    revision: Optional[RevisionState] = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SemanticPayload:
        validity_data = _get_field(data, "validity", "Validity")
        revision_data = _get_field(data, "revision", "Revision")
        return cls(
            kind=_get_field(data, "kind", "Kind", default="semantic"),
            subject=_get_field(data, "subject", "Subject", default=""),
            predicate=_get_field(data, "predicate", "Predicate", default=""),
            object=_from_proto_value(_get_field(data, "object", "Object")),
            validity=Validity.from_dict(validity_data) if validity_data else None,
            evidence=[
                ProvenanceRef.from_dict(e)
                for e in _get_field(data, "evidence", "Evidence", default=[])
            ],
            revision_policy=_get_field(
                data, "revision_policy", "RevisionPolicy", "revisionPolicy", default=""
            ),
            revision=RevisionState.from_dict(revision_data) if revision_data else None,
        )


@dataclass
class Trigger:
    """A trigger condition that activates a competence skill."""

    signal: str = ""
    conditions: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Trigger:
        return cls(
            signal=data.get("signal", ""),
            conditions=data.get("conditions", {}),
        )


@dataclass
class RecipeStep:
    """A single step in a competence skill recipe."""

    step: str = ""
    tool: str = ""
    args_schema: dict[str, Any] = field(default_factory=dict)
    validation: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RecipeStep:
        return cls(
            step=data.get("step", ""),
            tool=data.get("tool", ""),
            args_schema=data.get("args_schema", {}),
            validation=data.get("validation", ""),
        )


@dataclass
class PerformanceStats:
    """Execution performance statistics for a competence or plan."""

    success_count: int = 0
    failure_count: int = 0
    success_rate: float = 0.0
    avg_latency_ms: float = 0.0
    last_used_at: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PerformanceStats:
        return cls(
            success_count=data.get("success_count", 0),
            failure_count=data.get("failure_count", 0),
            success_rate=data.get("success_rate", 0.0),
            avg_latency_ms=data.get("avg_latency_ms", 0.0),
            last_used_at=data.get("last_used_at"),
        )


@dataclass
class CompetencePayload:
    """Typed payload for competence memory records (RFC 15A.10)."""

    kind: str = "competence"
    skill_name: str = ""
    triggers: list[Trigger] = field(default_factory=list)
    recipe: list[RecipeStep] = field(default_factory=list)
    required_tools: list[str] = field(default_factory=list)
    failure_modes: list[str] = field(default_factory=list)
    fallbacks: list[str] = field(default_factory=list)
    performance: Optional[PerformanceStats] = None
    version: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CompetencePayload:
        perf_data = data.get("performance")
        return cls(
            kind=data.get("kind", "competence"),
            skill_name=data.get("skill_name", ""),
            triggers=[Trigger.from_dict(t) for t in data.get("triggers", [])],
            recipe=[RecipeStep.from_dict(r) for r in data.get("recipe", [])],
            required_tools=data.get("required_tools", []),
            failure_modes=data.get("failure_modes", []),
            fallbacks=data.get("fallbacks", []),
            performance=PerformanceStats.from_dict(perf_data) if perf_data else None,
            version=data.get("version", ""),
        )


@dataclass
class PlanNode:
    """An action node in a plan graph (RFC 15A.11)."""

    id: str = ""
    op: str = ""
    params: dict[str, Any] = field(default_factory=dict)
    guards: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PlanNode:
        return cls(
            id=_get_field(data, "id", "ID", default=""),
            op=_get_field(data, "op", "Op", default=""),
            params=_from_proto_value_map(_get_field(data, "params", "Params")) or {},
            guards=_from_proto_value_map(_get_field(data, "guards", "Guards")) or {},
        )


@dataclass
class PlanEdge:
    """A dependency edge in a plan graph (RFC 15A.11).

    Note: the JSON field name is ``"from"``; the Python attribute is
    ``from_`` to avoid shadowing the built-in keyword.
    """

    from_: str = ""
    to: str = ""
    kind: EdgeKind = EdgeKind.DATA

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PlanEdge:
        return cls(
            from_=data.get("from", ""),
            to=data.get("to", ""),
            kind=EdgeKind(data.get("kind", "data")),
        )


@dataclass
class PlanMetrics:
    """Execution metrics for a plan graph (RFC 15A.11)."""

    avg_latency_ms: float = 0.0
    failure_rate: float = 0.0
    execution_count: int = 0
    last_executed_at: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PlanMetrics:
        return cls(
            avg_latency_ms=_get_field(
                data, "avg_latency_ms", "AvgLatencyMs", "avgLatencyMs", default=0.0
            ),
            failure_rate=_get_field(
                data, "failure_rate", "FailureRate", "failureRate", default=0.0
            ),
            execution_count=_get_field(
                data, "execution_count", "ExecutionCount", "executionCount", default=0
            ),
            last_executed_at=_get_field(
                data, "last_executed_at", "LastExecutedAt", "lastExecutedAt"
            ),
        )


@dataclass
class PlanGraphPayload:
    """Typed payload for plan-graph memory records (RFC 15A.11)."""

    kind: str = "plan_graph"
    plan_id: str = ""
    version: str = ""
    intent: str = ""
    constraints: dict[str, Any] = field(default_factory=dict)
    inputs_schema: dict[str, Any] = field(default_factory=dict)
    outputs_schema: dict[str, Any] = field(default_factory=dict)
    nodes: list[PlanNode] = field(default_factory=list)
    edges: list[PlanEdge] = field(default_factory=list)
    metrics: Optional[PlanMetrics] = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PlanGraphPayload:
        data = _normalize_plan_graph_payload(data)
        metrics_data = data.get("metrics")
        return cls(
            kind=_get_field(data, "kind", "Kind", default="plan_graph"),
            plan_id=data.get("plan_id", ""),
            version=data.get("version", ""),
            intent=data.get("intent", ""),
            constraints=data.get("constraints", {}),
            inputs_schema=data.get("inputs_schema", {}),
            outputs_schema=data.get("outputs_schema", {}),
            nodes=[PlanNode.from_dict(n) for n in data.get("nodes", [])],
            edges=[PlanEdge.from_dict(e) for e in data.get("edges", [])],
            metrics=PlanMetrics.from_dict(metrics_data) if metrics_data else None,
        )


@dataclass
class EntityAlias:
    """Alternate surface form for an entity."""

    value: str = ""
    kind: str = ""
    locale: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any] | str) -> EntityAlias:
        if isinstance(data, str):
            return cls(value=data)
        return cls(
            value=_get_field(data, "value", "Value", default=""),
            kind=_get_field(data, "kind", "Kind", default=""),
            locale=_get_field(data, "locale", "Locale", default=""),
        )


@dataclass
class EntityIdentifier:
    """External or scoped identifier for an entity."""

    namespace: str = ""
    value: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> EntityIdentifier:
        return cls(
            namespace=_get_field(data, "namespace", "Namespace", default=""),
            value=_get_field(data, "value", "Value", default=""),
        )


@dataclass
class EntityPayload:
    """Typed payload for canonical entity records (RFC 15A.9)."""

    kind: str = "entity"
    canonical_name: str = ""
    primary_type: str = ""
    types: list[str] = field(default_factory=list)
    aliases: list[EntityAlias] = field(default_factory=list)
    identifiers: list[EntityIdentifier] = field(default_factory=list)
    summary: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> EntityPayload:
        primary_type = _get_field(data, "primary_type", "PrimaryType", "primaryType", default="")
        raw_entity_kind = _get_field(data, "entity_kind", "EntityKind", "entityKind")
        if not primary_type and raw_entity_kind is not None:
            primary_type = _legacy_entity_type(raw_entity_kind)

        types = [str(item) for item in _get_field(data, "types", "Types", default=[]) or []]
        if primary_type and primary_type not in types:
            types.insert(0, primary_type)

        return cls(
            kind=_get_field(data, "kind", "Kind", default="entity"),
            canonical_name=_get_field(data, "canonical_name", "CanonicalName", "canonicalName", default=""),
            primary_type=primary_type,
            types=types,
            aliases=[
                EntityAlias.from_dict(item)
                for item in _get_field(data, "aliases", "Aliases", default=[]) or []
            ],
            identifiers=[
                EntityIdentifier.from_dict(item)
                for item in _get_field(data, "identifiers", "Identifiers", default=[]) or []
            ],
            summary=_get_field(data, "summary", "Summary", default=""),
        )
