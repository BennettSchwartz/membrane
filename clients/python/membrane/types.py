"""Core types for the Membrane Python client.

Defines enums and dataclasses that mirror the Go schema package,
providing type-safe representations for memory records, sensitivity
levels, trust contexts, and related structures.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


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
    """Type of edge in a plan graph (RFC 15A.10)."""

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


def _unwrap_payload(payload: Any) -> Any:
    if isinstance(payload, dict):
        for key in _PAYLOAD_ONEOF_KEYS:
            if key in payload:
                return payload[key]
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
            target_id=data.get("target_id", ""),
            # "predicate" is the canonical field; fall back to legacy "kind"
            predicate=data.get("predicate", data.get("kind", "")),
            weight=data.get("weight", 1.0),
            created_at=data.get("created_at", ""),
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
            source_id=data.get("source_id", ""),
            predicate=data.get("predicate", ""),
            target_id=data.get("target_id", ""),
            weight=data.get("weight", 1.0),
            created_at=data.get("created_at", ""),
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
            action=data.get("action", ""),
            actor=data.get("actor", ""),
            timestamp=data.get("timestamp", ""),
            rationale=data.get("rationale", ""),
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
        raw_kind = data.get("entity_kind")
        entity_kind: EntityKind | str | None = None
        if raw_kind:
            try:
                entity_kind = EntityKind(raw_kind)
            except ValueError:
                entity_kind = str(raw_kind)
        return cls(
            surface=data.get("surface", ""),
            entity_kind=entity_kind,
            canonical_entity_id=data.get("canonical_entity_id", ""),
            confidence=float(data.get("confidence", 0.0)),
            aliases=data.get("aliases", []) or [],
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
            predicate=data.get("predicate", ""),
            target_record_id=data.get("target_record_id", ""),
            target_entity_id=data.get("target_entity_id", ""),
            confidence=float(data.get("confidence", 0.0)),
            resolved=bool(data.get("resolved", False)),
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
            ref=data.get("ref", ""),
            target_record_id=data.get("target_record_id", ""),
            target_entity_id=data.get("target_entity_id", ""),
            confidence=float(data.get("confidence", 0.0)),
            resolved=bool(data.get("resolved", False)),
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
        raw_status = data.get("status", InterpretationStatus.TENTATIVE.value)
        raw_type = data.get("proposed_type")
        return cls(
            status=InterpretationStatus(raw_status) if raw_status else InterpretationStatus.TENTATIVE,
            summary=data.get("summary", ""),
            proposed_type=MemoryType(raw_type) if raw_type else None,
            topical_labels=data.get("topical_labels", []) or [],
            mentions=[Mention.from_dict(item) for item in data.get("mentions", [])],
            relation_candidates=[
                RelationCandidate.from_dict(item)
                for item in data.get("relation_candidates", [])
            ],
            reference_candidates=[
                ReferenceCandidate.from_dict(item)
                for item in data.get("reference_candidates", [])
            ],
            extraction_confidence=float(data.get("extraction_confidence", 0.0)),
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
        if "lifecycle" in data:
            lifecycle = Lifecycle.from_dict(data["lifecycle"])

        provenance = None
        if "provenance" in data:
            provenance = Provenance.from_dict(data["provenance"])

        relations = [
            Relation.from_dict(r) for r in data.get("relations", [])
        ]
        interpretation = None
        if "interpretation" in data and data["interpretation"] is not None:
            interpretation = Interpretation.from_dict(data["interpretation"])
        audit_log = [
            AuditEntry.from_dict(a) for a in data.get("audit_log", [])
        ]

        mem_type = data.get("type", "episodic")
        sensitivity = data.get("sensitivity", "low")

        return cls(
            id=data.get("id", ""),
            type=MemoryType(mem_type) if mem_type else MemoryType.EPISODIC,
            sensitivity=(
                Sensitivity(sensitivity) if sensitivity else Sensitivity.LOW
            ),
            confidence=data.get("confidence", 1.0),
            salience=data.get("salience", 1.0),
            scope=data.get("scope", ""),
            tags=data.get("tags", []) or [],
            created_at=data.get("created_at", ""),
            updated_at=data.get("updated_at", ""),
            lifecycle=lifecycle,
            provenance=provenance,
            relations=relations,
            interpretation=interpretation,
            payload=_unwrap_payload(data.get("payload", {})),
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
        needs_more = data.get("needs_more", data.get("NeedsMore", False))
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
            record=MemoryRecord.from_dict(data.get("record", {})),
            root=bool(data.get("root", False)),
            hop=int(data.get("hop", 0)),
        )


@dataclass
class RetrieveGraphResult:
    """Graph-aware retrieval result with nodes, edges, and roots."""

    nodes: list[GraphNode] = field(default_factory=list)
    edges: list[GraphEdge] = field(default_factory=list)
    root_ids: list[str] = field(default_factory=list)
    selection: Optional[SelectionResult] = None


@dataclass
class CaptureMemoryResult:
    """Rich capture response including created records and graph edges."""

    primary_record: MemoryRecord = field(default_factory=MemoryRecord)
    created_records: list[MemoryRecord] = field(default_factory=list)
    edges: list[GraphEdge] = field(default_factory=list)


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
        raw_status = data.get("status")
        return cls(
            supersedes=data.get("supersedes", ""),
            superseded_by=data.get("superseded_by", ""),
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
            mode=ValidityMode(data.get("mode", "global")),
            conditions=data.get("conditions", {}),
            start=data.get("start"),
            end=data.get("end"),
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
# Payload types (RFC 15A.2, 15A.6 – 15A.10)
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
        validity_data = data.get("validity")
        revision_data = data.get("revision")
        return cls(
            kind=data.get("kind", "semantic"),
            subject=data.get("subject", ""),
            predicate=data.get("predicate", ""),
            object=data.get("object"),
            validity=Validity.from_dict(validity_data) if validity_data else None,
            evidence=[ProvenanceRef.from_dict(e) for e in data.get("evidence", [])],
            revision_policy=data.get("revision_policy", ""),
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
    """Typed payload for competence memory records (RFC 15A.9)."""

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
    """An action node in a plan graph (RFC 15A.10)."""

    id: str = ""
    op: str = ""
    params: dict[str, Any] = field(default_factory=dict)
    guards: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PlanNode:
        return cls(
            id=data.get("id", ""),
            op=data.get("op", ""),
            params=data.get("params", {}),
            guards=data.get("guards", {}),
        )


@dataclass
class PlanEdge:
    """A dependency edge in a plan graph (RFC 15A.10).

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
    """Execution metrics for a plan graph (RFC 15A.10)."""

    avg_latency_ms: float = 0.0
    failure_rate: float = 0.0
    execution_count: int = 0
    last_executed_at: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PlanMetrics:
        return cls(
            avg_latency_ms=data.get("avg_latency_ms", 0.0),
            failure_rate=data.get("failure_rate", 0.0),
            execution_count=data.get("execution_count", 0),
            last_executed_at=data.get("last_executed_at"),
        )


@dataclass
class PlanGraphPayload:
    """Typed payload for plan-graph memory records (RFC 15A.10)."""

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
        metrics_data = data.get("metrics")
        return cls(
            kind=data.get("kind", "plan_graph"),
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
            value=data.get("value", ""),
            kind=data.get("kind", ""),
            locale=data.get("locale", ""),
        )


@dataclass
class EntityIdentifier:
    """External or scoped identifier for an entity."""

    namespace: str = ""
    value: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> EntityIdentifier:
        return cls(
            namespace=data.get("namespace", ""),
            value=data.get("value", ""),
        )


@dataclass
class EntityPayload:
    """Typed payload for canonical entity records."""

    kind: str = "entity"
    canonical_name: str = ""
    primary_type: str = ""
    types: list[str] = field(default_factory=list)
    aliases: list[EntityAlias] = field(default_factory=list)
    identifiers: list[EntityIdentifier] = field(default_factory=list)
    summary: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> EntityPayload:
        primary_type = data.get("primary_type", "")
        if not primary_type and "entity_kind" in data:
            primary_type = _legacy_entity_type(data.get("entity_kind"))

        types = [str(item) for item in data.get("types", []) or []]
        if primary_type and primary_type not in types:
            types.insert(0, primary_type)

        return cls(
            kind=data.get("kind", "entity"),
            canonical_name=data.get("canonical_name", ""),
            primary_type=primary_type,
            types=types,
            aliases=[
                EntityAlias.from_dict(item)
                for item in data.get("aliases", []) or []
            ],
            identifiers=[
                EntityIdentifier.from_dict(item)
                for item in data.get("identifiers", []) or []
            ],
            summary=data.get("summary", ""),
        )
