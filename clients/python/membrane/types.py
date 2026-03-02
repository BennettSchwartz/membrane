"""Core types for the Membrane Python client.

Defines enums and dataclasses that mirror the Go schema package,
providing type-safe representations for memory records, sensitivity
levels, trust contexts, and related structures.
"""

from __future__ import annotations

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
    LINEAR = "linear"
    CUSTOM = "custom"


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
            "max_sensitivity": self.max_sensitivity.value,
            "authenticated": self.authenticated,
            "actor_id": self.actor_id,
            "scopes": self.scopes,
        }


@dataclass
class DecayProfile:
    """Decay configuration for a memory record."""

    curve: DecayCurve = DecayCurve.EXPONENTIAL
    half_life_seconds: int = 86400

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DecayProfile:
        return cls(
            curve=DecayCurve(data.get("curve", "exponential")),
            half_life_seconds=data.get("half_life_seconds", 86400),
        )


@dataclass
class Lifecycle:
    """Lifecycle metadata for a memory record."""

    decay: DecayProfile = field(default_factory=DecayProfile)
    last_reinforced_at: str = ""
    deletion_policy: DeletionPolicy = DeletionPolicy.AUTO_PRUNE

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Lifecycle:
        return cls(
            decay=DecayProfile.from_dict(data.get("decay", {})),
            last_reinforced_at=data.get("last_reinforced_at", ""),
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

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ProvenanceSource:
        return cls(
            kind=data.get("kind", ""),
            ref=data.get("ref", ""),
            timestamp=data.get("timestamp", ""),
        )


@dataclass
class Provenance:
    """Provenance tracking for a memory record."""

    sources: list[ProvenanceSource] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Provenance:
        sources = [
            ProvenanceSource.from_dict(s) for s in data.get("sources", [])
        ]
        return cls(sources=sources)


@dataclass
class Relation:
    """A graph edge to another MemoryRecord."""

    target_id: str = ""
    kind: str = ""
    weight: float = 1.0

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Relation:
        return cls(
            target_id=data.get("target_id", ""),
            kind=data.get("kind", ""),
            weight=data.get("weight", 1.0),
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
            payload=data.get("payload", {}),
            audit_log=audit_log,
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a dictionary matching the JSON wire format."""
        d: dict[str, Any] = {
            "id": self.id,
            "type": self.type.value,
            "sensitivity": self.sensitivity.value,
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
        if self.payload:
            d["payload"] = self.payload
        return d


@dataclass
class SelectionResult:
    """Selector metadata returned alongside retrieval results."""

    selected: list[MemoryRecord] = field(default_factory=list)
    confidence: float = 0.0
    needs_more: bool = False

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SelectionResult:
        selected = data.get("selected", data.get("Selected", []))
        confidence = data.get("confidence", data.get("Confidence", 0.0))
        needs_more = data.get("needs_more", data.get("NeedsMore", False))
        return cls(
            selected=[MemoryRecord.from_dict(item) for item in selected or []],
            confidence=float(confidence),
            needs_more=bool(needs_more),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "selected": [item.to_dict() for item in self.selected],
            "confidence": self.confidence,
            "needs_more": self.needs_more,
        }


@dataclass
class RetrieveResult:
    """Typed retrieval result with records and optional selector metadata."""

    records: list[MemoryRecord] = field(default_factory=list)
    selection: Optional[SelectionResult] = None
