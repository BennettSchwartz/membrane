"""Unit tests for membrane.types."""

from membrane.types import (
    AuditEntry,
    DecayCurve,
    DecayProfile,
    DeletionPolicy,
    Lifecycle,
    MemoryRecord,
    MemoryType,
    OutcomeStatus,
    Provenance,
    ProvenanceSource,
    Relation,
    RevisionStatus,
    Sensitivity,
    TrustContext,
)


class TestEnums:
    def test_memory_type_values(self):
        assert MemoryType.EPISODIC == "episodic"
        assert MemoryType.WORKING == "working"
        assert MemoryType.SEMANTIC == "semantic"
        assert MemoryType.COMPETENCE == "competence"
        assert MemoryType.PLAN_GRAPH == "plan_graph"

    def test_sensitivity_values(self):
        assert Sensitivity.PUBLIC == "public"
        assert Sensitivity.LOW == "low"
        assert Sensitivity.MEDIUM == "medium"
        assert Sensitivity.HIGH == "high"
        assert Sensitivity.HYPER == "hyper"

    def test_outcome_status_values(self):
        assert OutcomeStatus.SUCCESS == "success"
        assert OutcomeStatus.FAILURE == "failure"
        assert OutcomeStatus.PARTIAL == "partial"

    def test_decay_curve_values(self):
        assert DecayCurve.EXPONENTIAL == "exponential"
        assert DecayCurve.LINEAR == "linear"
        assert DecayCurve.CUSTOM == "custom"

    def test_deletion_policy_values(self):
        assert DeletionPolicy.AUTO_PRUNE == "auto_prune"
        assert DeletionPolicy.MANUAL_ONLY == "manual_only"
        assert DeletionPolicy.NEVER == "never"

    def test_revision_status_values(self):
        assert RevisionStatus.ACTIVE == "active"
        assert RevisionStatus.CONTESTED == "contested"
        assert RevisionStatus.RETRACTED == "retracted"

    def test_enum_from_string(self):
        assert MemoryType("episodic") is MemoryType.EPISODIC
        assert Sensitivity("high") is Sensitivity.HIGH


class TestTrustContext:
    def test_defaults(self):
        tc = TrustContext()
        assert tc.max_sensitivity is Sensitivity.LOW
        assert tc.authenticated is False
        assert tc.actor_id == ""
        assert tc.scopes == []

    def test_to_dict(self):
        tc = TrustContext(
            max_sensitivity=Sensitivity.MEDIUM,
            authenticated=True,
            actor_id="agent-1",
            scopes=["read", "write"],
        )
        d = tc.to_dict()
        assert d == {
            "max_sensitivity": "medium",
            "authenticated": True,
            "actor_id": "agent-1",
            "scopes": ["read", "write"],
        }


class TestMemoryRecord:
    def test_defaults(self):
        rec = MemoryRecord()
        assert rec.id == ""
        assert rec.type is MemoryType.EPISODIC
        assert rec.sensitivity is Sensitivity.LOW
        assert rec.confidence == 1.0
        assert rec.salience == 1.0

    def test_from_dict_minimal(self):
        data = {
            "id": "abc-123",
            "type": "semantic",
            "sensitivity": "medium",
            "confidence": 0.85,
            "salience": 0.9,
        }
        rec = MemoryRecord.from_dict(data)
        assert rec.id == "abc-123"
        assert rec.type is MemoryType.SEMANTIC
        assert rec.sensitivity is Sensitivity.MEDIUM
        assert rec.confidence == 0.85
        assert rec.salience == 0.9

    def test_from_dict_full(self):
        data = {
            "id": "rec-001",
            "type": "episodic",
            "sensitivity": "low",
            "confidence": 1.0,
            "salience": 1.0,
            "scope": "project",
            "tags": ["test", "demo"],
            "created_at": "2025-01-01T00:00:00Z",
            "updated_at": "2025-01-01T00:00:00Z",
            "lifecycle": {
                "decay": {
                    "curve": "exponential",
                    "half_life_seconds": 86400,
                },
                "last_reinforced_at": "2025-01-01T00:00:00Z",
                "deletion_policy": "auto_prune",
            },
            "provenance": {
                "sources": [
                    {
                        "kind": "event",
                        "ref": "evt-123",
                        "timestamp": "2025-01-01T00:00:00Z",
                    }
                ]
            },
            "relations": [
                {"target_id": "rec-002", "kind": "depends_on", "weight": 0.8}
            ],
            "payload": {"event_kind": "test", "summary": "A test event"},
            "audit_log": [
                {
                    "action": "create",
                    "actor": "system",
                    "timestamp": "2025-01-01T00:00:00Z",
                    "rationale": "Initial creation",
                }
            ],
        }
        rec = MemoryRecord.from_dict(data)
        assert rec.id == "rec-001"
        assert rec.scope == "project"
        assert rec.tags == ["test", "demo"]
        assert rec.lifecycle is not None
        assert rec.lifecycle.decay.curve is DecayCurve.EXPONENTIAL
        assert rec.lifecycle.decay.half_life_seconds == 86400
        assert rec.provenance is not None
        assert len(rec.provenance.sources) == 1
        assert rec.provenance.sources[0].kind == "event"
        assert len(rec.relations) == 1
        assert rec.relations[0].target_id == "rec-002"
        assert rec.payload == {"event_kind": "test", "summary": "A test event"}
        assert len(rec.audit_log) == 1
        assert rec.audit_log[0].action == "create"

    def test_to_dict(self):
        rec = MemoryRecord(
            id="xyz",
            type=MemoryType.COMPETENCE,
            sensitivity=Sensitivity.HIGH,
            confidence=0.7,
            salience=0.5,
            scope="workspace",
            tags=["skill"],
            payload={"tool_name": "grep"},
        )
        d = rec.to_dict()
        assert d["id"] == "xyz"
        assert d["type"] == "competence"
        assert d["sensitivity"] == "high"
        assert d["confidence"] == 0.7
        assert d["salience"] == 0.5
        assert d["scope"] == "workspace"
        assert d["tags"] == ["skill"]
        assert d["payload"] == {"tool_name": "grep"}

    def test_from_dict_empty(self):
        rec = MemoryRecord.from_dict({})
        assert rec.id == ""
        assert rec.type is MemoryType.EPISODIC
        assert rec.sensitivity is Sensitivity.LOW


class TestSubstructures:
    def test_decay_profile_from_dict(self):
        dp = DecayProfile.from_dict(
            {"curve": "linear", "half_life_seconds": 3600}
        )
        assert dp.curve is DecayCurve.LINEAR
        assert dp.half_life_seconds == 3600

    def test_lifecycle_from_dict(self):
        lc = Lifecycle.from_dict(
            {
                "decay": {"curve": "exponential", "half_life_seconds": 43200},
                "last_reinforced_at": "2025-06-01T12:00:00Z",
                "deletion_policy": "never",
            }
        )
        assert lc.decay.curve is DecayCurve.EXPONENTIAL
        assert lc.decay.half_life_seconds == 43200
        assert lc.deletion_policy is DeletionPolicy.NEVER

    def test_provenance_source_from_dict(self):
        ps = ProvenanceSource.from_dict(
            {
                "kind": "tool_call",
                "ref": "tc-456",
                "timestamp": "2025-01-01T00:00:00Z",
            }
        )
        assert ps.kind == "tool_call"
        assert ps.ref == "tc-456"

    def test_relation_from_dict(self):
        r = Relation.from_dict(
            {"target_id": "rec-999", "kind": "supersedes", "weight": 1.0}
        )
        assert r.target_id == "rec-999"
        assert r.kind == "supersedes"
        assert r.weight == 1.0

    def test_audit_entry_from_dict(self):
        ae = AuditEntry.from_dict(
            {
                "action": "revise",
                "actor": "agent-2",
                "timestamp": "2025-03-01T00:00:00Z",
                "rationale": "Updated based on feedback",
            }
        )
        assert ae.action == "revise"
        assert ae.actor == "agent-2"
        assert ae.rationale == "Updated based on feedback"
