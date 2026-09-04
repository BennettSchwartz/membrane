"""Unit tests for membrane.types."""

from membrane.types import (
    AuditEntry,
    CaptureMemoryResult,
    CompetencePayload,
    Constraint,
    DecayCurve,
    DecayProfile,
    DeletionPolicy,
    EdgeKind,
    BUILTIN_ENTITY_TYPES,
    EntityAlias,
    EntityKind,
    EntityIdentifier,
    EntityPayload,
    EntityType,
    EpisodicPayload,
    EnvironmentSnapshot,
    GraphEdge,
    GraphNode,
    GraphPredicate,
    Interpretation,
    InterpretationStatus,
    Lifecycle,
    MemoryRecord,
    MetricsSnapshot,
    MemoryType,
    Mention,
    OutcomeStatus,
    PerformanceStats,
    PlanEdge,
    PlanGraphPayload,
    PlanMetrics,
    PlanNode,
    Provenance,
    ProvenanceRef,
    ProvenanceSource,
    RecipeStep,
    ReferenceCandidate,
    Relation,
    RelationCandidate,
    RecordProjection,
    RetrieveGraphResult,
    RevisionState,
    RevisionStatus,
    SelectionResult,
    SemanticPayload,
    Sensitivity,
    TimelineEvent,
    ToolNode,
    Trigger,
    TrustContext,
    Validity,
    ValidityMode,
    WorkingPayload,
    inverse_graph_predicate,
    normalize_graph_predicate,
    normalize_semantic_predicate,
)


def test_metrics_snapshot_type_includes_embedding_coverage():
    snapshot: MetricsSnapshot = {
        "total_records": 10,
        "embedded_records": 8,
        "missing_embeddings": 2,
        "embedding_coverage": 0.8,
        "embedding_model": "text-embedding-current",
    }

    assert snapshot["embedding_coverage"] == 0.8


class TestEnums:
    def test_memory_type_values(self):
        assert MemoryType.EPISODIC == "episodic"
        assert MemoryType.WORKING == "working"
        assert MemoryType.SEMANTIC == "semantic"
        assert MemoryType.COMPETENCE == "competence"
        assert MemoryType.PLAN_GRAPH == "plan_graph"
        assert MemoryType.ENTITY == "entity"

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

    def test_builtin_entity_types_are_open_string_constants(self):
        assert EntityType.PERSON == "Person"
        assert EntityType.PROJECT in BUILTIN_ENTITY_TYPES
        assert EntityType.OTHER == "Other"
        assert EntityType.OTHER in BUILTIN_ENTITY_TYPES
        assert GraphPredicate.MENTIONS_ENTITY == "mentions_entity"
        assert GraphPredicate.DEPENDS_ON == "depends_on"
        assert GraphPredicate.DEPENDENCY_OF == "dependency_of"
        assert GraphPredicate.SUPPORTS == "supports"
        assert GraphPredicate.SUPPORTED_BY == "supported_by"
        assert GraphPredicate.CONTRADICTS == "contradicts"
        assert GraphPredicate.SUPERSEDED_BY == "superseded_by"
        assert GraphPredicate.CONTESTED_BY == "contested_by"
        assert GraphPredicate.CONTESTS == "contests"

    def test_graph_predicate_helpers_match_go_schema_behavior(self):
        assert normalize_graph_predicate(" Depends On ") == GraphPredicate.DEPENDS_ON
        assert normalize_graph_predicate("Depends-On") == GraphPredicate.DEPENDS_ON
        assert normalize_graph_predicate("dependsOn") == GraphPredicate.DEPENDS_ON
        assert normalize_graph_predicate("factSubjectOf") == GraphPredicate.FACT_SUBJECT_OF
        assert normalize_graph_predicate("HTTPServerUses") == "http_server_uses"
        assert normalize_graph_predicate("  Many---Separators__Here  ") == "many_separators_here"
        assert normalize_graph_predicate("Custom Predicate") == "custom_predicate"
        assert normalize_semantic_predicate("DeployTargetFor") == "deploy_target_for"
        assert inverse_graph_predicate("Depends On") == GraphPredicate.DEPENDENCY_OF
        assert inverse_graph_predicate("mentionsEntity") == GraphPredicate.MENTIONED_IN
        assert inverse_graph_predicate(GraphPredicate.SUPERSEDES) == GraphPredicate.SUPERSEDED_BY
        assert inverse_graph_predicate(GraphPredicate.CONTESTED_BY) == GraphPredicate.CONTESTS
        assert inverse_graph_predicate("Custom Predicate") == "inverse_of_custom_predicate"


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
            "interpretation": {
                "status": "resolved",
                "summary": "Linked Orchid mention",
                "proposed_type": "semantic",
                "mentions": [
                    {
                        "surface": "Orchid",
                        "entity_kind": "project",
                        "canonical_entity_id": "entity-1",
                        "confidence": 0.9,
                    }
                ],
            },
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
        assert rec.interpretation is not None
        assert rec.interpretation.status is InterpretationStatus.RESOLVED
        assert rec.interpretation.mentions[0].surface == "Orchid"
        assert rec.payload == {"event_kind": "test", "summary": "A test event"}
        assert len(rec.audit_log) == 1
        assert rec.audit_log[0].action == "create"

    def test_from_dict_accepts_go_record_field_names(self):
        rec = MemoryRecord.from_dict(
            {
                "ID": "rec-go",
                "Type": "semantic",
                "Sensitivity": "medium",
                "Confidence": 0.82,
                "Salience": 0.91,
                "Scope": "project:orchid",
                "Tags": ["go-fixture"],
                "CreatedAt": "2026-05-07T12:00:00Z",
                "UpdatedAt": "2026-05-07T12:30:00Z",
                "Relations": [
                    {
                        "Predicate": "depends_on",
                        "TargetID": "semantic-postgres",
                        "Weight": 0.7,
                    }
                ],
                "Payload": {
                    "semantic": {
                        "kind": "semantic",
                        "subject": "runtime",
                        "predicate": "uses",
                        "object": "postgres",
                    }
                },
                "AuditLog": [
                    {
                        "Action": "create",
                        "Actor": "tester",
                        "Timestamp": "2026-05-07T12:00:00Z",
                    }
                ],
            }
        )

        assert rec.id == "rec-go"
        assert rec.type is MemoryType.SEMANTIC
        assert rec.sensitivity is Sensitivity.MEDIUM
        assert rec.confidence == 0.82
        assert rec.salience == 0.91
        assert rec.scope == "project:orchid"
        assert rec.tags == ["go-fixture"]
        assert rec.created_at == "2026-05-07T12:00:00Z"
        assert rec.updated_at == "2026-05-07T12:30:00Z"
        assert rec.relations[0].target_id == "semantic-postgres"
        assert rec.payload["subject"] == "runtime"
        assert rec.audit_log[0].action == "create"

    def test_from_dict_accepts_lower_camel_record_field_names(self):
        rec = MemoryRecord.from_dict(
            {
                "id": "rec-camel",
                "type": "plan_graph",
                "sensitivity": "low",
                "confidence": 0.8,
                "salience": 0.9,
                "createdAt": "2026-05-07T13:00:00Z",
                "updatedAt": "2026-05-07T13:30:00Z",
                "auditLog": [{"action": "create", "actor": "tester"}],
                "payload": {
                    "planGraph": {
                        "kind": "plan_graph",
                        "planId": "plan-camel",
                        "intent": "debug production",
                        "inputsSchema": {
                            "repo": {"kind": "stringValue", "stringValue": "membrane"}
                        },
                        "outputsSchema": {
                            "summary": {"kind": "stringValue", "stringValue": "done"}
                        },
                        "nodes": [
                            {
                                "id": "n1",
                                "op": "run_tests",
                                "params": {
                                    "command": {
                                        "kind": "stringValue",
                                        "stringValue": "make verify",
                                    }
                                },
                                "guards": {
                                    "branch": {
                                        "kind": "stringValue",
                                        "stringValue": "main",
                                    }
                                },
                            }
                        ],
                        "metrics": {
                            "avgLatencyMs": 42,
                            "failureRate": 0.1,
                            "executionCount": 3,
                            "lastExecutedAt": "2026-05-07T13:00:00Z",
                        },
                    }
                },
            }
        )

        assert rec.created_at == "2026-05-07T13:00:00Z"
        assert rec.updated_at == "2026-05-07T13:30:00Z"
        assert rec.audit_log[0].actor == "tester"
        assert rec.payload["plan_id"] == "plan-camel"
        assert rec.payload["inputs_schema"] == {"repo": "membrane"}
        assert rec.payload["outputs_schema"] == {"summary": "done"}
        assert rec.payload["nodes"][0]["params"] == {"command": "make verify"}
        assert rec.payload["nodes"][0]["guards"] == {"branch": "main"}
        assert rec.payload["metrics"] == {
            "avg_latency_ms": 42,
            "failure_rate": 0.1,
            "execution_count": 3,
            "last_executed_at": "2026-05-07T13:00:00Z",
        }

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

    def test_from_dict_unwraps_proto_payload_oneof(self):
        rec = MemoryRecord.from_dict(
            {
                "id": "fact-1",
                "type": "semantic",
                "payload": {
                    "semantic": {
                        "kind": "semantic",
                        "subject": "entity-orchid",
                        "predicate": "uses",
                        "object": "Postgres",
                    }
                },
            }
        )
        assert rec.payload == {
            "kind": "semantic",
            "subject": "entity-orchid",
            "predicate": "uses",
            "object": "Postgres",
        }


class TestSubstructures:
    def test_decay_profile_from_dict(self):
        dp = DecayProfile.from_dict(
            {"curve": "exponential", "half_life_seconds": 3600}
        )
        assert dp.curve is DecayCurve.EXPONENTIAL
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
            {"target_id": "rec-999", "predicate": "supersedes", "weight": 1.0}
        )
        assert r.target_id == "rec-999"
        assert r.predicate == "supersedes"
        assert r.weight == 1.0

    def test_relation_from_dict_legacy_kind(self):
        """Legacy 'kind' key is accepted as a fallback for 'predicate'."""
        r = Relation.from_dict(
            {"target_id": "rec-999", "kind": "supersedes", "weight": 1.0}
        )
        assert r.target_id == "rec-999"
        assert r.predicate == "supersedes"

    def test_relation_from_dict_go_json_shape(self):
        r = Relation.from_dict(
            {
                "TargetID": "rec-999",
                "Predicate": "supersedes",
                "Weight": 0.75,
                "CreatedAt": "2026-05-07T00:00:00Z",
            }
        )

        assert r.target_id == "rec-999"
        assert r.predicate == "supersedes"
        assert r.weight == 0.75
        assert r.created_at == "2026-05-07T00:00:00Z"

    def test_graph_edge_from_dict(self):
        edge = GraphEdge.from_dict(
            {
                "source_id": "rec-1",
                "predicate": "mentions_entity",
                "target_id": "entity-1",
                "weight": 1.0,
            }
        )
        assert edge.source_id == "rec-1"
        assert edge.target_id == "entity-1"

    def test_graph_edge_from_go_json_shape(self):
        edge = GraphEdge.from_dict(
            {
                "SourceID": "source-go",
                "Predicate": "derived_semantic",
                "TargetID": "semantic-go",
                "Weight": 0.75,
            }
        )

        assert edge.source_id == "source-go"
        assert edge.predicate == "derived_semantic"
        assert edge.target_id == "semantic-go"
        assert edge.weight == 0.75

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

    def test_selection_result_from_go_json_shape(self):
        selection = SelectionResult.from_dict(
            {
                "Selected": [
                    {
                        "id": "rec-1",
                        "type": "competence",
                        "sensitivity": "low",
                        "confidence": 0.9,
                        "salience": 0.8,
                    }
                ],
                "Confidence": 0.42,
                "NeedsMore": True,
                "Scores": {"rec-1": 0.71},
            }
        )

        assert len(selection.selected) == 1
        assert selection.selected[0].id == "rec-1"
        assert selection.confidence == 0.42
        assert selection.needs_more is True
        assert selection.scores == {"rec-1": 0.71}

    def test_graph_and_capture_result_defaults(self):
        assert RetrieveGraphResult().nodes == []
        assert CaptureMemoryResult().created_records == []

    def test_retrieve_graph_result_from_go_json_shape(self):
        result = RetrieveGraphResult.from_dict(
            {
                "Nodes": [
                    {
                        "record": {
                            "id": "root-1",
                            "type": "entity",
                            "sensitivity": "low",
                            "confidence": 0.9,
                            "salience": 0.8,
                            "Interpretation": {
                                "Status": "resolved",
                                "ProposedType": "semantic",
                                "Mentions": [{"Surface": "Orchid"}],
                            },
                        },
                        "root": True,
                        "hop": 0,
                    }
                ],
                "Edges": [
                    {
                        "SourceID": "root-1",
                        "Predicate": "mentioned_in",
                        "TargetID": "rec-1",
                    }
                ],
                "RootIDs": ["root-1"],
                "Selection": {
                    "Selected": [
                        {
                            "id": "root-1",
                            "type": "entity",
                            "sensitivity": "low",
                            "confidence": 0.9,
                            "salience": 0.8,
                        }
                    ],
                    "Confidence": 0.9,
                    "Scores": {"root-1": 0.91},
                },
                "Diagnostics": [
                    {
                        "Code": "embedding_query_failed",
                        "Message": "embedding service unavailable",
                    }
                ],
                "Projection": {
                    "RelationsOmitted": True,
                    "RelationsTruncated": False,
                    "HistoryOmitted": True,
                    "RecordsTruncated": False,
                },
            }
        )

        assert result.root_ids == ["root-1"]
        assert result.nodes[0].record.id == "root-1"
        assert result.nodes[0].record.interpretation is not None
        assert result.nodes[0].record.interpretation.status is InterpretationStatus.RESOLVED
        assert result.nodes[0].record.interpretation.mentions[0].surface == "Orchid"
        assert result.edges[0].target_id == "rec-1"
        assert result.selection is not None
        assert result.selection.scores == {"root-1": 0.91}
        assert result.diagnostics[0].code == "embedding_query_failed"
        assert "embedding service" in result.diagnostics[0].message
        assert isinstance(result.projection, RecordProjection)
        assert result.projection.relations_omitted is True
        assert result.projection.history_omitted is True

    def test_capture_memory_result_from_go_json_shape(self):
        result = CaptureMemoryResult.from_dict(
            {
                "PrimaryRecord": {
                    "id": "source-1",
                    "type": "episodic",
                    "sensitivity": "low",
                    "confidence": 0.9,
                    "salience": 0.8,
                },
                "CreatedRecords": [
                    {
                        "id": "entity-1",
                        "type": "entity",
                        "sensitivity": "low",
                        "confidence": 1.0,
                        "salience": 1.0,
                    }
                ],
                "Edges": [
                    {
                        "source_id": "source-1",
                        "predicate": "mentions_entity",
                        "target_id": "entity-1",
                    }
                ],
            }
        )

        assert result.primary_record.id == "source-1"
        assert [record.id for record in result.created_records] == ["entity-1"]
        assert result.edges[0].predicate == "mentions_entity"

    def test_graph_and_capture_results_accept_lower_camel_json_shape(self):
        graph = RetrieveGraphResult.from_dict(
            {
                "nodes": [
                    {
                        "record": {
                            "id": "root-1",
                            "type": "entity",
                            "sensitivity": "low",
                            "confidence": 0.9,
                            "salience": 0.8,
                        },
                        "root": True,
                        "hop": 0,
                    }
                ],
                "edges": [
                    {
                        "sourceId": "root-1",
                        "predicate": "derived_semantic",
                        "targetId": "semantic-1",
                        "createdAt": "2026-05-07T00:00:00Z",
                    }
                ],
                "rootIds": ["root-1"],
                "selection": {
                    "selected": [],
                    "confidence": 0.75,
                    "needsMore": True,
                    "scores": {"root-1": 0.9},
                },
            }
        )
        assert graph.root_ids == ["root-1"]
        assert graph.edges[0].source_id == "root-1"
        assert graph.edges[0].target_id == "semantic-1"
        assert graph.edges[0].created_at == "2026-05-07T00:00:00Z"
        assert graph.selection is not None
        assert graph.selection.needs_more is True

        capture = CaptureMemoryResult.from_dict(
            {
                "primaryRecord": {
                    "id": "source-1",
                    "type": "episodic",
                    "sensitivity": "low",
                    "confidence": 0.9,
                    "salience": 0.8,
                },
                "createdRecords": [],
                "edges": [
                    {
                        "sourceId": "source-1",
                        "predicate": "derived_semantic",
                        "targetId": "semantic-1",
                    }
                ],
            }
        )
        assert capture.primary_record.id == "source-1"
        assert capture.edges[0].target_id == "semantic-1"

    def test_interpretation_from_dict(self):
        interpretation = Interpretation.from_dict(
            {
                "status": "tentative",
                "summary": "Potential Orchid mention",
                "proposed_type": "semantic",
                "topical_labels": ["deploy"],
                "mentions": [
                    {
                        "surface": "Orchid",
                        "entity_kind": "project",
                        "aliases": ["orchid"],
                    }
                ],
                "relation_candidates": [
                    {"predicate": "mentions_entity", "confidence": 0.6}
                ],
                "reference_candidates": [
                    {"ref": "rec-123", "confidence": 0.4}
                ],
            }
        )
        assert interpretation.status is InterpretationStatus.TENTATIVE
        assert interpretation.proposed_type is MemoryType.SEMANTIC
        assert interpretation.mentions[0] == Mention(
            surface="Orchid",
            entity_kind=EntityKind.PROJECT,
            canonical_entity_id="",
            confidence=0.0,
            aliases=["orchid"],
        )
        assert interpretation.relation_candidates[0] == RelationCandidate(
            predicate="mentions_entity",
            target_record_id="",
            target_entity_id="",
            confidence=0.6,
            resolved=False,
        )
        assert interpretation.reference_candidates[0] == ReferenceCandidate(
            ref="rec-123",
            target_record_id="",
            target_entity_id="",
            confidence=0.4,
            resolved=False,
        )

    def test_interpretation_from_dict_accepts_lower_camel_json_shape(self):
        interpretation = Interpretation.from_dict(
            {
                "status": "resolved",
                "summary": "Resolved Orchid links",
                "proposedType": "semantic",
                "topicalLabels": ["deploy", "graph"],
                "mentions": [
                    {
                        "surface": "Orchid",
                        "entityKind": "project",
                        "canonicalEntityId": "entity-orchid",
                        "confidence": 0.92,
                        "aliases": ["Project Orchid"],
                    }
                ],
                "relationCandidates": [
                    {
                        "predicate": "depends_on",
                        "targetRecordId": "semantic-postgres",
                        "targetEntityId": "entity-postgres",
                        "confidence": 0.81,
                        "resolved": True,
                    }
                ],
                "referenceCandidates": [
                    {
                        "ref": "trace-1",
                        "targetRecordId": "episodic-trace",
                        "targetEntityId": "entity-trace",
                        "confidence": 0.73,
                        "resolved": True,
                    }
                ],
                "extractionConfidence": 0.88,
            }
        )

        assert interpretation.proposed_type is MemoryType.SEMANTIC
        assert interpretation.topical_labels == ["deploy", "graph"]
        assert interpretation.extraction_confidence == 0.88
        assert interpretation.mentions[0] == Mention(
            surface="Orchid",
            entity_kind=EntityKind.PROJECT,
            canonical_entity_id="entity-orchid",
            confidence=0.92,
            aliases=["Project Orchid"],
        )
        assert interpretation.relation_candidates[0] == RelationCandidate(
            predicate="depends_on",
            target_record_id="semantic-postgres",
            target_entity_id="entity-postgres",
            confidence=0.81,
            resolved=True,
        )
        assert interpretation.reference_candidates[0] == ReferenceCandidate(
            ref="trace-1",
            target_record_id="episodic-trace",
            target_entity_id="entity-trace",
            confidence=0.73,
            resolved=True,
        )

    def test_interpretation_from_dict_accepts_go_json_shape(self):
        interpretation = Interpretation.from_dict(
            {
                "Status": "resolved",
                "Summary": "Resolved Go fixture links",
                "ProposedType": "semantic",
                "TopicalLabels": ["deploy", "graph"],
                "Mentions": [
                    {
                        "Surface": "Orchid",
                        "EntityKind": "project",
                        "CanonicalEntityID": "entity-orchid",
                        "Confidence": 0.92,
                        "Aliases": ["Project Orchid"],
                    }
                ],
                "RelationCandidates": [
                    {
                        "Predicate": "depends_on",
                        "TargetRecordID": "semantic-postgres",
                        "TargetEntityID": "entity-postgres",
                        "Confidence": 0.81,
                        "Resolved": True,
                    }
                ],
                "ReferenceCandidates": [
                    {
                        "Ref": "trace-1",
                        "TargetRecordID": "episodic-trace",
                        "TargetEntityID": "entity-trace",
                        "Confidence": 0.73,
                        "Resolved": True,
                    }
                ],
                "ExtractionConfidence": 0.88,
            }
        )

        assert interpretation.status is InterpretationStatus.RESOLVED
        assert interpretation.proposed_type is MemoryType.SEMANTIC
        assert interpretation.topical_labels == ["deploy", "graph"]
        assert interpretation.extraction_confidence == 0.88
        assert interpretation.mentions[0] == Mention(
            surface="Orchid",
            entity_kind=EntityKind.PROJECT,
            canonical_entity_id="entity-orchid",
            confidence=0.92,
            aliases=["Project Orchid"],
        )
        assert interpretation.relation_candidates[0] == RelationCandidate(
            predicate="depends_on",
            target_record_id="semantic-postgres",
            target_entity_id="entity-postgres",
            confidence=0.81,
            resolved=True,
        )
        assert interpretation.reference_candidates[0] == ReferenceCandidate(
            ref="trace-1",
            target_record_id="episodic-trace",
            target_entity_id="entity-trace",
            confidence=0.73,
            resolved=True,
        )

    def test_mention_accepts_open_entity_type_string(self):
        mention = Mention.from_dict({"surface": "Orchid", "entity_kind": "Project"})
        assert mention.entity_kind == "Project"

    def test_graph_node_and_entity_payload_from_dict(self):
        node = GraphNode.from_dict(
            {
                "record": {
                    "id": "entity-1",
                    "type": "entity",
                    "sensitivity": "low",
                    "confidence": 1.0,
                    "salience": 1.0,
                },
                "root": True,
                "hop": 0,
            }
        )
        payload = EntityPayload.from_dict(
            {
                "kind": "entity",
                "canonical_name": "Orchid",
                "primary_type": "Project",
                "types": ["Project", "Service"],
                "aliases": [{"value": "orchid", "kind": "lowercase"}],
                "identifiers": [{"namespace": "repo", "value": "orchid"}],
                "summary": "Staging deploy target",
            }
        )
        assert node.record.type is MemoryType.ENTITY
        assert payload.primary_type == EntityType.PROJECT
        assert payload.types == ["Project", "Service"]
        assert payload.aliases == [EntityAlias(value="orchid", kind="lowercase")]
        assert payload.identifiers == [EntityIdentifier(namespace="repo", value="orchid")]

    def test_entity_payload_accepts_protobuf_and_go_field_names(self):
        payload = EntityPayload.from_dict(
            {
                "Kind": "entity",
                "canonicalName": "Project Orchid",
                "PrimaryType": "Project",
                "Types": ["Project", "Repository"],
                "Aliases": [{"Value": "orchid", "Kind": "nickname", "Locale": "en"}],
                "Identifiers": [
                    {"Namespace": "github", "Value": "BennettSchwartz/orchid"}
                ],
                "Summary": "Staging deploy target",
            }
        )

        assert payload.canonical_name == "Project Orchid"
        assert payload.primary_type == EntityType.PROJECT
        assert payload.types == ["Project", "Repository"]
        assert payload.aliases == [
            EntityAlias(value="orchid", kind="nickname", locale="en")
        ]
        assert payload.identifiers == [
            EntityIdentifier(namespace="github", value="BennettSchwartz/orchid")
        ]
        assert payload.summary == "Staging deploy target"

    def test_memory_record_normalizes_entity_payload_identity_fields(self):
        rec = MemoryRecord.from_dict(
            {
                "id": "entity-1",
                "type": "entity",
                "sensitivity": "low",
                "payload": {
                    "entity": {
                        "kind": "entity",
                        "canonicalName": "Project Orchid",
                        "primaryType": "Project",
                        "Aliases": ["orchid"],
                        "Identifiers": [
                            {
                                "Namespace": "github",
                                "Value": "BennettSchwartz/orchid",
                            }
                        ],
                    }
                },
            }
        )

        assert rec.payload["canonical_name"] == "Project Orchid"
        assert rec.payload["primary_type"] == "Project"
        assert rec.payload["aliases"] == ["orchid"]
        assert rec.payload["identifiers"] == [
            {"namespace": "github", "value": "BennettSchwartz/orchid"}
        ]

    def test_entity_payload_accepts_legacy_entity_kind_and_alias_strings(self):
        payload = EntityPayload.from_dict(
            {
                "kind": "entity",
                "canonical_name": "Orchid",
                "entity_kind": "project",
                "aliases": ["orchid"],
            }
        )
        assert payload.primary_type == EntityType.PROJECT
        assert payload.types == [EntityType.PROJECT]
        assert payload.aliases == [EntityAlias(value="orchid")]


class TestDecayProfileExtended:
    def test_optional_fields(self):
        dp = DecayProfile.from_dict(
            {
                "curve": "exponential",
                "half_life_seconds": 3600,
                "min_salience": 0.1,
                "max_age_seconds": 604800,
                "reinforcement_gain": 0.5,
            }
        )
        assert dp.min_salience == 0.1
        assert dp.max_age_seconds == 604800
        assert dp.reinforcement_gain == 0.5

    def test_optional_fields_absent(self):
        dp = DecayProfile.from_dict({"curve": "exponential", "half_life_seconds": 86400})
        assert dp.min_salience is None
        assert dp.max_age_seconds is None
        assert dp.reinforcement_gain is None


class TestLifecycleExtended:
    def test_pinned_field(self):
        lc = Lifecycle.from_dict(
            {
                "decay": {"curve": "exponential", "half_life_seconds": 86400},
                "last_reinforced_at": "2025-01-01T00:00:00Z",
                "pinned": True,
                "deletion_policy": "never",
            }
        )
        assert lc.pinned is True

    def test_pinned_defaults_false(self):
        lc = Lifecycle.from_dict(
            {"decay": {"curve": "exponential", "half_life_seconds": 86400}}
        )
        assert lc.pinned is False


class TestProvenanceSourceExtended:
    def test_hash_and_created_by(self):
        ps = ProvenanceSource.from_dict(
            {
                "kind": "artifact",
                "ref": "file.py",
                "timestamp": "2025-01-01T00:00:00Z",
                "hash": "sha256:abc123",
                "created_by": "agent-1",
            }
        )
        assert ps.hash == "sha256:abc123"
        assert ps.created_by == "agent-1"

    def test_absent_optional_fields(self):
        ps = ProvenanceSource.from_dict({"kind": "event", "ref": "evt-1"})
        assert ps.hash == ""
        assert ps.created_by == ""


class TestProvenanceExtended:
    def test_created_by(self):
        prov = Provenance.from_dict(
            {"sources": [], "created_by": "consolidator-v1"}
        )
        assert prov.created_by == "consolidator-v1"

    def test_created_by_absent(self):
        prov = Provenance.from_dict({"sources": []})
        assert prov.created_by == ""


class TestRelationExtended:
    def test_predicate_field(self):
        r = Relation.from_dict(
            {"target_id": "rec-1", "predicate": "supports", "weight": 0.9}
        )
        assert r.predicate == "supports"

    def test_created_at(self):
        r = Relation.from_dict(
            {
                "target_id": "rec-1",
                "predicate": "derived_from",
                "created_at": "2025-01-01T00:00:00Z",
            }
        )
        assert r.created_at == "2025-01-01T00:00:00Z"


class TestConstraint:
    def test_from_dict(self):
        c = Constraint.from_dict(
            {"type": "scope", "key": "workspace", "value": "proj-1", "required": True}
        )
        assert c.type == "scope"
        assert c.key == "workspace"
        assert c.value == "proj-1"
        assert c.required is True

    def test_defaults(self):
        c = Constraint()
        assert c.type == ""
        assert c.required is False


class TestProvenanceRef:
    def test_from_dict(self):
        pr = ProvenanceRef.from_dict(
            {
                "source_type": "tool",
                "source_id": "tool-123",
                "timestamp": "2025-01-01T00:00:00Z",
            }
        )
        assert pr.source_type == "tool"
        assert pr.source_id == "tool-123"


class TestRevisionState:
    def test_from_dict(self):
        rs = RevisionState.from_dict(
            {
                "supersedes": "old-id",
                "superseded_by": "",
                "status": "active",
            }
        )
        assert rs.supersedes == "old-id"
        assert rs.status is RevisionStatus.ACTIVE

    def test_absent_status(self):
        rs = RevisionState.from_dict({})
        assert rs.status is None

    def test_from_dict_accepts_go_and_lower_camel_shapes(self):
        go_rs = RevisionState.from_dict(
            {"Supersedes": "old-id", "SupersededBy": "new-id", "Status": "retracted"}
        )
        assert go_rs.supersedes == "old-id"
        assert go_rs.superseded_by == "new-id"
        assert go_rs.status is RevisionStatus.RETRACTED

        camel_rs = RevisionState.from_dict(
            {"supersedes": "old-id", "supersededBy": "new-id", "status": "contested"}
        )
        assert camel_rs.superseded_by == "new-id"
        assert camel_rs.status is RevisionStatus.CONTESTED


class TestValidity:
    def test_from_dict(self):
        v = Validity.from_dict(
            {
                "mode": "timeboxed",
                "start": "2025-01-01T00:00:00Z",
                "end": "2026-01-01T00:00:00Z",
            }
        )
        assert v.mode is ValidityMode.TIMEBOXED
        assert v.start == "2025-01-01T00:00:00Z"
        assert v.end == "2026-01-01T00:00:00Z"

    def test_conditional_with_conditions(self):
        v = Validity.from_dict({"mode": "conditional", "conditions": {"env": "prod"}})
        assert v.mode is ValidityMode.CONDITIONAL
        assert v.conditions == {"env": "prod"}


class TestEpisodicPayload:
    def test_from_dict(self):
        data = {
            "kind": "episodic",
            "timeline": [
                {"t": "2025-01-01T00:00:00Z", "event_kind": "file_edit", "ref": "src/main.py"}
            ],
            "outcome": "success",
            "artifacts": ["log.txt"],
        }
        p = EpisodicPayload.from_dict(data)
        assert p.kind == "episodic"
        assert len(p.timeline) == 1
        assert p.timeline[0].event_kind == "file_edit"
        assert p.outcome == "success"
        assert p.artifacts == ["log.txt"]

    def test_tool_graph(self):
        data = {
            "kind": "episodic",
            "timeline": [],
            "tool_graph": [
                {
                    "id": "node-1",
                    "tool": "bash",
                    "args": {"cmd": "ls"},
                    "result": {"exit_code": 0},
                    "depends_on": [],
                }
            ],
        }
        p = EpisodicPayload.from_dict(data)
        assert len(p.tool_graph) == 1
        assert p.tool_graph[0].tool == "bash"
        assert p.tool_graph[0].args == {"cmd": "ls"}

    def test_environment(self):
        data = {
            "kind": "episodic",
            "timeline": [],
            "environment": {
                "os": "linux",
                "os_version": "6.1",
                "tool_versions": {"python": "3.11"},
                "working_directory": "/workspace",
            },
        }
        p = EpisodicPayload.from_dict(data)
        assert p.environment is not None
        assert p.environment.os == "linux"
        assert p.environment.tool_versions == {"python": "3.11"}


class TestWorkingPayload:
    def test_from_dict(self):
        data = {
            "kind": "working",
            "thread_id": "t-1",
            "state": "executing",
            "next_actions": ["run tests"],
            "open_questions": ["how to handle error?"],
            "context_summary": "in progress",
            "active_constraints": [
                {"type": "scope", "key": "workspace", "value": "proj-1"}
            ],
        }
        p = WorkingPayload.from_dict(data)
        assert p.thread_id == "t-1"
        assert p.state == "executing"
        assert p.next_actions == ["run tests"]
        assert len(p.active_constraints) == 1
        assert p.active_constraints[0].key == "workspace"


class TestSemanticPayload:
    def test_from_dict(self):
        data = {
            "kind": "semantic",
            "subject": "user",
            "predicate": "prefers",
            "object": "dark mode",
            "validity": {"mode": "global"},
            "revision_policy": "replace",
        }
        p = SemanticPayload.from_dict(data)
        assert p.subject == "user"
        assert p.predicate == "prefers"
        assert p.object == "dark mode"
        assert p.validity is not None
        assert p.validity.mode is ValidityMode.GLOBAL

    def test_revision(self):
        data = {
            "kind": "semantic",
            "subject": "x",
            "predicate": "is",
            "object": 42,
            "validity": {"mode": "global"},
            "revision": {"supersedes": "old-id", "status": "active"},
        }
        p = SemanticPayload.from_dict(data)
        assert p.revision is not None
        assert p.revision.supersedes == "old-id"

    def test_from_dict_accepts_protobuf_json_shape(self):
        p = SemanticPayload.from_dict(
            {
                "kind": "semantic",
                "subject": "database",
                "predicate": "uses",
                "object": {"kind": "stringValue", "stringValue": "Postgres"},
                "validity": {
                    "mode": "conditional",
                    "conditions": {
                        "env": {"kind": "stringValue", "stringValue": "prod"}
                    },
                },
                "revisionPolicy": "contest",
                "revision": {"supersededBy": "new-id", "status": "retracted"},
            }
        )

        assert p.object == "Postgres"
        assert p.validity is not None
        assert p.validity.conditions == {"env": "prod"}
        assert p.revision_policy == "contest"
        assert p.revision is not None
        assert p.revision.superseded_by == "new-id"
        assert p.revision.status is RevisionStatus.RETRACTED


class TestCompetencePayload:
    def test_from_dict(self):
        data = {
            "kind": "competence",
            "skill_name": "run_tests",
            "triggers": [{"signal": "test_needed", "conditions": {}}],
            "recipe": [{"step": "run pytest", "tool": "bash"}],
            "required_tools": ["bash"],
            "version": "1.0",
        }
        p = CompetencePayload.from_dict(data)
        assert p.skill_name == "run_tests"
        assert len(p.triggers) == 1
        assert p.triggers[0].signal == "test_needed"
        assert len(p.recipe) == 1
        assert p.recipe[0].step == "run pytest"
        assert p.version == "1.0"

    def test_performance(self):
        data = {
            "kind": "competence",
            "skill_name": "deploy",
            "triggers": [],
            "recipe": [],
            "performance": {
                "success_count": 10,
                "failure_count": 2,
                "success_rate": 0.83,
                "avg_latency_ms": 1200.0,
            },
        }
        p = CompetencePayload.from_dict(data)
        assert p.performance is not None
        assert p.performance.success_count == 10
        assert p.performance.success_rate == 0.83


class TestPlanGraphPayload:
    def test_from_dict(self):
        data = {
            "kind": "plan_graph",
            "plan_id": "plan-1",
            "version": "2",
            "intent": "setup_project",
            "nodes": [{"id": "n1", "op": "clone_repo"}, {"id": "n2", "op": "install_deps"}],
            "edges": [{"from": "n1", "to": "n2", "kind": "control"}],
        }
        p = PlanGraphPayload.from_dict(data)
        assert p.plan_id == "plan-1"
        assert p.intent == "setup_project"
        assert len(p.nodes) == 2
        assert p.nodes[0].op == "clone_repo"
        assert len(p.edges) == 1
        assert p.edges[0].from_ == "n1"
        assert p.edges[0].to == "n2"
        assert p.edges[0].kind is EdgeKind.CONTROL

    def test_metrics(self):
        data = {
            "kind": "plan_graph",
            "plan_id": "plan-2",
            "version": "1",
            "nodes": [],
            "edges": [],
            "metrics": {
                "avg_latency_ms": 500.0,
                "failure_rate": 0.05,
                "execution_count": 20,
            },
        }
        p = PlanGraphPayload.from_dict(data)
        assert p.metrics is not None
        assert p.metrics.execution_count == 20
        assert p.metrics.failure_rate == 0.05

    def test_from_dict_accepts_protobuf_lower_camel_shape(self):
        p = PlanGraphPayload.from_dict(
            {
                "kind": "plan_graph",
                "planId": "plan-camel",
                "inputsSchema": {
                    "repo": {"kind": "stringValue", "stringValue": "membrane"}
                },
                "outputsSchema": {
                    "summary": {"kind": "stringValue", "stringValue": "done"}
                },
                "nodes": [
                    {
                        "id": "n1",
                        "op": "run_tests",
                        "params": {
                            "command": {
                                "kind": "stringValue",
                                "stringValue": "make verify",
                            }
                        },
                        "guards": {
                            "branch": {
                                "kind": "stringValue",
                                "stringValue": "main",
                            }
                        },
                    }
                ],
                "metrics": {
                    "avgLatencyMs": 42,
                    "failureRate": 0.1,
                    "executionCount": 3,
                    "lastExecutedAt": "2026-05-07T13:00:00Z",
                },
            }
        )

        assert p.plan_id == "plan-camel"
        assert p.inputs_schema == {"repo": "membrane"}
        assert p.outputs_schema == {"summary": "done"}
        assert p.nodes[0].params == {"command": "make verify"}
        assert p.nodes[0].guards == {"branch": "main"}
        assert p.metrics is not None
        assert p.metrics.avg_latency_ms == 42
        assert p.metrics.failure_rate == 0.1
        assert p.metrics.execution_count == 3
        assert p.metrics.last_executed_at == "2026-05-07T13:00:00Z"
