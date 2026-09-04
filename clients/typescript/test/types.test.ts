import {
  BuiltinEntityTypes,
  EdgeKind,
  EntityType,
  GraphPredicate,
  MemoryType,
  inverseGraphPredicate,
  OutcomeStatus,
  RevisionStatus,
  Sensitivity,
  TaskState,
  ValidityMode,
  createDefaultTrustContext,
  normalizeGraphPredicate,
  normalizeSemanticPredicate,
  type CompetencePayload,
  type Constraint,
  type EntityAlias,
  type EntityIdentifier,
  type EntityPayload,
  type EpisodicPayload,
  type MetricsSnapshot,
  type PlanEdge,
  type PlanGraphPayload,
  type PlanMetrics,
  type PlanNode,
  type ProvenanceRef,
  type RecipeStep,
  type RevisionState,
  type SemanticPayload,
  type TimelineEvent,
  type ToolNode,
  type Trigger,
  type Validity,
  type WorkingPayload
} from "../src/index";
import type { RetrieveGraphEnvelope } from "../src/internal/grpc";

describe("types", () => {
  it("exports expected enum-like values", () => {
    expect(MemoryType.EPISODIC).toBe("episodic");
    expect(MemoryType.PLAN_GRAPH).toBe("plan_graph");
    expect(Sensitivity.HIGH).toBe("high");
    expect(OutcomeStatus.PARTIAL).toBe("partial");
    expect(TaskState.EXECUTING).toBe("executing");
  });

  it("creates the default trust context", () => {
    expect(createDefaultTrustContext()).toEqual({
      max_sensitivity: "low",
      authenticated: false,
      actor_id: "",
      scopes: ["default"]
    });
  });

  it("EdgeKind enum has data and control values", () => {
    expect(EdgeKind.DATA).toBe("data");
    expect(EdgeKind.CONTROL).toBe("control");
  });

  it("EntityType and graph predicates expose grounded graph values", () => {
    expect(EntityType.PROJECT).toBe("Project");
    expect(EntityType.PULL_REQUEST).toBe("PullRequest");
    expect(EntityType.OTHER).toBe("Other");
    expect(BuiltinEntityTypes).toContain("Other");
    expect(GraphPredicate.SUBJECT_ENTITY).toBe("subject_entity");
    expect(GraphPredicate.DEPENDS_ON).toBe("depends_on");
    expect(GraphPredicate.DEPENDENCY_OF).toBe("dependency_of");
    expect(GraphPredicate.SUPPORTS).toBe("supports");
    expect(GraphPredicate.SUPPORTED_BY).toBe("supported_by");
    expect(GraphPredicate.CONTRADICTS).toBe("contradicts");
    expect(GraphPredicate.SUPERSEDED_BY).toBe("superseded_by");
    expect(GraphPredicate.CONTESTED_BY).toBe("contested_by");
    expect(GraphPredicate.CONTESTS).toBe("contests");
  });

  it("ValidityMode enum has all values", () => {
    expect(ValidityMode.GLOBAL).toBe("global");
    expect(ValidityMode.CONDITIONAL).toBe("conditional");
    expect(ValidityMode.TIMEBOXED).toBe("timeboxed");
  });

  it("RevisionStatus enum has all values", () => {
    expect(RevisionStatus.ACTIVE).toBe("active");
    expect(RevisionStatus.CONTESTED).toBe("contested");
    expect(RevisionStatus.RETRACTED).toBe("retracted");
  });

  it("RetrieveGraphEnvelope types diagnostics returned by the daemon", () => {
    const envelope = {
      nodes: [],
      edges: [],
      root_ids: [],
      diagnostics: [{ code: "vector_rank_failed", message: "vector ranker unavailable" }],
      projection: {
        relations_omitted: true,
        relations_truncated: false,
        history_omitted: true,
        records_truncated: false
      }
    } satisfies RetrieveGraphEnvelope;

    expect(envelope.diagnostics[0]?.code).toBe("vector_rank_failed");
    expect(envelope.projection.history_omitted).toBe(true);
  });

  it("MetricsSnapshot exposes vector coverage fields", () => {
    const snapshot: MetricsSnapshot = {
      total_records: 10,
      embedded_records: 8,
      missing_embeddings: 2,
      embedding_coverage: 0.8,
      embedding_model: "text-embedding-current",
      retrieval_usefulness: 0.5
    };

    expect(snapshot.embedding_coverage).toBe(0.8);
  });

  it("normalizes and inverts graph predicates like the Go schema package", () => {
    expect(normalizeGraphPredicate(" Depends On ")).toBe(GraphPredicate.DEPENDS_ON);
    expect(normalizeGraphPredicate("Depends-On")).toBe(GraphPredicate.DEPENDS_ON);
    expect(normalizeGraphPredicate("dependsOn")).toBe(GraphPredicate.DEPENDS_ON);
    expect(normalizeGraphPredicate("factSubjectOf")).toBe(GraphPredicate.FACT_SUBJECT_OF);
    expect(normalizeGraphPredicate("HTTPServerUses")).toBe("http_server_uses");
    expect(normalizeGraphPredicate("  Many---Separators__Here  ")).toBe("many_separators_here");
    expect(normalizeGraphPredicate("Custom Predicate")).toBe("custom_predicate");
    expect(normalizeSemanticPredicate("DeployTargetFor")).toBe("deploy_target_for");
    expect(inverseGraphPredicate("Depends On")).toBe(GraphPredicate.DEPENDENCY_OF);
    expect(inverseGraphPredicate("mentionsEntity")).toBe(GraphPredicate.MENTIONED_IN);
    expect(inverseGraphPredicate(GraphPredicate.SUPERSEDES)).toBe(GraphPredicate.SUPERSEDED_BY);
    expect(inverseGraphPredicate(GraphPredicate.CONTESTED_BY)).toBe(GraphPredicate.CONTESTS);
    expect(inverseGraphPredicate("Custom Predicate")).toBe("inverse_of_custom_predicate");
  });
});

describe("payload types are structurally correct", () => {
  it("Constraint interface accepts valid data", () => {
    const c: Constraint = { type: "scope", key: "workspace", value: "proj-1", required: true };
    expect(c.type).toBe("scope");
    expect(c.required).toBe(true);
  });

  it("ProvenanceRef interface accepts valid data", () => {
    const pr: ProvenanceRef = { source_type: "tool", source_id: "t-1", timestamp: "2025-01-01T00:00:00Z" };
    expect(pr.source_type).toBe("tool");
  });

  it("RevisionState interface accepts valid data", () => {
    const rs: RevisionState = { supersedes: "old-id", status: RevisionStatus.ACTIVE };
    expect(rs.supersedes).toBe("old-id");
  });

  it("Validity interface accepts valid data", () => {
    const v: Validity = { mode: ValidityMode.TIMEBOXED, start: "2025-01-01T00:00:00Z", end: "2026-01-01T00:00:00Z" };
    expect(v.mode).toBe("timeboxed");
  });

  it("TimelineEvent interface accepts valid data", () => {
    const e: TimelineEvent = { t: "2025-01-01T00:00:00Z", event_kind: "file_edit", ref: "src/main.py" };
    expect(e.event_kind).toBe("file_edit");
  });

  it("ToolNode interface accepts valid data", () => {
    const n: ToolNode = { id: "n1", tool: "bash", args: { cmd: "ls" }, result: { exit_code: 0 }, depends_on: [] };
    expect(n.tool).toBe("bash");
  });

  it("EpisodicPayload interface accepts valid data", () => {
    const p: EpisodicPayload = {
      kind: "episodic",
      timeline: [{ t: "2025-01-01T00:00:00Z", event_kind: "file_edit", ref: "main.py" }],
      outcome: OutcomeStatus.SUCCESS,
      artifacts: ["log.txt"]
    };
    expect(p.kind).toBe("episodic");
    expect(p.timeline).toHaveLength(1);
    expect(p.outcome).toBe("success");
  });

  it("WorkingPayload interface accepts valid data", () => {
    const p: WorkingPayload = {
      kind: "working",
      thread_id: "t-1",
      state: TaskState.EXECUTING,
      next_actions: ["run tests"],
      active_constraints: [{ type: "scope", key: "ws", value: "proj" }]
    };
    expect(p.state).toBe("executing");
    expect(p.active_constraints).toHaveLength(1);
  });

  it("SemanticPayload interface accepts valid data", () => {
    const p: SemanticPayload = {
      kind: "semantic",
      subject: "user",
      predicate: "prefers",
      object: "dark mode",
      validity: { mode: ValidityMode.GLOBAL }
    };
    expect(p.predicate).toBe("prefers");
  });

  it("Trigger and RecipeStep interfaces accept valid data", () => {
    const t: Trigger = { signal: "error_detected", conditions: { env: "prod" } };
    const s: RecipeStep = { step: "run pytest", tool: "bash", validation: "exit 0" };
    expect(t.signal).toBe("error_detected");
    expect(s.step).toBe("run pytest");
  });

  it("CompetencePayload interface accepts valid data", () => {
    const p: CompetencePayload = {
      kind: "competence",
      skill_name: "run_tests",
      triggers: [{ signal: "test_needed" }],
      recipe: [{ step: "pytest .", tool: "bash" }],
      version: "1.0"
    };
    expect(p.skill_name).toBe("run_tests");
    expect(p.triggers).toHaveLength(1);
  });

  it("PlanNode and PlanEdge interfaces accept valid data", () => {
    const node: PlanNode = { id: "n1", op: "clone_repo", params: { url: "https://github.com/x/y" } };
    const edge: PlanEdge = { from: "n1", to: "n2", kind: EdgeKind.CONTROL };
    expect(node.op).toBe("clone_repo");
    expect(edge.kind).toBe("control");
  });

  it("PlanGraphPayload interface accepts valid data", () => {
    const metrics: PlanMetrics = { avg_latency_ms: 500, failure_rate: 0.05, execution_count: 20 };
    const p: PlanGraphPayload = {
      kind: "plan_graph",
      plan_id: "plan-1",
      version: "2",
      intent: "setup_project",
      nodes: [{ id: "n1", op: "clone_repo" }],
      edges: [{ from: "n1", to: "n2", kind: "control" }],
      metrics
    };
    expect(p.plan_id).toBe("plan-1");
    expect(p.nodes).toHaveLength(1);
    expect(p.metrics?.execution_count).toBe(20);
  });

  it("EntityPayload interface accepts ontology-backed identity data", () => {
    const alias: EntityAlias = { value: "Membrane", kind: "surface" };
    const identifier: EntityIdentifier = { namespace: "github", value: "BennettSchwartz/membrane" };
    const p: EntityPayload = {
      kind: "entity",
      canonical_name: "Membrane",
      primary_type: EntityType.PROJECT,
      types: [EntityType.PROJECT, EntityType.REPOSITORY],
      aliases: [alias],
      identifiers: [identifier],
      summary: "Memory substrate repository"
    };

    expect(p.primary_type).toBe("Project");
    expect(p.aliases?.[0]?.value).toBe("Membrane");
    expect(p.identifiers?.[0]?.namespace).toBe("github");
  });
});
