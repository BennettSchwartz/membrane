import {
  EdgeKind,
  MemoryType,
  OutcomeStatus,
  RevisionStatus,
  Sensitivity,
  TaskState,
  ValidityMode,
  createDefaultTrustContext,
  type CompetencePayload,
  type Constraint,
  type EpisodicPayload,
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
      scopes: []
    });
  });

  it("EdgeKind enum has data and control values", () => {
    expect(EdgeKind.DATA).toBe("data");
    expect(EdgeKind.CONTROL).toBe("control");
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
});
