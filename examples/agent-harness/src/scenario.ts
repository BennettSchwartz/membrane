import {
  EntityType,
  MemoryType,
  OutcomeStatus,
  ProvenanceKind,
  Sensitivity,
  SourceKind,
  TaskState,
  type CaptureMemoryResult,
  type CompetencePayload,
  type EntityPayload,
  type GraphEdge,
  type MemoryRecord,
  type PlanGraphPayload,
  type Relation,
  type SemanticPayload
} from "@bennettschwartz/membrane";
import { MembraneClient } from "@bennettschwartz/membrane";

export const HARNESS_SCOPE = "project:agent-harness-orion";
export const HARNESS_ACTOR = "agent-harness";
export const PROJECT_NAME = "Project Orion";
export const SERVICE_NAME = "auth-service";
export const DATABASE_NAME = "PostgreSQL";
export const OBSERVABILITY_TOOL = "Grafana";
export const FEATURE_FLAG_TOOL = "LaunchDarkly";

export const FIVE_MEMORY_TYPES = [
  MemoryType.EPISODIC,
  MemoryType.WORKING,
  MemoryType.SEMANTIC,
  MemoryType.COMPETENCE,
  MemoryType.PLAN_GRAPH
] as const;

export const GRAPH_MEMORY_TYPES = [...FIVE_MEMORY_TYPES, MemoryType.ENTITY] as const;

export interface SeededScenario {
  entities: Record<string, string>;
  episodic: MemoryRecord;
  working: MemoryRecord;
  semantic: MemoryRecord;
  competence: MemoryRecord;
  planGraph: MemoryRecord;
}

export interface CapturedFact {
  capture: CaptureMemoryResult;
  semantic: MemoryRecord;
}

export function trustContext(actorId = HARNESS_ACTOR) {
  return {
    max_sensitivity: Sensitivity.LOW,
    authenticated: true,
    actor_id: actorId,
    scopes: [HARNESS_SCOPE]
  };
}

export function nowIso(): string {
  return new Date().toISOString();
}

export function uniqueStrings(values: string[]): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const value of values) {
    const trimmed = value.trim();
    if (!trimmed || seen.has(trimmed)) {
      continue;
    }
    seen.add(trimmed);
    out.push(trimmed);
  }
  return out;
}

function payloadKind(record: MemoryRecord): string | undefined {
  const payload = record.payload;
  if (payload && typeof payload === "object" && "kind" in payload) {
    return String((payload as { kind?: unknown }).kind);
  }
  return undefined;
}

export function isEntityRecord(record: MemoryRecord): record is MemoryRecord & { payload: EntityPayload } {
  return record.type === MemoryType.ENTITY && payloadKind(record) === "entity";
}

export function isSemanticRecord(record: MemoryRecord): record is MemoryRecord & { payload: SemanticPayload } {
  return record.type === MemoryType.SEMANTIC && payloadKind(record) === "semantic";
}

export function compactRecord(record: MemoryRecord): Record<string, unknown> {
  return {
    id: record.id,
    type: record.type,
    tags: record.tags ?? [],
    scope: record.scope,
    salience: record.salience,
    payload: record.payload
  };
}

function recordsFromCapture(capture: CaptureMemoryResult): MemoryRecord[] {
  return [capture.primary_record, ...capture.created_records].filter((record) => Boolean(record.id));
}

function collectEntityMap(capture: CaptureMemoryResult, entities: Map<string, string>): void {
  for (const record of recordsFromCapture(capture)) {
    if (!isEntityRecord(record)) {
      continue;
    }
    entities.set(record.payload.canonical_name, record.id);
    for (const alias of record.payload.aliases ?? []) {
      entities.set(alias.value, record.id);
    }
  }
}

function relationTo(entityId: string, predicate = "mentions_entity"): Relation {
  return {
    predicate,
    target_id: entityId,
    weight: 1,
    created_at: nowIso()
  };
}

function relationsToEntities(entityIds: string[]): Relation[] {
  return [...new Set(entityIds)].map((id) => relationTo(id));
}

function baseRecord(type: string, payload: unknown, tags: string[], relations: Relation[]): MemoryRecord {
  const ts = nowIso();
  return {
    id: "",
    type,
    sensitivity: Sensitivity.LOW,
    confidence: 0.94,
    salience: 1,
    scope: HARNESS_SCOPE,
    tags,
    created_at: ts,
    updated_at: ts,
    lifecycle: {
      decay: {
        curve: "exponential",
        half_life_seconds: 30 * 24 * 60 * 60,
        min_salience: 0,
        reinforcement_gain: 0.1
      },
      last_reinforced_at: ts,
      deletion_policy: "auto_prune"
    },
    provenance: {
      sources: [
        {
          kind: ProvenanceKind.OBSERVATION,
          ref: "agent-harness-seed",
          timestamp: ts,
          created_by: HARNESS_ACTOR
        }
      ],
      created_by: HARNESS_ACTOR
    },
    relations,
    payload,
    audit_log: []
  };
}

function competenceRecord(entityIds: string[]): MemoryRecord {
  const payload: CompetencePayload = {
    kind: "competence",
    skill_name: "auth-latency-incident-playbook",
    triggers: [
      { signal: "auth-service latency spike" },
      { signal: "p95 above SLO" },
      { signal: "database connection pool pressure" }
    ],
    recipe: [
      {
        step: "Retrieve recent incidents, current working state, service facts, and rollout plan from Membrane",
        tool: "membrane_retrieve_graph",
        validation: "Graph contains episodic, working, semantic, competence, plan_graph, and entity context"
      },
      {
        step: "Compare Grafana p95 and database wait metrics before changing code",
        tool: "Grafana",
        validation: "Baseline p95 and DB wait time are recorded"
      },
      {
        step: "Patch the rate-limit retry path behind a LaunchDarkly flag",
        tool: "LaunchDarkly",
        validation: "Canary can be rolled back without redeploy"
      },
      {
        step: "Capture the outcome and any new facts back into Membrane",
        tool: "membrane_capture_fact",
        validation: "Follow-up retrieval includes the taught fact"
      }
    ],
    required_tools: ["membrane_retrieve_graph", "Grafana", "psql", "LaunchDarkly"],
    failure_modes: ["query-plan regression", "retry storm", "canary error-rate increase"],
    fallbacks: ["disable LaunchDarkly flag", "roll back canary", "restore previous pool settings"],
    performance: {
      success_count: 12,
      failure_count: 1,
      success_rate: 0.92,
      avg_latency_ms: 18,
      last_used_at: nowIso()
    },
    version: "1.0.0"
  };

  return baseRecord(MemoryType.COMPETENCE, payload, ["agent-harness", "competence", "incident", "auth-service"], relationsToEntities(entityIds));
}

function planGraphRecord(entityIds: string[]): MemoryRecord {
  const payload: PlanGraphPayload = {
    kind: "plan_graph",
    plan_id: "auth-service-safe-latency-rollout",
    version: "1.0.0",
    intent: "diagnose and safely roll out an auth-service latency fix",
    constraints: {
      max_sensitivity: Sensitivity.LOW,
      rollout_style: "canary",
      must_capture_memory: true
    },
    inputs_schema: {
      required: ["incident_summary", "slo_target", "candidate_fix"]
    },
    outputs_schema: {
      required: ["diagnosis", "rollout_decision", "new_memories"]
    },
    nodes: [
      { id: "retrieve", op: "membrane_retrieve_graph", params: { include_entity_layer: true } },
      { id: "baseline", op: "collect_baseline_metrics", params: { tools: ["Grafana", "psql"] } },
      { id: "patch", op: "apply_flagged_fix", params: { flag_tool: FEATURE_FLAG_TOOL } },
      { id: "canary", op: "deploy_canary", params: { percent: 5 } },
      { id: "verify", op: "verify_slo", params: { p95_ms: 200, error_rate: "no increase" } },
      { id: "capture", op: "membrane_capture_episode", params: { required: true } }
    ],
    edges: [
      { from: "retrieve", to: "baseline", kind: "control" },
      { from: "baseline", to: "patch", kind: "control" },
      { from: "patch", to: "canary", kind: "control" },
      { from: "canary", to: "verify", kind: "control" },
      { from: "verify", to: "capture", kind: "control" }
    ],
    metrics: {
      avg_latency_ms: 22,
      failure_rate: 0.04,
      execution_count: 9,
      last_executed_at: nowIso()
    }
  };

  return baseRecord(MemoryType.PLAN_GRAPH, payload, ["agent-harness", "plan-graph", "rollout", "auth-service"], relationsToEntities(entityIds));
}

async function semanticSeedForPromotion(client: MembraneClient, name: string): Promise<MemoryRecord> {
  const fact = await captureFact(client, {
    subject: SERVICE_NAME,
    predicate: `${name}_promotion_slot`,
    object: "placeholder",
    tags: ["agent-harness", "promotion-slot"],
    reason: `Temporary semantic slot promoted into ${name}`
  });
  return fact.semantic;
}

export async function captureFact(client: MembraneClient, input: {
  subject: string;
  predicate: string;
  object: unknown;
  tags?: string[];
  reason?: string;
}): Promise<CapturedFact> {
  const capture = await client.captureMemory(
    {
      subject: input.subject,
      predicate: input.predicate,
      object: input.object,
      project: PROJECT_NAME
    },
    {
      source: HARNESS_ACTOR,
      sourceKind: SourceKind.OBSERVATION,
      reasonToRemember: input.reason ?? `${input.subject} ${input.predicate}`,
      summary: `${input.subject} ${input.predicate}`,
      tags: uniqueStrings(["agent-harness", "fact", ...(input.tags ?? [])]),
      scope: HARNESS_SCOPE,
      sensitivity: Sensitivity.LOW
    }
  );

  const semantic = capture.created_records.find((record) => record.type === MemoryType.SEMANTIC) ?? capture.primary_record;
  if (semantic.type !== MemoryType.SEMANTIC) {
    throw new Error(`captureFact did not produce a semantic record; primary type was ${semantic.type}`);
  }

  return { capture, semantic };
}

export async function seedScenario(client: MembraneClient): Promise<SeededScenario> {
  const entities = new Map<string, string>();

  const anchor = await client.captureMemory(
    {
      ref: "entity-anchor",
      project: PROJECT_NAME,
      subject: SERVICE_NAME,
      tool_name: OBSERVABILITY_TOOL,
      file: "services/auth/rate_limit.go",
      database_entity: DATABASE_NAME,
      flag_tool_entity: FEATURE_FLAG_TOOL
    },
    {
      source: HARNESS_ACTOR,
      sourceKind: SourceKind.EVENT,
      reasonToRemember: "Seed canonical entities for the agent harness graph",
      summary: "Seed Project Orion auth-service entity anchors",
      tags: ["agent-harness", "seed", "entity"],
      scope: HARNESS_SCOPE,
      sensitivity: Sensitivity.LOW
    }
  );
  collectEntityMap(anchor, entities);

  const incident = await client.captureMemory(
    {
      tool_name: OBSERVABILITY_TOOL,
      args: { dashboard: "auth-service latency", window: "30m" },
      result: {
        p95_ms: 740,
        error_rate: "1.8%",
        finding: "latency spike aligns with database connection wait time"
      },
      project: PROJECT_NAME,
      subject: SERVICE_NAME,
      database_entity: DATABASE_NAME,
      file: "services/auth/rate_limit.go"
    },
    {
      source: HARNESS_ACTOR,
      sourceKind: SourceKind.TOOL_OUTPUT,
      reasonToRemember: "Prior auth-service incident showed p95 latency tied to database wait time",
      summary: "Grafana showed auth-service p95 spike tied to database waits",
      tags: ["agent-harness", "episodic", "incident", "latency"],
      scope: HARNESS_SCOPE,
      sensitivity: Sensitivity.LOW
    }
  );
  collectEntityMap(incident, entities);

  const working = await client.captureMemory(
    {
      thread_id: "orion-auth-latency-001",
      state: TaskState.EXECUTING,
      context_summary: "Diagnose auth-service latency without risky production changes.",
      next_actions: ["retrieve Membrane graph", "compare Grafana and database wait metrics", "ship flagged canary"],
      open_questions: ["is the pool wait causal", "which owner should approve rollout"],
      active_constraints: [
        { type: "safety", key: "rollout", value: "canary first", required: true },
        { type: "memory", key: "must_capture_fact", value: true, required: true }
      ],
      project: PROJECT_NAME,
      subject: SERVICE_NAME,
      tool_name: OBSERVABILITY_TOOL
    },
    {
      source: HARNESS_ACTOR,
      sourceKind: SourceKind.WORKING_STATE,
      reasonToRemember: "Current active task state for auth-service latency investigation",
      summary: "Active auth-service latency investigation",
      tags: ["agent-harness", "working", "latency"],
      scope: HARNESS_SCOPE,
      sensitivity: Sensitivity.LOW
    }
  );
  collectEntityMap(working, entities);

  const fact = await captureFact(client, {
    subject: SERVICE_NAME,
    predicate: "uses_database",
    object: DATABASE_NAME,
    tags: ["semantic", "database"],
    reason: "auth-service depends on PostgreSQL during latency diagnosis"
  });
  collectEntityMap(fact.capture, entities);

  const requiredEntityNames = [PROJECT_NAME, SERVICE_NAME, DATABASE_NAME, OBSERVABILITY_TOOL, FEATURE_FLAG_TOOL];
  const missing = requiredEntityNames.filter((name) => !entities.has(name));
  if (missing.length > 0) {
    throw new Error(`seed did not create expected entities: ${missing.join(", ")}`);
  }

  const linkedEntityIds = requiredEntityNames.map((name) => entities.get(name)).filter((id): id is string => Boolean(id));

  const competenceSlot = await semanticSeedForPromotion(client, "competence");
  const competence = await client.supersede(
    competenceSlot.id,
    competenceRecord(linkedEntityIds),
    HARNESS_ACTOR,
    "Promote stable auth incident handling into a competence memory"
  );

  const planSlot = await semanticSeedForPromotion(client, "plan_graph");
  const planGraph = await client.supersede(
    planSlot.id,
    planGraphRecord(linkedEntityIds),
    HARNESS_ACTOR,
    "Promote safe rollout workflow into a plan graph memory"
  );

  const entityRecord = await client.retrieveById(entities.get(PROJECT_NAME) ?? "", { trust: trustContext() });
  if (isEntityRecord(entityRecord) && !entityRecord.payload.types?.includes(EntityType.PROJECT)) {
    // The fallback extractor already created the entity; this read keeps the
    // seed path honest without mutating ontology details out of band.
  }

  return {
    entities: Object.fromEntries(entities.entries()),
    episodic: incident.primary_record,
    working: working.primary_record,
    semantic: fact.semantic,
    competence,
    planGraph
  };
}

export function graphHasEntityConnection(record: MemoryRecord, entityIds: Set<string>, edges: GraphEdge[]): boolean {
  if (record.type === MemoryType.ENTITY) {
    return true;
  }

  for (const rel of record.relations ?? []) {
    if (entityIds.has(rel.target_id)) {
      return true;
    }
  }

  return edges.some((edge) => {
    if (edge.source_id === record.id && entityIds.has(edge.target_id)) {
      return true;
    }
    return edge.target_id === record.id && entityIds.has(edge.source_id);
  });
}

export function outcomeSummary(status: string, details: Record<string, unknown>): Record<string, unknown> {
  return {
    status,
    details,
    recorded_at: nowIso(),
    outcome: OutcomeStatus.SUCCESS
  };
}
