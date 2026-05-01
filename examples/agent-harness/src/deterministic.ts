import "dotenv/config";

import assert from "node:assert/strict";

import {
  GraphPredicate,
  MembraneClient,
  MemoryType,
  RevisionStatus,
  Sensitivity,
  SourceKind,
  TaskState,
  type CaptureMemoryResult,
  type MemoryRecord
} from "@gustycube/membrane";

import { startOrConnectMembrane } from "./membrane-daemon.js";
import {
  DATABASE_NAME,
  FIVE_MEMORY_TYPES,
  GRAPH_MEMORY_TYPES,
  HARNESS_ACTOR,
  HARNESS_SCOPE,
  OBSERVABILITY_TOOL,
  PROJECT_NAME,
  SERVICE_NAME,
  captureFact,
  graphHasEntityConnection,
  isSemanticRecord,
  seedScenario,
  trustContext,
  uniqueStrings
} from "./scenario.js";

function countByType(records: MemoryRecord[]): Record<string, number> {
  const counts: Record<string, number> = {};
  for (const record of records) {
    counts[String(record.type)] = (counts[String(record.type)] ?? 0) + 1;
  }
  return counts;
}

function allCaptureRecords(capture: CaptureMemoryResult): MemoryRecord[] {
  return [capture.primary_record, ...capture.created_records];
}

function objectPayload(record: MemoryRecord): Record<string, unknown> {
  assert.ok(record.payload && typeof record.payload === "object" && !Array.isArray(record.payload), `${record.id} should have object payload`);
  return record.payload as Record<string, unknown>;
}

function semanticPayload(record: MemoryRecord): Record<string, unknown> {
  assert.equal(record.type, MemoryType.SEMANTIC, `${record.id} should be semantic`);
  const payload = objectPayload(record);
  assert.equal(payload.kind, "semantic", `${record.id} should have semantic payload`);
  return payload;
}

function assertRecordEnvelope(record: MemoryRecord, type: string): void {
  assert.equal(record.type, type);
  assert.ok(record.id.length > 0, `${type} record should have an id`);
  assert.equal(record.scope, HARNESS_SCOPE);
  assert.equal(record.sensitivity, Sensitivity.LOW);
  assert.ok(record.confidence >= 0 && record.confidence <= 1, `${record.id} confidence should be normalized`);
  assert.ok(record.salience >= 0 && record.salience <= 1, `${record.id} salience should be normalized`);
  assert.ok(Array.isArray(record.tags), `${record.id} should expose tags`);
  assert.ok(record.created_at, `${record.id} should expose created_at`);
  assert.ok(record.updated_at, `${record.id} should expose updated_at`);
  assert.ok(record.lifecycle?.decay?.curve, `${record.id} should expose lifecycle decay`);
  assert.ok(record.provenance === undefined || Array.isArray(record.provenance.sources), `${record.id} provenance sources should be an array`);
}

function assertSeedPayloads(seeded: {
  entities: Record<string, string>;
  episodic: MemoryRecord;
  working: MemoryRecord;
  semantic: MemoryRecord;
  competence: MemoryRecord;
  planGraph: MemoryRecord;
}): void {
  assertRecordEnvelope(seeded.episodic, MemoryType.EPISODIC);
  const episodic = objectPayload(seeded.episodic);
  assert.equal(episodic.kind, "episodic");
  assert.ok(Array.isArray(episodic.timeline) && episodic.timeline.length > 0, "episodic payload should expose timeline");
  assert.ok(Array.isArray(episodic.tool_graph) && episodic.tool_graph.length > 0, "tool output should expose tool_graph");

  assertRecordEnvelope(seeded.working, MemoryType.WORKING);
  const working = objectPayload(seeded.working);
  assert.equal(working.kind, "working");
  assert.equal(working.thread_id, "orion-auth-latency-001");
  assert.equal(working.state, TaskState.EXECUTING);
  assert.ok(Array.isArray(working.active_constraints), "working payload should expose active constraints");
  assert.ok(Array.isArray(working.next_actions), "working payload should expose next actions");
  assert.ok(Array.isArray(working.open_questions), "working payload should expose open questions");

  assertRecordEnvelope(seeded.semantic, MemoryType.SEMANTIC);
  const semantic = semanticPayload(seeded.semantic);
  assert.ok([SERVICE_NAME, seeded.entities[SERVICE_NAME]].includes(String(semantic.subject)));
  assert.equal(semantic.predicate, "uses_database");
  assert.ok([DATABASE_NAME, seeded.entities[DATABASE_NAME]].includes(String(semantic.object)));
  assert.ok(semantic.validity, "semantic payload should expose validity");
  assert.ok(Array.isArray(semantic.evidence), "semantic payload should expose evidence");

  assertRecordEnvelope(seeded.competence, MemoryType.COMPETENCE);
  const competence = objectPayload(seeded.competence);
  assert.equal(competence.kind, "competence");
  assert.ok(Array.isArray(competence.triggers), "competence payload should expose triggers");
  assert.ok(Array.isArray(competence.recipe), "competence payload should expose recipe");
  assert.ok(Array.isArray(competence.required_tools), "competence payload should expose required tools");
  assert.ok(competence.performance && typeof competence.performance === "object", "competence payload should expose performance stats");

  assertRecordEnvelope(seeded.planGraph, MemoryType.PLAN_GRAPH);
  const planGraph = objectPayload(seeded.planGraph);
  assert.equal(planGraph.kind, "plan_graph");
  assert.ok(Array.isArray(planGraph.nodes) && planGraph.nodes.length > 0, "plan graph should expose nodes");
  assert.ok(Array.isArray(planGraph.edges) && planGraph.edges.length > 0, "plan graph should expose edges");
  assert.ok(planGraph.inputs_schema, "plan graph should expose input schema");
  assert.ok(planGraph.outputs_schema, "plan graph should expose output schema");
}

function assertGraphShape(graph: Awaited<ReturnType<MembraneClient["retrieveGraph"]>>, limits: { nodeLimit: number; edgeLimit: number; maxHops: number }): void {
  assert.ok(graph.root_ids.length > 0, "graph should expose root_ids");
  assert.ok(graph.nodes.length <= limits.nodeLimit, "graph should respect node_limit");
  assert.ok(graph.edges.length <= limits.edgeLimit, "graph should respect edge_limit");

  const nodeIds = new Set(graph.nodes.map((node) => node.record.id));
  assert.ok(graph.root_ids.every((id) => nodeIds.has(id)), "root_ids should reference returned nodes");
  assert.ok(graph.nodes.every((node) => node.hop <= limits.maxHops), "graph nodes should respect max_hops");
  assert.ok(graph.nodes.some((node) => node.root), "graph should mark at least one root node");

  if (graph.selection) {
    assert.ok(graph.selection.confidence >= 0 && graph.selection.confidence <= 1, "selection confidence should be normalized");
    assert.equal(typeof graph.selection.needs_more, "boolean", "selection should expose needs_more");
  }
}

function graphHasAnyId(graph: Awaited<ReturnType<MembraneClient["retrieveGraph"]>>, ids: Set<string>): boolean {
  return graph.nodes.some((node) => ids.has(node.record.id));
}

function assertSameInstant(actual: unknown, expected: string, message: string): void {
  assert.equal(Date.parse(String(actual)), Date.parse(expected), message);
}

function cloneSemanticRecord(source: MemoryRecord, patch: Record<string, unknown>, extraTag: string): MemoryRecord {
  const clone = JSON.parse(JSON.stringify(source)) as MemoryRecord;
  clone.id = "";
  clone.tags = uniqueStrings([...(clone.tags ?? []), "agent-harness-revision", extraTag]);
  clone.relations = [...(clone.relations ?? [])];
  clone.audit_log = [];
  clone.payload = {
    ...semanticPayload(source),
    ...patch,
    revision: {
      status: RevisionStatus.ACTIVE
    }
  };
  return clone;
}

async function captureObservation(
  client: MembraneClient,
  input: {
    subject: string;
    predicate: string;
    object: unknown;
    tags: string[];
    scope?: string;
    sensitivity?: string;
  }
): Promise<{ capture: CaptureMemoryResult; semantic: MemoryRecord }> {
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
      reasonToRemember: `${input.subject} ${input.predicate}`,
      summary: `${input.subject} ${input.predicate}`,
      tags: uniqueStrings(["agent-harness", "deterministic-feature", ...input.tags]),
      scope: input.scope ?? HARNESS_SCOPE,
      sensitivity: input.sensitivity ?? Sensitivity.LOW
    }
  );

  const semantic = capture.created_records.find((record) => record.type === MemoryType.SEMANTIC) ?? capture.primary_record;
  assert.equal(semantic.type, MemoryType.SEMANTIC, "observation capture should create semantic memory");
  return { capture, semantic };
}

async function assertCaptureInputOutputFeatures(client: MembraneClient): Promise<{ rich_tool_output: string; medium_fact: string; other_scope_fact: string }> {
  const capturedAt = new Date(Date.now() - 60_000).toISOString();
  const toolOutput = await client.captureMemory(
    {
      tool_name: "psql",
      args: { command: "select wait_event_type, count(*) from pg_stat_activity group by 1" },
      result: { wait_event_type: "ClientRead", count: 14 },
      depends_on: ["grafana-panel-auth-p95"],
      project: PROJECT_NAME,
      subject: SERVICE_NAME,
      database_entity: DATABASE_NAME
    },
    {
      source: HARNESS_ACTOR,
      sourceKind: SourceKind.TOOL_OUTPUT,
      context: { active_entities: [PROJECT_NAME, SERVICE_NAME, DATABASE_NAME], request_id: "deterministic-rich-capture" },
      reasonToRemember: "psql wait-event sample should remain available for incident diagnosis",
      proposedType: MemoryType.EPISODIC,
      summary: "psql sampled database wait events for auth-service",
      tags: ["agent-harness", "tool-output", "psql", "feature-coverage"],
      scope: HARNESS_SCOPE,
      sensitivity: Sensitivity.LOW,
      timestamp: capturedAt
    }
  );

  assertRecordEnvelope(toolOutput.primary_record, MemoryType.EPISODIC);
  assert.ok(toolOutput.primary_record.tags?.includes("psql"), "capture should preserve tags");
  const episodic = objectPayload(toolOutput.primary_record);
  assert.ok(Array.isArray(episodic.tool_graph), "tool_output capture should expose tool_graph");
  assertSameInstant(((episodic.timeline as Array<Record<string, unknown>>)[0] ?? {}).t, capturedAt, "timeline should preserve timestamp input");
  assertSameInstant(
    ((episodic.tool_graph as Array<Record<string, unknown>>)[0] ?? {}).timestamp,
    capturedAt,
    "tool graph should preserve timestamp input"
  );
  const environment = episodic.environment as { context?: Record<string, unknown> } | undefined;
  assert.equal(environment?.context?.reason_to_remember, "psql wait-event sample should remain available for incident diagnosis");
  assert.deepEqual((environment?.context?.capture_context as Record<string, unknown> | undefined)?.active_entities, [
    PROJECT_NAME,
    SERVICE_NAME,
    DATABASE_NAME
  ]);
  assert.ok(
    toolOutput.edges.some((edge) => edge.predicate === GraphPredicate.MENTIONS_ENTITY || edge.predicate === GraphPredicate.SUBJECT_ENTITY),
    "capture should return entity graph edges"
  );

  const medium = await captureObservation(client, {
    subject: SERVICE_NAME,
    predicate: "restricted_escalation_code",
    object: "medium-only-7319",
    tags: ["medium-filter"],
    sensitivity: Sensitivity.MEDIUM
  });
  const mediumIds = new Set([medium.semantic.id, medium.capture.primary_record.id]);
  const lowTrustGraph = await client.retrieveGraph("auth-service restricted escalation code medium-only-7319", {
    trust: trustContext("agent-harness-low-trust"),
    memoryTypes: [MemoryType.EPISODIC, MemoryType.SEMANTIC, MemoryType.ENTITY],
    rootLimit: 20,
    nodeLimit: 50,
    edgeLimit: 80,
    maxHops: 2
  });
  const redactedMediumNodes = lowTrustGraph.nodes.filter((node) => mediumIds.has(node.record.id));
  assert.ok(redactedMediumNodes.length > 0, "LOW trust should expose one-level-higher records as redacted metadata");
  assert.ok(redactedMediumNodes.every((node) => node.record.payload == null), "redacted records should not expose payloads");
  assert.ok(redactedMediumNodes.every((node) => (node.record.relations ?? []).length === 0), "redacted records should not expose relations");

  const mediumTrustGraph = await client.retrieveGraph("auth-service restricted escalation code medium-only-7319", {
    trust: {
      ...trustContext("agent-harness-medium-trust"),
      max_sensitivity: Sensitivity.MEDIUM
    },
    memoryTypes: [MemoryType.EPISODIC, MemoryType.SEMANTIC, MemoryType.ENTITY],
    rootLimit: 20,
    nodeLimit: 50,
    edgeLimit: 80,
    maxHops: 2
  });
  assert.ok(graphHasAnyId(mediumTrustGraph, mediumIds), "MEDIUM trust should retrieve MEDIUM sensitivity records");
  assert.ok(
    mediumTrustGraph.nodes.some((node) => node.record.id === medium.semantic.id && node.record.payload != null),
    "MEDIUM trust should retrieve unredacted MEDIUM semantic payload"
  );

  const otherScope = "project:agent-harness-other";
  const other = await captureObservation(client, {
    subject: "other-scope-service",
    predicate: "scope_marker",
    object: "other-scope-marker-4421",
    tags: ["scope-filter"],
    scope: otherScope
  });
  const otherIds = new Set(allCaptureRecords(other.capture).map((record) => record.id));
  const harnessScopeGraph = await client.retrieveGraph("other-scope-service other-scope-marker-4421", {
    trust: trustContext("agent-harness-scope-filter"),
    memoryTypes: [MemoryType.EPISODIC, MemoryType.SEMANTIC, MemoryType.ENTITY],
    rootLimit: 20,
    nodeLimit: 50,
    edgeLimit: 80,
    maxHops: 2
  });
  assert.equal(graphHasAnyId(harnessScopeGraph, otherIds), false, "scope-limited trust should filter other scopes");

  const otherScopeGraph = await client.retrieveGraph("other-scope-service other-scope-marker-4421", {
    trust: {
      ...trustContext("agent-harness-other-scope"),
      scopes: [otherScope]
    },
    memoryTypes: [MemoryType.EPISODIC, MemoryType.SEMANTIC, MemoryType.ENTITY],
    rootLimit: 20,
    nodeLimit: 50,
    edgeLimit: 80,
    maxHops: 2
  });
  assert.ok(graphHasAnyId(otherScopeGraph, otherIds), "matching scope trust should retrieve scoped records");

  return {
    rich_tool_output: toolOutput.primary_record.id,
    medium_fact: medium.semantic.id,
    other_scope_fact: other.semantic.id
  };
}

async function assertRevisionAndDecayFeatures(client: MembraneClient): Promise<Record<string, string>> {
  const forkBase = await captureObservation(client, {
    subject: SERVICE_NAME,
    predicate: "approval_owner",
    object: "Dana",
    tags: ["revision-fork-base"]
  });
  const forked = await client.fork(
    forkBase.semantic.id,
    cloneSemanticRecord(forkBase.semantic, { predicate: "approval_owner_when_primary_absent", object: "Riley" }, "fork"),
    HARNESS_ACTOR,
    "deterministic harness fork coverage"
  );
  assert.ok(forked.relations?.some((relation) => relation.predicate === GraphPredicate.DERIVED_FROM && relation.target_id === forkBase.semantic.id));

  await client.contest(forked.id, forkBase.semantic.id, HARNESS_ACTOR, "deterministic harness contest coverage");
  const contested = await client.retrieveById(forked.id, { trust: trustContext("agent-harness-revision") });
  assert.equal((semanticPayload(contested).revision as Record<string, unknown> | undefined)?.status, RevisionStatus.CONTESTED);
  assert.ok(contested.relations?.some((relation) => relation.predicate === "contested_by" && relation.target_id === forkBase.semantic.id));

  await client.penalize(forked.id, 0.2, HARNESS_ACTOR, "deterministic harness penalize coverage");
  const penalized = await client.retrieveById(forked.id, { trust: trustContext("agent-harness-revision") });
  assert.ok(penalized.salience <= contested.salience, "penalize should not increase salience");

  await client.reinforce(forked.id, HARNESS_ACTOR, "deterministic harness reinforce coverage");
  const reinforced = await client.retrieveById(forked.id, { trust: trustContext("agent-harness-revision") });
  assert.ok(reinforced.salience >= penalized.salience, "reinforce should increase or preserve salience");
  assert.ok(reinforced.audit_log?.some((entry) => entry.action === "reinforce"), "reinforce should add audit log entry");

  const mergeA = await captureObservation(client, {
    subject: SERVICE_NAME,
    predicate: "rollback_owner_candidate",
    object: "Dana",
    tags: ["revision-merge-a"]
  });
  const mergeB = await captureObservation(client, {
    subject: SERVICE_NAME,
    predicate: "rollback_owner_candidate",
    object: "Riley",
    tags: ["revision-merge-b"]
  });
  const merged = await client.merge(
    [mergeA.semantic.id, mergeB.semantic.id],
    cloneSemanticRecord(mergeA.semantic, { predicate: "rollback_owner_candidates", object: ["Dana", "Riley"] }, "merge"),
    HARNESS_ACTOR,
    "deterministic harness merge coverage"
  );
  assert.ok(merged.relations?.some((relation) => relation.predicate === GraphPredicate.DERIVED_FROM && relation.target_id === mergeA.semantic.id));
  assert.ok(merged.relations?.some((relation) => relation.predicate === GraphPredicate.DERIVED_FROM && relation.target_id === mergeB.semantic.id));
  const mergedSource = await client.retrieveById(mergeA.semantic.id, { trust: trustContext("agent-harness-revision") });
  assert.equal(mergedSource.salience, 0);
  assert.equal((semanticPayload(mergedSource).revision as Record<string, unknown> | undefined)?.status, RevisionStatus.RETRACTED);

  const retractable = await captureObservation(client, {
    subject: SERVICE_NAME,
    predicate: "obsolete_probe",
    object: "temporary",
    tags: ["revision-retract"]
  });
  await client.retract(retractable.semantic.id, HARNESS_ACTOR, "deterministic harness retract coverage");
  const retracted = await client.retrieveById(retractable.semantic.id, { trust: trustContext("agent-harness-revision") });
  assert.equal(retracted.salience, 0);
  assert.equal((semanticPayload(retracted).revision as Record<string, unknown> | undefined)?.status, RevisionStatus.RETRACTED);

  return {
    forked: forked.id,
    contested: contested.id,
    merged: merged.id,
    retracted: retracted.id
  };
}

async function main(): Promise<void> {
  const runtime = await startOrConnectMembrane();
  const client = new MembraneClient(runtime.addr, { apiKey: runtime.apiKey, timeoutMs: 8_000 });

  try {
    const seeded = await seedScenario(client);
    assertSeedPayloads(seeded);

    const metrics = await client.getMetrics();
    const recordsByType = (metrics.records_by_type ?? {}) as Record<string, unknown>;

    for (const memoryType of [...FIVE_MEMORY_TYPES, MemoryType.ENTITY]) {
      assert.ok(Number(recordsByType[memoryType] ?? 0) > 0, `metrics should include ${memoryType}`);
    }

    const graph = await client.retrieveGraph("Project Orion auth-service PostgreSQL latency rollout", {
      trust: trustContext("agent-harness-deterministic"),
      memoryTypes: [...GRAPH_MEMORY_TYPES],
      rootLimit: 40,
      nodeLimit: 120,
      edgeLimit: 240,
      maxHops: 2
    });
    assertGraphShape(graph, { nodeLimit: 120, edgeLimit: 240, maxHops: 2 });

    const records = graph.nodes.map((node) => node.record);
    const typeCounts = countByType(records);
    for (const memoryType of [...FIVE_MEMORY_TYPES, MemoryType.ENTITY]) {
      assert.ok((typeCounts[memoryType] ?? 0) > 0, `retrieveGraph should include ${memoryType}`);
    }

    const entityIds = new Set(records.filter((record) => record.type === MemoryType.ENTITY).map((record) => record.id));
    assert.ok(entityIds.size > 0, "retrieveGraph should include entity nodes");

    for (const memoryType of FIVE_MEMORY_TYPES) {
      const candidates = records.filter((record) => record.type === memoryType);
      assert.ok(candidates.length > 0, `graph should include ${memoryType}`);
      assert.ok(
        candidates.some((record) => graphHasEntityConnection(record, entityIds, graph.edges)),
        `${memoryType} should be connected to at least one entity`
      );
    }

    assert.ok(
      graph.edges.some((edge) => edge.predicate === GraphPredicate.MENTIONS_ENTITY || edge.predicate === GraphPredicate.SUBJECT_ENTITY),
      "retrieveGraph should expose entity graph edges"
    );

    const bounded = await client.retrieveGraph("Project Orion auth-service bounded graph smoke", {
      trust: trustContext("agent-harness-deterministic"),
      memoryTypes: [...GRAPH_MEMORY_TYPES],
      rootLimit: 1,
      nodeLimit: 3,
      edgeLimit: 5,
      maxHops: 1
    });
    assertGraphShape(bounded, { nodeLimit: 3, edgeLimit: 5, maxHops: 1 });

    for (const [name, id] of Object.entries(seeded.entities)) {
      const entity = await client.retrieveById(id, { trust: trustContext("agent-harness-entity-check") });
      assert.equal(entity.type, MemoryType.ENTITY, `${name} should retrieve as entity`);
      assert.equal(objectPayload(entity).kind, "entity", `${name} should expose entity payload`);
    }

    const taught = await captureFact(client, {
      subject: SERVICE_NAME,
      predicate: "runbook_owner",
      object: "Dana",
      tags: ["deterministic-taught-fact"],
      reason: "Teach a new fact and verify later retrieval can see it"
    });

    const later = await client.retrieveGraph("auth-service runbook owner Dana", {
      trust: trustContext("agent-harness-deterministic"),
      memoryTypes: [MemoryType.SEMANTIC, MemoryType.ENTITY],
      rootLimit: 20,
      nodeLimit: 60,
      edgeLimit: 120,
      maxHops: 2
    });

    assert.ok(
      later.nodes.some((node) => node.record.id === taught.semantic.id),
      "later retrieval should include the newly taught semantic fact"
    );
    assert.ok(
      isSemanticRecord(taught.semantic) && taught.semantic.payload.predicate === "runbook_owner",
      "taught fact should be stored as semantic memory"
    );

    assert.ok(
      taught.capture.edges.some((edge) => edge.predicate === GraphPredicate.SUBJECT_ENTITY || edge.predicate === GraphPredicate.OBJECT_ENTITY),
      "fact capture should expose subject/object entity edges"
    );

    const byId = await client.retrieveById(taught.semantic.id, { trust: trustContext("agent-harness-deterministic") });
    assert.deepEqual(objectPayload(byId), objectPayload(taught.semantic), "retrieveById should return exact semantic payload");

    const featureCoverage = await assertCaptureInputOutputFeatures(client);
    const revisionCoverage = await assertRevisionAndDecayFeatures(client);

    const finalMetrics = await client.getMetrics();

    const summary = {
      db_path: runtime.dbPath ?? "(external daemon)",
      seeded: {
        episodic: seeded.episodic.id,
        working: seeded.working.id,
        semantic: seeded.semantic.id,
        competence: seeded.competence.id,
        plan_graph: seeded.planGraph.id,
        entities: seeded.entities
      },
      graph_counts: typeCounts,
      taught_fact: taught.semantic.id,
      capture_input_output: featureCoverage,
      revision_decay: revisionCoverage,
      final_total_records: finalMetrics.total_records,
      covered_features: [
        "captureMemory sourceKind/event-tool-working-observation metadata",
        "capture created_records and graph edges",
        "retrieveGraph root_ids/nodes/edges/bounds/entity traversal",
        "retrieveById exact record payloads",
        "trust sensitivity filtering",
        "scope filtering",
        "metrics records_by_type and total_records",
        "revision fork/merge/contest/retract",
        "decay reinforce/penalize"
      ]
    };

    console.log("PASS deterministic agent harness");
    console.log(JSON.stringify(summary, null, 2));
  } finally {
    client.close();
    await runtime.close();
  }
}

main().catch((error: unknown) => {
  console.error("FAIL deterministic agent harness");
  console.error(error);
  process.exitCode = 1;
});
