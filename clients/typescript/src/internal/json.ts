import {
  normalizeGraphPredicate,
  type CaptureMemoryResult,
  type GraphEdge,
  type GraphNode,
  type Interpretation,
  type JsonObject,
  type MemoryRecord,
  type MetricsSnapshot,
  type Mention,
  type Relation,
  type ReferenceCandidate,
  type RelationCandidate,
  type RetrievalDiagnostic,
  type RecordProjection,
  type RetrieveGraphResult,
  type RetrieveResult,
  type SelectionResult
} from "../types";

type ProtoValue = JsonObject;

const VALUE_KINDS = new Set([
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
  "list_value"
]);

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseJsonText(text: string): unknown {
  return JSON.parse(text) as unknown;
}

export function encodeJsonBytes(value: unknown): Buffer {
  return Buffer.from(JSON.stringify(value), "utf8");
}

export function decodeJsonValue(value: unknown): unknown {
  if (Buffer.isBuffer(value)) {
    return parseJsonText(value.toString("utf8"));
  }
  if (value instanceof Uint8Array) {
    return parseJsonText(Buffer.from(value).toString("utf8"));
  }
  if (typeof value === "string") {
    return parseJsonText(value);
  }
  return value;
}

export function toProtoValue(value: unknown): ProtoValue {
  if (value === null || value === undefined) {
    return { nullValue: "NULL_VALUE", kind: "nullValue" };
  }
  if (typeof value === "string") {
    return { stringValue: value, kind: "stringValue" };
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new TypeError("google.protobuf.Value numbers must be finite");
    }
    return { numberValue: value, kind: "numberValue" };
  }
  if (typeof value === "boolean") {
    return { boolValue: value, kind: "boolValue" };
  }
  if (Array.isArray(value)) {
    return { listValue: { values: value.map((item) => toProtoValue(item)) }, kind: "listValue" };
  }
  if (isObject(value)) {
    return { structValue: { fields: toProtoValueMap(value) }, kind: "structValue" };
  }
  return { stringValue: String(value), kind: "stringValue" };
}

function toProtoValueMap(value: unknown): Record<string, ProtoValue> {
  if (!isObject(value)) {
    return {};
  }
  return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, toProtoValue(item)]));
}

export function fromProtoValue(value: unknown): unknown {
  const decoded = decodeJsonValue(value);
  if (!isObject(decoded)) {
    return decoded;
  }

  const valueKind =
    typeof decoded.kind === "string" && VALUE_KINDS.has(decoded.kind) ? decoded.kind : inferProtoValueKind(decoded);

  switch (valueKind) {
    case "nullValue":
    case "null_value":
      return null;
    case "numberValue":
    case "number_value": {
      const raw = decoded.numberValue ?? decoded.number_value;
      return typeof raw === "number" ? raw : Number(raw ?? 0);
    }
    case "stringValue":
    case "string_value": {
      const raw = decoded.stringValue ?? decoded.string_value;
      return typeof raw === "string" ? raw : String(raw ?? "");
    }
    case "boolValue":
    case "bool_value":
      return (decoded.boolValue ?? decoded.bool_value) === true;
    case "structValue":
    case "struct_value": {
      const structValue = decoded.structValue ?? decoded.struct_value;
      const fields = isObject(structValue) && isObject(structValue.fields) ? structValue.fields : {};
      return Object.fromEntries(Object.entries(fields).map(([key, item]) => [key, fromProtoValue(item)]));
    }
    case "listValue":
    case "list_value": {
      const listValue = decoded.listValue ?? decoded.list_value;
      const values = isObject(listValue) && Array.isArray(listValue.values) ? listValue.values : [];
      return values.map((item) => fromProtoValue(item));
    }
    default:
      return decoded;
  }
}

function inferProtoValueKind(value: JsonObject): string | undefined {
  if ("stringValue" in value) return "stringValue";
  if ("numberValue" in value) return "numberValue";
  if ("boolValue" in value) return "boolValue";
  if ("structValue" in value) return "structValue";
  if ("listValue" in value) return "listValue";
  if ("nullValue" in value) return "nullValue";
  if ("string_value" in value) return "string_value";
  if ("number_value" in value) return "number_value";
  if ("bool_value" in value) return "bool_value";
  if ("struct_value" in value) return "struct_value";
  if ("list_value" in value) return "list_value";
  if ("null_value" in value) return "null_value";
  return undefined;
}

function fromProtoValueMap(value: unknown): Record<string, unknown> | undefined {
  if (!isObject(value)) {
    return undefined;
  }
  return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, fromProtoValue(item)]));
}

function asString(value: unknown): string {
  return typeof value === "string" ? value : value == null ? "" : String(value);
}

function field(value: JsonObject, ...names: string[]): unknown {
  for (const name of names) {
    if (name in value) {
      return value[name];
    }
  }
  return undefined;
}

function asNumber(value: unknown): number {
  return typeof value === "number" ? value : Number(value ?? 0);
}

function asStringArray(value: unknown): string[] | undefined {
  return Array.isArray(value) ? value.map((item) => asString(item)) : undefined;
}

function parseObjectArray<T>(value: unknown, parser: (value: unknown) => T): T[] | undefined {
  return Array.isArray(value) ? value.map((item) => parser(item)) : undefined;
}

function parseRelation(value: unknown): Relation {
  const decoded = decodeJsonValue(value);
  if (!isObject(decoded)) {
    throw new TypeError("Expected relation object");
  }
  const relation: Relation = { target_id: asString(field(decoded, "target_id", "TargetID", "targetId")) };
  const predicate = field(decoded, "predicate", "Predicate");
  const kind = field(decoded, "kind", "Kind");
  const weight = field(decoded, "weight", "Weight");
  const createdAt = field(decoded, "created_at", "CreatedAt", "createdAt");
  if (predicate !== undefined) relation.predicate = asString(predicate);
  if (kind !== undefined) relation.kind = asString(kind);
  if (weight !== undefined) relation.weight = asNumber(weight);
  if (createdAt !== undefined) relation.created_at = asString(createdAt);
  return relation;
}

function parseMention(value: unknown): Mention {
  const decoded = decodeJsonValue(value);
  if (!isObject(decoded)) {
    throw new TypeError("Expected mention object");
  }
  const mention: Mention = {
    surface: asString(field(decoded, "surface", "Surface"))
  };
  const entityKind = field(decoded, "entity_kind", "EntityKind", "entityKind");
  const canonicalEntityID = field(decoded, "canonical_entity_id", "CanonicalEntityID", "canonicalEntityId");
  const confidence = field(decoded, "confidence", "Confidence");
  const aliases = field(decoded, "aliases", "Aliases");
  if (entityKind !== undefined) mention.entity_kind = asString(entityKind);
  if (canonicalEntityID !== undefined) mention.canonical_entity_id = asString(canonicalEntityID);
  if (confidence !== undefined) mention.confidence = asNumber(confidence);
  if (aliases !== undefined) mention.aliases = asStringArray(aliases) ?? [];
  return mention;
}

function parseRelationCandidate(value: unknown): RelationCandidate {
  const decoded = decodeJsonValue(value);
  if (!isObject(decoded)) {
    throw new TypeError("Expected relation candidate object");
  }
  const candidate: RelationCandidate = {
    predicate: asString(field(decoded, "predicate", "Predicate"))
  };
  const targetRecordID = field(decoded, "target_record_id", "TargetRecordID", "targetRecordId");
  const targetEntityID = field(decoded, "target_entity_id", "TargetEntityID", "targetEntityId");
  const confidence = field(decoded, "confidence", "Confidence");
  const resolved = field(decoded, "resolved", "Resolved");
  if (targetRecordID !== undefined) candidate.target_record_id = asString(targetRecordID);
  if (targetEntityID !== undefined) candidate.target_entity_id = asString(targetEntityID);
  if (confidence !== undefined) candidate.confidence = asNumber(confidence);
  if (resolved !== undefined) candidate.resolved = resolved === true;
  return candidate;
}

function parseReferenceCandidate(value: unknown): ReferenceCandidate {
  const decoded = decodeJsonValue(value);
  if (!isObject(decoded)) {
    throw new TypeError("Expected reference candidate object");
  }
  const candidate: ReferenceCandidate = {
    ref: asString(field(decoded, "ref", "Ref"))
  };
  const targetRecordID = field(decoded, "target_record_id", "TargetRecordID", "targetRecordId");
  const targetEntityID = field(decoded, "target_entity_id", "TargetEntityID", "targetEntityId");
  const confidence = field(decoded, "confidence", "Confidence");
  const resolved = field(decoded, "resolved", "Resolved");
  if (targetRecordID !== undefined) candidate.target_record_id = asString(targetRecordID);
  if (targetEntityID !== undefined) candidate.target_entity_id = asString(targetEntityID);
  if (confidence !== undefined) candidate.confidence = asNumber(confidence);
  if (resolved !== undefined) candidate.resolved = resolved === true;
  return candidate;
}

function parseInterpretation(value: unknown): Interpretation {
  const decoded = decodeJsonValue(value);
  if (!isObject(decoded)) {
    throw new TypeError("Expected interpretation object");
  }
  const interpretation: Interpretation = {
    status: asString(field(decoded, "status", "Status"))
  };
  const summary = field(decoded, "summary", "Summary");
  const proposedType = field(decoded, "proposed_type", "ProposedType", "proposedType");
  const topicalLabels = field(decoded, "topical_labels", "TopicalLabels", "topicalLabels");
  const mentions = field(decoded, "mentions", "Mentions");
  const relationCandidates = field(decoded, "relation_candidates", "RelationCandidates", "relationCandidates");
  const referenceCandidates = field(decoded, "reference_candidates", "ReferenceCandidates", "referenceCandidates");
  const extractionConfidence = field(decoded, "extraction_confidence", "ExtractionConfidence", "extractionConfidence");
  if (summary !== undefined) interpretation.summary = asString(summary);
  if (proposedType !== undefined) interpretation.proposed_type = asString(proposedType);
  if (topicalLabels !== undefined) interpretation.topical_labels = asStringArray(topicalLabels) ?? [];
  if (mentions !== undefined) interpretation.mentions = parseObjectArray(mentions, parseMention) ?? [];
  if (relationCandidates !== undefined) {
    interpretation.relation_candidates = parseObjectArray(relationCandidates, parseRelationCandidate) ?? [];
  }
  if (referenceCandidates !== undefined) {
    interpretation.reference_candidates = parseObjectArray(referenceCandidates, parseReferenceCandidate) ?? [];
  }
  if (extractionConfidence !== undefined) interpretation.extraction_confidence = asNumber(extractionConfidence);
  return interpretation;
}

export function parseRecord(value: unknown): MemoryRecord {
  const decoded = decodeJsonValue(value);
  if (!isObject(decoded)) {
    throw new TypeError("Expected MemoryRecord object response");
  }

  const record: MemoryRecord = {
    id: asString(field(decoded, "id", "ID")),
    type: asString(field(decoded, "type", "Type")),
    sensitivity: asString(field(decoded, "sensitivity", "Sensitivity")),
    confidence: asNumber(field(decoded, "confidence", "Confidence")),
    salience: asNumber(field(decoded, "salience", "Salience"))
  };

  const scope = field(decoded, "scope", "Scope");
  const tags = field(decoded, "tags", "Tags");
  const createdAt = field(decoded, "created_at", "CreatedAt", "createdAt");
  const updatedAt = field(decoded, "updated_at", "UpdatedAt", "updatedAt");
  const lifecycle = field(decoded, "lifecycle", "Lifecycle");
  const provenance = field(decoded, "provenance", "Provenance");
  const relations = field(decoded, "relations", "Relations");
  const payload = field(decoded, "payload", "Payload");
  const auditLog = field(decoded, "audit_log", "AuditLog", "auditLog");
  if (scope !== undefined) record.scope = asString(scope);
  if (tags !== undefined) record.tags = asStringArray(tags) ?? [];
  if (createdAt !== undefined) record.created_at = asString(createdAt);
  if (updatedAt !== undefined) record.updated_at = asString(updatedAt);
  if (lifecycle !== undefined) record.lifecycle = lifecycle as NonNullable<MemoryRecord["lifecycle"]>;
  if (provenance !== undefined) record.provenance = provenance as NonNullable<MemoryRecord["provenance"]>;
  if (relations !== undefined) record.relations = parseObjectArray(relations, parseRelation) ?? [];
  const interpretation = field(decoded, "interpretation", "Interpretation");
  if (interpretation != null) record.interpretation = parseInterpretation(interpretation);
  if (payload !== undefined) record.payload = parsePayload(payload);
  if (auditLog !== undefined) record.audit_log = auditLog as NonNullable<MemoryRecord["audit_log"]>;

  return record;
}

export function parseRecordEnvelope(response: unknown): MemoryRecord {
  const decoded = decodeJsonValue(response);
  if (!isObject(decoded)) {
    throw new TypeError("Expected object response for record envelope");
  }

  if ("record" in decoded) {
    return parseRecord(decoded.record);
  }

  return parseRecord(decoded);
}

function parseGraphEdge(value: unknown): GraphEdge {
  const decoded = decodeJsonValue(value);
  if (!isObject(decoded)) {
    throw new TypeError("Expected graph edge object");
  }
  const edge: GraphEdge = {
    source_id: asString(field(decoded, "source_id", "SourceID", "sourceId")),
    predicate: asString(field(decoded, "predicate", "Predicate")),
    target_id: asString(field(decoded, "target_id", "TargetID", "targetId"))
  };
  const weight = field(decoded, "weight", "Weight");
  const createdAt = field(decoded, "created_at", "CreatedAt", "createdAt");
  if (weight !== undefined) edge.weight = asNumber(weight);
  if (createdAt !== undefined) edge.created_at = asString(createdAt);
  return edge;
}

function parseGraphNode(value: unknown): GraphNode {
  const decoded = decodeJsonValue(value);
  if (!isObject(decoded)) {
    throw new TypeError("Expected graph node object");
  }
  const hop = field(decoded, "hop", "Hop");
  return {
    record: parseRecord(field(decoded, "record", "Record")),
    root: field(decoded, "root", "Root") === true,
    hop: typeof hop === "number" ? hop : Number(hop ?? 0)
  };
}

function parseRetrievalDiagnostic(value: unknown): RetrievalDiagnostic {
  const decoded = decodeJsonValue(value);
  if (!isObject(decoded)) {
    throw new TypeError("Expected retrieval diagnostic object");
  }
  return {
    code: asString(field(decoded, "code", "Code")),
    message: asString(field(decoded, "message", "Message"))
  };
}

function parseRecordProjection(value: unknown): RecordProjection {
  const decoded = decodeJsonValue(value);
  if (!isObject(decoded)) {
    throw new TypeError("Expected record projection object");
  }
  return {
    relations_omitted: field(decoded, "relations_omitted", "RelationsOmitted", "relationsOmitted") === true,
    relations_truncated: field(decoded, "relations_truncated", "RelationsTruncated", "relationsTruncated") === true,
    history_omitted: field(decoded, "history_omitted", "HistoryOmitted", "historyOmitted") === true,
    records_truncated: field(decoded, "records_truncated", "RecordsTruncated", "recordsTruncated") === true
  };
}

export function parseRecordsEnvelope(response: unknown): MemoryRecord[] {
  return parseRetrieveEnvelope(response).records;
}

function isEmptyPayload(value: unknown): boolean {
  if (value == null) {
    return true;
  }
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    return value.length === 0;
  }
  if (typeof value === "string") {
    return value.length === 0;
  }
  return false;
}

export function parseSelection(value: unknown): SelectionResult {
  const decoded = decodeJsonValue(value);
  if (!isObject(decoded)) {
    throw new TypeError("Expected selection metadata object");
  }

  const rawSelected = field(decoded, "selected", "Selected");
  const confidence = field(decoded, "confidence", "Confidence");
  const needsMore = field(decoded, "needs_more", "NeedsMore", "needsMore");
  const rawScores = isObject(decoded.scores) ? decoded.scores : isObject(decoded.Scores) ? decoded.Scores : undefined;
  const scores =
    rawScores !== undefined
      ? Object.fromEntries(Object.entries(rawScores).map(([key, score]) => [key, asNumber(score)]))
      : undefined;

  const selection: SelectionResult = {
    selected: Array.isArray(rawSelected) ? rawSelected.map((raw) => parseRecord(raw)) : [],
    confidence: typeof confidence === "number" ? confidence : 0,
    needs_more: needsMore === true
  };
  if (scores !== undefined) {
    selection.scores = scores;
  }
  return selection;
}

export function parseRetrieveEnvelope(response: unknown): RetrieveResult {
  const decoded = decodeJsonValue(response);
  if (!isObject(decoded)) {
    throw new TypeError("Expected object response for records envelope");
  }

  const rawRecords = decoded.records;
  const records = Array.isArray(rawRecords) ? rawRecords.map((raw) => parseRecord(raw)) : [];

  const selectionValue = field(decoded, "selection", "Selection");
  let selection: SelectionResult | undefined;
  if (selectionValue !== undefined && !isEmptyPayload(selectionValue)) {
    selection = parseSelection(selectionValue);
  }

  if (selection === undefined) {
    return { records };
  }

  return { records, selection };
}

export function parseCaptureMemoryEnvelope(response: unknown): CaptureMemoryResult {
  const decoded = decodeJsonValue(response);
  if (!isObject(decoded)) {
    throw new TypeError("Expected object response for capture envelope");
  }

  const rawCreatedRecords = field(decoded, "created_records", "CreatedRecords", "createdRecords");
  const createdRecords = Array.isArray(rawCreatedRecords) ? rawCreatedRecords.map((raw) => parseRecord(raw)) : [];
  const edgesValue = decodeJsonValue(field(decoded, "edges", "Edges"));
  const edges = Array.isArray(edgesValue) ? edgesValue.map((raw) => parseGraphEdge(raw)) : [];

  return {
    primary_record: parseRecord(field(decoded, "primary_record", "PrimaryRecord", "primaryRecord")),
    created_records: createdRecords,
    edges
  };
}

export function parseRetrieveGraphEnvelope(response: unknown): RetrieveGraphResult {
  const decoded = decodeJsonValue(response);
  if (!isObject(decoded)) {
    throw new TypeError("Expected object response for graph envelope");
  }

  const nodesValue = decodeJsonValue(field(decoded, "nodes", "Nodes"));
  const edgesValue = decodeJsonValue(field(decoded, "edges", "Edges"));
  const nodes = Array.isArray(nodesValue) ? nodesValue.map((raw) => parseGraphNode(raw)) : [];
  const edges = Array.isArray(edgesValue) ? edgesValue.map((raw) => parseGraphEdge(raw)) : [];

  const selectionValue = field(decoded, "selection", "Selection");
  let selection: SelectionResult | undefined;
  if (selectionValue !== undefined && !isEmptyPayload(selectionValue)) {
    selection = parseSelection(selectionValue);
  }

  const rootIDsValue = field(decoded, "root_ids", "RootIDs", "rootIds");
  const rawRootIDs = Array.isArray(rootIDsValue) ? rootIDsValue : [];
  const diagnosticsValue = decodeJsonValue(field(decoded, "diagnostics", "Diagnostics"));
  const diagnostics = Array.isArray(diagnosticsValue)
    ? diagnosticsValue.map((raw) => parseRetrievalDiagnostic(raw))
    : [];
  const result: RetrieveGraphResult = {
    nodes,
    edges,
    root_ids: rawRootIDs.map((value) => String(value))
  };

  if (selection !== undefined) {
    result.selection = selection;
  }
  if (diagnostics.length > 0) {
    result.diagnostics = diagnostics;
  }

  const projectionValue = field(decoded, "projection", "Projection");
  if (projectionValue !== undefined && !isEmptyPayload(projectionValue)) {
    result.projection = parseRecordProjection(projectionValue);
  }

  return result;
}

export function parseMetricsEnvelope(response: unknown): MetricsSnapshot {
  const decoded = decodeJsonValue(response);
  if (!isObject(decoded)) {
    throw new TypeError("Expected object response for metrics envelope");
  }

  const snapshot = "snapshot" in decoded ? fromProtoValue(decoded.snapshot) : decoded;
  if (!isObject(snapshot)) {
    throw new TypeError("Expected metrics snapshot object");
  }

  return snapshot as MetricsSnapshot;
}

function parsePayload(value: unknown): unknown {
  const decoded = decodeJsonValue(value);
  if (!isObject(decoded)) {
    return decoded;
  }

  if ("episodic" in decoded) return parseEpisodicPayload(decoded.episodic);
  if ("working" in decoded) return parseWorkingPayload(decoded.working);
  if ("semantic" in decoded) return parseSemanticPayload(decoded.semantic);
  if ("competence" in decoded) return parseCompetencePayload(decoded.competence);
  if ("plan_graph" in decoded) return parsePlanGraphPayload(decoded.plan_graph);
  if ("planGraph" in decoded) return parsePlanGraphPayload(decoded.planGraph);
  if ("entity" in decoded) return parseEntityPayload(decoded.entity);

  switch (decoded.kind) {
    case "episodic":
      return parseEpisodicPayload(decoded);
    case "working":
      return parseWorkingPayload(decoded);
    case "semantic":
      return parseSemanticPayload(decoded);
    case "competence":
      return parseCompetencePayload(decoded);
    case "plan_graph":
      return parsePlanGraphPayload(decoded);
    case "entity":
      return parseEntityPayload(decoded);
    default:
      return decoded;
  }
}

function parseEpisodicPayload(value: unknown): JsonObject | undefined {
  if (!isObject(value)) return undefined;
  const payload: JsonObject = { ...value };
  if (Array.isArray(value.tool_graph)) {
    payload.tool_graph = value.tool_graph.map((raw) => {
      if (!isObject(raw)) return raw;
      return { ...raw, args: fromProtoValueMap(raw.args), result: fromProtoValue(raw.result) };
    });
  }
  if (isObject(value.environment)) {
    payload.environment = { ...value.environment, context: fromProtoValueMap(value.environment.context) };
  }
  return payload;
}

function parseWorkingPayload(value: unknown): JsonObject | undefined {
  if (!isObject(value)) return undefined;
  const payload: JsonObject = { ...value };
  if (Array.isArray(value.active_constraints)) {
    payload.active_constraints = value.active_constraints.map((raw) => {
      if (!isObject(raw)) return raw;
      return { ...raw, value: fromProtoValue(raw.value) };
    });
  }
  return payload;
}

function parseSemanticPayload(value: unknown): JsonObject | undefined {
  if (!isObject(value)) return undefined;
  const payload: JsonObject = { ...value, object: fromProtoValue(value.object) };
  const revisionPolicy = field(value, "revision_policy", "RevisionPolicy", "revisionPolicy");
  const revision = field(value, "revision", "Revision");
  const validity = field(value, "validity", "Validity");
  if (revisionPolicy !== undefined) {
    payload.revision_policy = asString(revisionPolicy);
  }
  if (isObject(revision)) {
    const supersedes = field(revision, "supersedes", "Supersedes");
    const supersededBy = field(revision, "superseded_by", "SupersededBy", "supersededBy");
    const status = field(revision, "status", "Status");
    const parsedRevision: JsonObject = {};
    if (supersedes !== undefined) parsedRevision.supersedes = asString(supersedes);
    if (supersededBy !== undefined) parsedRevision.superseded_by = asString(supersededBy);
    if (status !== undefined) parsedRevision.status = asString(status);
    payload.revision = parsedRevision;
  }
  if (isObject(validity)) {
    payload.validity = { ...validity, conditions: fromProtoValueMap(field(validity, "conditions", "Conditions")) };
  }
  return payload;
}

function parseCompetencePayload(value: unknown): JsonObject | undefined {
  if (!isObject(value)) return undefined;
  const payload: JsonObject = { ...value };
  if (Array.isArray(value.triggers)) {
    payload.triggers = value.triggers.map((raw) => {
      if (!isObject(raw)) return raw;
      return { ...raw, conditions: fromProtoValueMap(raw.conditions) };
    });
  }
  if (Array.isArray(value.recipe)) {
    payload.recipe = value.recipe.map((raw) => {
      if (!isObject(raw)) return raw;
      return { ...raw, args_schema: fromProtoValueMap(raw.args_schema) };
    });
  }
  return payload;
}

function parsePlanGraphPayload(value: unknown): JsonObject | undefined {
  if (!isObject(value)) return undefined;
  const metrics = field(value, "metrics", "Metrics");
  const payload: JsonObject = {
    ...value,
    constraints: fromProtoValueMap(field(value, "constraints", "Constraints")),
    inputs_schema: fromProtoValueMap(field(value, "inputs_schema", "InputsSchema", "inputsSchema")),
    outputs_schema: fromProtoValueMap(field(value, "outputs_schema", "OutputsSchema", "outputsSchema"))
  };
  const planID = field(value, "plan_id", "PlanID", "planId");
  const version = field(value, "version", "Version");
  const intent = field(value, "intent", "Intent");
  if (planID !== undefined) payload.plan_id = asString(planID);
  if (version !== undefined) payload.version = asString(version);
  if (intent !== undefined) payload.intent = asString(intent);
  const nodes = field(value, "nodes", "Nodes");
  if (Array.isArray(nodes)) {
    payload.nodes = nodes.map((raw) => {
      if (!isObject(raw)) return raw;
      return {
        ...raw,
        params: fromProtoValueMap(field(raw, "params", "Params")),
        guards: fromProtoValueMap(field(raw, "guards", "Guards"))
      };
    });
  }
  const edges = field(value, "edges", "Edges");
  if (Array.isArray(edges)) {
    payload.edges = edges;
  }
  if (isObject(metrics)) {
    const parsedMetrics: JsonObject = {};
    const avgLatencyMs = field(metrics, "avg_latency_ms", "AvgLatencyMs", "avgLatencyMs");
    const failureRate = field(metrics, "failure_rate", "FailureRate", "failureRate");
    const executionCount = field(metrics, "execution_count", "ExecutionCount", "executionCount");
    const lastExecutedAt = field(metrics, "last_executed_at", "LastExecutedAt", "lastExecutedAt");
    if (avgLatencyMs !== undefined) parsedMetrics.avg_latency_ms = asNumber(avgLatencyMs);
    if (failureRate !== undefined) parsedMetrics.failure_rate = asNumber(failureRate);
    if (executionCount !== undefined) parsedMetrics.execution_count = asNumber(executionCount);
    if (lastExecutedAt !== undefined) parsedMetrics.last_executed_at = asString(lastExecutedAt);
    payload.metrics = parsedMetrics;
  }
  return payload;
}

function parseEntityPayload(value: unknown): JsonObject | undefined {
  if (!isObject(value)) return undefined;
  const payload: JsonObject = { ...value };
  const kind = field(value, "kind", "Kind");
  const canonicalName = field(value, "canonical_name", "CanonicalName", "canonicalName");
  const primaryType = field(value, "primary_type", "PrimaryType", "primaryType");
  const types = field(value, "types", "Types");
  const aliases = field(value, "aliases", "Aliases");
  const identifiers = field(value, "identifiers", "Identifiers");
  const summary = field(value, "summary", "Summary");
  if (kind !== undefined) payload.kind = asString(kind);
  if (canonicalName !== undefined) payload.canonical_name = asString(canonicalName);
  if (primaryType !== undefined) payload.primary_type = asString(primaryType);
  if (Array.isArray(types)) payload.types = types.map((item) => asString(item));
  if (Array.isArray(aliases)) {
    payload.aliases = aliases.map((alias) => {
      if (typeof alias === "string") return { value: alias };
      if (!isObject(alias)) return alias;
      return {
        value: asString(field(alias, "value", "Value")),
        kind: asString(field(alias, "kind", "Kind")),
        locale: asString(field(alias, "locale", "Locale"))
      };
    });
  }
  if (Array.isArray(identifiers)) {
    payload.identifiers = identifiers.map((identifier) => {
      if (!isObject(identifier)) return identifier;
      return {
        namespace: asString(field(identifier, "namespace", "Namespace")),
        value: asString(field(identifier, "value", "Value"))
      };
    });
  }
  if (summary !== undefined) payload.summary = asString(summary);
  return payload;
}

export function toRpcMemoryRecord(record: MemoryRecord | JsonObject): JsonObject {
  const out: JsonObject = { ...(record as JsonObject) };
  validateMemoryRecordPayload(out);
  if (out.payload !== undefined) {
    out.payload = toRpcPayload(out.payload);
  }
  return out;
}

const VALID_VALIDITY_MODES = new Set(["global", "conditional", "timeboxed"]);

function validateMemoryRecordPayload(record: JsonObject): void {
  validateMemoryRecordRelations(record);

  const payload = record.payload;
  if (!isObject(payload)) {
    return;
  }
  const recordType = typeof record.type === "string" ? record.type : undefined;
  const semanticPayload = payloadForKind(payload, recordType, "semantic");
  if (semanticPayload) {
    validateSemanticPayload(semanticPayload);
  }
  const entityPayload = payloadForKind(payload, recordType, "entity");
  if (entityPayload) {
    validateEntityPayload(entityPayload);
  }
}

function validateMemoryRecordRelations(record: JsonObject): void {
  const relations = field(record, "relations", "Relations");
  if (relations === undefined) {
    return;
  }
  if (!Array.isArray(relations)) {
    throw new TypeError("relations must be an array");
  }
  relations.forEach((relation, index) => {
    if (!isObject(relation)) {
      throw new TypeError(`relations[${index}] must be an object`);
    }
    const predicateValue = field(relation, "predicate", "Predicate") ?? field(relation, "kind", "Kind");
    const predicate = predicateValue === undefined ? "" : asString(predicateValue);
    if (normalizeGraphPredicate(predicate) === "") {
      throw new TypeError(`relations[${index}].predicate is required`);
    }
    const targetID = asString(field(relation, "target_id", "TargetID", "targetId"));
    if (targetID.trim() === "") {
      throw new TypeError(`relations[${index}].target_id is required`);
    }
    const weight = field(relation, "weight", "Weight");
    if (weight === undefined) {
      return;
    }
    if (typeof weight !== "number" || !Number.isFinite(weight) || weight < 0 || weight > 1) {
      throw new TypeError(`relations[${index}].weight must be finite and between 0 and 1`);
    }
  });
}

function payloadForKind(payload: JsonObject, recordType: string | undefined, kind: string): JsonObject | undefined {
  const oneof = field(payload, kind);
  if (isObject(oneof)) {
    return oneof;
  }
  const kindValue = payloadKind(payload);
  if (kindValue === kind || (!kindValue && recordType === kind)) {
    return payload;
  }
  return undefined;
}

function validateSemanticPayload(payload: JsonObject): void {
  const subject = field(payload, "subject", "Subject");
  if (typeof subject !== "string" || subject.trim() === "") {
    throw new TypeError("payload.subject is required for semantic records");
  }
  const predicate = field(payload, "predicate", "Predicate");
  if (typeof predicate !== "string" || predicate.trim() === "") {
    throw new TypeError("payload.predicate is required for semantic records");
  }
  const object = field(payload, "object", "Object");
  if (object === undefined || object === null) {
    throw new TypeError("payload.object is required for semantic records");
  }
  const validity = field(payload, "validity", "Validity");
  const mode = isObject(validity) ? field(validity, "mode", "Mode") : undefined;
  if (typeof mode !== "string" || !VALID_VALIDITY_MODES.has(mode)) {
    throw new TypeError("payload.validity.mode must be one of: global, conditional, timeboxed");
  }
}

function validateEntityPayload(payload: JsonObject): void {
  const canonicalName = field(payload, "canonical_name", "CanonicalName", "canonicalName");
  if (typeof canonicalName !== "string" || canonicalName.trim() === "") {
    throw new TypeError("payload.canonical_name is required for entity records");
  }
  const identifiers = field(payload, "identifiers", "Identifiers");
  if (identifiers === undefined) {
    return;
  }
  if (!Array.isArray(identifiers)) {
    throw new TypeError("payload.identifiers must be an array");
  }
  identifiers.forEach((identifier, index) => {
    if (!isObject(identifier)) {
      throw new TypeError(`payload.identifiers[${index}] must be an object`);
    }
    const namespace = field(identifier, "namespace", "Namespace");
    if (typeof namespace !== "string" || namespace.trim() === "") {
      throw new TypeError(`payload.identifiers[${index}].namespace is required`);
    }
    const value = field(identifier, "value", "Value");
    if (typeof value !== "string" || value.trim() === "") {
      throw new TypeError(`payload.identifiers[${index}].value is required`);
    }
  });
}

function toRpcPayload(value: unknown): unknown {
  if (!isObject(value)) {
    return value;
  }
  const semantic = payloadOneof(value, "semantic", "Semantic");
  if (semantic) {
    return { semantic: toRpcSemanticPayload(semantic), kind: "semantic" };
  }
  const episodic = payloadOneof(value, "episodic", "Episodic");
  if (episodic) {
    return { episodic: toRpcEpisodicPayload(episodic), kind: "episodic" };
  }
  const working = payloadOneof(value, "working", "Working");
  if (working) {
    return { working: toRpcWorkingPayload(working), kind: "working" };
  }
  const competence = payloadOneof(value, "competence", "Competence");
  if (competence) {
    return { competence: toRpcCompetencePayload(competence), kind: "competence" };
  }
  const planGraph = payloadOneof(value, "plan_graph", "PlanGraph", "planGraph");
  if (planGraph) {
    return { plan_graph: toRpcPlanGraphPayload(planGraph), kind: "plan_graph" };
  }
  const entity = payloadOneof(value, "entity", "Entity");
  if (entity) {
    return { entity: toRpcEntityPayload(entity), kind: "entity" };
  }
  switch (payloadKind(value)) {
    case "episodic":
      return { episodic: toRpcEpisodicPayload(value), kind: "episodic" };
    case "working":
      return { working: toRpcWorkingPayload(value), kind: "working" };
    case "semantic":
      return { semantic: toRpcSemanticPayload(value), kind: "semantic" };
    case "competence":
      return { competence: toRpcCompetencePayload(value), kind: "competence" };
    case "plan_graph":
      return { plan_graph: toRpcPlanGraphPayload(value), kind: "plan_graph" };
    case "entity":
      return { entity: toRpcEntityPayload(value), kind: "entity" };
    default:
      return value;
  }
}

function payloadKind(value: JsonObject): string | undefined {
  const kind = field(value, "kind", "Kind");
  return typeof kind === "string" ? kind : undefined;
}

function payloadOneof(value: JsonObject, ...names: string[]): JsonObject | undefined {
  const oneof = field(value, ...names);
  return isObject(oneof) ? oneof : undefined;
}

function toRpcEpisodicPayload(value: JsonObject): JsonObject {
  const out: JsonObject = { ...value };
  if (Array.isArray(value.tool_graph)) {
    out.tool_graph = value.tool_graph.map((raw) => {
      if (!isObject(raw)) return raw;
      return { ...raw, args: toProtoValueMap(raw.args), result: toProtoValue(raw.result) };
    });
  }
  if (isObject(value.environment)) {
    out.environment = { ...value.environment, context: toProtoValueMap(value.environment.context) };
  }
  return out;
}

function toRpcWorkingPayload(value: JsonObject): JsonObject {
  const out: JsonObject = { ...value };
  if (Array.isArray(value.active_constraints)) {
    out.active_constraints = value.active_constraints.map((raw) => {
      if (!isObject(raw)) return raw;
      return { ...raw, value: toProtoValue(raw.value) };
    });
  }
  return out;
}

function toRpcSemanticPayload(value: JsonObject): JsonObject {
  const out: JsonObject = { ...value };
  const kind = field(value, "kind", "Kind");
  const subject = field(value, "subject", "Subject");
  const predicate = field(value, "predicate", "Predicate");
  const object = field(value, "object", "Object");
  const validity = field(value, "validity", "Validity");
  if (kind !== undefined) out.kind = asString(kind);
  if (subject !== undefined) out.subject = asString(subject);
  if (predicate !== undefined) out.predicate = asString(predicate);
  if (object !== undefined) out.object = toProtoValue(object);
  if (isObject(validity)) {
    out.validity = { ...validity, conditions: toProtoValueMap(field(validity, "conditions", "Conditions")) };
  }
  return out;
}

function toRpcCompetencePayload(value: JsonObject): JsonObject {
  const out: JsonObject = { ...value };
  if (Array.isArray(value.triggers)) {
    out.triggers = value.triggers.map((raw) => {
      if (!isObject(raw)) return raw;
      return { ...raw, conditions: toProtoValueMap(raw.conditions) };
    });
  }
  if (Array.isArray(value.recipe)) {
    out.recipe = value.recipe.map((raw) => {
      if (!isObject(raw)) return raw;
      return { ...raw, args_schema: toProtoValueMap(raw.args_schema) };
    });
  }
  return out;
}

function toRpcPlanGraphPayload(value: JsonObject): JsonObject {
  const out: JsonObject = {
    ...value,
    constraints: toProtoValueMap(value.constraints),
    inputs_schema: toProtoValueMap(value.inputs_schema),
    outputs_schema: toProtoValueMap(value.outputs_schema)
  };
  if (Array.isArray(value.nodes)) {
    out.nodes = value.nodes.map((raw) => {
      if (!isObject(raw)) return raw;
      return { ...raw, params: toProtoValueMap(raw.params), guards: toProtoValueMap(raw.guards) };
    });
  }
  return out;
}

function toRpcEntityPayload(value: JsonObject): JsonObject {
  const out: JsonObject = { ...value };
  const kind = field(value, "kind", "Kind");
  const canonicalName = field(value, "canonical_name", "CanonicalName", "canonicalName");
  const primaryType = field(value, "primary_type", "PrimaryType", "primaryType");
  const types = field(value, "types", "Types");
  const aliases = field(value, "aliases", "Aliases");
  const identifiers = field(value, "identifiers", "Identifiers");
  const summary = field(value, "summary", "Summary");
  if (kind !== undefined) out.kind = asString(kind);
  if (canonicalName !== undefined) out.canonical_name = asString(canonicalName);
  if (primaryType !== undefined) out.primary_type = asString(primaryType);
  if (Array.isArray(types)) out.types = types.map((item) => asString(item));
  if (Array.isArray(aliases)) {
    out.aliases = aliases.map((alias) => {
      if (typeof alias === "string") return { value: alias };
      if (!isObject(alias)) return alias;
      return {
        value: asString(field(alias, "value", "Value")),
        kind: asString(field(alias, "kind", "Kind")),
        locale: asString(field(alias, "locale", "Locale"))
      };
    });
  }
  if (Array.isArray(identifiers)) {
    out.identifiers = identifiers.map((identifier) => {
      if (!isObject(identifier)) return identifier;
      return {
        namespace: asString(field(identifier, "namespace", "Namespace")),
        value: asString(field(identifier, "value", "Value"))
      };
    });
  }
  if (summary !== undefined) out.summary = asString(summary);
  return out;
}
