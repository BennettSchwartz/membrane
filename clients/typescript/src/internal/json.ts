import {
  type CaptureMemoryResult,
  type GraphEdge,
  type GraphNode,
  type JsonObject,
  type MemoryRecord,
  type Relation,
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
  const relation: Relation = { target_id: asString(decoded.target_id) };
  if (decoded.predicate !== undefined) relation.predicate = asString(decoded.predicate);
  if (decoded.kind !== undefined) relation.kind = asString(decoded.kind);
  if (decoded.weight !== undefined) relation.weight = asNumber(decoded.weight);
  if (decoded.created_at !== undefined) relation.created_at = asString(decoded.created_at);
  return relation;
}

export function parseRecord(value: unknown): MemoryRecord {
  const decoded = decodeJsonValue(value);
  if (!isObject(decoded)) {
    throw new TypeError("Expected MemoryRecord object response");
  }

  const record: MemoryRecord = {
    id: asString(decoded.id),
    type: asString(decoded.type),
    sensitivity: asString(decoded.sensitivity),
    confidence: asNumber(decoded.confidence),
    salience: asNumber(decoded.salience)
  };

  if (decoded.scope !== undefined) record.scope = asString(decoded.scope);
  if (decoded.tags !== undefined) record.tags = asStringArray(decoded.tags) ?? [];
  if (decoded.created_at !== undefined) record.created_at = asString(decoded.created_at);
  if (decoded.updated_at !== undefined) record.updated_at = asString(decoded.updated_at);
  if (decoded.lifecycle !== undefined) record.lifecycle = decoded.lifecycle as NonNullable<MemoryRecord["lifecycle"]>;
  if (decoded.provenance !== undefined) record.provenance = decoded.provenance as NonNullable<MemoryRecord["provenance"]>;
  if (decoded.relations !== undefined) record.relations = parseObjectArray(decoded.relations, parseRelation) ?? [];
  if (decoded.interpretation !== undefined) record.interpretation = decoded.interpretation as NonNullable<MemoryRecord["interpretation"]>;
  if (decoded.payload !== undefined) record.payload = parsePayload(decoded.payload);
  if (decoded.audit_log !== undefined) record.audit_log = decoded.audit_log as NonNullable<MemoryRecord["audit_log"]>;

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
    source_id: asString(decoded.source_id),
    predicate: asString(decoded.predicate),
    target_id: asString(decoded.target_id)
  };
  if (decoded.weight !== undefined) edge.weight = asNumber(decoded.weight);
  if (decoded.created_at !== undefined) edge.created_at = asString(decoded.created_at);
  return edge;
}

function parseGraphNode(value: unknown): GraphNode {
  const decoded = decodeJsonValue(value);
  if (!isObject(decoded)) {
    throw new TypeError("Expected graph node object");
  }
  return {
    record: parseRecord(decoded.record),
    root: decoded.root === true,
    hop: typeof decoded.hop === "number" ? decoded.hop : Number(decoded.hop ?? 0)
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

  const rawSelected = "selected" in decoded ? decoded.selected : decoded.Selected;
  const confidence = "confidence" in decoded ? decoded.confidence : decoded.Confidence;
  const needsMore = "needs_more" in decoded ? decoded.needs_more : decoded.NeedsMore;
  const scores = isObject(decoded.scores) ? Object.fromEntries(Object.entries(decoded.scores).map(([key, score]) => [key, asNumber(score)])) : undefined;

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

  let selection: SelectionResult | undefined;
  if ("selection" in decoded && !isEmptyPayload(decoded.selection)) {
    selection = parseSelection(decoded.selection);
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

  const createdRecords = Array.isArray(decoded.created_records)
    ? decoded.created_records.map((raw) => parseRecord(raw))
    : [];
  const edgesValue = decodeJsonValue(decoded.edges);
  const edges = Array.isArray(edgesValue) ? edgesValue.map((raw) => parseGraphEdge(raw)) : [];

  return {
    primary_record: parseRecord(decoded.primary_record),
    created_records: createdRecords,
    edges
  };
}

export function parseRetrieveGraphEnvelope(response: unknown): RetrieveGraphResult {
  const decoded = decodeJsonValue(response);
  if (!isObject(decoded)) {
    throw new TypeError("Expected object response for graph envelope");
  }

  const nodesValue = decodeJsonValue(decoded.nodes);
  const edgesValue = decodeJsonValue(decoded.edges);
  const nodes = Array.isArray(nodesValue) ? nodesValue.map((raw) => parseGraphNode(raw)) : [];
  const edges = Array.isArray(edgesValue) ? edgesValue.map((raw) => parseGraphEdge(raw)) : [];

  let selection: SelectionResult | undefined;
  if ("selection" in decoded && !isEmptyPayload(decoded.selection)) {
    selection = parseSelection(decoded.selection);
  }

  const result: RetrieveGraphResult = {
    nodes,
    edges,
    root_ids: Array.isArray(decoded.root_ids) ? decoded.root_ids.map((value) => String(value)) : []
  };

  if (selection !== undefined) {
    result.selection = selection;
  }

  return result;
}

export function parseMetricsEnvelope(response: unknown): JsonObject {
  const decoded = decodeJsonValue(response);
  if (!isObject(decoded)) {
    throw new TypeError("Expected object response for metrics envelope");
  }

  const snapshot = "snapshot" in decoded ? fromProtoValue(decoded.snapshot) : decoded;
  if (!isObject(snapshot)) {
    throw new TypeError("Expected metrics snapshot object");
  }

  return snapshot;
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
  if (isObject(value.validity)) {
    payload.validity = { ...value.validity, conditions: fromProtoValueMap(value.validity.conditions) };
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
  const payload: JsonObject = {
    ...value,
    constraints: fromProtoValueMap(value.constraints),
    inputs_schema: fromProtoValueMap(value.inputs_schema),
    outputs_schema: fromProtoValueMap(value.outputs_schema)
  };
  if (Array.isArray(value.nodes)) {
    payload.nodes = value.nodes.map((raw) => {
      if (!isObject(raw)) return raw;
      return { ...raw, params: fromProtoValueMap(raw.params), guards: fromProtoValueMap(raw.guards) };
    });
  }
  return payload;
}

function parseEntityPayload(value: unknown): JsonObject | undefined {
  if (!isObject(value)) return undefined;
  const payload: JsonObject = { ...value };
  if (Array.isArray(value.aliases)) {
    payload.aliases = value.aliases.map((alias) => (typeof alias === "string" ? { value: alias } : alias));
  }
  return payload;
}

export function toRpcMemoryRecord(record: MemoryRecord | JsonObject): JsonObject {
  const out: JsonObject = { ...(record as JsonObject) };
  if (out.payload !== undefined) {
    out.payload = toRpcPayload(out.payload);
  }
  return out;
}

function toRpcPayload(value: unknown): unknown {
  if (!isObject(value)) {
    return value;
  }
  switch (value.kind) {
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
      return { entity: value, kind: "entity" };
    default:
      return value;
  }
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
  const out: JsonObject = { ...value, object: toProtoValue(value.object) };
  if (isObject(value.validity)) {
    out.validity = { ...value.validity, conditions: toProtoValueMap(value.validity.conditions) };
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
