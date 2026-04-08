import {
  type CaptureMemoryResult,
  type GraphEdge,
  type GraphNode,
  type JsonObject,
  type MemoryRecord,
  type RetrieveGraphResult,
  type RetrieveResult,
  type SelectionResult
} from "../types";

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

export function parseRecord(value: unknown): MemoryRecord {
  const decoded = decodeJsonValue(value);
  if (!isObject(decoded)) {
    throw new TypeError("Expected MemoryRecord object response");
  }
  return decoded as unknown as MemoryRecord;
}

export function parseRecordEnvelope(response: unknown): MemoryRecord {
  const decoded = decodeJsonValue(response);
  if (!isObject(decoded)) {
    throw new TypeError("Expected object response for record envelope");
  }

  if ("record" in decoded) {
    return parseRecord(decoded.record);
  }

  return decoded as unknown as MemoryRecord;
}

function parseGraphEdge(value: unknown): GraphEdge {
  const decoded = decodeJsonValue(value);
  if (!isObject(decoded)) {
    throw new TypeError("Expected graph edge object");
  }
  return decoded as unknown as GraphEdge;
}

function parseGraphNode(value: unknown): GraphNode {
  const decoded = decodeJsonValue(value);
  if (!isObject(decoded)) {
    throw new TypeError("Expected graph node object");
  }
  return {
    record: parseRecord(decoded.record),
    root: decoded.root === true,
    hop: typeof decoded.hop === "number" ? decoded.hop : 0
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

  return {
    selected: Array.isArray(rawSelected) ? rawSelected.map((raw) => parseRecord(raw)) : [],
    confidence: typeof confidence === "number" ? confidence : 0,
    needs_more: needsMore === true
  };
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
  const edges = Array.isArray(decodeJsonValue(decoded.edges))
    ? (decodeJsonValue(decoded.edges) as unknown[]).map((raw) => parseGraphEdge(raw))
    : [];

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

  if (!("snapshot" in decoded)) {
    return decoded;
  }

  const snapshot = decodeJsonValue(decoded.snapshot);
  if (!isObject(snapshot)) {
    throw new TypeError("Expected metrics snapshot object");
  }

  return snapshot;
}
