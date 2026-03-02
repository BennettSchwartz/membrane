import { type JsonObject, type MemoryRecord, type RetrieveResult, type SelectionResult } from "../types";

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
