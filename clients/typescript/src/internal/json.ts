import { type JsonObject, type MemoryRecord } from "../types";

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
  const decoded = decodeJsonValue(response);
  if (!isObject(decoded)) {
    throw new TypeError("Expected object response for records envelope");
  }

  const rawRecords = decoded.records;
  if (!Array.isArray(rawRecords)) {
    return [];
  }

  return rawRecords.map((raw) => parseRecord(raw));
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
