import {
  type CaptureMemoryRpcRequest,
  createGrpcTransport,
  type RetrieveGraphRpcRequest,
  type RetrieveByIdRpcRequest,
  type RpcTransport
} from "./internal/grpc";
import {
  parseCaptureMemoryEnvelope,
  parseMetricsEnvelope,
  parseRecordEnvelope,
  parseRetrieveGraphEnvelope,
  toProtoValue,
  toRpcMemoryRecord
} from "./internal/json";
import { nowRfc3339 } from "./internal/util";
import {
  MemoryType,
  Sensitivity,
  SourceKind,
  createDefaultTrustContext,
  type CaptureMemoryResult,
  type JsonObject,
  type MemoryRecord,
  type MetricsSnapshot,
  type RetrieveGraphResult,
  type SourceKind as SourceKindValue,
  type TrustContext
} from "./types";

export interface MembraneClientOptions {
  tls?: boolean;
  tlsCaCertPath?: string;
  apiKey?: string;
  allowInsecureCredentials?: boolean;
  timeoutMs?: number;
  transport?: RpcTransport;
}

export interface CaptureMemoryOptions {
  source?: string;
  sourceKind?: SourceKindValue | string;
  source_kind?: SourceKindValue | string;
  context?: unknown;
  reasonToRemember?: string;
  reason_to_remember?: string;
  proposedType?: MemoryType | string;
  proposed_type?: MemoryType | string;
  summary?: string;
  tags?: string[];
  scope?: string;
  sensitivity?: Sensitivity | string;
  timestamp?: string;
}

export interface RetrieveGraphOptions {
  trust?: TrustContext;
  queryEmbedding?: number[];
  query_embedding?: number[];
  rootOnly?: boolean;
  root_only?: boolean;
  memoryTypes?: Array<MemoryType | string>;
  memory_types?: Array<MemoryType | string>;
  minSalience?: number;
  min_salience?: number;
  rootLimit?: number;
  root_limit?: number;
  nodeLimit?: number;
  node_limit?: number;
  edgeLimit?: number;
  edge_limit?: number;
  maxHops?: number;
  max_hops?: number;
}

const DEFAULT_ADDR = "localhost:9090";
const DEFAULT_SOURCE = "typescript-client";
const MAX_GRAPH_LIMIT = 10_000;
const VALID_MEMORY_TYPES = new Set<string>(Object.values(MemoryType));
const VALID_SENSITIVITIES = new Set<string>(Object.values(Sensitivity));
const VALID_SOURCE_KINDS = new Set<string>(Object.values(SourceKind));
const CANONICAL_MEMORY_TYPE_ORDER = [
  MemoryType.WORKING,
  MemoryType.ENTITY,
  MemoryType.SEMANTIC,
  MemoryType.COMPETENCE,
  MemoryType.PLAN_GRAPH,
  MemoryType.EPISODIC
];

function validatedEnum(name: string, value: string, valid: Set<string>, allowEmpty = false): string {
  if (allowEmpty && value === "") {
    return value;
  }
  if (!valid.has(value)) {
    throw new TypeError(`${name} must be one of: ${[...valid].join(", ")}`);
  }
  return value;
}

function validatedMemoryType(name: string, value: MemoryType | string, allowEmpty = false): string {
  return validatedEnum(name, value, VALID_MEMORY_TYPES, allowEmpty);
}

function validatedSensitivity(name: string, value: Sensitivity | string): string {
  return validatedEnum(name, value, VALID_SENSITIVITIES);
}

function validatedSourceKind(value: SourceKindValue | string): string {
  return validatedEnum("sourceKind", value, VALID_SOURCE_KINDS);
}

function validatedString(name: string, value: unknown): string {
  if (typeof value !== "string") {
    throw new TypeError(`${name} must be a string`);
  }
  return value;
}

function validatedOptionalString(name: string, value: unknown): string {
  if (value === undefined) {
    return "";
  }
  return validatedString(name, value);
}

function validatedRequiredString(name: string, value: unknown): string {
  const text = validatedString(name, value);
  if (text.trim() === "") {
    throw new TypeError(`${name} is required`);
  }
  return text;
}

function validatedBoolean(name: string, value: unknown): boolean {
  if (typeof value !== "boolean") {
    throw new TypeError(`${name} must be a boolean`);
  }
  return value;
}

function validatedStringArray(name: string, value: unknown): string[] {
  if (value === undefined) {
    return [];
  }
  if (!Array.isArray(value)) {
    throw new TypeError(`${name} must be an array of strings`);
  }
  return value.map((item, index) => validatedString(`${name}[${index}]`, item));
}

function validatedNonEmptyStringArray(name: string, value: unknown): string[] {
  const values = validatedStringArray(name, value);
  if (values.length === 0) {
    throw new TypeError(`${name} must include at least one scope`);
  }
  values.forEach((item, index) => {
    if (item.trim() === "") {
      throw new TypeError(`${name}[${index}] must be non-empty`);
    }
  });
  return values;
}

function validatedTrust(trust: TrustContext): TrustContext {
  return {
    ...trust,
    max_sensitivity: validatedSensitivity("trust.max_sensitivity", trust.max_sensitivity),
    authenticated: validatedBoolean("trust.authenticated", trust.authenticated),
    actor_id: validatedString("trust.actor_id", trust.actor_id),
    scopes: validatedNonEmptyStringArray("trust.scopes", trust.scopes)
  };
}

function validatedMemoryTypes(values: Array<MemoryType | string>): string[] {
  const seen = new Set<string>();
  for (const [index, value] of values.entries()) {
    seen.add(validatedMemoryType(`memoryTypes[${index}]`, value));
  }
  return CANONICAL_MEMORY_TYPE_ORDER.filter((value) => seen.has(value));
}

function validatedQueryEmbedding(values: number[] | undefined): number[] {
  if (!values || values.length === 0) {
    return [];
  }
  let nonZero = false;
  for (let i = 0; i < values.length; i++) {
    const value = values[i];
    if (!Number.isFinite(value)) {
      throw new TypeError(`queryEmbedding[${i}] must be finite`);
    }
    if (value !== 0) {
      nonZero = true;
    }
  }
  if (!nonZero) {
    throw new TypeError("queryEmbedding must contain at least one non-zero value");
  }
  return values;
}

function validatedMaxHops(value: number): number {
  if (!Number.isInteger(value)) {
    throw new TypeError("maxHops must be an integer");
  }
  if (value < -1) {
    throw new TypeError("maxHops must be -1 or non-negative");
  }
  return value;
}

function validatedGraphLimit(name: string, value: number): number {
  if (!Number.isInteger(value)) {
    throw new TypeError(`${name} must be an integer`);
  }
  if (value < 0 || value > MAX_GRAPH_LIMIT) {
    throw new TypeError(`${name} must be between 0 and ${MAX_GRAPH_LIMIT}`);
  }
  return value;
}

function validatedMinSalience(value: number): number {
  if (!Number.isFinite(value) || value < 0 || value > 1) {
    throw new TypeError("minSalience must be finite and between 0 and 1");
  }
  return value;
}

function validatedPenaltyAmount(value: number): number {
	if (!Number.isFinite(value) || value < 0) {
		throw new TypeError("amount must be non-negative and finite");
	}
	return value;
}

export class MembraneClient {
  private readonly transport: RpcTransport;

  constructor(addr: string = DEFAULT_ADDR, options: MembraneClientOptions = {}) {
    this.transport =
      options.transport ??
      createGrpcTransport({
        addr,
        tls: options.tls ?? false,
        tlsCaCertPath: options.tlsCaCertPath,
        apiKey: options.apiKey,
        allowInsecureCredentials: options.allowInsecureCredentials,
        timeoutMs: options.timeoutMs
      });
  }

  async captureMemory(content: unknown, options: CaptureMemoryOptions = {}): Promise<CaptureMemoryResult> {
    const sourceKind = validatedSourceKind(options.sourceKind ?? options.source_kind ?? SourceKind.EVENT);
    const proposedType = validatedMemoryType("proposedType", options.proposedType ?? options.proposed_type ?? "", true);
    const sensitivity = validatedSensitivity("sensitivity", options.sensitivity ?? "low");
    const source = validatedRequiredString("source", options.source ?? DEFAULT_SOURCE);
    const reasonToRemember = validatedOptionalString("reasonToRemember", options.reasonToRemember ?? options.reason_to_remember);
    const summary = validatedOptionalString("summary", options.summary);
    const timestamp = options.timestamp === undefined ? nowRfc3339() : validatedString("timestamp", options.timestamp);
    const tags = validatedStringArray("tags", options.tags);
    const scope = validatedString("scope", options.scope ?? "");

    const request: CaptureMemoryRpcRequest = {
      source,
      source_kind: sourceKind,
      content: toProtoValue(content),
      reason_to_remember: reasonToRemember,
      proposed_type: proposedType,
      summary,
      tags,
      scope,
      sensitivity,
      timestamp
    };

    if (options.context !== undefined) {
      request.context = toProtoValue(options.context);
    }

    const response = await this.transport.unary("CaptureMemory", request);
    return parseCaptureMemoryEnvelope(response);
  }

  async capture_memory(content: unknown, options: CaptureMemoryOptions = {}): Promise<CaptureMemoryResult> {
    return await this.captureMemory(content, options);
  }

	async retrieveGraph(taskDescriptor: string, options: RetrieveGraphOptions = {}): Promise<RetrieveGraphResult> {
		const descriptor = validatedString("taskDescriptor", taskDescriptor);
		const trust = validatedTrust(options.trust ?? createDefaultTrustContext());
		const rootOnly = validatedBoolean("rootOnly", options.rootOnly ?? options.root_only ?? false);
		const queryEmbedding = validatedQueryEmbedding(options.queryEmbedding ?? options.query_embedding);
		const maxHops = rootOnly ? -1 : validatedMaxHops(options.maxHops ?? options.max_hops ?? 1);
		const minSalience = validatedMinSalience(options.minSalience ?? options.min_salience ?? 0);
		const rootLimit = validatedGraphLimit("rootLimit", options.rootLimit ?? options.root_limit ?? 10);
		const nodeLimit = validatedGraphLimit("nodeLimit", options.nodeLimit ?? options.node_limit ?? 25);
		const edgeLimit = validatedGraphLimit("edgeLimit", options.edgeLimit ?? options.edge_limit ?? 100);
    const memoryTypes = validatedMemoryTypes(options.memoryTypes ?? options.memory_types ?? []);

		const request: RetrieveGraphRpcRequest = {
			task_descriptor: descriptor,
			trust,
      memory_types: memoryTypes,
      min_salience: minSalience,
			root_limit: rootLimit,
			node_limit: nodeLimit,
			edge_limit: edgeLimit,
			max_hops: maxHops,
			query_embedding: queryEmbedding
		};

    const response = await this.transport.unary("RetrieveGraph", request);
    return parseRetrieveGraphEnvelope(response);
  }

  async retrieve_graph(taskDescriptor: string, options: RetrieveGraphOptions = {}): Promise<RetrieveGraphResult> {
    return await this.retrieveGraph(taskDescriptor, options);
  }

  async retrieveById(recordId: string, options: { trust?: TrustContext } = {}): Promise<MemoryRecord> {
    const request: RetrieveByIdRpcRequest = {
      id: validatedRequiredString("recordId", recordId),
      trust: validatedTrust(options.trust ?? createDefaultTrustContext())
    };

    const response = await this.transport.unary("RetrieveByID", request);
    return parseRecordEnvelope(response);
  }

  async retrieve_by_id(recordId: string, options: { trust?: TrustContext } = {}): Promise<MemoryRecord> {
    return await this.retrieveById(recordId, options);
  }

  async supersede(oldId: string, newRecord: MemoryRecord | JsonObject, actor: string, rationale: string): Promise<MemoryRecord> {
    const request = {
      old_id: validatedRequiredString("oldId", oldId),
      new_record: toRpcMemoryRecord(newRecord),
      actor,
      rationale
    };

    const response = await this.transport.unary("Supersede", request);
    return parseRecordEnvelope(response);
  }

  async fork(sourceId: string, forkedRecord: MemoryRecord | JsonObject, actor: string, rationale: string): Promise<MemoryRecord> {
    const request = {
      source_id: validatedRequiredString("sourceId", sourceId),
      forked_record: toRpcMemoryRecord(forkedRecord),
      actor,
      rationale
    };

    const response = await this.transport.unary("Fork", request);
    return parseRecordEnvelope(response);
  }

  async retract(recordId: string, actor: string, rationale: string): Promise<void> {
    const request = {
      id: validatedRequiredString("recordId", recordId),
      actor,
      rationale
    };

    await this.transport.unary("Retract", request);
  }

  async merge(
    recordIds: string[],
    mergedRecord: MemoryRecord | JsonObject,
    actor: string,
    rationale: string
  ): Promise<MemoryRecord> {
    if (!Array.isArray(recordIds) || recordIds.length === 0) {
      throw new TypeError("recordIds must contain at least one record ID");
    }
    const ids = recordIds.map((recordId, index) => validatedRequiredString(`recordIds[${index}]`, recordId));
    const seen = new Set<string>();
    for (const [index, id] of ids.entries()) {
      if (seen.has(id)) {
        throw new TypeError(`recordIds[${index}] duplicates an earlier merge source ID`);
      }
      seen.add(id);
    }
    const request = {
      ids,
      merged_record: toRpcMemoryRecord(mergedRecord),
      actor,
      rationale
    };

    const response = await this.transport.unary("Merge", request);
    return parseRecordEnvelope(response);
  }

  async contest(recordId: string, contestingRef: string, actor: string, rationale: string): Promise<void> {
    const request = {
      id: validatedRequiredString("recordId", recordId),
      contesting_ref: contestingRef,
      actor,
      rationale
    };

    await this.transport.unary("Contest", request);
  }

  async reinforce(recordId: string, actor: string, rationale: string): Promise<void> {
    const request = {
      id: validatedRequiredString("recordId", recordId),
      actor,
      rationale
    };

    await this.transport.unary("Reinforce", request);
  }

  async penalize(recordId: string, amount: number, actor: string, rationale: string): Promise<void> {
    const request = {
      id: validatedRequiredString("recordId", recordId),
      amount: validatedPenaltyAmount(amount),
      actor,
      rationale
    };

    await this.transport.unary("Penalize", request);
  }

  async getMetrics(): Promise<MetricsSnapshot> {
    const response = await this.transport.unary("GetMetrics", {});
    return parseMetricsEnvelope(response);
  }

  async get_metrics(): Promise<MetricsSnapshot> {
    return await this.getMetrics();
  }

  close(): void {
    this.transport.close();
  }
}
