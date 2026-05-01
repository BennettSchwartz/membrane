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
  SourceKind,
  createDefaultTrustContext,
  type CaptureMemoryResult,
  type JsonObject,
  type MemoryRecord,
  type MemoryType,
  type RetrieveGraphResult,
  type Sensitivity,
  type SourceKind as SourceKindValue,
  type TrustContext
} from "./types";

export interface MembraneClientOptions {
  tls?: boolean;
  tlsCaCertPath?: string;
  apiKey?: string;
  timeoutMs?: number;
  transport?: RpcTransport;
}

export interface CaptureMemoryOptions {
  source?: string;
  sourceKind?: SourceKindValue | string;
  context?: unknown;
  reasonToRemember?: string;
  proposedType?: MemoryType | string;
  summary?: string;
  tags?: string[];
  scope?: string;
  sensitivity?: Sensitivity | string;
  timestamp?: string;
}

export interface RetrieveGraphOptions {
  trust?: TrustContext;
  memoryTypes?: Array<MemoryType | string>;
  minSalience?: number;
  rootLimit?: number;
  nodeLimit?: number;
  edgeLimit?: number;
  maxHops?: number;
}

const DEFAULT_ADDR = "localhost:9090";
const DEFAULT_SOURCE = "typescript-client";

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
        timeoutMs: options.timeoutMs
      });
  }

  async captureMemory(content: unknown, options: CaptureMemoryOptions = {}): Promise<CaptureMemoryResult> {
    const request: CaptureMemoryRpcRequest = {
      source: options.source ?? DEFAULT_SOURCE,
      source_kind: options.sourceKind ?? SourceKind.AGENT_TURN,
      content: toProtoValue(content),
      reason_to_remember: options.reasonToRemember ?? "",
      proposed_type: options.proposedType ?? "",
      summary: options.summary ?? "",
      tags: options.tags ?? [],
      scope: options.scope ?? "",
      sensitivity: options.sensitivity ?? "low",
      timestamp: options.timestamp ?? nowRfc3339()
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
    const trust = options.trust ?? createDefaultTrustContext();

    const request: RetrieveGraphRpcRequest = {
      task_descriptor: taskDescriptor,
      trust,
      memory_types: options.memoryTypes ?? [],
      min_salience: options.minSalience ?? 0,
      root_limit: options.rootLimit ?? 10,
      node_limit: options.nodeLimit ?? 25,
      edge_limit: options.edgeLimit ?? 100,
      max_hops: options.maxHops ?? 1
    };

    const response = await this.transport.unary("RetrieveGraph", request);
    return parseRetrieveGraphEnvelope(response);
  }

  async retrieve_graph(taskDescriptor: string, options: RetrieveGraphOptions = {}): Promise<RetrieveGraphResult> {
    return await this.retrieveGraph(taskDescriptor, options);
  }

  async retrieveById(recordId: string, options: { trust?: TrustContext } = {}): Promise<MemoryRecord> {
    const request: RetrieveByIdRpcRequest = {
      id: recordId,
      trust: options.trust ?? createDefaultTrustContext()
    };

    const response = await this.transport.unary("RetrieveByID", request);
    return parseRecordEnvelope(response);
  }

  async retrieve_by_id(recordId: string, options: { trust?: TrustContext } = {}): Promise<MemoryRecord> {
    return await this.retrieveById(recordId, options);
  }

  async supersede(oldId: string, newRecord: MemoryRecord | JsonObject, actor: string, rationale: string): Promise<MemoryRecord> {
    const request = {
      old_id: oldId,
      new_record: toRpcMemoryRecord(newRecord),
      actor,
      rationale
    };

    const response = await this.transport.unary("Supersede", request);
    return parseRecordEnvelope(response);
  }

  async fork(sourceId: string, forkedRecord: MemoryRecord | JsonObject, actor: string, rationale: string): Promise<MemoryRecord> {
    const request = {
      source_id: sourceId,
      forked_record: toRpcMemoryRecord(forkedRecord),
      actor,
      rationale
    };

    const response = await this.transport.unary("Fork", request);
    return parseRecordEnvelope(response);
  }

  async retract(recordId: string, actor: string, rationale: string): Promise<void> {
    const request = {
      id: recordId,
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
    const request = {
      ids: recordIds,
      merged_record: toRpcMemoryRecord(mergedRecord),
      actor,
      rationale
    };

    const response = await this.transport.unary("Merge", request);
    return parseRecordEnvelope(response);
  }

  async contest(recordId: string, contestingRef: string, actor: string, rationale: string): Promise<void> {
    const request = {
      id: recordId,
      contesting_ref: contestingRef,
      actor,
      rationale
    };

    await this.transport.unary("Contest", request);
  }

  async reinforce(recordId: string, actor: string, rationale: string): Promise<void> {
    const request = {
      id: recordId,
      actor,
      rationale
    };

    await this.transport.unary("Reinforce", request);
  }

  async penalize(recordId: string, amount: number, actor: string, rationale: string): Promise<void> {
    const request = {
      id: recordId,
      amount,
      actor,
      rationale
    };

    await this.transport.unary("Penalize", request);
  }

  async getMetrics(): Promise<JsonObject> {
    const response = await this.transport.unary("GetMetrics", {});
    return parseMetricsEnvelope(response);
  }

  async get_metrics(): Promise<JsonObject> {
    return await this.getMetrics();
  }

  close(): void {
    this.transport.close();
  }
}
