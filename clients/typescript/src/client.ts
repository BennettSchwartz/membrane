import { createGrpcTransport, type RpcTransport } from "./internal/grpc";
import { encodeJsonBytes, parseMetricsEnvelope, parseRecordEnvelope, parseRecordsEnvelope } from "./internal/json";
import { nowRfc3339 } from "./internal/util";
import {
  createDefaultTrustContext,
  type JsonObject,
  type MemoryRecord,
  type MemoryType,
  type OutcomeStatus,
  type Sensitivity,
  type TrustContext
} from "./types";

export interface MembraneClientOptions {
  tls?: boolean;
  tlsCaCertPath?: string;
  apiKey?: string;
  timeoutMs?: number;
  transport?: RpcTransport;
}

export interface IngestEventOptions {
  summary?: string;
  sensitivity?: Sensitivity | string;
  source?: string;
  tags?: string[];
  scope?: string;
  timestamp?: string;
}

export interface IngestToolOutputOptions {
  args?: Record<string, unknown>;
  result?: unknown;
  sensitivity?: Sensitivity | string;
  source?: string;
  dependsOn?: string[];
  tags?: string[];
  scope?: string;
  timestamp?: string;
}

export interface IngestObservationOptions {
  sensitivity?: Sensitivity | string;
  source?: string;
  tags?: string[];
  scope?: string;
  timestamp?: string;
}

export interface IngestOutcomeOptions {
  source?: string;
  timestamp?: string;
}

export interface IngestWorkingStateOptions {
  nextActions?: string[];
  openQuestions?: string[];
  contextSummary?: string;
  activeConstraints?: Record<string, unknown>[];
  sensitivity?: Sensitivity | string;
  source?: string;
  tags?: string[];
  scope?: string;
  timestamp?: string;
}

export interface RetrieveOptions {
  trust?: TrustContext;
  memoryTypes?: Array<MemoryType | string>;
  minSalience?: number;
  limit?: number;
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

  async ingestEvent(eventKind: string, ref: string, options: IngestEventOptions = {}): Promise<MemoryRecord> {
    const request = {
      source: options.source ?? DEFAULT_SOURCE,
      event_kind: eventKind,
      ref,
      summary: options.summary ?? "",
      timestamp: options.timestamp ?? nowRfc3339(),
      tags: options.tags ?? [],
      scope: options.scope ?? "",
      sensitivity: options.sensitivity ?? "low"
    };

    const response = await this.transport.unary<Record<string, unknown>>("IngestEvent", request);
    return parseRecordEnvelope(response);
  }

  async ingest_event(eventKind: string, ref: string, options: IngestEventOptions = {}): Promise<MemoryRecord> {
    return await this.ingestEvent(eventKind, ref, options);
  }

  async ingestToolOutput(toolName: string, options: IngestToolOutputOptions = {}): Promise<MemoryRecord> {
    const request: Record<string, unknown> = {
      source: options.source ?? DEFAULT_SOURCE,
      tool_name: toolName,
      timestamp: options.timestamp ?? nowRfc3339(),
      tags: options.tags ?? [],
      scope: options.scope ?? "",
      depends_on: options.dependsOn ?? [],
      sensitivity: options.sensitivity ?? "low"
    };

    if (options.args !== undefined) {
      request.args = encodeJsonBytes(options.args);
    }
    if (options.result !== undefined) {
      request.result = encodeJsonBytes(options.result);
    }

    const response = await this.transport.unary<Record<string, unknown>>("IngestToolOutput", request);
    return parseRecordEnvelope(response);
  }

  async ingest_tool_output(toolName: string, options: IngestToolOutputOptions = {}): Promise<MemoryRecord> {
    return await this.ingestToolOutput(toolName, options);
  }

  async ingestObservation(
    subject: string,
    predicate: string,
    obj: unknown,
    options: IngestObservationOptions = {}
  ): Promise<MemoryRecord> {
    const request = {
      source: options.source ?? DEFAULT_SOURCE,
      subject,
      predicate,
      object: encodeJsonBytes(obj),
      timestamp: options.timestamp ?? nowRfc3339(),
      tags: options.tags ?? [],
      scope: options.scope ?? "",
      sensitivity: options.sensitivity ?? "low"
    };

    const response = await this.transport.unary<Record<string, unknown>>("IngestObservation", request);
    return parseRecordEnvelope(response);
  }

  async ingest_observation(
    subject: string,
    predicate: string,
    obj: unknown,
    options: IngestObservationOptions = {}
  ): Promise<MemoryRecord> {
    return await this.ingestObservation(subject, predicate, obj, options);
  }

  async ingestOutcome(
    targetRecordId: string,
    outcomeStatus: OutcomeStatus | string,
    options: IngestOutcomeOptions = {}
  ): Promise<MemoryRecord> {
    const request = {
      source: options.source ?? DEFAULT_SOURCE,
      target_record_id: targetRecordId,
      outcome_status: outcomeStatus,
      timestamp: options.timestamp ?? nowRfc3339()
    };

    const response = await this.transport.unary<Record<string, unknown>>("IngestOutcome", request);
    return parseRecordEnvelope(response);
  }

  async ingest_outcome(
    targetRecordId: string,
    outcomeStatus: OutcomeStatus | string,
    options: IngestOutcomeOptions = {}
  ): Promise<MemoryRecord> {
    return await this.ingestOutcome(targetRecordId, outcomeStatus, options);
  }

  async ingestWorkingState(threadId: string, state: string, options: IngestWorkingStateOptions = {}): Promise<MemoryRecord> {
    const request: Record<string, unknown> = {
      source: options.source ?? DEFAULT_SOURCE,
      thread_id: threadId,
      state,
      next_actions: options.nextActions ?? [],
      open_questions: options.openQuestions ?? [],
      context_summary: options.contextSummary ?? "",
      timestamp: options.timestamp ?? nowRfc3339(),
      tags: options.tags ?? [],
      scope: options.scope ?? "",
      sensitivity: options.sensitivity ?? "low"
    };

    if (options.activeConstraints !== undefined) {
      request.active_constraints = encodeJsonBytes(options.activeConstraints);
    }

    const response = await this.transport.unary<Record<string, unknown>>("IngestWorkingState", request);
    return parseRecordEnvelope(response);
  }

  async ingest_working_state(threadId: string, state: string, options: IngestWorkingStateOptions = {}): Promise<MemoryRecord> {
    return await this.ingestWorkingState(threadId, state, options);
  }

  async retrieve(taskDescriptor: string, options: RetrieveOptions = {}): Promise<MemoryRecord[]> {
    const trust = options.trust ?? createDefaultTrustContext();

    const request = {
      task_descriptor: taskDescriptor,
      trust,
      memory_types: options.memoryTypes ?? [],
      min_salience: options.minSalience ?? 0,
      limit: options.limit ?? 10
    };

    const response = await this.transport.unary<Record<string, unknown>>("Retrieve", request);
    return parseRecordsEnvelope(response);
  }

  async retrieveById(recordId: string, options: { trust?: TrustContext } = {}): Promise<MemoryRecord> {
    const request = {
      id: recordId,
      trust: options.trust ?? createDefaultTrustContext()
    };

    const response = await this.transport.unary<Record<string, unknown>>("RetrieveByID", request);
    return parseRecordEnvelope(response);
  }

  async retrieve_by_id(recordId: string, options: { trust?: TrustContext } = {}): Promise<MemoryRecord> {
    return await this.retrieveById(recordId, options);
  }

  async supersede(oldId: string, newRecord: MemoryRecord | JsonObject, actor: string, rationale: string): Promise<MemoryRecord> {
    const request = {
      old_id: oldId,
      new_record: encodeJsonBytes(newRecord),
      actor,
      rationale
    };

    const response = await this.transport.unary<Record<string, unknown>>("Supersede", request);
    return parseRecordEnvelope(response);
  }

  async fork(sourceId: string, forkedRecord: MemoryRecord | JsonObject, actor: string, rationale: string): Promise<MemoryRecord> {
    const request = {
      source_id: sourceId,
      forked_record: encodeJsonBytes(forkedRecord),
      actor,
      rationale
    };

    const response = await this.transport.unary<Record<string, unknown>>("Fork", request);
    return parseRecordEnvelope(response);
  }

  async retract(recordId: string, actor: string, rationale: string): Promise<void> {
    const request = {
      id: recordId,
      actor,
      rationale
    };

    await this.transport.unary<Record<string, unknown>>("Retract", request);
  }

  async merge(
    recordIds: string[],
    mergedRecord: MemoryRecord | JsonObject,
    actor: string,
    rationale: string
  ): Promise<MemoryRecord> {
    const request = {
      ids: recordIds,
      merged_record: encodeJsonBytes(mergedRecord),
      actor,
      rationale
    };

    const response = await this.transport.unary<Record<string, unknown>>("Merge", request);
    return parseRecordEnvelope(response);
  }

  async contest(recordId: string, contestingRef: string, actor: string, rationale: string): Promise<void> {
    const request = {
      id: recordId,
      contesting_ref: contestingRef,
      actor,
      rationale
    };

    await this.transport.unary<Record<string, unknown>>("Contest", request);
  }

  async reinforce(recordId: string, actor: string, rationale: string): Promise<void> {
    const request = {
      id: recordId,
      actor,
      rationale
    };

    await this.transport.unary<Record<string, unknown>>("Reinforce", request);
  }

  async penalize(recordId: string, amount: number, actor: string, rationale: string): Promise<void> {
    const request = {
      id: recordId,
      amount,
      actor,
      rationale
    };

    await this.transport.unary<Record<string, unknown>>("Penalize", request);
  }

  async getMetrics(): Promise<JsonObject> {
    const response = await this.transport.unary<Record<string, unknown>>("GetMetrics", {});
    return parseMetricsEnvelope(response);
  }

  async get_metrics(): Promise<JsonObject> {
    return await this.getMetrics();
  }

  close(): void {
    this.transport.close();
  }
}
