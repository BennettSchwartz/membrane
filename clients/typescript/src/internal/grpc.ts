import fs from "node:fs";
import path from "node:path";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";

export interface RecordEnvelope {
  record: unknown;
}

export interface RetrieveEnvelope {
  records: unknown[];
  selection?: unknown;
}

export interface CaptureMemoryEnvelope {
  primary_record: unknown;
  created_records: unknown[];
  edges: unknown;
}

export interface RetrieveGraphEnvelope {
  nodes: unknown;
  edges: unknown;
  root_ids: string[];
  selection?: unknown;
}

export interface MetricsEnvelope {
  snapshot: unknown;
}

export type EmptyEnvelope = Record<string, never>;
export type JsonBytes = Uint8Array;

export interface TrustContextRpcRequest {
  max_sensitivity: string;
  authenticated: boolean;
  actor_id: string;
  scopes: string[];
}

export interface CaptureMemoryRpcRequest {
  source: string;
  source_kind: string;
  content: JsonBytes;
  context?: JsonBytes;
  reason_to_remember: string;
  proposed_type: string;
  summary: string;
  tags: string[];
  scope: string;
  sensitivity: string;
  timestamp: string;
}

export interface RetrieveGraphRpcRequest {
  task_descriptor: string;
  trust: TrustContextRpcRequest;
  memory_types: string[];
  min_salience: number;
  root_limit: number;
  node_limit: number;
  edge_limit: number;
  max_hops: number;
}

export interface RetrieveByIdRpcRequest {
  id: string;
  trust: TrustContextRpcRequest;
}

export interface SupersedeRpcRequest {
  old_id: string;
  new_record: JsonBytes;
  actor: string;
  rationale: string;
}

export interface ForkRpcRequest {
  source_id: string;
  forked_record: JsonBytes;
  actor: string;
  rationale: string;
}

export interface RetractRpcRequest {
  id: string;
  actor: string;
  rationale: string;
}

export interface MergeRpcRequest {
  ids: string[];
  merged_record: JsonBytes;
  actor: string;
  rationale: string;
}

export interface ContestRpcRequest {
  id: string;
  contesting_ref: string;
  actor: string;
  rationale: string;
}

export interface ReinforceRpcRequest {
  id: string;
  actor: string;
  rationale: string;
}

export interface PenalizeRpcRequest {
  id: string;
  amount: number;
  actor: string;
  rationale: string;
}

export type GetMetricsRpcRequest = Record<string, never>;

export interface MembraneRpcSchema {
  CaptureMemory: { request: CaptureMemoryRpcRequest; response: CaptureMemoryEnvelope };
  RetrieveGraph: { request: RetrieveGraphRpcRequest; response: RetrieveGraphEnvelope };
  RetrieveByID: { request: RetrieveByIdRpcRequest; response: RecordEnvelope };
  Supersede: { request: SupersedeRpcRequest; response: RecordEnvelope };
  Fork: { request: ForkRpcRequest; response: RecordEnvelope };
  Retract: { request: RetractRpcRequest; response: EmptyEnvelope };
  Merge: { request: MergeRpcRequest; response: RecordEnvelope };
  Contest: { request: ContestRpcRequest; response: EmptyEnvelope };
  Reinforce: { request: ReinforceRpcRequest; response: EmptyEnvelope };
  Penalize: { request: PenalizeRpcRequest; response: EmptyEnvelope };
  GetMetrics: { request: GetMetricsRpcRequest; response: MetricsEnvelope };
}

export type RpcMethodName = keyof MembraneRpcSchema;
export type RpcRequest<M extends RpcMethodName> = MembraneRpcSchema[M]["request"];
export type RpcResponse<M extends RpcMethodName> = MembraneRpcSchema[M]["response"];

export interface RpcTransport {
  unary<M extends RpcMethodName>(methodName: M, request: RpcRequest<M>): Promise<RpcResponse<M>>;
  close(): void;
}

export interface GrpcTransportOptions {
  addr: string;
  tls: boolean;
  tlsCaCertPath?: string | undefined;
  apiKey?: string | undefined;
  timeoutMs?: number | undefined;
}

export interface MembraneErrorOptions {
  code?: grpc.status | undefined;
  details?: string | undefined;
  metadata?: Record<string, string[]> | undefined;
  cause?: unknown;
}

export class MembraneError extends Error {
  readonly code: grpc.status | undefined;
  readonly codeName: string | undefined;
  readonly details: string | undefined;
  readonly metadata: Record<string, string[]>;
  override readonly cause: unknown;

  constructor(message: string, options: MembraneErrorOptions = {}) {
    super(message);
    this.name = "MembraneError";
    this.code = options.code;
    this.codeName = grpcStatusName(options.code);
    this.details = options.details;
    this.metadata = options.metadata ?? {};
    this.cause = options.cause;
  }
}

type UnaryCallback<M extends RpcMethodName> = (err: grpc.ServiceError | null, response: RpcResponse<M>) => void;
type GrpcUnaryMethod<M extends RpcMethodName> = (
  req: RpcRequest<M>,
  meta: grpc.Metadata,
  options: grpc.CallOptions,
  cb: UnaryCallback<M>
) => grpc.ClientUnaryCall;

type MembraneServiceClient = grpc.Client & {
  [K in RpcMethodName]: GrpcUnaryMethod<K>;
};

type MethodTable = {
  [K in RpcMethodName]: GrpcUnaryMethod<K>;
};

type MembraneServiceClientConstructor = new (address: string, credentials: grpc.ChannelCredentials) => MembraneServiceClient;

let cachedClientCtor: MembraneServiceClientConstructor | undefined;

function resolveProtoPath(): string {
  const candidates = [
    path.resolve(__dirname, "proto/membrane/v1/membrane.proto"),
    path.resolve(__dirname, "../proto/membrane/v1/membrane.proto"),
    path.resolve(__dirname, "../../proto/membrane/v1/membrane.proto"),
    path.resolve(process.cwd(), "proto/membrane/v1/membrane.proto"),
    path.resolve(process.cwd(), "clients/typescript/proto/membrane/v1/membrane.proto")
  ];

  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) {
      return candidate;
    }
  }

  throw new Error(`Unable to locate membrane.proto. Tried: ${candidates.join(", ")}`);
}

function loadClientConstructor(): MembraneServiceClientConstructor {
  if (cachedClientCtor) {
    return cachedClientCtor;
  }

  const packageDefinition = protoLoader.loadSync(resolveProtoPath(), {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true
  });

  const loaded = grpc.loadPackageDefinition(packageDefinition) as {
    membrane?: {
      v1?: {
        MembraneService?: MembraneServiceClientConstructor;
      };
    };
  };

  const ctor = loaded.membrane?.v1?.MembraneService;
  if (!ctor) {
    throw new Error("Failed to load membrane.v1.MembraneService from proto definition");
  }

  cachedClientCtor = ctor;
  return ctor;
}

function createCredentials(options: GrpcTransportOptions): grpc.ChannelCredentials {
  if (options.tls || options.tlsCaCertPath) {
    const rootCerts = options.tlsCaCertPath ? fs.readFileSync(options.tlsCaCertPath) : undefined;
    return grpc.credentials.createSsl(rootCerts);
  }
  return grpc.credentials.createInsecure();
}

function grpcStatusName(code: grpc.status | undefined): string | undefined {
  if (code === undefined) {
    return undefined;
  }

  const name = grpc.status[code];
  return typeof name === "string" ? name : undefined;
}

function metadataToObject(metadata: grpc.Metadata | undefined): Record<string, string[]> {
  if (!metadata) {
    return {};
  }

  const result: Record<string, string[]> = {};
  for (const key of Object.keys(metadata.getMap())) {
    result[key] = metadata
      .get(key)
      .map((value) => (Buffer.isBuffer(value) ? value.toString("utf8") : String(value)));
  }
  return result;
}

function isServiceError(error: unknown): error is grpc.ServiceError {
  return (
    error instanceof Error &&
    typeof (error as Partial<grpc.ServiceError>).code === "number" &&
    (error as Partial<grpc.ServiceError>).metadata instanceof grpc.Metadata
  );
}

export function normalizeRpcError(error: unknown): MembraneError {
  if (error instanceof MembraneError) {
    return error;
  }

  if (isServiceError(error)) {
    return new MembraneError(error.message, {
      code: error.code,
      details: error.details,
      metadata: metadataToObject(error.metadata),
      cause: error
    });
  }

  if (error instanceof Error) {
    return new MembraneError(error.message, { cause: error });
  }

  return new MembraneError("Unknown gRPC transport error", { cause: error });
}

export function assertMethodBinding<M extends RpcMethodName>(methodName: M, method: unknown): GrpcUnaryMethod<M> {
  if (typeof method !== "function") {
    throw new MembraneError(`Missing gRPC method binding: ${methodName}`);
  }
  return method as GrpcUnaryMethod<M>;
}

function bindMethodTable(client: MembraneServiceClient): MethodTable {
  return {
    CaptureMemory: assertMethodBinding("CaptureMemory", client.CaptureMemory).bind(client) as GrpcUnaryMethod<"CaptureMemory">,
    RetrieveGraph: assertMethodBinding("RetrieveGraph", client.RetrieveGraph).bind(client) as GrpcUnaryMethod<"RetrieveGraph">,
    RetrieveByID: assertMethodBinding("RetrieveByID", client.RetrieveByID).bind(client) as GrpcUnaryMethod<"RetrieveByID">,
    Supersede: assertMethodBinding("Supersede", client.Supersede).bind(client) as GrpcUnaryMethod<"Supersede">,
    Fork: assertMethodBinding("Fork", client.Fork).bind(client) as GrpcUnaryMethod<"Fork">,
    Retract: assertMethodBinding("Retract", client.Retract).bind(client) as GrpcUnaryMethod<"Retract">,
    Merge: assertMethodBinding("Merge", client.Merge).bind(client) as GrpcUnaryMethod<"Merge">,
    Contest: assertMethodBinding("Contest", client.Contest).bind(client) as GrpcUnaryMethod<"Contest">,
    Reinforce: assertMethodBinding("Reinforce", client.Reinforce).bind(client) as GrpcUnaryMethod<"Reinforce">,
    Penalize: assertMethodBinding("Penalize", client.Penalize).bind(client) as GrpcUnaryMethod<"Penalize">,
    GetMetrics: assertMethodBinding("GetMetrics", client.GetMetrics).bind(client) as GrpcUnaryMethod<"GetMetrics">
  };
}

class GrpcTransport implements RpcTransport {
  private readonly apiKey: string | undefined;
  private readonly client: MembraneServiceClient;
  private readonly methods: MethodTable;
  private readonly timeoutMs: number | undefined;

  constructor(options: GrpcTransportOptions) {
    const ClientCtor = loadClientConstructor();
    this.client = new ClientCtor(options.addr, createCredentials(options));
    this.methods = bindMethodTable(this.client);
    this.apiKey = options.apiKey;
    this.timeoutMs = options.timeoutMs;
  }

  async unary<M extends RpcMethodName>(methodName: M, request: RpcRequest<M>): Promise<RpcResponse<M>> {
    const metadata = new grpc.Metadata();
    if (this.apiKey) {
      metadata.set("authorization", `Bearer ${this.apiKey}`);
    }

    const callOptions: grpc.CallOptions = {};
    if (typeof this.timeoutMs === "number") {
      callOptions.deadline = new Date(Date.now() + this.timeoutMs);
    }

    return await new Promise<RpcResponse<M>>((resolve, reject) => {
      try {
        this.methods[methodName](request, metadata, callOptions, (err, response) => {
          if (err) {
            reject(normalizeRpcError(err));
            return;
          }
          resolve(response);
        });
      } catch (error) {
        reject(normalizeRpcError(error));
      }
    });
  }

  close(): void {
    this.client.close();
  }
}

export function createGrpcTransport(options: GrpcTransportOptions): RpcTransport {
  return new GrpcTransport(options);
}
