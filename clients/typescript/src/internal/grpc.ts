import fs from "node:fs";
import path from "node:path";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";

export interface RpcTransport {
  unary<TResponse>(methodName: string, request: Record<string, unknown>): Promise<TResponse>;
  close(): void;
}

export interface GrpcTransportOptions {
  addr: string;
  tls: boolean;
  tlsCaCertPath?: string | undefined;
  apiKey?: string | undefined;
  timeoutMs?: number | undefined;
}

type MembraneServiceClientConstructor = new (address: string, credentials: grpc.ChannelCredentials) => grpc.Client;

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

class GrpcTransport implements RpcTransport {
  private readonly apiKey: string | undefined;
  private readonly client: grpc.Client;
  private readonly timeoutMs: number | undefined;

  constructor(options: GrpcTransportOptions) {
    const ClientCtor = loadClientConstructor();
    this.client = new ClientCtor(options.addr, createCredentials(options));
    this.apiKey = options.apiKey;
    this.timeoutMs = options.timeoutMs;
  }

  async unary<TResponse>(methodName: string, request: Record<string, unknown>): Promise<TResponse> {
    const callable = (this.client as unknown as Record<string, unknown>)[methodName];
    if (typeof callable !== "function") {
      throw new Error(`Unknown gRPC method: ${methodName}`);
    }

    const metadata = new grpc.Metadata();
    if (this.apiKey) {
      metadata.set("authorization", `Bearer ${this.apiKey}`);
    }

    const callOptions: grpc.CallOptions = {};
    if (typeof this.timeoutMs === "number") {
      callOptions.deadline = new Date(Date.now() + this.timeoutMs);
    }

    return await new Promise<TResponse>((resolve, reject) => {
      (
        callable as (
          req: Record<string, unknown>,
          meta: grpc.Metadata,
          options: grpc.CallOptions,
          cb: (err: grpc.ServiceError | null, response: TResponse) => void
        ) => grpc.ClientUnaryCall
      ).call(this.client, request, metadata, callOptions, (err, response) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(response);
      });
    });
  }

  close(): void {
    this.client.close();
  }
}

export function createGrpcTransport(options: GrpcTransportOptions): RpcTransport {
  return new GrpcTransport(options);
}
