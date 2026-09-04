/**
 * Types for the OpenClaw Membrane plugin.
 */

export interface PluginConfig {
  /** Membrane gRPC endpoint (default: localhost:9090) */
  grpc_endpoint: string;
  /** Bearer token passed to Membrane gRPC requests */
  api_key?: string;
  /** Use TLS for the Membrane gRPC channel */
  tls?: boolean;
  /** Optional CA certificate path for TLS connections */
  tls_ca_cert_path?: string;
  /** Per-RPC timeout in milliseconds */
  timeout_ms?: number;
  /** Scope assigned to captured memories */
  scope?: string;
  /** Scopes allowed during retrieval; defaults to [scope] when scope is set */
  trust_scopes: string[];
  /** Default sensitivity for ingested events */
  default_sensitivity: string;
  /** Maximum sensitivity requested for retrieval */
  max_read_sensitivity: string;
  /** Auto-inject context on agent start */
  auto_context: boolean;
  /** Max graph roots to retrieve for injected context */
  context_limit: number;
  /** Min salience for context injection */
  min_salience: number;
  /** Memory types to include in context */
  context_types: string[];
}

/** Valid Membrane memory types for retrieval filtering */
export const VALID_MEMORY_TYPES = ["episodic", "working", "semantic", "competence", "plan_graph", "entity"] as const;

/** Valid Membrane sensitivity levels for ingestion and retrieval trust. */
export const VALID_SENSITIVITIES = ["public", "low", "medium", "high", "hyper"] as const;

export const DEFAULT_CONFIG: PluginConfig = {
  grpc_endpoint: "localhost:9090",
  trust_scopes: [],
  default_sensitivity: "low",
  max_read_sensitivity: "low",
  auto_context: true,
  context_limit: 5,
  min_salience: 0.3,
  context_types: ["entity", "episodic", "semantic", "competence"],
};

/** OpenClaw hook event passed to plugin hooks */
export interface OpenClawEvent {
  hook: string;
  agentId?: string;
  sessionKey?: string;
  toolName?: string;
  toolParams?: Record<string, unknown>;
  toolResult?: unknown;
  message?: string;
  response?: string;
  timestamp?: string;
}

/** OpenClaw plugin API interface */
export interface PluginApi {
  log: PluginLogger;
  config: Record<string, unknown>;
}

export interface PluginLogger {
  info(msg: string, ...args: unknown[]): void;
  warn(msg: string, ...args: unknown[]): void;
  error(msg: string, ...args: unknown[]): void;
  debug(msg: string, ...args: unknown[]): void;
}
