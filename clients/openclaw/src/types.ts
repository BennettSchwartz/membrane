/**
 * Types for the OpenClaw Membrane plugin.
 */

export interface PluginConfig {
  /** Membrane gRPC endpoint (default: localhost:4222) */
  grpc_endpoint: string;
  /** Default sensitivity for ingested events */
  default_sensitivity: string;
  /** Auto-inject context on agent start */
  auto_context: boolean;
  /** Max memories to inject as context */
  context_limit: number;
  /** Min salience for context injection */
  min_salience: number;
  /** Memory types to include in context */
  context_types: string[];
}

/** Valid Membrane memory types for retrieval filtering */
export const VALID_MEMORY_TYPES = ["episodic", "working", "semantic", "competence", "plan_graph", "entity"] as const;

export const DEFAULT_CONFIG: PluginConfig = {
  grpc_endpoint: "localhost:4222",
  default_sensitivity: "low",
  auto_context: true,
  context_limit: 5,
  min_salience: 0.3,
  context_types: ["episodic", "semantic", "competence"],
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
