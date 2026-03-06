/**
 * Types for the OpenClaw Membrane plugin.
 */

export interface PluginConfig {
  /** Membrane gRPC endpoint (default: localhost:4222) */
  grpc_endpoint: string;
  /** Default sensitivity for ingested events */
  default_sensitivity: string;
  /** Event buffer size for reliability */
  buffer_size: number;
  /** Auto-inject context on agent start */
  auto_context: boolean;
  /** Max memories to inject as context */
  context_limit: number;
  /** Min salience for context injection */
  min_salience: number;
  /** Memory types to include in context */
  context_types: string[];
  /** Flush interval in ms for buffered events */
  flush_interval_ms: number;
}

export const DEFAULT_CONFIG: PluginConfig = {
  grpc_endpoint: "localhost:4222",
  default_sensitivity: "low",
  buffer_size: 100,
  auto_context: true,
  context_limit: 5,
  min_salience: 0.3,
  context_types: ["event", "tool_output", "observation"],
  flush_interval_ms: 5000,
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
