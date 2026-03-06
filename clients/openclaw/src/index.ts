/**
 * @vainplex/openclaw-membrane — Membrane bridge plugin for OpenClaw
 *
 * Provides:
 * - Event ingestion (write path) via @gustycube/membrane client
 * - `membrane_search` tool for episodic memory queries
 * - `before_agent_start` hook for auto-context injection
 * - `/membrane` command for status and stats
 */

import { MembraneClient, type MemoryRecord, type MemoryType, type RetrieveOptions } from "@gustycube/membrane";
import { mapSensitivity, mapEventKind, summarize, buildTags } from "./mapping.js";
import type { PluginConfig, PluginApi, PluginLogger, OpenClawEvent } from "./types.js";
import { DEFAULT_CONFIG } from "./types.js";

// ── Config ──

export function createConfig(raw: Record<string, unknown>): PluginConfig {
  return { ...DEFAULT_CONFIG, ...validateConfig(raw) };
}

export function validateConfig(raw: Record<string, unknown> | undefined): Partial<PluginConfig> {
  if (!raw) return {};
  const result: Partial<PluginConfig> = {};
  if (typeof raw.grpc_endpoint === "string") result.grpc_endpoint = raw.grpc_endpoint;
  if (typeof raw.default_sensitivity === "string") result.default_sensitivity = raw.default_sensitivity;
  if (typeof raw.buffer_size === "number") result.buffer_size = raw.buffer_size;
  if (typeof raw.auto_context === "boolean") result.auto_context = raw.auto_context;
  if (typeof raw.context_limit === "number") result.context_limit = raw.context_limit;
  if (typeof raw.min_salience === "number") result.min_salience = raw.min_salience;
  if (typeof raw.flush_interval_ms === "number") result.flush_interval_ms = raw.flush_interval_ms;
  if (Array.isArray(raw.context_types)) {
    result.context_types = raw.context_types.filter((t): t is string => typeof t === "string");
  }
  return result;
}

// ── Plugin Lifecycle ──

let client: MembraneClient | null = null;
let config: PluginConfig = DEFAULT_CONFIG;
let log: PluginLogger = console;

/** Initialize the plugin — called by OpenClaw on load */
export function activate(api: PluginApi): void {
  config = createConfig(api.config);
  log = api.log;

  client = new MembraneClient(config.grpc_endpoint);
  log.info(`[membrane] Connected to ${config.grpc_endpoint}`);
}

/** Cleanup — called by OpenClaw on shutdown */
export function deactivate(): void {
  if (client) client.close();
  client = null;
  log.info("[membrane] Disconnected");
}

// ── Hooks ──

/** Ingest agent replies and tool outputs into Membrane */
export async function handleEvent(event: OpenClawEvent): Promise<void> {
  if (!client) return;

  const kind = mapEventKind(event);
  const sensitivity = mapSensitivity(config.default_sensitivity);
  const tags = buildTags(event);
  const source = event.agentId ?? "openclaw";

  try {
    if (kind === "tool_output" && event.toolName) {
      await client.ingestToolOutput(event.toolName, {
        args: (event.toolParams ?? {}) as Record<string, unknown>,
        result: event.toolResult ?? null,
        sensitivity,
        source,
        tags,
      });
    } else {
      await client.ingestEvent(event.hook, source, {
        summary: summarize(event),
        sensitivity,
        tags,
      });
    }
  } catch (err) {
    log.warn(`[membrane] Ingestion failed: ${err instanceof Error ? err.message : String(err)}`);
  }
}

/** Search Membrane for relevant memories (exposed as membrane_search tool) */
export async function search(
  query: string,
  options?: { limit?: number; memoryTypes?: string[]; minSalience?: number },
): Promise<MemoryRecord[]> {
  if (!client) return [];

  try {
    const retrieveOpts: RetrieveOptions = {
      limit: options?.limit ?? config.context_limit,
      minSalience: options?.minSalience ?? config.min_salience,
    };
    if (options?.memoryTypes) {
      retrieveOpts.memoryTypes = options.memoryTypes as MemoryType[];
    }
    return await client.retrieve(query, retrieveOpts);
  } catch (err) {
    log.warn(`[membrane] Search failed: ${err instanceof Error ? err.message : String(err)}`);
    return [];
  }
}

/** Auto-inject context before agent starts (if enabled) */
export async function getContext(agentId: string): Promise<string | null> {
  if (!config.auto_context || !client) return null;

  try {
    const records = await client.retrieve(`context for agent ${agentId}`, {
      limit: config.context_limit,
      memoryTypes: config.context_types as MemoryType[],
      minSalience: config.min_salience,
    });

    if (records.length === 0) return null;

    const lines = records.map((r: MemoryRecord, i: number) =>
      `${i + 1}. [${r.type}] ${r.id}`
    );
    return `Episodic memory from Membrane:\n${lines.join("\n")}`;
  } catch (err) {
    log.debug(`[membrane] Context injection skipped: ${err instanceof Error ? err.message : String(err)}`);
    return null;
  }
}

/** Get connection status and stats */
export async function getStatus(): Promise<{ connected: boolean; endpoint: string; metrics?: unknown }> {
  if (!client) {
    return { connected: false, endpoint: config.grpc_endpoint };
  }

  try {
    const metrics = await client.getMetrics();
    return { connected: true, endpoint: config.grpc_endpoint, metrics };
  } catch {
    return { connected: false, endpoint: config.grpc_endpoint };
  }
}

// Re-exports
export type { PluginConfig, PluginApi, PluginLogger, OpenClawEvent } from "./types.js";
export { DEFAULT_CONFIG } from "./types.js";
export { mapSensitivity, mapEventKind, summarize, buildTags } from "./mapping.js";
