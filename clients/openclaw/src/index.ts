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

// ── Plugin Class ──

/**
 * OpenClaw plugin bridge to Membrane episodic memory.
 * Each instance owns its own client and config — no module-level singletons.
 */
export class OpenClawMembranePlugin {
  private client: MembraneClient | null = null;
  private config: PluginConfig;
  private log: PluginLogger;

  constructor(api: PluginApi) {
    this.config = createConfig(api.config);
    this.log = api.log;
  }

  /** Connect to Membrane */
  activate(): void {
    if (this.client) {
      this.client.close();
    }
    this.client = new MembraneClient(this.config.grpc_endpoint);
    this.log.info(`[membrane] Connected to ${this.config.grpc_endpoint}`);
  }

  /** Disconnect from Membrane */
  deactivate(): void {
    if (this.client) {
      this.client.close();
      this.client = null;
    }
    this.log.info("[membrane] Disconnected");
  }

  /** Ingest agent replies and tool outputs into Membrane */
  async handleEvent(event: OpenClawEvent): Promise<void> {
    if (!this.client) return;

    const kind = mapEventKind(event);
    const sensitivity = mapSensitivity(this.config.default_sensitivity);
    const tags = buildTags(event);
    const source = event.agentId ?? "openclaw";

    try {
      if (kind === "tool_output" && event.toolName) {
        await this.client.ingestToolOutput(event.toolName, {
          args: (event.toolParams ?? {}) as Record<string, unknown>,
          result: event.toolResult ?? null,
          sensitivity,
          source,
          tags,
        });
      } else {
        await this.client.ingestEvent(event.hook, source, {
          summary: summarize(event),
          sensitivity,
          tags,
        });
      }
    } catch (err) {
      this.log.warn(`[membrane] Ingestion failed: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  /** Search Membrane for relevant memories */
  async search(
    query: string,
    options?: { limit?: number; memoryTypes?: string[]; minSalience?: number },
  ): Promise<MemoryRecord[]> {
    if (!this.client) return [];

    try {
      const retrieveOpts: RetrieveOptions = {
        limit: options?.limit ?? this.config.context_limit,
        minSalience: options?.minSalience ?? this.config.min_salience,
      };
      if (options?.memoryTypes) {
        retrieveOpts.memoryTypes = options.memoryTypes as MemoryType[];
      }
      return await this.client.retrieve(query, retrieveOpts);
    } catch (err) {
      this.log.warn(`[membrane] Search failed: ${err instanceof Error ? err.message : String(err)}`);
      return [];
    }
  }

  /** Auto-inject context before agent starts */
  async getContext(agentId: string): Promise<string | null> {
    if (!this.config.auto_context || !this.client) return null;

    try {
      const records = await this.client.retrieve(`context for agent ${agentId}`, {
        limit: this.config.context_limit,
        memoryTypes: this.config.context_types as MemoryType[],
        minSalience: this.config.min_salience,
      });

      if (records.length === 0) return null;

      const lines = records.map((r: MemoryRecord, i: number) =>
        `${i + 1}. [${r.type}] ${r.id}`
      );
      return `Episodic memory from Membrane:\n${lines.join("\n")}`;
    } catch (err) {
      this.log.debug(`[membrane] Context injection skipped: ${err instanceof Error ? err.message : String(err)}`);
      return null;
    }
  }

  /** Get connection status and stats */
  async getStatus(): Promise<{ connected: boolean; endpoint: string; metrics?: unknown }> {
    if (!this.client) {
      return { connected: false, endpoint: this.config.grpc_endpoint };
    }

    try {
      const metrics = await this.client.getMetrics();
      return { connected: true, endpoint: this.config.grpc_endpoint, metrics };
    } catch {
      return { connected: false, endpoint: this.config.grpc_endpoint };
    }
  }
}

// Re-exports
export type { PluginConfig, PluginApi, PluginLogger, OpenClawEvent } from "./types.js";
export { DEFAULT_CONFIG } from "./types.js";
export { mapSensitivity, mapEventKind, summarize, buildTags } from "./mapping.js";
