/**
 * @vainplex/openclaw-membrane — Membrane bridge plugin for OpenClaw
 *
 * Provides:
 * - Event ingestion (write path) via rich capture on @bennettschwartz/membrane client
 * - `membrane_search` tool for graph-aware memory queries
 * - `before_agent_start` hook for auto-context injection
 * - `/membrane` command for status and stats
 */

import {
  MembraneClient,
  SourceKind,
  type GraphNode,
  type MemoryRecord,
  type MemoryType,
  type RetrieveGraphOptions,
} from "@bennettschwartz/membrane";
import { mapSensitivity, mapEventKind, summarize, buildTags } from "./mapping.js";
import type { PluginConfig, PluginApi, PluginLogger, OpenClawEvent } from "./types.js";
import { DEFAULT_CONFIG, VALID_MEMORY_TYPES } from "./types.js";

// ── Config ──

export function createConfig(raw: Record<string, unknown>): PluginConfig {
  return { ...DEFAULT_CONFIG, ...validateConfig(raw) };
}

export function validateConfig(raw: Record<string, unknown> | undefined): Partial<PluginConfig> {
  if (!raw) return {};
  const result: Partial<PluginConfig> = {};
  if (typeof raw.grpc_endpoint === "string") result.grpc_endpoint = raw.grpc_endpoint;
  if (typeof raw.default_sensitivity === "string") result.default_sensitivity = raw.default_sensitivity;
  if (typeof raw.auto_context === "boolean") result.auto_context = raw.auto_context;
  if (typeof raw.context_limit === "number" && Number.isInteger(raw.context_limit) && raw.context_limit > 0) {
    result.context_limit = raw.context_limit;
  }
  if (typeof raw.min_salience === "number" && Number.isFinite(raw.min_salience) && raw.min_salience >= 0 && raw.min_salience <= 1) {
    result.min_salience = raw.min_salience;
  }
  if (Array.isArray(raw.context_types)) {
    const filtered = raw.context_types.filter(
      (t): t is string => typeof t === "string" && (VALID_MEMORY_TYPES as readonly string[]).includes(t),
    );
    if (filtered.length > 0) result.context_types = filtered;
  }
  return result;
}

// ── Plugin Class ──

/**
 * OpenClaw plugin bridge to Membrane graph-aware memory.
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
      const summary = summarize(event);
      const context = {
        agent_id: event.agentId,
        session_key: event.sessionKey,
        hook: event.hook,
      };

      if (kind === "tool_output" && event.toolName) {
        await this.client.captureMemory(
          {
            tool_name: event.toolName,
            args: (event.toolParams ?? {}) as Record<string, unknown>,
            result: event.toolResult ?? null,
            text: summary,
          },
          {
            sourceKind: SourceKind.TOOL_OUTPUT,
            context,
            reasonToRemember: summary,
            summary,
            sensitivity,
            source,
            tags,
          },
        );
      } else if (kind === "observation") {
        await this.client.captureMemory(
          {
            subject: source,
            predicate: event.hook,
            object: summarize(event),
            text: summary,
          },
          {
            sourceKind: SourceKind.OBSERVATION,
            context,
            reasonToRemember: summary,
            summary,
            sensitivity,
            source,
            tags,
          },
        );
      } else {
        const ref = [
          event.sessionKey ?? source,
          event.hook,
          event.timestamp ?? String(Date.now()),
        ].join(":");
        await this.client.captureMemory(
          {
            ref,
            hook: event.hook,
            text: summary,
            message: event.message,
            response: event.response,
          },
          {
            sourceKind: SourceKind.EVENT,
            context,
            reasonToRemember: summary,
            summary,
            sensitivity,
            source,
            tags,
          },
        );
      }
    } catch (err) {
      this.log.warn(`[membrane] Ingestion failed: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  /** Search Membrane for relevant memories */
  async search(
    query: string,
    options?: {
      limit?: number;
      memoryTypes?: string[];
      memory_types?: string[];
      minSalience?: number;
      min_salience?: number;
    },
  ): Promise<MemoryRecord[]> {
    if (!this.client) return [];

    try {
      const effectiveMemoryTypes = options?.memoryTypes ?? options?.memory_types;
      const effectiveMinSalience =
        options?.minSalience ?? options?.min_salience ?? this.config.min_salience;

      const retrieveOpts: RetrieveGraphOptions = {
        rootLimit: options?.limit ?? this.config.context_limit,
        nodeLimit: Math.max((options?.limit ?? this.config.context_limit) * 2, options?.limit ?? this.config.context_limit),
        edgeLimit: Math.max((options?.limit ?? this.config.context_limit) * 4, 10),
        maxHops: 1,
        minSalience: effectiveMinSalience,
      };
      if (effectiveMemoryTypes) {
        const validTypes = effectiveMemoryTypes.filter(
          (t) => (VALID_MEMORY_TYPES as readonly string[]).includes(t),
        );
        if (validTypes.length === 0) {
          this.log.warn(`[membrane] All provided memoryTypes are invalid: ${effectiveMemoryTypes.join(", ")}`);
          return [];
        }
        retrieveOpts.memoryTypes = validTypes as MemoryType[];
      }
      const graph = await this.client.retrieveGraph(query, retrieveOpts);
      return flattenGraphNodes(graph.nodes);
    } catch (err) {
      this.log.warn(`[membrane] Search failed: ${err instanceof Error ? err.message : String(err)}`);
      return [];
    }
  }

  /** Auto-inject context before agent starts */
  async getContext(agentId: string): Promise<string | null> {
    if (!this.config.auto_context || !this.client) return null;

    try {
      const graph = await this.client.retrieveGraph(`context for agent ${agentId}`, {
        rootLimit: this.config.context_limit,
        nodeLimit: Math.max(this.config.context_limit * 2, this.config.context_limit),
        edgeLimit: Math.max(this.config.context_limit * 4, 10),
        maxHops: 1,
        memoryTypes: this.config.context_types as MemoryType[],
        minSalience: this.config.min_salience,
      });

      if (graph.nodes.length === 0) return null;

      const roots = graph.nodes.filter((node) => node.root);
      const neighbors = graph.nodes.filter((node) => !node.root);
      const sections: string[] = [];
      if (roots.length > 0) {
        sections.push(
          "Roots:",
          ...roots.map((node, i) => `${i + 1}. [${node.record.type}] ${extractContextSummary(node.record)}`),
        );
      }
      if (neighbors.length > 0) {
        sections.push(
          "Neighbors:",
          ...neighbors.map((node, i) => `${i + 1}. [hop=${node.hop}] [${node.record.type}] ${extractContextSummary(node.record)}`),
        );
      }
      return `Membrane graph context:\n${sections.join("\n")}`;
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
    } catch (err) {
      this.log.debug(`[membrane] Metrics unavailable: ${err instanceof Error ? err.message : String(err)}`);
      return { connected: true, endpoint: this.config.grpc_endpoint };
    }
  }
}

// Re-exports
export type { PluginConfig, PluginApi, PluginLogger, OpenClawEvent } from "./types.js";
export { DEFAULT_CONFIG, VALID_MEMORY_TYPES } from "./types.js";
export { mapSensitivity, mapEventKind, summarize, buildTags } from "./mapping.js";

function flattenGraphNodes(nodes: GraphNode[]): MemoryRecord[] {
  const seen = new Set<string>();
  const ordered = [...nodes].sort((a, b) => {
    if (a.root !== b.root) return a.root ? -1 : 1;
    if (a.hop !== b.hop) return a.hop - b.hop;
    return a.record.id.localeCompare(b.record.id);
  });
  const records: MemoryRecord[] = [];
  for (const node of ordered) {
    if (seen.has(node.record.id)) continue;
    seen.add(node.record.id);
    records.push(node.record);
  }
  return records;
}

function extractContextSummary(record: MemoryRecord): string {
  const payload = (record.payload ?? {}) as Record<string, unknown>;
  const interpretation = record.interpretation;
  if (interpretation?.summary) {
    return interpretation.summary;
  }
  if (typeof payload.summary === "string" && payload.summary.length > 0) {
    return payload.summary;
  }
  if (typeof payload.canonical_name === "string" && payload.canonical_name.length > 0) {
    return payload.canonical_name;
  }
  if (Array.isArray(payload.timeline)) {
    const entry = (payload.timeline as Array<Record<string, unknown>>).find(
      (item) => typeof item?.summary === "string" && item.summary.length > 0,
    );
    if (entry?.summary) {
      return String(entry.summary);
    }
  }
  return record.id;
}
