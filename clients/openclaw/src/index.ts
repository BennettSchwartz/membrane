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
  type GraphEdge,
  type GraphNode,
  type MemoryRecord,
  type MemoryType,
  type MetricsSnapshot,
  type RetrievalDiagnostic,
  type RetrieveGraphOptions,
  type TrustContext,
} from "@bennettschwartz/membrane";
import { mapSensitivity, mapEventKind, summarize, buildTags } from "./mapping.js";
import type { PluginConfig, PluginApi, PluginLogger, OpenClawEvent } from "./types.js";
import { DEFAULT_CONFIG, VALID_MEMORY_TYPES, VALID_SENSITIVITIES } from "./types.js";

// ── Config ──

const MAX_GRAPH_BUDGET = 10_000;

export function createConfig(raw: Record<string, unknown>): PluginConfig {
  return { ...DEFAULT_CONFIG, ...validateConfig(raw) };
}

export function validateConfig(raw: Record<string, unknown> | undefined): Partial<PluginConfig> {
  if (!raw) return {};
  const result: Partial<PluginConfig> = {};
  if (typeof raw.grpc_endpoint === "string") result.grpc_endpoint = raw.grpc_endpoint;
  if (typeof raw.api_key === "string" && raw.api_key.length > 0) result.api_key = raw.api_key;
  if (typeof raw.tls === "boolean") result.tls = raw.tls;
  if (typeof raw.tls_ca_cert_path === "string" && raw.tls_ca_cert_path.length > 0) {
    result.tls_ca_cert_path = raw.tls_ca_cert_path;
  }
  if (typeof raw.timeout_ms === "number" && Number.isInteger(raw.timeout_ms) && raw.timeout_ms > 0) {
    result.timeout_ms = raw.timeout_ms;
  }
  if (typeof raw.scope === "string" && raw.scope.trim().length > 0) {
    result.scope = raw.scope.trim();
  }
  if (Array.isArray(raw.trust_scopes)) {
    const filtered = raw.trust_scopes.filter(
      (scope): scope is string => typeof scope === "string" && scope.trim().length > 0,
    ).map((scope) => scope.trim());
    if (filtered.length > 0) result.trust_scopes = [...new Set(filtered)];
  }
  if (
    typeof raw.default_sensitivity === "string" &&
    (VALID_SENSITIVITIES as readonly string[]).includes(raw.default_sensitivity)
  ) {
    result.default_sensitivity = raw.default_sensitivity;
  }
  if (
    typeof raw.max_read_sensitivity === "string" &&
    (VALID_SENSITIVITIES as readonly string[]).includes(raw.max_read_sensitivity)
  ) {
    result.max_read_sensitivity = raw.max_read_sensitivity;
  }
  if (typeof raw.auto_context === "boolean") result.auto_context = raw.auto_context;
  if (
    typeof raw.context_limit === "number" &&
    Number.isInteger(raw.context_limit) &&
    raw.context_limit > 0 &&
    raw.context_limit <= MAX_GRAPH_BUDGET
  ) {
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

function validateSearchMemoryTypes(values: unknown): MemoryType[] | undefined {
  if (values === undefined) {
    return undefined;
  }
  if (!Array.isArray(values)) {
    throw new TypeError("memoryTypes must be an array of Membrane memory types");
  }
  return values.map((value, index) => {
    if (typeof value !== "string") {
      throw new TypeError(`memoryTypes[${index}] must be a string`);
    }
    if (!(VALID_MEMORY_TYPES as readonly string[]).includes(value)) {
      throw new TypeError(`memoryTypes[${index}] must be one of: ${VALID_MEMORY_TYPES.join(", ")}`);
    }
    return value as MemoryType;
  });
}

function validateSearchLimit(value: unknown, fallback: number): number {
  if (value === undefined) {
    return fallback;
  }
  if (typeof value !== "number" || !Number.isInteger(value) || value <= 0 || value > MAX_GRAPH_BUDGET) {
    throw new TypeError(`limit must be an integer from 1 to ${MAX_GRAPH_BUDGET}`);
  }
  return value;
}

function validateSearchMinSalience(value: unknown, fallback: number): number {
  if (value === undefined) {
    return fallback;
  }
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0 || value > 1) {
    throw new TypeError("minSalience must be a finite number from 0 to 1");
  }
  return value;
}

function graphBudget(limit: number, multiplier: number, floor: number): number {
  return Math.min(Math.max(limit * multiplier, floor, limit), MAX_GRAPH_BUDGET);
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
    this.client = new MembraneClient(this.config.grpc_endpoint, {
      apiKey: this.config.api_key,
      tls: this.config.tls ?? false,
      tlsCaCertPath: this.config.tls_ca_cert_path,
      timeoutMs: this.config.timeout_ms,
    });
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
            scope: this.config.scope,
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
            scope: this.config.scope,
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
            scope: this.config.scope,
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
      const limit = validateSearchLimit(options?.limit, this.config.context_limit);
      const effectiveMemoryTypes = options?.memoryTypes ?? options?.memory_types;
      const effectiveMinSalience = validateSearchMinSalience(
        options?.minSalience ?? options?.min_salience,
        this.config.min_salience,
      );
      const memoryTypes = validateSearchMemoryTypes(effectiveMemoryTypes);

      const retrieveOpts: RetrieveGraphOptions = {
        rootLimit: limit,
        nodeLimit: graphBudget(limit, 2, 1),
        edgeLimit: graphBudget(limit, 4, 10),
        maxHops: 1,
        minSalience: effectiveMinSalience,
        trust: this.retrievalTrust(),
      };
      if (memoryTypes !== undefined) {
        retrieveOpts.memoryTypes = memoryTypes;
      }
      const graph = await this.client.retrieveGraph(query, retrieveOpts);
      logRetrievalDiagnostics(this.log, "search", graph.diagnostics);
      return flattenGraphNodes(graph.nodes).slice(0, limit);
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
        nodeLimit: graphBudget(this.config.context_limit, 2, 1),
        edgeLimit: graphBudget(this.config.context_limit, 4, 10),
        maxHops: 1,
        memoryTypes: this.config.context_types as MemoryType[],
        minSalience: this.config.min_salience,
        trust: this.retrievalTrust(),
      });

      if (graph.nodes.length === 0) return null;
      logRetrievalDiagnostics(this.log, "context", graph.diagnostics);

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
      const relations = formatGraphRelations(graph.nodes, graph.edges ?? []);
      if (relations.length > 0) {
        sections.push("Relations:", ...relations);
      }
      return `Membrane graph context:\n${sections.join("\n")}`;
    } catch (err) {
      this.log.debug(`[membrane] Context injection skipped: ${err instanceof Error ? err.message : String(err)}`);
      return null;
    }
  }

  /** Get connection status and stats */
  async getStatus(): Promise<{ connected: boolean; endpoint: string; metrics?: MetricsSnapshot; warnings?: string[] }> {
    if (!this.client) {
      return { connected: false, endpoint: this.config.grpc_endpoint };
    }

    try {
      const metrics = await this.client.getMetrics();
      const warnings = metricsWarnings(metrics);
      return {
        connected: true,
        endpoint: this.config.grpc_endpoint,
        metrics,
        ...(warnings.length > 0 ? { warnings } : {}),
      };
    } catch (err) {
      this.log.debug(`[membrane] Metrics unavailable: ${err instanceof Error ? err.message : String(err)}`);
      return { connected: true, endpoint: this.config.grpc_endpoint };
    }
  }

  private retrievalTrust(): TrustContext {
    const scopes = this.config.trust_scopes.length > 0
      ? this.config.trust_scopes
      : this.config.scope
        ? [this.config.scope]
        : [];
    if (scopes.length === 0) {
      throw new Error("Configure scope or trust_scopes before Membrane retrieval");
    }
    return {
      max_sensitivity: this.config.max_read_sensitivity,
      authenticated: true,
      actor_id: "openclaw",
      scopes,
    };
  }
}

// Re-exports
export type { PluginConfig, PluginApi, PluginLogger, OpenClawEvent } from "./types.js";
export { DEFAULT_CONFIG, VALID_MEMORY_TYPES, VALID_SENSITIVITIES } from "./types.js";
export { mapSensitivity, mapEventKind, summarize, buildTags } from "./mapping.js";

function formatGraphRelations(nodes: GraphNode[], edges: GraphEdge[]): string[] {
  if (edges.length === 0) return [];
  const recordByID = new Map(nodes.map((node) => [node.record.id, node.record]));
  const lines: string[] = [];
  for (const edge of edges) {
    const source = recordByID.get(edge.source_id);
    const target = recordByID.get(edge.target_id);
    if (!source || !target) continue;
    lines.push(`${lines.length + 1}. ${extractContextSummary(source)} --${edge.predicate}--> ${extractContextSummary(target)}`);
  }
  return lines;
}

function logRetrievalDiagnostics(log: PluginLogger, surface: "search" | "context", diagnostics?: RetrievalDiagnostic[]): void {
  if (!diagnostics || diagnostics.length === 0) {
    return;
  }
  for (const diagnostic of diagnostics) {
    log.warn(`[membrane] Retrieval degraded during ${surface}: ${diagnostic.code}: ${diagnostic.message}`);
  }
}

function metricsWarnings(metrics: MetricsSnapshot): string[] {
  const warnings: string[] = [];
  const missing = numberMetric(metrics.missing_embeddings);
  const total = numberMetric(metrics.total_records);
  const coverage = numberMetric(metrics.embedding_coverage);
  if (missing > 0) {
    const model = typeof metrics.embedding_model === "string" && metrics.embedding_model
      ? ` for ${metrics.embedding_model}`
      : "";
    warnings.push(`${missing} Membrane record(s) are missing pgvector embeddings${model}`);
  } else if (total > 0 && coverage >= 0 && coverage < 1) {
    warnings.push(`Membrane pgvector embedding coverage is ${(coverage * 100).toFixed(1)}%`);
  }
  return warnings;
}

function numberMetric(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function flattenGraphNodes(nodes: GraphNode[]): MemoryRecord[] {
  const seen = new Set<string>();
  const records: MemoryRecord[] = [];
  const append = (root: boolean) => {
    for (const node of nodes) {
      if (node.root !== root || seen.has(node.record.id)) continue;
      seen.add(node.record.id);
      records.push(node.record);
    }
  };
  append(true);
  append(false);
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
