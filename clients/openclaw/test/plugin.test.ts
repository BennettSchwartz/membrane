import fs from "node:fs";
import path from "node:path";
import { describe, it, expect, vi, beforeEach } from "vitest";
import { OpenClawMembranePlugin, createConfig, validateConfig, DEFAULT_CONFIG } from "../src/index.js";
import type { PluginApi, OpenClawEvent } from "../src/types.js";

const membraneClientMock = vi.hoisted(() => ({
  instances: [] as Array<{ addr: string; options: Record<string, unknown> | undefined; close: ReturnType<typeof vi.fn>; getMetrics: ReturnType<typeof vi.fn> }>,
}));

vi.mock("@bennettschwartz/membrane", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@bennettschwartz/membrane")>();
  return {
    ...actual,
    MembraneClient: vi.fn().mockImplementation(function (addr: string, options?: Record<string, unknown>) {
      const instance = {
        addr,
        options,
        close: vi.fn(),
        getMetrics: vi.fn().mockResolvedValue({ total_records: 0 }),
      };
      membraneClientMock.instances.push(instance);
      return instance;
    }),
  };
});

function mockApi(overrides: Record<string, unknown> = {}): PluginApi {
  return {
    config: { ...overrides },
    log: {
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
      debug: vi.fn(),
    },
  };
}

describe("createConfig", () => {
  it("returns defaults when raw is empty", () => {
    const config = createConfig({});
    expect(config.grpc_endpoint).toBe("localhost:9090");
    expect(config.auto_context).toBe(true);
    expect(config.context_limit).toBe(5);
    expect(config.context_types).toContain("entity");
  });

  it("merges user config over defaults", () => {
    const config = createConfig({ context_limit: 10, auto_context: false });
    expect(config.context_limit).toBe(10);
    expect(config.auto_context).toBe(false);
    expect(config.grpc_endpoint).toBe("localhost:9090"); // default preserved
  });

  it("ignores invalid types", () => {
    const config = createConfig({ context_limit: "not-a-number" });
    expect(config.context_limit).toBe(DEFAULT_CONFIG.context_limit);
  });
});

describe("validateConfig", () => {
  it("returns empty for undefined input", () => {
    expect(validateConfig(undefined)).toEqual({});
  });

  it("filters context_types to valid string types only", () => {
    const result = validateConfig({ context_types: ["episodic", 42, "working"] });
    expect(result.context_types).toEqual(["episodic", "working"]);
  });

  it("accepts capture scope and read trust scopes", () => {
    const result = validateConfig({
      scope: "project:alpha",
      trust_scopes: ["project:alpha", 42, "", "project:beta"],
    });

    expect(result.scope).toBe("project:alpha");
    expect(result.trust_scopes).toEqual(["project:alpha", "project:beta"]);
  });

  it("rejects NaN and negative context_limit", () => {
    expect(validateConfig({ context_limit: NaN })).toEqual({});
    expect(validateConfig({ context_limit: -1 })).toEqual({});
    expect(validateConfig({ context_limit: 0 })).toEqual({});
    expect(validateConfig({ context_limit: 3.5 })).toEqual({});
    expect(validateConfig({ context_limit: 10_001 })).toEqual({});
  });

  it("rejects out-of-range min_salience", () => {
    expect(validateConfig({ min_salience: -0.1 })).toEqual({});
    expect(validateConfig({ min_salience: 1.5 })).toEqual({});
    expect(validateConfig({ min_salience: NaN })).toEqual({});
  });

  it("accepts valid min_salience", () => {
    expect(validateConfig({ min_salience: 0 })).toEqual({ min_salience: 0 });
    expect(validateConfig({ min_salience: 0.5 })).toEqual({ min_salience: 0.5 });
    expect(validateConfig({ min_salience: 1 })).toEqual({ min_salience: 1 });
  });

  it("accepts only valid default sensitivity values", () => {
    expect(validateConfig({ default_sensitivity: "medium" })).toEqual({ default_sensitivity: "medium" });
    expect(validateConfig({ default_sensitivity: "classified" })).toEqual({});
    expect(validateConfig({ default_sensitivity: "" })).toEqual({});
  });

  it("accepts separate read sensitivity values", () => {
    expect(validateConfig({ max_read_sensitivity: "medium" })).toEqual({ max_read_sensitivity: "medium" });
    expect(validateConfig({ max_read_sensitivity: "classified" })).toEqual({});
  });

  it("accepts SDK transport options and rejects invalid timeout", () => {
    expect(validateConfig({
      api_key: "plugin-secret",
      tls: true,
      tls_ca_cert_path: "/etc/membrane/ca.pem",
      timeout_ms: 10_000,
    })).toEqual({
      api_key: "plugin-secret",
      tls: true,
      tls_ca_cert_path: "/etc/membrane/ca.pem",
      timeout_ms: 10_000,
    });
    expect(validateConfig({ timeout_ms: 0 })).toEqual({});
    expect(validateConfig({ timeout_ms: 2.5 })).toEqual({});
  });

  it("drops empty context_types array to preserve defaults", () => {
    const result = validateConfig({ context_types: [42, true] });
    expect(result.context_types).toBeUndefined();
  });

  it("filters context_types to valid Membrane memory types only", () => {
    const result = validateConfig({ context_types: ["episodic", "unsupported", "semantic", "entity", "event"] });
    expect(result.context_types).toEqual(["episodic", "semantic", "entity"]);
  });

  it("drops context_types when none are valid Membrane types", () => {
    const result = validateConfig({ context_types: ["event", "tool_output", "observation"] });
    expect(result.context_types).toBeUndefined();
  });
});

describe("plugin manifest", () => {
  it("ships the MIT license in the npm package", () => {
    const packagePath = path.resolve(__dirname, "../package.json");
    const packageJson = JSON.parse(fs.readFileSync(packagePath, "utf8")) as {
      license: string;
      files?: string[];
    };
    const licensePath = path.resolve(__dirname, "../LICENSE");

    expect(packageJson.license).toBe("MIT");
    expect(packageJson.files).toContain("LICENSE");
    expect(fs.readFileSync(licensePath, "utf8")).toContain("MIT License");
  });

  it("uses registry-compatible runtime dependencies for npm consumers", () => {
    const packagePath = path.resolve(__dirname, "../package.json");
    const packageJson = JSON.parse(fs.readFileSync(packagePath, "utf8")) as {
      dependencies?: Record<string, string>;
      peerDependencies?: Record<string, string>;
    };

    expect(Object.values(packageJson.dependencies ?? {}).some((value) => value.startsWith("file:"))).toBe(false);
    expect(packageJson.peerDependencies?.["@bennettschwartz/membrane"]).toMatch(/^\^?\d+\.\d+\.\d+/);
  });

  it("advertises every valid Membrane memory type accepted by runtime config", () => {
    const manifestPath = path.resolve(__dirname, "../openclaw.plugin.json");
    const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8")) as {
      configSchema: {
        properties: {
          api_key: { type: string };
          tls: { default: boolean };
          tls_ca_cert_path: { type: string };
          timeout_ms: { minimum: number };
          scope: { type: string };
          trust_scopes: { items: { type: string } };
          max_read_sensitivity: { default: string };
          context_limit: { maximum: number };
          context_types: { items: { enum: string[] } };
        };
      };
      tools: Array<{ parameters: { properties: { limit?: { maximum: number }; memory_types?: { items: { enum: string[] }; description?: string } } } }>;
    };
    const expected = [...new Set([...DEFAULT_CONFIG.context_types, "working", "plan_graph"])].sort();

    const contextTypes = manifest.configSchema.properties.context_types.items.enum.slice().sort();
    const searchTypes = manifest.tools[0]?.parameters.properties.memory_types?.items.enum.slice().sort();

    expect(contextTypes).toEqual(expected);
    expect(searchTypes).toEqual(expected);
    expect(manifest.tools[0]?.parameters.properties.memory_types?.description).toContain("entity");
    expect(manifest.configSchema.properties.api_key.type).toBe("string");
    expect(manifest.configSchema.properties.tls.default).toBe(false);
    expect(manifest.configSchema.properties.tls_ca_cert_path.type).toBe("string");
    expect(manifest.configSchema.properties.timeout_ms.minimum).toBe(1);
    expect(manifest.configSchema.properties.scope.type).toBe("string");
    expect(manifest.configSchema.properties.trust_scopes.items.type).toBe("string");
    expect(manifest.configSchema.properties.max_read_sensitivity.default).toBe("low");
    expect(manifest.configSchema.properties.context_limit.maximum).toBe(10_000);
    expect(manifest.tools[0]?.parameters.properties.limit?.maximum).toBe(10_000);
  });
});

describe("OpenClawMembranePlugin", () => {
  let plugin: OpenClawMembranePlugin;
  let api: PluginApi;

  beforeEach(() => {
    membraneClientMock.instances.length = 0;
    vi.clearAllMocks();
    api = mockApi();
    plugin = new OpenClawMembranePlugin(api);
  });

  it("constructs without activating", async () => {
    // Plugin created but not connected — search should return empty
    const result = await plugin.search("test");
    expect(result).toEqual([]);
  });

  it("deactivate is safe without activate", () => {
    expect(() => plugin.deactivate()).not.toThrow();
    expect(api.log.info).toHaveBeenCalledWith("[membrane] Disconnected");
  });

  it("getContext returns null when auto_context disabled", async () => {
    const disabledApi = mockApi({ auto_context: false });
    const p = new OpenClawMembranePlugin(disabledApi);
    expect(await p.getContext("test-agent")).toBeNull();
  });

  it("getContext returns null when not activated", async () => {
    expect(await plugin.getContext("test-agent")).toBeNull();
  });

  it("getStatus returns disconnected when not activated", async () => {
    const status = await plugin.getStatus();
    expect(status.connected).toBe(false);
    expect(status.endpoint).toBe("localhost:9090");
  });

  it("getStatus includes embedding coverage warnings from metrics", async () => {
    const fakeClient = {
      getMetrics: vi.fn().mockResolvedValue({
        total_records: 10,
        embedded_records: 7,
        missing_embeddings: 3,
        embedding_coverage: 0.7,
        embedding_model: "text-embedding-current",
      }),
    };
    (plugin as unknown as { client: unknown }).client = fakeClient;

    const status = await plugin.getStatus();

    expect(status.connected).toBe(true);
    expect(status.metrics?.embedding_coverage).toBe(0.7);
    expect(status.warnings).toEqual([
      "3 Membrane record(s) are missing pgvector embeddings for text-embedding-current",
    ]);
  });

  it("handleEvent is a no-op when not activated", async () => {
    const event: OpenClawEvent = { hook: "after_agent_reply", response: "Hello" };
    // Should not throw
    await plugin.handleEvent(event);
  });

  it("handleEvent uses captureMemory for tool output", async () => {
    const fakeClient = {
      captureMemory: vi.fn().mockResolvedValue({}),
    };
    (plugin as unknown as { client: unknown }).client = fakeClient;

    await plugin.handleEvent({
      hook: "after_tool_call",
      agentId: "agent-1",
      toolName: "bash",
      toolParams: { cmd: "go test ./..." },
      toolResult: { exit_code: 0 },
      timestamp: "2026-04-08T12:00:00Z",
    });

    expect(fakeClient.captureMemory).toHaveBeenCalledTimes(1);
    const [, options] = fakeClient.captureMemory.mock.calls[0];
    expect(options.sourceKind).toBe("tool_output");
  });

  it("writes captured events into configured scope", async () => {
    const scopedPlugin = new OpenClawMembranePlugin(mockApi({ scope: "project:alpha" }));
    const fakeClient = {
      captureMemory: vi.fn().mockResolvedValue({}),
    };
    (scopedPlugin as unknown as { client: unknown }).client = fakeClient;

    await scopedPlugin.handleEvent({
      hook: "after_agent_reply",
      agentId: "agent-1",
      response: "Scoped memory",
    });

    expect(fakeClient.captureMemory).toHaveBeenCalledTimes(1);
    const [, options] = fakeClient.captureMemory.mock.calls[0];
    expect(options.scope).toBe("project:alpha");
  });

  it("activate forwards auth, TLS, and timeout config to the Membrane client", () => {
    const configured = new OpenClawMembranePlugin(
      mockApi({
        grpc_endpoint: "membrane.example.com:443",
        api_key: "plugin-secret",
        tls: true,
        tls_ca_cert_path: "/etc/membrane/ca.pem",
        timeout_ms: 15_000,
      }),
    );

    configured.activate();

    expect(membraneClientMock.instances[0]).toMatchObject({
      addr: "membrane.example.com:443",
      options: {
        apiKey: "plugin-secret",
        tls: true,
        tlsCaCertPath: "/etc/membrane/ca.pem",
        timeoutMs: 15_000,
      },
    });
  });

  it("search uses retrieveGraph and flattens roots before neighbors", async () => {
    const scopedApi = mockApi({ scope: "project:alpha" });
    const scopedPlugin = new OpenClawMembranePlugin(scopedApi);
    const fakeClient = {
      retrieveGraph: vi.fn().mockResolvedValue({
        nodes: [
          {
            root: false,
            hop: 1,
            record: { id: "neighbor-1", type: "episodic", sensitivity: "low", confidence: 1, salience: 1 },
          },
          {
            root: true,
            hop: 0,
            record: { id: "root-1", type: "entity", sensitivity: "low", confidence: 1, salience: 1 },
          },
        ],
        diagnostics: [{ code: "vector_rank_failed", message: "vector index unavailable" }],
      }),
    };
    (scopedPlugin as unknown as { client: unknown }).client = fakeClient;

    const result = await scopedPlugin.search("orchid", { limit: 3, memoryTypes: ["entity", "episodic"] });

    expect(fakeClient.retrieveGraph).toHaveBeenCalledTimes(1);
    expect(fakeClient.retrieveGraph.mock.calls[0]?.[1].memoryTypes).toEqual(["entity", "episodic"]);
    expect(result.map((record) => record.id)).toEqual(["root-1", "neighbor-1"]);
    expect(scopedApi.log.warn).toHaveBeenCalledWith("[membrane] Retrieval degraded during search: vector_rank_failed: vector index unavailable");
  });

  it("search caps flattened graph results to the requested limit", async () => {
    const scopedPlugin = new OpenClawMembranePlugin(mockApi({ scope: "project:alpha" }));
    const fakeClient = {
      retrieveGraph: vi.fn().mockResolvedValue({
        nodes: [
          { root: true, hop: 0, record: { id: "root-1", type: "entity", sensitivity: "low", confidence: 1, salience: 1 } },
          { root: false, hop: 1, record: { id: "neighbor-1", type: "episodic", sensitivity: "low", confidence: 1, salience: 1 } },
          { root: false, hop: 1, record: { id: "neighbor-2", type: "semantic", sensitivity: "low", confidence: 1, salience: 1 } },
        ],
        diagnostics: [],
      }),
    };
    (scopedPlugin as unknown as { client: unknown }).client = fakeClient;

    const result = await scopedPlugin.search("orchid", { limit: 1 });

    expect(fakeClient.retrieveGraph.mock.calls[0]?.[1]).toMatchObject({
      rootLimit: 1,
      nodeLimit: 2,
      edgeLimit: 10,
    });
    expect(result.map((record) => record.id)).toEqual(["root-1"]);
  });

  it("search caps expanded graph budgets at SDK limits", async () => {
    const scopedPlugin = new OpenClawMembranePlugin(mockApi({ scope: "project:alpha" }));
    const fakeClient = {
      retrieveGraph: vi.fn().mockResolvedValue({
        nodes: [
          { root: true, hop: 0, record: { id: "root-1", type: "entity", sensitivity: "low", confidence: 1, salience: 1 } },
        ],
        diagnostics: [],
      }),
    };
    (scopedPlugin as unknown as { client: unknown }).client = fakeClient;

    const result = await scopedPlugin.search("orchid", { limit: 6_000 });

    expect(result.map((record) => record.id)).toEqual(["root-1"]);
    expect(fakeClient.retrieveGraph.mock.calls[0]?.[1]).toMatchObject({
      rootLimit: 6_000,
      nodeLimit: 10_000,
      edgeLimit: 10_000,
    });
  });

  it("search preserves Membrane root relevance order while flattening", async () => {
    const scopedPlugin = new OpenClawMembranePlugin(mockApi({ scope: "project:alpha" }));
    const fakeClient = {
      retrieveGraph: vi.fn().mockResolvedValue({
        nodes: [
          { root: true, hop: 0, record: { id: "root-b", type: "semantic", sensitivity: "low", confidence: 1, salience: 1 } },
          { root: false, hop: 1, record: { id: "neighbor-a", type: "episodic", sensitivity: "low", confidence: 1, salience: 1 } },
          { root: true, hop: 0, record: { id: "root-a", type: "entity", sensitivity: "low", confidence: 1, salience: 1 } },
          { root: false, hop: 1, record: { id: "neighbor-b", type: "competence", sensitivity: "low", confidence: 1, salience: 1 } },
        ],
        diagnostics: [],
      }),
    };
    (scopedPlugin as unknown as { client: unknown }).client = fakeClient;

    const result = await scopedPlugin.search("ranked roots", { limit: 4 });

    expect(result.map((record) => record.id)).toEqual(["root-b", "root-a", "neighbor-a", "neighbor-b"]);
  });

  it("search rejects invalid limits before retrieval", async () => {
    const fakeClient = {
      retrieveGraph: vi.fn().mockResolvedValue({ nodes: [], diagnostics: [] }),
    };
    (plugin as unknown as { client: unknown }).client = fakeClient;

    const result = await plugin.search("orchid", { limit: 0 });

    expect(result).toEqual([]);
    expect(fakeClient.retrieveGraph).not.toHaveBeenCalled();
    expect(api.log.warn).toHaveBeenCalledWith(expect.stringContaining("limit must be an integer from 1 to 10000"));
  });

  it("search rejects invalid salience overrides before retrieval", async () => {
    const fakeClient = {
      retrieveGraph: vi.fn().mockResolvedValue({ nodes: [], diagnostics: [] }),
    };
    (plugin as unknown as { client: unknown }).client = fakeClient;

    const result = await plugin.search("orchid", { min_salience: Number.NaN });

    expect(result).toEqual([]);
    expect(fakeClient.retrieveGraph).not.toHaveBeenCalled();
    expect(api.log.warn).toHaveBeenCalledWith(expect.stringContaining("minSalience must be a finite number from 0 to 1"));
  });

  it("search rejects invalid memory type filters before retrieval", async () => {
    const fakeClient = {
      retrieveGraph: vi.fn().mockResolvedValue({ nodes: [], diagnostics: [] }),
    };
    (plugin as unknown as { client: unknown }).client = fakeClient;

    const result = await plugin.search("orchid", { memoryTypes: ["entity", "unsupported"] });

    expect(result).toEqual([]);
    expect(fakeClient.retrieveGraph).not.toHaveBeenCalled();
    expect(api.log.warn).toHaveBeenCalledWith(expect.stringContaining("memoryTypes[1] must be one of"));
  });

  it("search and context pass configured trust scopes", async () => {
    const scopedPlugin = new OpenClawMembranePlugin(
      mockApi({
        scope: "project:capture",
        trust_scopes: ["project:read", "project:shared"],
        default_sensitivity: "medium",
        max_read_sensitivity: "low",
      }),
    );
    const fakeClient = {
      retrieveGraph: vi.fn().mockResolvedValue({
        nodes: [
          {
            root: true,
            hop: 0,
            record: { id: "root-1", type: "entity", sensitivity: "low", confidence: 1, salience: 1 },
          },
        ],
        edges: [],
        diagnostics: [],
      }),
    };
    (scopedPlugin as unknown as { client: unknown }).client = fakeClient;

    await scopedPlugin.search("orchid");
    await scopedPlugin.getContext("agent-1");

    expect(fakeClient.retrieveGraph).toHaveBeenCalledTimes(2);
    for (const [, options] of fakeClient.retrieveGraph.mock.calls) {
      expect(options.trust).toMatchObject({
        max_sensitivity: "low",
        authenticated: true,
        actor_id: "openclaw",
        scopes: ["project:read", "project:shared"],
      });
    }
  });

  it("fails closed when retrieval scopes are not configured", async () => {
    const fakeClient = {
      retrieveGraph: vi.fn().mockResolvedValue({ nodes: [], diagnostics: [] }),
    };
    (plugin as unknown as { client: unknown }).client = fakeClient;

    const result = await plugin.search("orchid");
    const context = await plugin.getContext("agent-1");

    expect(result).toEqual([]);
    expect(context).toBeNull();
    expect(fakeClient.retrieveGraph).not.toHaveBeenCalled();
    expect(api.log.warn).toHaveBeenCalledWith(expect.stringContaining("Configure scope or trust_scopes"));
    expect(api.log.debug).toHaveBeenCalledWith(expect.stringContaining("Configure scope or trust_scopes"));
  });

  it("getContext formats root and neighbor sections from graph retrieval and logs diagnostics", async () => {
    const scopedApi = mockApi({ scope: "project:alpha" });
    const scopedPlugin = new OpenClawMembranePlugin(scopedApi);
    const fakeClient = {
      retrieveGraph: vi.fn().mockResolvedValue({
        nodes: [
          {
            root: true,
            hop: 0,
            record: {
              id: "root-1",
              type: "entity",
              sensitivity: "low",
              confidence: 1,
              salience: 1,
              interpretation: { summary: "Orchid deploy target" },
              payload: {},
            },
          },
          {
            root: false,
            hop: 1,
            record: {
              id: "neighbor-1",
              type: "episodic",
              sensitivity: "low",
              confidence: 1,
              salience: 1,
              payload: { summary: "Used Orchid during rollout verification" },
            },
          },
        ],
        edges: [
          {
            source_id: "root-1",
            predicate: "mentioned_in",
            target_id: "neighbor-1",
            weight: 1,
          },
        ],
        diagnostics: [{ code: "embedding_query_failed", message: "embedding service unavailable" }],
      }),
    };
    (scopedPlugin as unknown as { client: unknown }).client = fakeClient;

    const context = await scopedPlugin.getContext("agent-1");

    expect(fakeClient.retrieveGraph).toHaveBeenCalledTimes(1);
    expect(context).toContain("Membrane graph context:");
    expect(context).toContain("Roots:");
    expect(context).toContain("Neighbors:");
    expect(context).toContain("Relations:");
    expect(context).toContain("Orchid deploy target");
    expect(context).toContain("Used Orchid during rollout verification");
    expect(context).toContain("Orchid deploy target --mentioned_in--> Used Orchid during rollout verification");
    expect(scopedApi.log.warn).toHaveBeenCalledWith("[membrane] Retrieval degraded during context: embedding_query_failed: embedding service unavailable");
  });
});
