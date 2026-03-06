import { describe, it, expect, vi, beforeEach } from "vitest";
import { OpenClawMembranePlugin, createConfig, validateConfig, DEFAULT_CONFIG } from "../src/index.js";
import type { PluginApi, OpenClawEvent } from "../src/types.js";

function mockApi(overrides: Record<string, unknown> = {}): PluginApi {
  return {
    config: { grpc_endpoint: "localhost:4222", ...overrides },
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
    expect(config.grpc_endpoint).toBe("localhost:4222");
    expect(config.auto_context).toBe(true);
    expect(config.context_limit).toBe(5);
  });

  it("merges user config over defaults", () => {
    const config = createConfig({ context_limit: 10, auto_context: false });
    expect(config.context_limit).toBe(10);
    expect(config.auto_context).toBe(false);
    expect(config.grpc_endpoint).toBe("localhost:4222"); // default preserved
  });

  it("ignores invalid types", () => {
    const config = createConfig({ buffer_size: "not-a-number" });
    expect(config.buffer_size).toBe(DEFAULT_CONFIG.buffer_size);
  });
});

describe("validateConfig", () => {
  it("returns empty for undefined input", () => {
    expect(validateConfig(undefined)).toEqual({});
  });

  it("filters context_types to strings only", () => {
    const result = validateConfig({ context_types: ["event", 42, "observation"] });
    expect(result.context_types).toEqual(["event", "observation"]);
  });
});

describe("OpenClawMembranePlugin", () => {
  let plugin: OpenClawMembranePlugin;
  let api: PluginApi;

  beforeEach(() => {
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
    expect(status.endpoint).toBe("localhost:4222");
  });

  it("handleEvent is a no-op when not activated", async () => {
    const event: OpenClawEvent = { hook: "after_agent_reply", response: "Hello" };
    // Should not throw
    await plugin.handleEvent(event);
  });
});
