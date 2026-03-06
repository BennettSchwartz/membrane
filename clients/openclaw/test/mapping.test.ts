import { describe, it, expect } from "vitest";
import { mapSensitivity, mapEventKind, summarize, buildTags } from "../src/mapping.js";
import type { OpenClawEvent } from "../src/types.js";

describe("mapSensitivity", () => {
  it("maps known levels", () => {
    expect(mapSensitivity("public")).toBe("public");
    expect(mapSensitivity("high")).toBe("high");
    expect(mapSensitivity("hyper")).toBe("hyper");
  });

  it("defaults unknown to low", () => {
    expect(mapSensitivity("unknown")).toBe("low");
    expect(mapSensitivity("")).toBe("low");
  });
});

describe("mapEventKind", () => {
  it("maps tool calls to tool_output", () => {
    const event: OpenClawEvent = { hook: "after_tool_call", toolName: "exec" };
    expect(mapEventKind(event)).toBe("tool_output");
  });

  it("maps agent replies to event", () => {
    const event: OpenClawEvent = { hook: "after_agent_reply", response: "Hello" };
    expect(mapEventKind(event)).toBe("event");
  });

  it("defaults to observation", () => {
    const event: OpenClawEvent = { hook: "before_agent_start" };
    expect(mapEventKind(event)).toBe("observation");
  });
});

describe("summarize", () => {
  it("summarizes tool calls", () => {
    const event: OpenClawEvent = {
      hook: "after_tool_call",
      toolName: "exec",
      toolParams: { command: "ls" },
    };
    expect(summarize(event)).toBe("Tool call: exec(command)");
  });

  it("summarizes agent replies with truncation", () => {
    const event: OpenClawEvent = {
      hook: "after_agent_reply",
      response: "x".repeat(300),
    };
    const result = summarize(event);
    expect(result).toContain("Agent reply:");
    expect(result).toContain("...");
    expect(result.length).toBeLessThan(250);
  });

  it("handles generic events", () => {
    const event: OpenClawEvent = { hook: "unknown_hook" };
    expect(summarize(event)).toBe("Event: unknown_hook");
  });
});

describe("buildTags", () => {
  it("builds tags from event", () => {
    const event: OpenClawEvent = {
      hook: "after_tool_call",
      agentId: "main",
      toolName: "exec",
      sessionKey: "agent:main:main",
    };
    const tags = buildTags(event);
    expect(tags).toContain("hook:after_tool_call");
    expect(tags).toContain("agent:main");
    expect(tags).toContain("tool:exec");
    expect(tags).toContain("session:agent:main:main");
  });

  it("omits missing fields", () => {
    const event: OpenClawEvent = { hook: "test" };
    const tags = buildTags(event);
    expect(tags).toEqual(["hook:test"]);
  });
});
