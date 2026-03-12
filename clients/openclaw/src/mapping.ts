/**
 * Maps OpenClaw events to Membrane ingestion formats.
 */

import type { OpenClawEvent } from "./types.js";

/** Map OpenClaw sensitivity strings to Membrane sensitivity levels */
export function mapSensitivity(level: string): string {
  const map: Record<string, string> = {
    public: "public",
    low: "low",
    medium: "medium",
    high: "high",
    hyper: "hyper",
  };
  return map[level] ?? "low";
}

/** Determine the best Membrane ingestion method for an OpenClaw event */
export function mapEventKind(event: OpenClawEvent): "event" | "tool_output" | "observation" {
  if (event.hook === "after_tool_call" && event.toolName) {
    return "tool_output";
  }
  if (event.hook === "after_agent_reply") {
    return "event";
  }
  return "observation";
}

/** Extract a human-readable summary from an OpenClaw event */
export function summarize(event: OpenClawEvent): string {
  if (event.hook === "after_tool_call" && event.toolName) {
    const args = event.toolParams
      ? Object.keys(event.toolParams).join(", ")
      : "";
    return `Tool call: ${event.toolName}(${args})`;
  }
  if (event.hook === "after_agent_reply" && event.response) {
    const preview = event.response.slice(0, 200);
    return `Agent reply: ${preview}${event.response.length > 200 ? "..." : ""}`;
  }
  return `Event: ${event.hook}`;
}

/** Build tags from an OpenClaw event */
export function buildTags(event: OpenClawEvent): string[] {
  const tags: string[] = [`hook:${event.hook}`];
  if (event.agentId) tags.push(`agent:${event.agentId}`);
  if (event.toolName) tags.push(`tool:${event.toolName}`);
  if (event.sessionKey) tags.push(`session:${event.sessionKey}`);
  return tags;
}
