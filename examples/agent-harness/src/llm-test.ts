import "dotenv/config";

import assert from "node:assert/strict";

import { MembraneClient, MemoryType } from "@gustycube/membrane";

import { runAgentTurn } from "./agent.js";
import { createLlmClient } from "./llm.js";
import { startOrConnectMembrane } from "./membrane-daemon.js";
import { GRAPH_MEMORY_TYPES, seedScenario, trustContext } from "./scenario.js";
import { MembraneToolRuntime, type ToolTraceEntry } from "./tools.js";

function traceSummary(entry: ToolTraceEntry): Record<string, unknown> {
  return {
    name: entry.name,
    args:
      entry.name.startsWith("membrane_capture_") || entry.name === "membrane_retrieve_by_id" ? entry.args : undefined,
    recordIds: entry.recordIds
  };
}

function modelTrace(trace: ToolTraceEntry[]): ToolTraceEntry[] {
  return trace.filter((entry) => !entry.forced);
}

async function runTurn(
  client: MembraneClient,
  task: string,
  expectations: { retrievedId?: string; expectedTools?: string[] } = {}
) {
  const { client: llm, model, timeoutMs, maxTokens } = createLlmClient();
  const runtime = new MembraneToolRuntime(client);
  console.error(`Running live LLM turn with ${model} (timeout ${timeoutMs}ms, max ${maxTokens} tokens)`);
  const result = await runAgentTurn({ llm, model, runtime, task, maxTokens });

  assert.ok(result.answer.length > 0, "agent should produce a final answer");
  assert.ok(
    result.llmCalls.every((call) => call.finishReason !== "length"),
    "LLM answers should finish before hitting max token length"
  );
  assert.match(result.answer, /Membrane trace:/, "final answer should include deterministic Membrane trace");
  assert.ok(
    result.trace.some((entry) => entry.recordIds.some((id) => result.answer.includes(id))),
    "final answer trace should cite exact memory IDs"
  );
  const chosenTools = modelTrace(result.trace);

  if (expectations.retrievedId) {
    assert.ok(
      chosenTools.some(
        (entry) => entry.name === "membrane_retrieve_by_id" && entry.args.id === expectations.retrievedId
      ),
      "natural-language exact-record requests should make the model call membrane_retrieve_by_id"
    );
  }
  for (const toolName of expectations.expectedTools ?? []) {
    assert.ok(
      chosenTools.some((entry) => entry.name === toolName),
      `natural-language requests should make the model choose ${toolName}`
    );
  }

  return result;
}

async function main(): Promise<void> {
  const runtime = await startOrConnectMembrane();
  const client = new MembraneClient(runtime.addr, { apiKey: runtime.apiKey, timeoutMs: 10_000 });

  try {
    const seeded = await seedScenario(client);

    const first = await runTurn(
      client,
      [
        "That auth-service latency issue we discussed is happening again during the Project Orion canary.",
        "Before you give me a plan, use whatever memory you have about this service and keep track of the current task state.",
        "Also remember for next time that auth-service's runbook_owner is Dana, and log the current observation that checkout retries spiked right after the deploy."
      ].join("\n"),
      {
        expectedTools: [
          "membrane_retrieve_graph",
          "membrane_capture_fact",
          "membrane_capture_working_state",
          "membrane_capture_episode"
        ]
      }
    );

    const second = await runTurn(
      client,
      [
        `Can you use the Dana owner memory from the last turn and fetch the exact memory record ${seeded.semantic.id} so I can cite it?`,
        "Also remember Project Orion's release_guardrail: the canary must stay at 5 percent until p95 is below 200ms.",
        "Then give me a safe rollout plan."
      ].join("\n"),
      {
        retrievedId: seeded.semantic.id,
        expectedTools: ["membrane_retrieve_graph", "membrane_retrieve_by_id", "membrane_capture_fact"]
      }
    );

    const graph = await client.retrieveGraph("auth-service runbook owner Dana Project Orion release guardrail", {
      trust: trustContext("agent-harness-llm-test"),
      memoryTypes: [...GRAPH_MEMORY_TYPES],
      rootLimit: 40,
      nodeLimit: 120,
      edgeLimit: 240,
      maxHops: 2
    });

    const finalTypes = new Set(graph.nodes.map((node) => node.record.type));
    for (const memoryType of GRAPH_MEMORY_TYPES) {
      assert.ok(finalTypes.has(memoryType), `final live graph should include ${memoryType}`);
    }

    assert.ok(
      graph.nodes.some((node) => {
        if (node.record.type !== MemoryType.SEMANTIC || !node.record.payload || typeof node.record.payload !== "object") {
          return false;
        }
        const payload = node.record.payload as { predicate?: string; object?: unknown };
        return payload.predicate === "runbook_owner";
      }),
      "final graph should contain the taught runbook_owner fact"
    );

    assert.ok(
      graph.nodes.some((node) => {
        if (node.record.type !== MemoryType.SEMANTIC || !node.record.payload || typeof node.record.payload !== "object") {
          return false;
        }
        const payload = node.record.payload as { predicate?: string; object?: unknown };
        return payload.predicate === "release_guardrail";
      }),
      "final graph should contain the taught release_guardrail fact"
    );

    console.log("PASS live LLM agent harness");
    console.log(
      JSON.stringify(
        {
          first_trace: modelTrace(first.trace).map(traceSummary),
          first_performance: {
            durationMs: first.durationMs,
            llmCalls: first.llmCalls,
            tools: first.trace.map((entry) => ({
              name: entry.name,
              durationMs: entry.durationMs,
              internal: entry.forced || undefined
            }))
          },
          second_trace: modelTrace(second.trace).map(traceSummary),
          second_performance: {
            durationMs: second.durationMs,
            llmCalls: second.llmCalls,
            tools: second.trace.map((entry) => ({
              name: entry.name,
              durationMs: entry.durationMs,
              internal: entry.forced || undefined
            }))
          },
          graph_nodes: graph.nodes.map((node) => ({ id: node.record.id, type: node.record.type }))
        },
        null,
        2
      )
    );
  } finally {
    client.close();
    await runtime.close();
  }
}

main().catch((error: unknown) => {
  console.error("FAIL live LLM agent harness");
  console.error(error);
  process.exitCode = 1;
});
