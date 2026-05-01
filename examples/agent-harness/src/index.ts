import "dotenv/config";

import { MembraneClient } from "@bennettschwartz/membrane";

import { runAgentTurn } from "./agent.js";
import { createLlmClient, hasLlmCredentials } from "./llm.js";
import { startOrConnectMembrane } from "./membrane-daemon.js";
import { GRAPH_MEMORY_TYPES, seedScenario, trustContext } from "./scenario.js";
import { MembraneToolRuntime } from "./tools.js";

async function main(): Promise<void> {
  const task =
    process.argv.slice(2).join(" ").trim() ||
    "The auth-service has a latency regression. Retrieve memory, teach any new facts, and propose a safe fix plan.";

  const runtime = await startOrConnectMembrane();
  const client = new MembraneClient(runtime.addr, { apiKey: runtime.apiKey, timeoutMs: 10_000 });

  try {
    await seedScenario(client);

    if (!hasLlmCredentials()) {
      const graph = await client.retrieveGraph(task, {
        trust: trustContext("agent-harness-cli"),
        memoryTypes: [...GRAPH_MEMORY_TYPES],
        rootLimit: 16,
        nodeLimit: 72,
        edgeLimit: 180,
        maxHops: 2
      });
      console.log("No LLM API key found. Seeded Membrane and retrieved graph context only.");
      console.log(JSON.stringify(graph, null, 2));
      return;
    }

    const { client: llm, model } = createLlmClient();
    const toolRuntime = new MembraneToolRuntime(client);
    const result = await runAgentTurn({ llm, model, runtime: toolRuntime, task });

    console.log("\n=== Agent Answer ===\n");
    console.log(result.answer);
    console.log("\n=== Membrane Tool Trace ===\n");
    console.log(JSON.stringify(result.trace, null, 2));
  } finally {
    client.close();
    await runtime.close();
  }
}

main().catch((error: unknown) => {
  console.error("Agent harness failed");
  console.error(error);
  process.exitCode = 1;
});
