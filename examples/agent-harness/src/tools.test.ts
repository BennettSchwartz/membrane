import assert from "node:assert/strict";
import test from "node:test";

import { MembraneToolRuntime } from "./tools.js";
import type { MembraneClient } from "@bennettschwartz/membrane";

test("membrane_retrieve_graph exposes retrieval diagnostics in tool output and trace", async () => {
  const client = {
    retrieveGraph: async () => ({
      root_ids: ["root-1"],
      nodes: [
        {
          root: true,
          hop: 0,
          record: {
            id: "root-1",
            type: "semantic",
            sensitivity: "low",
            confidence: 1,
            salience: 1,
            payload: { summary: "Root memory" }
          }
        }
      ],
      edges: [],
      diagnostics: [{ code: "vector_rank_failed", message: "pgvector ranking unavailable" }]
    })
  } as unknown as MembraneClient;

  const runtime = new MembraneToolRuntime(client);

  const result = await runtime.execute("membrane_retrieve_graph", { task_descriptor: "auth incident" });

  assert.deepEqual((result.value as { diagnostics?: unknown }).diagnostics, [
    { code: "vector_rank_failed", message: "pgvector ranking unavailable" }
  ]);
  assert.match(result.content, /vector_rank_failed/);
  assert.deepEqual((runtime.trace[0]?.result as { diagnostics?: unknown }).diagnostics, [
    { code: "vector_rank_failed", message: "pgvector ranking unavailable" }
  ]);
});

test("membrane_retrieve_graph sanitizes optional budgets and memory types", async () => {
  const calls: Array<{ task: string; options: Record<string, unknown> }> = [];
  const client = {
    retrieveGraph: async (task: string, options: Record<string, unknown>) => {
      calls.push({ task, options });
      return {
        root_ids: [],
        nodes: [],
        edges: []
      };
    }
  } as unknown as MembraneClient;

  const runtime = new MembraneToolRuntime(client);

  await runtime.execute("membrane_retrieve_graph", {
    task_descriptor: "auth incident",
    memory_types: ["entity", "unsupported", 42],
    root_limit: "many",
    node_limit: -1,
    edge_limit: 2.5,
    max_hops: -9
  });

  assert.equal(calls.length, 1);
  assert.equal(calls[0]?.task, "auth incident");
  assert.deepEqual(calls[0]?.options.memoryTypes, ["entity"]);
  assert.equal(calls[0]?.options.rootLimit, 16);
  assert.equal(calls[0]?.options.nodeLimit, 64);
  assert.equal(calls[0]?.options.edgeLimit, 160);
  assert.equal(calls[0]?.options.maxHops, 2);
});
