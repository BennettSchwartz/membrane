import { MembraneClient, Sensitivity, SourceKind, type MemoryRecord } from "../src/index";
import type { RpcMethodName, RpcRequest, RpcResponse, RpcTransport } from "../src/internal/grpc";

class FakeTransport implements RpcTransport {
  readonly calls: Array<{ method: string; request: Record<string, unknown> }> = [];
  private readonly responses: unknown[];

  constructor(...responses: unknown[]) {
    this.responses = responses;
  }

  async unary<M extends RpcMethodName>(methodName: M, request: RpcRequest<M>): Promise<RpcResponse<M>> {
    this.calls.push({ method: methodName, request: request as unknown as Record<string, unknown> });
    const response = this.responses.shift();
    return response as RpcResponse<M>;
  }

  close(): void {
    // no-op for test transport
  }
}

function asRecord(id: string): MemoryRecord {
  return {
    id,
    type: "episodic",
    sensitivity: "low",
    confidence: 1,
    salience: 1
  };
}

describe("MembraneClient unit", () => {
  it("captureMemory encodes rich content and parses created graph artifacts", async () => {
    const primary = asRecord("source-1");
    const entity = {
      ...asRecord("entity-1"),
      type: "entity",
      payload: {
        kind: "entity",
        canonical_name: "Membrane",
        primary_type: "Project",
        types: ["Project"],
        aliases: [{ value: "membrane" }]
      }
    };

    const transport = new FakeTransport({
      primary_record: primary,
      created_records: [entity],
      edges: [{ source_id: "source-1", predicate: "mentions_entity", target_id: "entity-1" }]
    });
    const client = new MembraneClient("localhost:9090", { transport });

    const result = await client.captureMemory(
      {
        text: "Membrane stores relational memory"
      },
      {
        sourceKind: SourceKind.AGENT_TURN,
        context: { thread_id: "thread-1" },
        reasonToRemember: "important architecture note",
        proposedType: "semantic",
        tags: ["memory"]
      }
    );

    expect(transport.calls[0]?.method).toBe("CaptureMemory");
    expect(transport.calls[0]?.request.content).toMatchObject({ kind: "structValue" });
    expect(transport.calls[0]?.request.context).toMatchObject({ kind: "structValue" });
    expect(transport.calls[0]?.request.source_kind).toBe("agent_turn");
    expect(result.primary_record.id).toBe("source-1");
    expect(result.created_records[0]?.id).toBe("entity-1");
    expect(result.edges[0]?.predicate).toBe("mentions_entity");
  });

  it("retrieveGraph parses graph nodes, edges, and selection", async () => {
    const transport = new FakeTransport({
      nodes: [
        { record: asRecord("root-1"), root: true, hop: 0 },
        { record: asRecord("neighbor-1"), root: false, hop: 1 }
      ],
      edges: [{ source_id: "root-1", predicate: "mentions_entity", target_id: "neighbor-1" }],
      root_ids: ["root-1"],
      selection: {
        selected: [asRecord("root-1")],
        confidence: 0.8,
        needs_more: false,
        scores: { "root-1": 0.91 }
      }
    });
    const client = new MembraneClient("localhost:9090", { transport });

    const result = await client.retrieveGraph("membrane graph", { rootLimit: 3, maxHops: 2 });

    expect(transport.calls[0]?.method).toBe("RetrieveGraph");
    expect(transport.calls[0]?.request.root_limit).toBe(3);
    expect(transport.calls[0]?.request.max_hops).toBe(2);
    expect(result.nodes).toHaveLength(2);
    expect(result.nodes[0]?.record.id).toBe("root-1");
    expect(result.edges[0]?.target_id).toBe("neighbor-1");
    expect(result.root_ids).toEqual(["root-1"]);
    expect(result.selection?.confidence).toBe(0.8);
    expect(result.selection?.scores?.["root-1"]).toBe(0.91);
  });

  it("getMetrics parses snapshot payload", async () => {
    const transport = new FakeTransport({
      snapshot: {
        kind: "structValue",
        structValue: {
          fields: {
            total_records: { kind: "numberValue", numberValue: 42 }
          }
        }
      }
    });
    const client = new MembraneClient("localhost:9090", { transport });

    const snapshot = await client.getMetrics();

    expect(snapshot.total_records).toBe(42);
  });

  it("supports snake_case method aliases", async () => {
    const aliasCases = [
      {
        name: "capture_memory",
        expectedMethod: "CaptureMemory",
        response: {
          primary_record: asRecord("alias-capture"),
          created_records: [],
          edges: []
        },
        invoke: (client: MembraneClient) => client.capture_memory({ text: "remember this" }),
        assertResult: (result: unknown) => expect((result as { primary_record: MemoryRecord }).primary_record.id).toBe("alias-capture")
      },
      {
        name: "retrieve_graph",
        expectedMethod: "RetrieveGraph",
        response: {
          nodes: [{ record: asRecord("alias-graph"), root: true, hop: 0 }],
          edges: [],
          root_ids: ["alias-graph"]
        },
        invoke: (client: MembraneClient) => client.retrieve_graph("alias graph"),
        assertResult: (result: unknown) =>
          expect((result as { nodes: Array<{ record: MemoryRecord }> }).nodes[0]?.record.id).toBe("alias-graph")
      },
      {
        name: "retrieve_by_id",
        expectedMethod: "RetrieveByID",
        response: { record: asRecord("alias-retrieve") },
        invoke: (client: MembraneClient) => client.retrieve_by_id("rec-1"),
        assertResult: (result: unknown) => expect((result as MemoryRecord).id).toBe("alias-retrieve")
      },
      {
        name: "get_metrics",
        expectedMethod: "GetMetrics",
        response: {
          snapshot: {
            kind: "structValue",
            structValue: {
              fields: {
                total_records: { kind: "numberValue", numberValue: 7 }
              }
            }
          }
        },
        invoke: (client: MembraneClient) => client.get_metrics(),
        assertResult: (result: unknown) => expect((result as { total_records: number }).total_records).toBe(7)
      }
    ] satisfies Array<{
      name: string;
      expectedMethod: string;
      response: unknown;
      invoke: (client: MembraneClient) => Promise<unknown>;
      assertResult: (result: unknown) => void;
    }>;

    for (const testCase of aliasCases) {
      const transport = new FakeTransport(testCase.response);
      const client = new MembraneClient("localhost:9090", { transport });

      const result = await testCase.invoke(client);

      testCase.assertResult(result);
      expect(transport.calls[0]?.method).toBe(testCase.expectedMethod);
    }
  });
});
