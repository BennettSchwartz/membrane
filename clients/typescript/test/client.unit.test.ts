import net from "node:net";

import { MembraneClient, MemoryType, Sensitivity, SourceKind, type CaptureMemoryOptions, type MembraneClientOptions, type MemoryRecord, type TrustContext } from "../src/index";
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

  it("captureMemory defaults omitted sourceKind to event", async () => {
    const transport = new FakeTransport({ primary_record: asRecord("source-default"), created_records: [], edges: [] });
    const client = new MembraneClient("localhost:9090", { transport });

    await client.captureMemory({ text: "default source kind" });

    expect(transport.calls[0]?.request.source_kind).toBe("event");
  });

  it("captureMemory accepts absent protobuf interpretation messages", async () => {
    const transport = new FakeTransport({
      primary_record: { ...asRecord("source-1"), interpretation: null },
      created_records: [{ ...asRecord("entity-1"), type: "entity", interpretation: null }],
      edges: []
    });
    const client = new MembraneClient("localhost:9090", { transport });

    const result = await client.captureMemory({ text: "no interpretation" });

    expect(result.primary_record.id).toBe("source-1");
    expect(result.primary_record.interpretation).toBeUndefined();
    expect(result.created_records[0]?.id).toBe("entity-1");
    expect(result.created_records[0]?.interpretation).toBeUndefined();
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
      },
      diagnostics: [{ code: "vector_rank_failed", message: "vector index unavailable" }],
      projection: {
        relations_omitted: false,
        relations_truncated: true,
        history_omitted: true,
        records_truncated: true
      }
    });
    const client = new MembraneClient("localhost:9090", { transport });

    const result = await client.retrieveGraph("membrane graph", {
      rootLimit: 3,
      maxHops: 2,
      queryEmbedding: [0.25, 0.5, 0.75]
    });

    expect(transport.calls[0]?.method).toBe("RetrieveGraph");
    expect(transport.calls[0]?.request.root_limit).toBe(3);
    expect(transport.calls[0]?.request.max_hops).toBe(2);
    expect(transport.calls[0]?.request.query_embedding).toEqual([0.25, 0.5, 0.75]);
    expect(result.nodes).toHaveLength(2);
    expect(result.nodes[0]?.record.id).toBe("root-1");
    expect(result.edges[0]?.target_id).toBe("neighbor-1");
    expect(result.root_ids).toEqual(["root-1"]);
    expect(result.selection?.confidence).toBe(0.8);
    expect(result.selection?.scores?.["root-1"]).toBe(0.91);
    expect(result.diagnostics?.[0]?.code).toBe("vector_rank_failed");
    expect(result.diagnostics?.[0]?.message).toContain("vector index");
    expect(result.projection).toEqual({
      relations_omitted: false,
      relations_truncated: true,
      history_omitted: true,
      records_truncated: true
    });
  });

  it("retrieveGraph maps rootOnly to the daemon roots-only sentinel", async () => {
    const transport = new FakeTransport({ nodes: [], edges: [], root_ids: [] });
    const client = new MembraneClient("localhost:9090", { transport });

    await client.retrieveGraph("roots only graph", { rootOnly: true, maxHops: 2 });

    expect(transport.calls[0]?.request.max_hops).toBe(-1);
  });

  it("capture_memory accepts snake_case option aliases", async () => {
    const primary = asRecord("source-snake");
    const transport = new FakeTransport({ primary_record: primary, created_records: [], edges: [] });
    const client = new MembraneClient("localhost:9090", { transport });

    await client.capture_memory(
      { text: "snake options" },
      {
        source_kind: SourceKind.OBSERVATION,
        reason_to_remember: "preserve snake_case ergonomics",
        proposed_type: "semantic",
        tags: ["sdk"],
        sensitivity: Sensitivity.LOW
      }
    );

    expect(transport.calls[0]?.request.source_kind).toBe("observation");
    expect(transport.calls[0]?.request.reason_to_remember).toBe("preserve snake_case ergonomics");
    expect(transport.calls[0]?.request.proposed_type).toBe("semantic");
  });

  it("retrieve_graph accepts snake_case option aliases", async () => {
    const transport = new FakeTransport({ nodes: [], edges: [], root_ids: [] });
    const client = new MembraneClient("localhost:9090", { transport });

    await client.retrieve_graph("snake graph options", {
      memory_types: [MemoryType.ENTITY, MemoryType.SEMANTIC],
      min_salience: 0.2,
      root_limit: 4,
      node_limit: 8,
      edge_limit: 12,
      max_hops: 3,
      query_embedding: [0.1, 0.2, 0.3]
    });

    expect(transport.calls[0]?.request.memory_types).toEqual(["entity", "semantic"]);
    expect(transport.calls[0]?.request.min_salience).toBe(0.2);
    expect(transport.calls[0]?.request.root_limit).toBe(4);
    expect(transport.calls[0]?.request.node_limit).toBe(8);
    expect(transport.calls[0]?.request.edge_limit).toBe(12);
    expect(transport.calls[0]?.request.max_hops).toBe(3);
    expect(transport.calls[0]?.request.query_embedding).toEqual([0.1, 0.2, 0.3]);
  });

  it("refuses to send API keys over non-local plaintext gRPC by default", () => {
    expect(() => new MembraneClient("membrane.example.com:9090", { apiKey: "secret" })).toThrow(/plaintext gRPC/);
    expect(() => new MembraneClient("127.0.0.1:9090", { apiKey: "secret" })).not.toThrow();
    expect(() => new MembraneClient("membrane.example.com:9090", {
      apiKey: "secret",
      allowInsecureCredentials: true
    })).not.toThrow();
  });

  it("rejects a non-boolean plaintext credential override at runtime", () => {
    const options = {
      apiKey: "secret",
      allowInsecureCredentials: "false"
    } as unknown as MembraneClientOptions;

    expect(() => new MembraneClient("membrane.example.com:9090", options)).toThrow(
      /allowInsecureCredentials must be a boolean/
    );
  });

  it("does not proxy loopback plaintext API-key connections", async () => {
    let targetConnections = 0;
    let proxyConnections = 0;
    const target = net.createServer((socket) => {
      targetConnections += 1;
      socket.destroy();
    });
    const proxy = net.createServer((socket) => {
      proxyConnections += 1;
      socket.destroy();
    });

    const listen = async (server: net.Server): Promise<number> => {
      await new Promise<void>((resolve, reject) => {
        server.once("error", reject);
        server.listen(0, "127.0.0.1", () => {
          server.off("error", reject);
          resolve();
        });
      });
      const address = server.address();
      if (!address || typeof address === "string") {
        throw new Error("test server did not expose a TCP port");
      }
      return address.port;
    };
    const close = async (server: net.Server): Promise<void> => {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    };
    const restoreEnv = (name: string, value: string | undefined): void => {
      if (value === undefined) {
        delete process.env[name];
      } else {
        process.env[name] = value;
      }
    };

    const targetPort = await listen(target);
    const proxyPort = await listen(proxy);
    const previousGrpcProxy = process.env.grpc_proxy;
    const previousNoGrpcProxy = process.env.no_grpc_proxy;
    const previousNoProxy = process.env.no_proxy;
    const previousUpperNoProxy = process.env.NO_PROXY;

    process.env.grpc_proxy = `http://127.0.0.1:${proxyPort}`;
    process.env.no_grpc_proxy = "";
    process.env.no_proxy = "";
    process.env.NO_PROXY = "";

    let client: MembraneClient | undefined;
    try {
      client = new MembraneClient(`127.0.0.1:${targetPort}`, {
        apiKey: "security-test-secret",
        timeoutMs: 300
      });
      await expect(client.getMetrics()).rejects.toThrow();
      await new Promise((resolve) => setTimeout(resolve, 100));

      expect(targetConnections).toBeGreaterThan(0);
      expect(proxyConnections).toBe(0);
    } finally {
      client?.close();
      restoreEnv("grpc_proxy", previousGrpcProxy);
      restoreEnv("no_grpc_proxy", previousNoGrpcProxy);
      restoreEnv("no_proxy", previousNoProxy);
      restoreEnv("NO_PROXY", previousUpperNoProxy);
      await Promise.all([close(target), close(proxy)]);
    }
  });

  it("retrieveGraph canonicalizes duplicate memory types before transport", async () => {
    const transport = new FakeTransport({ nodes: [], edges: [], root_ids: [] });
    const client = new MembraneClient("localhost:9090", { transport });

    await client.retrieveGraph("canonical memory types", {
      memoryTypes: [MemoryType.EPISODIC, MemoryType.SEMANTIC, MemoryType.ENTITY, MemoryType.SEMANTIC],
    });

    expect(transport.calls[0]?.request.memory_types).toEqual(["entity", "semantic", "episodic"]);
  });

  it("retrieve_graph root_only takes precedence over max_hops", async () => {
    const transport = new FakeTransport({ nodes: [], edges: [], root_ids: [] });
    const client = new MembraneClient("localhost:9090", { transport });

    await client.retrieve_graph("snake roots only", {
      root_only: true,
      max_hops: 3
    });

    expect(transport.calls[0]?.request.max_hops).toBe(-1);
  });

  it("retrieveGraph rejects non-boolean rootOnly before transport", async () => {
    const transport = new FakeTransport({ nodes: [], edges: [], root_ids: [] });
    const client = new MembraneClient("localhost:9090", { transport });

    await expect(client.retrieveGraph("invalid roots-only flag", { rootOnly: "true" as unknown as boolean })).rejects.toThrow(/rootOnly/);
    expect(transport.calls).toHaveLength(0);
  });

  it("retrieveGraph rejects invalid query embeddings before transport", async () => {
    for (const queryEmbedding of [[0, 0], [0.1, Number.NaN], [Number.POSITIVE_INFINITY]]) {
      const transport = new FakeTransport({ nodes: [], edges: [], root_ids: [] });
      const client = new MembraneClient("localhost:9090", { transport });

      await expect(client.retrieveGraph("invalid vector", { queryEmbedding })).rejects.toThrow(/queryEmbedding/);
      expect(transport.calls).toHaveLength(0);
    }
  });

  it("retrieveGraph rejects out-of-range minSalience before transport", async () => {
    for (const minSalience of [-0.1, Number.NaN, Number.POSITIVE_INFINITY, 1.1]) {
      const transport = new FakeTransport({ nodes: [], edges: [], root_ids: [] });
      const client = new MembraneClient("localhost:9090", { transport });

      await expect(client.retrieveGraph("invalid salience", { minSalience })).rejects.toThrow(/minSalience/);
      expect(transport.calls).toHaveLength(0);
    }
  });

  it("retrieveGraph rejects invalid negative maxHops before transport", async () => {
    const transport = new FakeTransport({ nodes: [], edges: [], root_ids: [] });
    const client = new MembraneClient("localhost:9090", { transport });

    await expect(client.retrieveGraph("invalid graph depth", { maxHops: -2 })).rejects.toThrow(/maxHops/);
    expect(transport.calls).toHaveLength(0);
  });

  it("retrieveGraph rejects invalid graph budgets before transport", async () => {
    for (const options of [
      { rootLimit: -1 },
      { nodeLimit: 1.5 },
      { edgeLimit: 10001 },
      { minSalience: Number.NaN },
      { minSalience: -0.1 }
    ]) {
      const transport = new FakeTransport({ nodes: [], edges: [], root_ids: [] });
      const client = new MembraneClient("localhost:9090", { transport });

      await expect(client.retrieveGraph("invalid graph budget", options)).rejects.toThrow(/rootLimit|nodeLimit|edgeLimit|minSalience/);
      expect(transport.calls).toHaveLength(0);
    }
  });

  it("retrieveGraph rejects non-string task descriptors before transport", async () => {
    const transport = new FakeTransport({ nodes: [], edges: [], root_ids: [] });
    const client = new MembraneClient("localhost:9090", { transport });

    await expect(client.retrieveGraph(42 as unknown as string)).rejects.toThrow(/taskDescriptor/);
    expect(transport.calls).toHaveLength(0);
  });

  it("captureMemory rejects invalid enum options before transport", async () => {
    for (const options of [
      { sourceKind: "unsupported" },
      { proposedType: "unsupported" },
      { sensitivity: "classified" }
    ]) {
      const transport = new FakeTransport({ primary_record: asRecord("unused"), created_records: [], edges: [] });
      const client = new MembraneClient("localhost:9090", { transport });

      await expect(client.captureMemory({ text: "invalid enum" }, options)).rejects.toThrow(/sourceKind|proposedType|sensitivity/);
      expect(transport.calls).toHaveLength(0);
    }
  });

  it("captureMemory rejects invalid tag and scope shapes before transport", async () => {
    for (const options of [
      { source: "" },
      { source: 7 },
      { reasonToRemember: 7 },
      { summary: false },
      { timestamp: 42 },
      { tags: "sdk" },
      { tags: ["sdk", 7] },
      { scope: 42 }
    ]) {
      const transport = new FakeTransport({ primary_record: asRecord("unused"), created_records: [], edges: [] });
      const client = new MembraneClient("localhost:9090", { transport });

      await expect(client.captureMemory({ text: "invalid metadata" }, options as CaptureMemoryOptions)).rejects.toThrow(/source|reasonToRemember|summary|timestamp|tags|scope/);
      expect(transport.calls).toHaveLength(0);
    }
  });

  it("retrieveGraph rejects invalid memory types and trust sensitivity before transport", async () => {
    for (const options of [
      { memoryTypes: ["entity", "unsupported"] },
      { trust: { max_sensitivity: "classified", authenticated: true, actor_id: "tester", scopes: ["prod"] } }
    ]) {
      const transport = new FakeTransport({ nodes: [], edges: [], root_ids: [] });
      const client = new MembraneClient("localhost:9090", { transport });

      await expect(client.retrieveGraph("invalid retrieval enums", options)).rejects.toThrow(/memoryTypes|trust.max_sensitivity/);
      expect(transport.calls).toHaveLength(0);
    }
  });

  it("retrieveGraph rejects invalid trust scope shapes before transport", async () => {
    for (const trust of [
      { max_sensitivity: Sensitivity.LOW, authenticated: "true", actor_id: "tester", scopes: [] },
      { max_sensitivity: Sensitivity.LOW, authenticated: true, actor_id: 7, scopes: [] },
      { max_sensitivity: Sensitivity.LOW, authenticated: true, actor_id: "tester", scopes: [] },
      { max_sensitivity: Sensitivity.LOW, authenticated: true, actor_id: "tester", scopes: "prod" },
      { max_sensitivity: Sensitivity.LOW, authenticated: true, actor_id: "tester", scopes: ["prod", 7] },
      { max_sensitivity: Sensitivity.LOW, authenticated: true, actor_id: "tester", scopes: ["prod", " \t"] }
    ]) {
      const transport = new FakeTransport({ nodes: [], edges: [], root_ids: [] });
      const client = new MembraneClient("localhost:9090", { transport });

      await expect(client.retrieveGraph("invalid trust fields", { trust: trust as unknown as TrustContext })).rejects.toThrow(/trust.authenticated|trust.actor_id|trust.scopes/);
      expect(transport.calls).toHaveLength(0);
    }
  });

  it("retrieveById rejects invalid trust sensitivity before transport", async () => {
    const transport = new FakeTransport({ record: asRecord("unused") });
    const client = new MembraneClient("localhost:9090", { transport });

    await expect(client.retrieveById("record-1", {
      trust: { max_sensitivity: "classified", authenticated: true, actor_id: "tester", scopes: ["prod"] }
    })).rejects.toThrow(/trust.max_sensitivity/);
    expect(transport.calls).toHaveLength(0);
  });

  it("retrieveById rejects invalid trust scope shapes before transport", async () => {
    const transport = new FakeTransport({ record: asRecord("unused") });
    const client = new MembraneClient("localhost:9090", { transport });

    await expect(client.retrieveById("record-1", {
      trust: { max_sensitivity: Sensitivity.LOW, authenticated: true, actor_id: "tester", scopes: "prod" } as unknown as TrustContext
    })).rejects.toThrow(/trust.scopes/);
    expect(transport.calls).toHaveLength(0);
  });

  it("rejects empty required record IDs before transport", async () => {
    const record = asRecord("replacement");
    const cases: Array<{ name: string; invoke: (client: MembraneClient) => Promise<unknown> }> = [
      { name: "retrieveById", invoke: (client) => client.retrieveById("") },
      { name: "retrieveById blank", invoke: (client) => client.retrieveById(" \t") },
      { name: "supersede", invoke: (client) => client.supersede("", record, "tester", "replace") },
      { name: "fork", invoke: (client) => client.fork("", record, "tester", "branch") },
      { name: "retract", invoke: (client) => client.retract("", "tester", "obsolete") },
      { name: "merge empty ids", invoke: (client) => client.merge([], record, "tester", "merge") },
      { name: "merge blank id", invoke: (client) => client.merge(["source-1", " "], record, "tester", "merge") },
      { name: "merge duplicate id", invoke: (client) => client.merge(["source-1", "source-1"], record, "tester", "merge") },
      { name: "contest", invoke: (client) => client.contest("", "", "tester", "conflict") },
      { name: "reinforce", invoke: (client) => client.reinforce("", "tester", "useful") },
      { name: "penalize", invoke: (client) => client.penalize("", 0.25, "tester", "stale") }
    ];

    for (const tc of cases) {
      const transport = new FakeTransport({ record });
      const client = new MembraneClient("localhost:9090", { transport });

      await expect(tc.invoke(client), tc.name).rejects.toThrow(/required|recordIds/);
      expect(transport.calls, tc.name).toHaveLength(0);
    }
  });

  it("rejects malformed revision replacement records before transport", async () => {
    const cases: Array<{ record: MemoryRecord; match: RegExp }> = [
      {
        record: {
          ...asRecord("semantic-missing-validity"),
          type: "semantic",
          payload: { kind: "semantic", subject: "Orchid", predicate: "uses", object: "Postgres" }
        } as MemoryRecord,
        match: /payload\.validity\.mode/
      },
      {
        record: {
          ...asRecord("entity-missing-name"),
          type: "entity",
          payload: { kind: "entity", primary_type: "Project" }
        } as MemoryRecord,
        match: /payload\.canonical_name/
      },
      {
        record: {
          ...asRecord("entity-missing-identifier-namespace"),
          type: "entity",
          payload: { kind: "entity", canonical_name: "Orchid", identifiers: [{ value: "orchid" }] }
        } as MemoryRecord,
        match: /payload\.identifiers\[0\]\.namespace/
      },
      {
        record: {
          ...asRecord("entity-missing-identifier-value"),
          type: "entity",
          payload: { kind: "entity", canonical_name: "Orchid", identifiers: [{ namespace: "slug", value: " " }] }
        } as MemoryRecord,
        match: /payload\.identifiers\[0\]\.value/
      },
      {
        record: {
          ...asRecord("relation-missing-predicate"),
          relations: [{ target_id: "target-1", weight: 0.5 }]
        } as MemoryRecord,
        match: /relations\[0\]\.predicate/
      },
      {
        record: {
          ...asRecord("relation-missing-target"),
          relations: [{ predicate: "supports", target_id: " " }]
        } as MemoryRecord,
        match: /relations\[0\]\.target_id/
      },
      {
        record: {
          ...asRecord("relation-invalid-weight"),
          relations: [{ predicate: "supports", target_id: "target-1", weight: Number.POSITIVE_INFINITY }]
        } as MemoryRecord,
        match: /relations\[0\]\.weight/
      }
    ];

    for (const tc of cases) {
      const transport = new FakeTransport({ record: asRecord("unused") });
      const client = new MembraneClient("localhost:9090", { transport });

      await expect(client.supersede("old-1", tc.record, "tester", "replace")).rejects.toThrow(tc.match);
      expect(transport.calls).toHaveLength(0);
    }
  });

  it("normalizes Go JSON semantic payload fields in revision replacement records", async () => {
    const record = {
      ...asRecord("semantic-go-outbound"),
      type: "semantic",
      payload: {
        Kind: "semantic",
        Subject: "Orchid",
        Predicate: "uses",
        Object: "Postgres",
        Validity: {
          Mode: "conditional",
          Conditions: { environment: "prod" }
        }
      }
    } as unknown as MemoryRecord;
    const transport = new FakeTransport({ record: asRecord("semantic-go-outbound") });
    const client = new MembraneClient("localhost:9090", { transport });

    await client.supersede("old-1", record, "tester", "replace");

    const request = transport.calls[0]?.request as {
      new_record?: { payload?: { semantic?: Record<string, unknown> } };
    };
    expect(request.new_record?.payload?.semantic).toMatchObject({
      kind: "semantic",
      subject: "Orchid",
      predicate: "uses",
      object: { kind: "stringValue", stringValue: "Postgres" },
      validity: {
        Mode: "conditional",
        conditions: { environment: { kind: "stringValue", stringValue: "prod" } }
      }
    });
  });

  it("normalizes Go JSON entity payload fields in revision replacement records", async () => {
    const record = {
      ...asRecord("entity-go-outbound"),
      type: "entity",
      payload: {
        Kind: "entity",
        CanonicalName: "Project Orchid",
        PrimaryType: "Project",
        Types: ["Project", "Repository"],
        Aliases: [{ Value: "orchid", Kind: "nickname", Locale: "en" }],
        Identifiers: [{ Namespace: "github", Value: "BennettSchwartz/orchid" }],
        Summary: "Deployment project"
      }
    } as unknown as MemoryRecord;
    const transport = new FakeTransport({ record: asRecord("entity-go-outbound") });
    const client = new MembraneClient("localhost:9090", { transport });

    await client.fork("source-1", record, "tester", "branch");

    const request = transport.calls[0]?.request as {
      forked_record?: { payload?: { entity?: Record<string, unknown> } };
    };
    expect(request.forked_record?.payload?.entity).toMatchObject({
      kind: "entity",
      canonical_name: "Project Orchid",
      primary_type: "Project",
      types: ["Project", "Repository"],
      aliases: [{ value: "orchid", kind: "nickname", locale: "en" }],
      identifiers: [{ namespace: "github", value: "BennettSchwartz/orchid" }],
      summary: "Deployment project"
    });
  });

  it("retrieveGraph accepts Go JSON field names for roots and selection scores", async () => {
    const transport = new FakeTransport({
      Nodes: [{ Record: asRecord("root-go"), Root: true, Hop: 0 }],
      Edges: [{ SourceID: "root-go", Predicate: "derived_semantic", TargetID: "semantic-go", Weight: 0.75 }],
      RootIDs: ["root-go"],
      Selection: {
        Selected: [asRecord("root-go")],
        Confidence: 0.7,
        NeedsMore: true,
        Scores: { "root-go": 0.84 }
      },
      Projection: {
        RelationsOmitted: true,
        RelationsTruncated: false,
        HistoryOmitted: true,
        RecordsTruncated: false
      }
    });
    const client = new MembraneClient("localhost:9090", { transport });

    const result = await client.retrieveGraph("go json graph");

    expect(result.root_ids).toEqual(["root-go"]);
    expect(result.nodes[0]?.record.id).toBe("root-go");
    expect(result.edges[0]).toMatchObject({
      source_id: "root-go",
      predicate: "derived_semantic",
      target_id: "semantic-go",
      weight: 0.75
    });
    expect(result.selection?.needs_more).toBe(true);
    expect(result.selection?.scores?.["root-go"]).toBe(0.84);
    expect(result.projection?.relations_omitted).toBe(true);
    expect(result.projection?.history_omitted).toBe(true);
  });

  it("retrieveGraph accepts Go JSON field names inside memory records", async () => {
    const transport = new FakeTransport({
      Nodes: [
        {
          Record: {
            ID: "root-go-record",
            Type: "semantic",
            Sensitivity: "medium",
            Confidence: 0.82,
            Salience: 0.91,
            Scope: "agent-harness",
            Tags: ["go-fixture"],
            CreatedAt: "2026-05-06T12:00:00Z",
            UpdatedAt: "2026-05-06T12:30:00Z",
            Relations: [
              {
                TargetID: "entity-postgres",
                Predicate: "supports",
                Kind: "entity",
                Weight: 0.74,
                CreatedAt: "2026-05-06T12:01:00Z"
              }
            ],
            AuditLog: [
              {
                action: "created",
                actor: "test",
                timestamp: "2026-05-06T12:00:00Z",
                rationale: "fixture"
              }
            ],
            Interpretation: {
              Status: "resolved",
              Summary: "Go fixture semantic"
            }
          },
          Root: true,
          Hop: 0
        }
      ],
      Edges: [],
      RootIDs: ["root-go-record"]
    });
    const client = new MembraneClient("localhost:9090", { transport });

    const result = await client.retrieveGraph("go memory record");
    const record = result.nodes[0]?.record;

    expect(record).toMatchObject({
      id: "root-go-record",
      type: "semantic",
      sensitivity: "medium",
      confidence: 0.82,
      salience: 0.91,
      scope: "agent-harness",
      tags: ["go-fixture"],
      created_at: "2026-05-06T12:00:00Z",
      updated_at: "2026-05-06T12:30:00Z"
    });
    expect(record?.relations?.[0]).toMatchObject({
      target_id: "entity-postgres",
      predicate: "supports",
      kind: "entity",
      weight: 0.74,
      created_at: "2026-05-06T12:01:00Z"
    });
    expect(record?.audit_log?.[0]?.action).toBe("created");
    expect(record?.interpretation?.summary).toBe("Go fixture semantic");
  });

  it("retrieveGraph accepts protobuf JSON lower-camel selection fields", async () => {
    const transport = new FakeTransport({
      nodes: [{ record: asRecord("root-camel"), root: true, hop: 0 }],
      edges: [],
      rootIds: ["root-camel"],
      selection: {
        selected: [asRecord("root-camel")],
        confidence: 0.61,
        needsMore: true,
        scores: { "root-camel": 0.73 }
      }
    });
    const client = new MembraneClient("localhost:9090", { transport });

    const result = await client.retrieveGraph("protobuf json graph");

    expect(result.root_ids).toEqual(["root-camel"]);
    expect(result.selection?.needs_more).toBe(true);
    expect(result.selection?.scores?.["root-camel"]).toBe(0.73);
  });

  it("retrieveGraph normalizes lower-camel interpretation metadata", async () => {
    const record = {
      ...asRecord("interpreted-1"),
      interpretation: {
        status: "resolved",
        summary: "Resolved Orchid links",
        proposedType: "semantic",
        topicalLabels: ["deploy", "graph"],
        mentions: [
          {
            surface: "Orchid",
            entityKind: "project",
            canonicalEntityId: "entity-orchid",
            confidence: 0.92,
            aliases: ["Project Orchid"]
          }
        ],
        relationCandidates: [
          {
            predicate: "depends_on",
            targetRecordId: "semantic-postgres",
            targetEntityId: "entity-postgres",
            confidence: 0.81,
            resolved: true
          }
        ],
        referenceCandidates: [
          {
            ref: "trace-1",
            targetRecordId: "episodic-trace",
            targetEntityId: "entity-trace",
            confidence: 0.73,
            resolved: true
          }
        ],
        extractionConfidence: 0.88
      }
    };
    const transport = new FakeTransport({
      nodes: [{ record, root: true, hop: 0 }],
      edges: [],
      rootIds: [record.id]
    });
    const client = new MembraneClient("localhost:9090", { transport });

    const result = await client.retrieveGraph("interpreted graph");
    const interpretation = result.nodes[0]?.record.interpretation;

    expect(interpretation?.proposed_type).toBe("semantic");
    expect(interpretation?.topical_labels).toEqual(["deploy", "graph"]);
    expect(interpretation?.extraction_confidence).toBe(0.88);
    expect(interpretation?.mentions?.[0]).toMatchObject({
      surface: "Orchid",
      entity_kind: "project",
      canonical_entity_id: "entity-orchid",
      confidence: 0.92,
      aliases: ["Project Orchid"]
    });
    expect(interpretation?.relation_candidates?.[0]).toMatchObject({
      predicate: "depends_on",
      target_record_id: "semantic-postgres",
      target_entity_id: "entity-postgres",
      confidence: 0.81,
      resolved: true
    });
    expect(interpretation?.reference_candidates?.[0]).toMatchObject({
      ref: "trace-1",
      target_record_id: "episodic-trace",
      target_entity_id: "entity-trace",
      confidence: 0.73,
      resolved: true
    });
  });

  it("retrieveGraph normalizes protobuf entity payload identity fields", async () => {
    const record = {
      ...asRecord("entity-camel"),
      type: "entity",
      payload: {
        entity: {
          kind: "entity",
          canonicalName: "Project Orchid",
          primaryType: "Project",
          Types: ["Project", "Repository"],
          Aliases: [{ Value: "orchid", Kind: "nickname", Locale: "en" }],
          Identifiers: [{ Namespace: "github", Value: "BennettSchwartz/orchid" }],
          Summary: "Staging deploy target"
        }
      }
    };
    const transport = new FakeTransport({
      nodes: [{ record, root: true, hop: 0 }],
      edges: [],
      rootIds: [record.id]
    });
    const client = new MembraneClient("localhost:9090", { transport });

    const result = await client.retrieveGraph("entity graph");
    const payload = result.nodes[0]?.record.payload as Record<string, unknown>;

    expect(payload.canonical_name).toBe("Project Orchid");
    expect(payload.primary_type).toBe("Project");
    expect(payload.types).toEqual(["Project", "Repository"]);
    expect(payload.aliases).toEqual([{ value: "orchid", kind: "nickname", locale: "en" }]);
    expect(payload.identifiers).toEqual([{ namespace: "github", value: "BennettSchwartz/orchid" }]);
    expect(payload.summary).toBe("Staging deploy target");
  });

  it("retrieveGraph normalizes protobuf lower-camel semantic revision payloads", async () => {
    const record = {
      ...asRecord("semantic-revision-camel"),
      type: "semantic",
      payload: {
        semantic: {
          kind: "semantic",
          subject: "database",
          predicate: "uses",
          object: { kind: "stringValue", stringValue: "Postgres" },
          validity: {
            mode: "conditional",
            conditions: {
              env: { kind: "stringValue", stringValue: "prod" }
            }
          },
          revisionPolicy: "contest",
          revision: {
            supersededBy: "semantic-new",
            status: "retracted"
          }
        }
      }
    };
    const transport = new FakeTransport({
      nodes: [{ record, root: true, hop: 0 }],
      edges: [],
      rootIds: [record.id]
    });
    const client = new MembraneClient("localhost:9090", { transport });

    const result = await client.retrieveGraph("semantic revision payload");
    const payload = result.nodes[0]?.record.payload as Record<string, unknown>;

    expect(payload.object).toBe("Postgres");
    expect(payload.validity).toMatchObject({ conditions: { env: "prod" } });
    expect(payload.revision_policy).toBe("contest");
    expect(payload.revision).toMatchObject({
      superseded_by: "semantic-new",
      status: "retracted"
    });
  });

  it("retrieveGraph normalizes protobuf lower-camel plan graph payloads", async () => {
    const record = {
      ...asRecord("plan-camel"),
      type: "plan_graph",
      payload: {
        planGraph: {
          kind: "plan_graph",
          planId: "plan-1",
          inputsSchema: {
            repo: { kind: "stringValue", stringValue: "membrane" }
          },
          outputsSchema: {
            summary: { kind: "stringValue", stringValue: "done" }
          },
          nodes: [
            {
              id: "n1",
              op: "run_tests",
              params: {
                command: { kind: "stringValue", stringValue: "make verify" }
              },
              guards: {
                branch: { kind: "stringValue", stringValue: "main" }
              }
            }
          ],
          metrics: {
            avgLatencyMs: 42,
            failureRate: 0.1,
            executionCount: 3,
            lastExecutedAt: "2026-05-07T13:00:00Z"
          }
        }
      }
    };
    const transport = new FakeTransport({
      nodes: [{ record, root: true, hop: 0 }],
      edges: [],
      rootIds: [record.id]
    });
    const client = new MembraneClient("localhost:9090", { transport });

    const result = await client.retrieveGraph("plan graph payload");
    const payload = result.nodes[0]?.record.payload as Record<string, unknown>;

    expect(payload.plan_id).toBe("plan-1");
    expect(payload.inputs_schema).toEqual({ repo: "membrane" });
    expect(payload.outputs_schema).toEqual({ summary: "done" });
    expect((payload.nodes as Array<Record<string, unknown>>)[0]?.params).toEqual({ command: "make verify" });
    expect((payload.nodes as Array<Record<string, unknown>>)[0]?.guards).toEqual({ branch: "main" });
    expect(payload.metrics).toMatchObject({
      avg_latency_ms: 42,
      failure_rate: 0.1,
      execution_count: 3,
      last_executed_at: "2026-05-07T13:00:00Z"
    });
  });

  it("captureMemory accepts Go JSON field names for repeated semantic reuse edges", async () => {
    const transport = new FakeTransport({
      PrimaryRecord: asRecord("source-go"),
      CreatedRecords: [],
      Edges: [
        { SourceID: "source-go", Predicate: "derived_semantic", TargetID: "semantic-existing", Weight: 1 },
        { SourceID: "semantic-existing", Predicate: "derived_from", TargetID: "source-go", Weight: 1 }
      ]
    });
    const client = new MembraneClient("localhost:9090", { transport });

    const result = await client.captureMemory(
      { subject: "Project Orchid", predicate: "uses", object: "Postgres" },
      { sourceKind: SourceKind.OBSERVATION }
    );

    expect(result.primary_record.id).toBe("source-go");
    expect(result.created_records).toHaveLength(0);
    expect(result.edges.map((edge) => edge.predicate)).toEqual(["derived_semantic", "derived_from"]);
    expect(result.edges[0]?.target_id).toBe("semantic-existing");
  });

  it("getMetrics parses snapshot payload", async () => {
    const transport = new FakeTransport({
      snapshot: {
        kind: "structValue",
        structValue: {
          fields: {
            total_records: { kind: "numberValue", numberValue: 42 },
            embedded_records: { kind: "numberValue", numberValue: 40 },
            missing_embeddings: { kind: "numberValue", numberValue: 2 },
            embedding_coverage: { kind: "numberValue", numberValue: 0.95 }
          }
        }
      }
    });
    const client = new MembraneClient("localhost:9090", { transport });

    const snapshot = await client.getMetrics();

    expect(snapshot.total_records).toBe(42);
    expect(snapshot.embedding_coverage).toBe(0.95);
  });

  it("penalize validates amount before transport", async () => {
    for (const amount of [-0.1, Number.NaN, Number.POSITIVE_INFINITY]) {
      const transport = new FakeTransport({});
      const client = new MembraneClient("localhost:9090", { transport });

      await expect(client.penalize("rec-1", amount, "tester", "not useful")).rejects.toThrow(/amount/);
      expect(transport.calls).toHaveLength(0);
    }

    const transport = new FakeTransport({});
    const client = new MembraneClient("localhost:9090", { transport });

    await client.penalize("rec-1", 0.25, "tester", "not useful");

    expect(transport.calls[0]?.method).toBe("Penalize");
    expect(transport.calls[0]?.request.amount).toBe(0.25);
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
