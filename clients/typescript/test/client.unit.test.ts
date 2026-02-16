import { MembraneClient, Sensitivity, type MemoryRecord } from "../src/index";
import type { RpcTransport } from "../src/internal/grpc";

class FakeTransport implements RpcTransport {
  readonly calls: Array<{ method: string; request: Record<string, unknown> }> = [];
  private readonly responses: unknown[];

  constructor(...responses: unknown[]) {
    this.responses = responses;
  }

  async unary<TResponse>(methodName: string, request: Record<string, unknown>): Promise<TResponse> {
    this.calls.push({ method: methodName, request });
    const response = this.responses.shift();
    return response as TResponse;
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
  it("ingestEvent applies defaults and parses envelope bytes", async () => {
    const transport = new FakeTransport({ record: Buffer.from(JSON.stringify(asRecord("rec-1")), "utf8") });
    const client = new MembraneClient("localhost:9090", { transport });

    const record = await client.ingestEvent("file_edit", "src/index.ts");

    expect(record.id).toBe("rec-1");
    expect(transport.calls[0]?.method).toBe("IngestEvent");
    expect(transport.calls[0]?.request.source).toBe("typescript-client");
    expect(transport.calls[0]?.request.summary).toBe("");
    expect(transport.calls[0]?.request.sensitivity).toBe("low");
    expect(typeof transport.calls[0]?.request.timestamp).toBe("string");
  });

  it("ingestToolOutput encodes args and result as bytes", async () => {
    const transport = new FakeTransport({ record: Buffer.from(JSON.stringify(asRecord("rec-2")), "utf8") });
    const client = new MembraneClient("localhost:9090", { transport });

    await client.ingestToolOutput("bash", {
      args: { command: "go test ./..." },
      result: { exit_code: 0 },
      sensitivity: Sensitivity.MEDIUM,
      dependsOn: ["abc"]
    });

    const call = transport.calls[0];
    expect(call?.method).toBe("IngestToolOutput");
    expect(Buffer.isBuffer(call?.request.args)).toBe(true);
    expect(Buffer.isBuffer(call?.request.result)).toBe(true);
    expect(call?.request.depends_on).toEqual(["abc"]);
    expect(call?.request.sensitivity).toBe("medium");
  });

  it("retrieve uses default trust context and parses records", async () => {
    const records = [asRecord("r1"), asRecord("r2")];
    const transport = new FakeTransport({
      records: records.map((record) => Buffer.from(JSON.stringify(record), "utf8"))
    });
    const client = new MembraneClient("localhost:9090", { transport });

    const result = await client.retrieve("debug task");

    expect(result).toHaveLength(2);
    expect(result[0]?.id).toBe("r1");
    expect(transport.calls[0]?.request.trust).toEqual({
      max_sensitivity: "low",
      authenticated: false,
      actor_id: "",
      scopes: []
    });
    expect(transport.calls[0]?.request.limit).toBe(10);
  });

  it("getMetrics parses snapshot payload", async () => {
    const transport = new FakeTransport({ snapshot: Buffer.from(JSON.stringify({ total_records: 42 }), "utf8") });
    const client = new MembraneClient("localhost:9090", { transport });

    const snapshot = await client.getMetrics();

    expect(snapshot.total_records).toBe(42);
  });

  it("supports snake_case method aliases", async () => {
    const transport = new FakeTransport({ record: Buffer.from(JSON.stringify(asRecord("alias-1")), "utf8") });
    const client = new MembraneClient("localhost:9090", { transport });

    const record = await client.ingest_event("user_input", "session-1");

    expect(record.id).toBe("alias-1");
    expect(transport.calls[0]?.method).toBe("IngestEvent");
  });
});
