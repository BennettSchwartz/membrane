import * as grpc from "@grpc/grpc-js";

import { MembraneError, assertMethodBinding, normalizeRpcError } from "../src/internal/grpc";

describe("gRPC transport helpers", () => {
  it("normalizes gRPC status errors into MembraneError", () => {
    const metadata = new grpc.Metadata();
    metadata.set("x-trace-id", "trace-123");

    const serviceError = Object.assign(new Error("unauthenticated"), {
      code: grpc.status.UNAUTHENTICATED,
      details: "missing bearer token",
      metadata
    }) as grpc.ServiceError;

    const error = normalizeRpcError(serviceError);

    expect(error).toBeInstanceOf(MembraneError);
    expect(error.code).toBe(grpc.status.UNAUTHENTICATED);
    expect(error.codeName).toBe("UNAUTHENTICATED");
    expect(error.details).toBe("missing bearer token");
    expect(error.metadata["x-trace-id"]).toEqual(["trace-123"]);
  });

  it("fails fast when a required method binding is missing", () => {
    expect(() => assertMethodBinding("Retrieve", undefined)).toThrow(MembraneError);
    expect(() => assertMethodBinding("Retrieve", undefined)).toThrow("Missing gRPC method binding: Retrieve");
  });
});
