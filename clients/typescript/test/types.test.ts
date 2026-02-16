import {
  MemoryType,
  OutcomeStatus,
  Sensitivity,
  TaskState,
  createDefaultTrustContext
} from "../src/index";

describe("types", () => {
  it("exports expected enum-like values", () => {
    expect(MemoryType.EPISODIC).toBe("episodic");
    expect(MemoryType.PLAN_GRAPH).toBe("plan_graph");
    expect(Sensitivity.HIGH).toBe("high");
    expect(OutcomeStatus.PARTIAL).toBe("partial");
    expect(TaskState.EXECUTING).toBe("executing");
  });

  it("creates the default trust context", () => {
    expect(createDefaultTrustContext()).toEqual({
      max_sensitivity: "low",
      authenticated: false,
      actor_id: "",
      scopes: []
    });
  });
});
