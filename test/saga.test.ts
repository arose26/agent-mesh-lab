import { describe, expect, it } from "vitest";
import { runSaga, type SagaStep } from "../src/patterns.js";

describe("saga", () => {
  it("runs every step in order and returns the final state", async () => {
    const order: string[] = [];
    const steps: SagaStep<number>[] = [
      { name: "a", run: (s) => (order.push("a"), s + 1) },
      { name: "b", run: (s) => (order.push("b"), s * 10) },
    ];
    const result = await runSaga("test", steps, 1);
    expect(result).toEqual({ ok: true, state: 20 });
    expect(order).toEqual(["a", "b"]);
  });

  it("compensates completed steps in reverse order on failure", async () => {
    const compensated: string[] = [];
    const steps: SagaStep<number>[] = [
      { name: "reserve", run: (s) => s, compensate: () => void compensated.push("reserve") },
      { name: "charge", run: (s) => s, compensate: () => void compensated.push("charge") },
      {
        name: "ship",
        run: () => {
          throw new Error("no trucks");
        },
        compensate: () => void compensated.push("ship"),
      },
    ];
    const result = await runSaga("test", steps, 0);
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.failedStep).toBe("ship");
    expect(String(result.error)).toContain("no trucks");
    // reverse completion order, and the failed step itself is NOT compensated
    expect(compensated).toEqual(["charge", "reserve"]);
    expect(result.compensated).toEqual(["charge", "reserve"]);
  });

  it("skips steps that declare no compensation", async () => {
    const compensated: string[] = [];
    const steps: SagaStep<number>[] = [
      { name: "log", run: (s) => s }, // no compensate
      { name: "charge", run: (s) => s, compensate: () => void compensated.push("charge") },
      {
        name: "boom",
        run: () => {
          throw new Error("x");
        },
      },
    ];
    const result = await runSaga("test", steps, 0);
    expect(result.ok).toBe(false);
    expect(compensated).toEqual(["charge"]);
  });

  it("emits started/completed/compensated/failed progress events", async () => {
    const topics: string[] = [];
    const steps: SagaStep<number>[] = [
      { name: "a", run: (s) => s, compensate: () => {} },
      {
        name: "b",
        run: () => {
          throw new Error("x");
        },
      },
    ];
    await runSaga("wf", steps, 0, (topic) => void topics.push(topic));
    expect(topics).toEqual([
      "saga/wf/a/started",
      "saga/wf/a/completed",
      "saga/wf/b/started",
      "saga/wf/a/compensated",
      "saga/wf/failed",
    ]);
  });
});
