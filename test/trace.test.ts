import { describe, expect, it } from "vitest";
import { agent } from "../src/agent.js";
import { Mesh } from "../src/mesh.js";
import { Tracer } from "../src/trace.js";

const ev = (overrides: Partial<Parameters<Tracer["record"]>[0]>) => ({
  kind: "publish" as const,
  topic: "t",
  envelopeId: "e",
  correlationId: "c",
  ...overrides,
});

describe("Tracer", () => {
  it("ring buffer evicts the oldest events beyond capacity", () => {
    const tracer = new Tracer(3);
    for (let i = 0; i < 5; i++) tracer.record(ev({ envelopeId: `e${i}` }));
    const ids = tracer.events().map((e) => e.envelopeId);
    expect(ids).toEqual(["e2", "e3", "e4"]);
  });

  it("traceOf returns only the requested correlation chain, in order", () => {
    const tracer = new Tracer();
    tracer.record(ev({ correlationId: "A", envelopeId: "1" }));
    tracer.record(ev({ correlationId: "B", envelopeId: "2" }));
    tracer.record(ev({ correlationId: "A", envelopeId: "3", kind: "ack", agent: "x" }));
    const trace = tracer.traceOf("A");
    expect(trace.map((e) => e.envelopeId)).toEqual(["1", "3"]);
  });

  it("toJSONL emits one parseable JSON object per line", () => {
    const tracer = new Tracer();
    tracer.record(ev({ envelopeId: "1" }));
    tracer.record(ev({ envelopeId: "2", kind: "fail", agent: "a", error: "boom" }));
    const lines = tracer.toJSONL().split("\n");
    expect(lines.length).toBe(2);
    const parsed = lines.map((l) => JSON.parse(l));
    expect(parsed[1].error).toBe("boom");
    expect(parsed[0].seq).toBe(0);
  });

  it("reconstructs the causal tree from a real mesh run", async () => {
    const mesh = new Mesh();
    mesh.register(agent("A", ["step/1"], async (_e, ctx) => void (await ctx.publish("step/2", {}))));
    mesh.register(
      agent("B", ["step/2"], async (_e, ctx) => {
        await ctx.publish("step/3a", {});
        await ctx.publish("step/3b", {});
      }),
    );
    mesh.register(
      agent("C", ["step/3a"], () => {
        throw new Error("kaput");
      }),
    );
    mesh.start();
    const root = await mesh.publish("step/1", {});
    await mesh.drain();

    const tree = mesh.tracer.renderTree(root.correlationId);
    const lines = tree.split("\n");
    expect(lines[0]).toMatch(/^step\/1 #\w{8}\s+-> \[A ok\]$/);
    expect(lines[1]).toMatch(/^`- step\/2 .* \[B ok\]$/);
    // step/3a and step/3b are siblings under step/2
    expect(lines[2]).toMatch(/^   \|- step\/3a .*C FAILED: Error: kaput/);
    expect(lines[3]).toMatch(/^   `- step\/3b/);
    await mesh.stop();
  });

  it("renders envelopes published to nobody (no consumer annotation)", () => {
    const tracer = new Tracer();
    tracer.record(ev({ correlationId: "r", envelopeId: "r", topic: "lonely/topic" }));
    expect(tracer.renderTree("r")).toBe("lonely/topic #r");
  });
});
