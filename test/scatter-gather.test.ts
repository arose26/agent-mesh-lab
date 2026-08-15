import { describe, expect, it } from "vitest";
import { agent } from "../src/agent.js";
import { createEnvelope } from "../src/envelope.js";
import { Mesh } from "../src/mesh.js";
import { scatterGather } from "../src/patterns.js";

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

function meshWithResponders(): Mesh {
  const mesh = new Mesh();
  mesh.register(agent("fast1", ["ping"], async (_env, ctx) => ctx.reply({ from: "fast1" })));
  mesh.register(agent("fast2", ["ping"], async (_env, ctx) => ctx.reply({ from: "fast2" })));
  mesh.register(
    agent("slow", ["ping"], async (_env, ctx) => {
      await sleep(200);
      await ctx.reply({ from: "slow" });
    }),
  );
  return mesh.start();
}

describe("scatter-gather", () => {
  it("resolves as soon as quorum replies arrive", async () => {
    const mesh = meshWithResponders();
    const started = performance.now();
    const result = await scatterGather(mesh.bus, "ping", {}, { quorum: 2, timeoutMs: 2000 });
    expect(result.quorumMet).toBe(true);
    expect(result.replies.length).toBe(2);
    expect(performance.now() - started).toBeLessThan(150); // did not wait for the slow responder or the timeout
    await mesh.stop();
  });

  it("times out with partial replies when quorum is not reached", async () => {
    const mesh = meshWithResponders();
    const result = await scatterGather(mesh.bus, "ping", {}, { quorum: 3, timeoutMs: 60 });
    expect(result.quorumMet).toBe(false);
    expect(result.replies.length).toBe(2); // both fast responders, slow one missed the window
    await mesh.stop();
  });

  it("preserves the correlation chain when scattered from a parent envelope", async () => {
    const mesh = meshWithResponders();
    const parent = createEnvelope("root", {});
    const result = await scatterGather(mesh.bus, "ping", {}, { quorum: 2, timeoutMs: 2000, parent });
    for (const reply of result.replies) {
      expect(reply.correlationId).toBe(parent.correlationId);
    }
    await mesh.stop();
  });

  it("returns zero replies (quorum unmet) when nobody subscribes", async () => {
    const mesh = new Mesh().start();
    const result = await scatterGather(mesh.bus, "nowhere", {}, { quorum: 1, timeoutMs: 40 });
    expect(result).toEqual({ replies: [], quorumMet: false });
    await mesh.stop();
  });
});
