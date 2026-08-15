import { describe, expect, it } from "vitest";
import { agent } from "../src/agent.js";
import { Mesh } from "../src/mesh.js";

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

describe("Mesh", () => {
  it("counts processed and failed per agent", async () => {
    const mesh = new Mesh();
    mesh.register(agent("Ok", ["jobs"], () => {}));
    mesh.register(
      agent("Bad", ["jobs"], () => {
        throw new Error("nope");
      }),
    );
    mesh.start();
    for (let i = 0; i < 3; i++) await mesh.publish("jobs", i);
    await mesh.drain();
    const m = mesh.metrics();
    expect(m.agents["Ok"]).toMatchObject({ processed: 3, failed: 0 });
    expect(m.agents["Bad"]).toMatchObject({ processed: 0, failed: 3 });
    await mesh.stop();
  });

  it("latency percentiles come from real handler timings and are ordered", async () => {
    const mesh = new Mesh();
    mesh.register(
      agent("Sleepy", ["jobs"], async (env) => {
        await sleep(env.payload as number);
      }),
    );
    mesh.start();
    for (const ms of [1, 1, 1, 1, 30]) await mesh.publish("jobs", ms);
    await mesh.drain();
    const s = mesh.metrics().agents["Sleepy"];
    expect(s.processed).toBe(5);
    expect(s.p50).toBeGreaterThan(0);
    expect(s.p50).toBeLessThanOrEqual(s.p95);
    expect(s.p95).toBeLessThanOrEqual(s.p99);
    expect(s.p99).toBeGreaterThanOrEqual(25); // the 30ms outlier lands in p99
    await mesh.stop();
  });

  it("exposes queue snapshots (label, pattern, depth, dropped)", async () => {
    const mesh = new Mesh();
    mesh.register(agent("Watcher", ["a/>", "b/*"], () => {}));
    mesh.start();
    const queues = mesh.metrics().queues;
    const patterns = queues.filter((q) => q.label === "Watcher").map((q) => q.pattern);
    expect(patterns.sort()).toEqual(["a/>", "b/*"]);
    await mesh.stop();
  });

  it("stop() closes subscriptions and disposes agents", async () => {
    const mesh = new Mesh();
    let disposed = false;
    let handled = 0;
    mesh.register({
      name: "Disposable",
      subscriptions: ["x"],
      handle: () => void handled++,
      dispose: () => void (disposed = true),
    });
    mesh.start();
    await mesh.publish("x", {});
    await mesh.drain();
    await mesh.stop();
    await mesh.bus.publish({ id: "1", topic: "x", correlationId: "1", timestamp: Date.now(), headers: {}, payload: {} });
    await sleep(20);
    expect(handled).toBe(1); // nothing delivered after stop
    expect(disposed).toBe(true);
  });

  it("drain waits for multi-hop cascades to settle", async () => {
    const mesh = new Mesh();
    const seen: string[] = [];
    mesh.register(
      agent("Hop1", ["h/1"], async (_e, ctx) => {
        await sleep(15);
        await ctx.publish("h/2", {});
      }),
    );
    mesh.register(
      agent("Hop2", ["h/2"], async (_e, ctx) => {
        await sleep(15);
        await ctx.publish("h/3", {});
      }),
    );
    mesh.register(agent("Hop3", ["h/3"], () => void seen.push("done")));
    mesh.start();
    await mesh.publish("h/1", {});
    await mesh.drain();
    expect(seen).toEqual(["done"]); // drain outlasted the whole cascade
    await mesh.stop();
  });

  it("registering after start wires the agent immediately", async () => {
    const mesh = new Mesh().start();
    const seen: unknown[] = [];
    mesh.register(agent("Late", ["late"], (env) => void seen.push(env.payload)));
    await mesh.publish("late", 42);
    await mesh.drain();
    expect(seen).toEqual([42]);
    await mesh.stop();
  });
});
