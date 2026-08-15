import { describe, expect, it } from "vitest";
import { InMemoryBus } from "../src/bus.js";
import { createEnvelope, type Envelope } from "../src/envelope.js";

const tick = (ms = 10) => new Promise<void>((r) => setTimeout(r, ms));

/** A gate the test opens to let a blocked handler proceed. */
function gate() {
  let open!: () => void;
  const opened = new Promise<void>((r) => (open = r));
  return { open, opened };
}

describe("InMemoryBus pub/sub", () => {
  it("delivers to matching subscribers only", async () => {
    const bus = new InMemoryBus();
    const hits: string[] = [];
    bus.subscribe("orders/*", (env) => void hits.push(`a:${env.topic}`));
    bus.subscribe("payments/>", (env) => void hits.push(`b:${env.topic}`));
    await bus.emit("orders/received", {});
    await bus.emit("payments/cards/charged", {});
    await bus.drain();
    expect(hits.sort()).toEqual(["a:orders/received", "b:payments/cards/charged"]);
  });

  it("fans out one publish to every matching subscriber", async () => {
    const bus = new InMemoryBus();
    const hits: string[] = [];
    bus.subscribe("orders/received", () => void hits.push("s1"));
    bus.subscribe("orders/>", () => void hits.push("s2"));
    await bus.emit("orders/received", {});
    await bus.drain();
    expect(hits.sort()).toEqual(["s1", "s2"]);
  });

  it("each subscriber processes its queue in FIFO order", async () => {
    const bus = new InMemoryBus();
    const seen: number[] = [];
    bus.subscribe("n", async (env) => {
      await tick(5);
      seen.push(env.payload as number);
    });
    for (let i = 0; i < 5; i++) await bus.emit("n", i);
    await bus.drain();
    expect(seen).toEqual([0, 1, 2, 3, 4]);
  });

  it("closed subscriptions receive nothing", async () => {
    const bus = new InMemoryBus();
    const hits: unknown[] = [];
    const sub = bus.subscribe("t", (env) => void hits.push(env.payload));
    await bus.emit("t", 1);
    await bus.drain();
    sub.close();
    await bus.emit("t", 2);
    await bus.drain();
    expect(hits).toEqual([1]);
  });
});

describe("backpressure policies", () => {
  /**
   * Setup: capacity 2. The first publish is handed straight to the waiting
   * pump (whose handler blocks on the gate), the next two fill the queue, the
   * fourth triggers the policy under test.
   */
  async function fillThenOverflow(policy: "drop-oldest" | "drop-new") {
    const bus = new InMemoryBus({ maxQueueDepth: 2, backpressure: policy });
    const g = gate();
    const seen: number[] = [];
    const sub = bus.subscribe("t", async (env) => {
      seen.push(env.payload as number);
      if (seen.length === 1) await g.opened;
    });
    for (const n of [1, 2, 3, 4]) await bus.emit("t", n);
    g.open();
    await bus.drain();
    return { seen, sub };
  }

  it("drop-oldest evicts the oldest queued envelope", async () => {
    const { seen, sub } = await fillThenOverflow("drop-oldest");
    expect(seen).toEqual([1, 3, 4]);
    expect(sub.dropped()).toBe(1);
  });

  it("drop-new discards the incoming envelope", async () => {
    const { seen, sub } = await fillThenOverflow("drop-new");
    expect(seen).toEqual([1, 2, 3]);
    expect(sub.dropped()).toBe(1);
  });

  it("block suspends the publisher until the consumer catches up, dropping nothing", async () => {
    const bus = new InMemoryBus({ maxQueueDepth: 1, backpressure: "block" });
    const g = gate();
    const seen: number[] = [];
    const sub = bus.subscribe("t", async (env) => {
      seen.push(env.payload as number);
      if (seen.length === 1) await g.opened;
    });
    await bus.emit("t", 1); // handed to pump, handler blocks
    await bus.emit("t", 2); // fills the queue
    let thirdSettled = false;
    const third = bus.emit("t", 3).then(() => (thirdSettled = true));
    await tick();
    expect(thirdSettled).toBe(false); // publisher is blocked on a full queue
    g.open();
    await third;
    await bus.drain();
    expect(seen).toEqual([1, 2, 3]);
    expect(sub.dropped()).toBe(0);
  });

  it("reports drops to the observer", async () => {
    const dropped: Envelope[] = [];
    const bus = new InMemoryBus({
      maxQueueDepth: 1,
      backpressure: "drop-new",
      observer: { onDrop: (env) => void dropped.push(env) },
    });
    const g = gate();
    bus.subscribe("t", async () => {
      await g.opened;
    });
    await bus.emit("t", 1);
    await bus.emit("t", 2);
    await bus.emit("t", 3);
    g.open();
    await bus.drain();
    expect(dropped.length).toBe(1);
    expect(dropped[0].payload).toBe(3);
  });
});

describe("request/reply", () => {
  it("round-trips a reply via the replyTo header", async () => {
    const bus = new InMemoryBus();
    bus.subscribe("math/double", async (env) => {
      const replyTo = env.headers["replyTo"];
      await bus.emit(replyTo, (env.payload as number) * 2);
    });
    const reply = await bus.request<number>("math/double", 21, { timeoutMs: 500 });
    expect(reply.payload).toBe(42);
  });

  it("rejects on timeout when nobody replies", async () => {
    const bus = new InMemoryBus();
    bus.subscribe("void", () => {});
    await expect(bus.request("void", {}, { timeoutMs: 30 })).rejects.toThrow(/timed out/);
  });

  it("preserves the correlation chain when given a parent envelope", async () => {
    const bus = new InMemoryBus();
    bus.subscribe("echo", async (env) => {
      await bus.publish({ ...createEnvelope(env.headers["replyTo"], env.payload), correlationId: env.correlationId });
    });
    const parent = createEnvelope("root", {});
    const reply = await bus.request("echo", "hi", { parent, timeoutMs: 500 });
    expect(reply.correlationId).toBe(parent.correlationId);
  });
});
