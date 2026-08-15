import { describe, expect, it } from "vitest";
import { agent } from "../src/agent.js";
import type { Envelope } from "../src/envelope.js";
import { Mesh } from "../src/mesh.js";
import { backoffDelays } from "../src/patterns.js";

describe("backoffDelays", () => {
  it("doubles by default: [base, 2x, 4x]", () => {
    expect(backoffDelays({ maxRetries: 3, baseDelayMs: 10 })).toEqual([10, 20, 40]);
  });

  it("honours a custom factor", () => {
    expect(backoffDelays({ maxRetries: 4, baseDelayMs: 5, factor: 3 })).toEqual([5, 15, 45, 135]);
  });

  it("zero retries means no delays", () => {
    expect(backoffDelays({ maxRetries: 0, baseDelayMs: 10 })).toEqual([]);
  });
});

describe("retry then DLQ (via mesh registration)", () => {
  it("a transient failure is retried and succeeds without touching the DLQ", async () => {
    const mesh = new Mesh();
    let attempts = 0;
    const dlq: Envelope[] = [];
    mesh.register(
      agent("Flaky", ["work"], () => {
        attempts++;
        if (attempts < 3) throw new Error("transient");
      }),
      { retry: { maxRetries: 3, baseDelayMs: 5 } },
    );
    mesh.bus.subscribe("dlq/>", (env) => void dlq.push(env), { label: "dlq" });
    mesh.start();
    await mesh.publish("work", {});
    await mesh.drain();
    expect(attempts).toBe(3); // initial + 2 retries
    expect(dlq).toEqual([]);
    expect(mesh.metrics().agents["Flaky"].processed).toBe(1);
    await mesh.stop();
  });

  it("exhausted retries dead-letter the envelope with error metadata", async () => {
    const mesh = new Mesh();
    let attempts = 0;
    const dlq: Envelope[] = [];
    mesh.register(
      agent("Broken", ["work"], () => {
        attempts++;
        throw new Error("permanently down");
      }),
      { retry: { maxRetries: 2, baseDelayMs: 5 } },
    );
    mesh.bus.subscribe("dlq/>", (env) => void dlq.push(env), { label: "dlq" });
    mesh.start();
    const root = await mesh.publish("work", { job: 7 });
    await mesh.drain();

    expect(attempts).toBe(3); // initial + maxRetries
    expect(dlq.length).toBe(1);
    const dead = dlq[0];
    expect(dead.topic).toBe("dlq/work");
    expect(dead.headers["x-error"]).toContain("permanently down");
    expect(dead.headers["x-retries"]).toBe("2");
    expect(dead.headers["x-failed-agent"]).toBe("Broken");
    expect(dead.headers["x-original-topic"]).toBe("work");
    expect(dead.payload).toEqual({ job: 7 }); // original payload preserved
    expect(dead.correlationId).toBe(root.id); // still on the original correlation chain
    // the failure is still counted against the agent (DLQ does not launder errors)
    expect(mesh.metrics().agents["Broken"].failed).toBe(1);
    await mesh.stop();
  });

  it("waits the backoff schedule before giving up", async () => {
    const mesh = new Mesh();
    const attemptTimes: number[] = [];
    mesh.register(
      agent("Timed", ["work"], () => {
        attemptTimes.push(performance.now());
        throw new Error("x");
      }),
      { retry: { maxRetries: 2, baseDelayMs: 30 } },
    );
    mesh.start();
    await mesh.publish("work", {});
    await mesh.drain();
    expect(attemptTimes.length).toBe(3);
    const gap1 = attemptTimes[1] - attemptTimes[0];
    const gap2 = attemptTimes[2] - attemptTimes[1];
    expect(gap1).toBeGreaterThanOrEqual(25); // ~30ms
    expect(gap2).toBeGreaterThanOrEqual(50); // ~60ms
    expect(gap2).toBeGreaterThan(gap1); // exponential, not linear
    await mesh.stop();
  });
});
