import { describe, expect, it } from "vitest";
import { childEnvelope, createEnvelope } from "../src/envelope.js";

describe("envelope", () => {
  it("root envelope starts a correlation chain with its own id", () => {
    const env = createEnvelope("orders/new", { x: 1 });
    expect(env.correlationId).toBe(env.id);
    expect(env.causationId).toBeUndefined();
    expect(env.topic).toBe("orders/new");
    expect(env.payload).toEqual({ x: 1 });
  });

  it("child envelope points causation at parent and preserves correlation", () => {
    const root = createEnvelope("orders/new", {});
    const child = childEnvelope(root, "orders/received", { ok: true });
    expect(child.id).not.toBe(root.id);
    expect(child.causationId).toBe(root.id);
    expect(child.correlationId).toBe(root.correlationId);
    expect(child.topic).toBe("orders/received");
  });

  it("correlation survives multi-hop chains (grandchild keeps root correlation)", () => {
    const root = createEnvelope("a", {});
    const child = childEnvelope(root, "b", {});
    const grandchild = childEnvelope(child, "c", {});
    expect(grandchild.correlationId).toBe(root.id);
    expect(grandchild.causationId).toBe(child.id);
  });

  it("headers are per-envelope, not inherited", () => {
    const root = createEnvelope("a", {}, { secret: "yes" });
    const child = childEnvelope(root, "b", {}, { other: "1" });
    expect(child.headers).toEqual({ other: "1" });
  });
});
