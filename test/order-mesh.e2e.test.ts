import { describe, expect, it } from "vitest";
import { runDemo, type Order } from "../src/examples/order-mesh.js";

describe("order mesh end-to-end", () => {
  it("processes both orders to fulfillment, routing the high-value one through HITL approval", async () => {
    const lines: string[] = [];
    const { mesh, fulfilled, dlq, orderA, orderB } = await runDemo((l) => void lines.push(l));

    // both orders fulfilled
    const fulfilledIds = fulfilled.map((e) => (e.payload as Order).orderId).sort();
    expect(fulfilledIds).toEqual(["A-1001", "B-2002"]);

    // enrichment ran (tier assigned) and fraud scoring ran (score recorded)
    const bigOrder = fulfilled.map((e) => e.payload as Order).find((o) => o.orderId === "B-2002")!;
    expect(bigOrder.tier).toBe("gold");
    expect(bigOrder.fraudScore).toBe(0.7); // max of FraudRules 0.7 / FraudVelocity 0.2

    // the high-value order went through the approval gate; the low-value one did not
    const treeB = mesh.tracer.renderTree(orderB.correlationId);
    expect(treeB).toContain("orders/approval/needed");
    expect(treeB).toContain("approvals/requested");
    expect(treeB).toContain("orders/ready");
    const treeA = mesh.tracer.renderTree(orderA.correlationId);
    expect(treeA).not.toContain("orders/approval/needed");
    expect(treeA).toContain("orders/ready");

    // the failing ERP sync dead-lettered both fulfilled orders with metadata
    expect(dlq.length).toBe(2);
    for (const dead of dlq) {
      expect(dead.topic).toBe("dlq/orders/fulfilled");
      expect(dead.headers["x-failed-agent"]).toBe("LegacyErpSync");
      expect(dead.headers["x-retries"]).toBe("3");
      expect(dead.headers["x-error"]).toContain("ERP endpoint unreachable");
    }
    // dead letters stay on their order's correlation chain
    const dlqCorrelations = dlq.map((e) => e.correlationId).sort();
    expect(dlqCorrelations).toEqual([orderA.correlationId, orderB.correlationId].sort());
  }, 15000);

  it("records the expected per-agent outcomes in metrics", async () => {
    const { mesh } = await runDemo(() => {});
    const agents = mesh.metrics().agents;
    expect(agents["OrderIntake"].processed).toBe(2);
    expect(agents["FraudCheck"].processed).toBe(2);
    expect(agents["FraudRules"].processed).toBe(2);
    expect(agents["FraudVelocity"].processed).toBe(2);
    expect(agents["Enrichment"].processed).toBe(2);
    expect(agents["Router"].processed).toBe(2);
    expect(agents["Fulfillment"].processed).toBe(2);
    // LegacyErpSync never succeeds: one recorded failure per order (after internal retries)
    expect(agents["LegacyErpSync"].failed).toBe(2);
    expect(agents["LegacyErpSync"].processed).toBe(0);
  }, 15000);

  it("saga progress events appear in the fulfillment trace", async () => {
    const { mesh, orderA } = await runDemo(() => {});
    const trace = mesh.tracer.traceOf(orderA.correlationId);
    const topics = trace.filter((e) => e.kind === "publish").map((e) => e.topic);
    expect(topics).toContain("saga/fulfillment/reserve-inventory/completed");
    expect(topics).toContain("saga/fulfillment/charge-payment/completed");
  }, 15000);
});
