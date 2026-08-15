import { describe, expect, it } from "vitest";
import type { Envelope } from "../src/envelope.js";
import { Mesh } from "../src/mesh.js";
import { ApprovalGate } from "../src/patterns.js";

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

function setup(timeoutMs = 1000) {
  const mesh = new Mesh();
  const gate = new ApprovalGate("Gate", "work/pending", {
    approvedTopic: "work/approved",
    rejectedTopic: "work/rejected",
    escalationTopic: "work/escalated",
    timeoutMs,
    idOf: (env) => (env.payload as { id: string }).id,
  });
  mesh.register(gate);
  const out: Record<string, Envelope[]> = { approved: [], rejected: [], escalated: [], requested: [] };
  mesh.bus.subscribe("work/approved", (e) => void out.approved.push(e), { label: "t" });
  mesh.bus.subscribe("work/rejected", (e) => void out.rejected.push(e), { label: "t" });
  mesh.bus.subscribe("work/escalated", (e) => void out.escalated.push(e), { label: "t" });
  mesh.bus.subscribe("approvals/requested", (e) => void out.requested.push(e), { label: "t" });
  mesh.start();
  return { mesh, gate, out };
}

describe("human-in-the-loop approval gate", () => {
  it("parks the envelope and announces the approval request", async () => {
    const { mesh, gate, out } = setup();
    await mesh.publish("work/pending", { id: "job-1", amount: 9000 });
    await mesh.drain();
    expect(gate.pending()).toEqual(["job-1"]);
    expect(out.requested.length).toBe(1);
    expect(out.requested[0].payload).toEqual({ id: "job-1", topic: "work/pending" });
    expect(out.approved).toEqual([]);
    await mesh.stop();
  });

  it("resumes on approval, preserving payload and correlation across the pause", async () => {
    const { mesh, gate, out } = setup();
    const root = await mesh.publish("work/pending", { id: "job-2", amount: 9000 });
    await mesh.drain();
    await mesh.publish("approvals/job-2", { approved: true }, { "x-approver": "alice" });
    await mesh.drain();
    expect(out.approved.length).toBe(1);
    expect(out.approved[0].payload).toEqual({ id: "job-2", amount: 9000 });
    expect(out.approved[0].correlationId).toBe(root.id); // chain survives the human pause
    expect(out.approved[0].headers["x-approved-by"]).toBe("alice");
    expect(gate.pending()).toEqual([]);
    await mesh.stop();
  });

  it("routes rejections to the rejected topic", async () => {
    const { mesh, out } = setup();
    await mesh.publish("work/pending", { id: "job-3" });
    await mesh.drain();
    await mesh.publish("approvals/job-3", { approved: false });
    await mesh.drain();
    expect(out.rejected.length).toBe(1);
    expect(out.approved).toEqual([]);
    await mesh.stop();
  });

  it("escalates after the timeout when no decision arrives", async () => {
    const { mesh, gate, out } = setup(50);
    await mesh.publish("work/pending", { id: "job-4" });
    await mesh.drain();
    expect(gate.pending()).toEqual(["job-4"]);
    await sleep(80);
    await mesh.drain();
    expect(out.escalated.length).toBe(1);
    expect(out.escalated[0].headers["x-reason"]).toBe("timeout");
    expect(gate.pending()).toEqual([]);
    expect(out.approved).toEqual([]);
    await mesh.stop();
  });

  it("ignores decisions for unknown ids and post-timeout approvals", async () => {
    const { mesh, out } = setup(40);
    await mesh.publish("approvals/never-parked", { approved: true });
    await mesh.publish("work/pending", { id: "job-5" });
    await mesh.drain();
    await sleep(70); // timeout fires first
    await mesh.publish("approvals/job-5", { approved: true }); // too late
    await mesh.drain();
    expect(out.approved).toEqual([]);
    expect(out.escalated.length).toBe(1);
    await mesh.stop();
  });

  it("handles multiple parked envelopes independently", async () => {
    const { mesh, gate, out } = setup();
    await mesh.publish("work/pending", { id: "a" });
    await mesh.publish("work/pending", { id: "b" });
    await mesh.drain();
    expect(gate.pending().sort()).toEqual(["a", "b"]);
    await mesh.publish("approvals/b", { approved: true });
    await mesh.drain();
    expect(out.approved.map((e) => (e.payload as { id: string }).id)).toEqual(["b"]);
    expect(gate.pending()).toEqual(["a"]);
    await mesh.stop();
  });
});
