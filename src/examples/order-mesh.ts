/**
 * Order-processing mesh demo. Fully offline and deterministic:
 *
 *   orders/new -> OrderIntake -> orders/received
 *     -> FraudCheck (scatter-gather across FraudRules + FraudVelocity, quorum 2)
 *     -> orders/scored -> Enrichment (ToolAgent) -> orders/enriched -> Router
 *          low value  -> orders/ready
 *          high value -> orders/approval/needed -> HighValueApproval (HITL gate,
 *                        parks until approvals/<id> arrives) -> orders/ready
 *     -> Fulfillment (2-step saga) -> orders/fulfilled
 *     -> LegacyErpSync (always fails) -> retries w/ backoff -> dlq/orders/fulfilled
 *
 * Run with: node dist/examples/order-mesh.js
 */
import { agent, ToolAgent } from "../agent.js";
import { childEnvelope, type Envelope } from "../envelope.js";
import { Mesh } from "../mesh.js";
import { ApprovalGate, runSaga, scatterGather } from "../patterns.js";

export interface Order {
  orderId: string;
  customerId: string;
  total: number;
  items: string[];
  tier?: string;
  fraudScore?: number;
}

export function buildOrderMesh(): { mesh: Mesh; gate: ApprovalGate } {
  const mesh = new Mesh();

  const intake = agent("OrderIntake", ["orders/new"], async (env, ctx) => {
    await ctx.publish("orders/received", env.payload);
  });

  // Two deterministic fraud heuristics answering scatter-gather requests.
  const fraudRules = agent("FraudRules", ["fraud/check"], async (env, ctx) => {
    const order = env.payload as Order;
    await ctx.reply({ agent: "FraudRules", score: order.total > 3000 ? 0.7 : 0.1 });
  });
  const fraudVelocity = agent("FraudVelocity", ["fraud/check"], async (env, ctx) => {
    const order = env.payload as Order;
    await ctx.reply({ agent: "FraudVelocity", score: order.items.length > 5 ? 0.6 : 0.2 });
  });

  const fraudCheck = agent("FraudCheck", ["orders/received"], async (env, ctx) => {
    const { replies, quorumMet } = await scatterGather(ctx.bus, "fraud/check", env.payload, {
      quorum: 2,
      timeoutMs: 1000,
      parent: env,
    });
    const scores = replies.map((r) => (r.payload as { score: number }).score);
    const fraudScore = quorumMet ? Math.max(...scores) : 1; // fail closed on missing quorum
    await ctx.publish("orders/scored", { ...(env.payload as Order), fraudScore });
  });

  const enrichment = new ToolAgent<Order, Order>("Enrichment", "orders/scored", "orders/enriched", (order) => ({
    ...order,
    tier: order.total > 1000 ? "gold" : "standard",
  }));

  const router = agent("Router", ["orders/enriched"], async (env, ctx) => {
    const order = env.payload as Order;
    await ctx.publish(order.total > 1000 ? "orders/approval/needed" : "orders/ready", order);
  });

  const gate = new ApprovalGate("HighValueApproval", "orders/approval/needed", {
    approvedTopic: "orders/ready",
    rejectedTopic: "orders/rejected",
    escalationTopic: "orders/approval/escalated",
    timeoutMs: 5000,
    idOf: (env) => (env.payload as Order).orderId,
  });

  const fulfillment = agent("Fulfillment", ["orders/ready"], async (env, ctx) => {
    const order = env.payload as Order;
    const result = await runSaga(
      "fulfillment",
      [
        { name: "reserve-inventory", run: (s: Order) => s, compensate: () => {} },
        { name: "charge-payment", run: (s: Order) => s, compensate: () => {} },
      ],
      order,
      (topic, payload) => ctx.publish(topic, payload),
    );
    if (result.ok) await ctx.publish("orders/fulfilled", result.state);
  });

  // Deliberately broken downstream sync: exercises retry w/ backoff, then DLQ.
  const legacySync = agent("LegacyErpSync", ["orders/fulfilled"], async () => {
    throw new Error("ERP endpoint unreachable");
  });

  mesh
    .register(intake)
    .register(fraudRules)
    .register(fraudVelocity)
    .register(fraudCheck)
    .register(enrichment)
    .register(router)
    .register(gate)
    .register(fulfillment)
    .register(legacySync, { retry: { maxRetries: 3, baseDelayMs: 25 } });

  return { mesh, gate };
}

export interface DemoResult {
  mesh: Mesh;
  fulfilled: Envelope[];
  dlq: Envelope[];
  orderA: Envelope<Order>;
  orderB: Envelope<Order>;
}

export async function runDemo(log: (line: string) => void = console.log): Promise<DemoResult> {
  const { mesh } = buildOrderMesh();

  const fulfilled: Envelope[] = [];
  const dlq: Envelope[] = [];
  let fulfilledDone!: () => void;
  let dlqDone!: () => void;
  const allFulfilled = new Promise<void>((r) => (fulfilledDone = r));
  const allDlq = new Promise<void>((r) => (dlqDone = r));

  mesh.bus.subscribe(
    "orders/fulfilled",
    (env) => {
      const order = env.payload as Order;
      log(`[FULFILLED] ${order.orderId} total=$${order.total} tier=${order.tier} fraudScore=${order.fraudScore}`);
      fulfilled.push(env);
      if (fulfilled.length === 2) fulfilledDone();
    },
    { label: "FulfillMonitor" },
  );

  mesh.bus.subscribe(
    "dlq/>",
    (env) => {
      log(
        `[DLQ] ${env.topic} agent=${env.headers["x-failed-agent"]} retries=${env.headers["x-retries"]} error="${env.headers["x-error"]}"`,
      );
      dlq.push(env);
      if (dlq.length === 2) dlqDone();
    },
    { label: "DlqMonitor" },
  );

  // Simulated human: sees the approval request, thinks for 250ms, approves.
  mesh.bus.subscribe(
    "approvals/requested",
    async (env) => {
      const { id } = env.payload as { id: string };
      log(`[HITL] ${id} parked, awaiting human approval...`);
      await new Promise((r) => setTimeout(r, 250)); // simulated human think time
      log(`[HITL] ${id} approved by demo-human, resuming flow`);
      await mesh.bus.publish(childEnvelope(env, `approvals/${id}`, { approved: true }, { "x-approver": "demo-human" }));
    },
    { label: "DemoApprover" },
  );

  mesh.start();
  log("mesh started: 9 agents wired\n");

  const orderA = await mesh.publish<Order>("orders/new", {
    orderId: "A-1001",
    customerId: "cust-42",
    total: 250,
    items: ["book"],
  });
  const orderB = await mesh.publish<Order>("orders/new", {
    orderId: "B-2002",
    customerId: "cust-77",
    total: 5200,
    items: ["laptop", "dock"],
  });

  await allFulfilled;
  await allDlq;
  await mesh.drain();

  log(`\n--- causal trace: order A-1001 (correlation ${orderA.correlationId.slice(0, 8)}) ---`);
  log(mesh.tracer.renderTree(orderA.correlationId));
  log(`\n--- causal trace: order B-2002 (correlation ${orderB.correlationId.slice(0, 8)}) ---`);
  log(mesh.tracer.renderTree(orderB.correlationId));

  const metrics = mesh.metrics();
  log("\n--- agent metrics ---");
  log(`${"agent".padEnd(18)}${"processed".padEnd(11)}${"failed".padEnd(8)}p50/p95/p99 ms`);
  for (const [name, s] of Object.entries(metrics.agents)) {
    log(
      `${name.padEnd(18)}${String(s.processed).padEnd(11)}${String(s.failed).padEnd(8)}${s.p50.toFixed(1)}/${s.p95.toFixed(1)}/${s.p99.toFixed(1)}`,
    );
  }

  log(`\ndead letters: ${dlq.length} (topics: ${dlq.map((e) => e.topic).join(", ")})`);

  await mesh.stop();
  return { mesh, fulfilled, dlq, orderA, orderB };
}

// Run directly: node dist/examples/order-mesh.js
import { pathToFileURL } from "node:url";
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  runDemo().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
