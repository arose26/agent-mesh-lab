import type { Agent, AgentContext } from "./agent.js";
import { InMemoryBus, type BackpressurePolicy, type Subscription } from "./bus.js";
import { childEnvelope, createEnvelope, type Envelope } from "./envelope.js";
import { withRetryDlq, type RetryPolicy } from "./patterns.js";
import { Tracer } from "./trace.js";

export interface AgentStats {
  processed: number;
  failed: number;
  /** Handler latency percentiles in ms, from real timings. */
  p50: number;
  p95: number;
  p99: number;
}

export interface MeshMetrics {
  agents: Record<string, AgentStats>;
  queues: Array<{ label: string; pattern: string; depth: number; dropped: number }>;
}

export interface MeshOptions {
  maxQueueDepth?: number;
  backpressure?: BackpressurePolicy;
  traceCapacity?: number;
}

interface Registered {
  agent: Agent;
  retry?: RetryPolicy;
}

/**
 * The orchestrator: owns the bus and the tracer, registers agents, wires
 * their subscriptions with per-delivery contexts (child-envelope publishing,
 * reply), optionally wraps handlers with retry+DLQ, and collects metrics.
 */
export class Mesh {
  readonly bus: InMemoryBus;
  readonly tracer: Tracer;
  private readonly registered: Registered[] = [];
  private readonly subs: Subscription[] = [];
  private readonly stats = new Map<string, { processed: number; failed: number; latencies: number[] }>();
  private started = false;

  constructor(opts: MeshOptions = {}) {
    this.tracer = new Tracer(opts.traceCapacity);
    const pick = (env: Envelope) => ({
      topic: env.topic,
      envelopeId: env.id,
      correlationId: env.correlationId,
      causationId: env.causationId,
    });
    this.bus = new InMemoryBus({
      maxQueueDepth: opts.maxQueueDepth,
      backpressure: opts.backpressure,
      observer: {
        onPublish: (env) => this.tracer.record({ kind: "publish", ...pick(env) }),
        onDeliver: (env, label) => this.tracer.record({ kind: "deliver", agent: label, ...pick(env) }),
        onAck: (env, label, latencyMs) => {
          this.tracer.record({ kind: "ack", agent: label, ...pick(env) });
          const s = this.statsFor(label);
          s.processed++;
          s.latencies.push(latencyMs);
        },
        onFail: (env, label, error) => {
          this.tracer.record({ kind: "fail", agent: label, error: String(error), ...pick(env) });
          this.statsFor(label).failed++;
        },
        onDrop: (env, label) => this.tracer.record({ kind: "drop", agent: label, ...pick(env) }),
      },
    });
  }

  /** Register an agent; pass a retry policy to get retry-then-DLQ semantics on its handler. */
  register(agent: Agent, opts: { retry?: RetryPolicy } = {}): this {
    const entry: Registered = { agent, retry: opts.retry };
    this.registered.push(entry);
    if (this.started) this.wire(entry);
    return this;
  }

  start(): this {
    if (this.started) return this;
    this.started = true;
    for (const entry of this.registered) this.wire(entry);
    return this;
  }

  async stop(): Promise<void> {
    for (const sub of this.subs) sub.close();
    this.subs.length = 0;
    for (const { agent } of this.registered) agent.dispose?.();
    this.started = false;
  }

  /** Publish a root envelope (starts a new correlation chain). */
  async publish<T>(topic: string, payload: T, headers: Record<string, string> = {}): Promise<Envelope<T>> {
    const env = createEnvelope(topic, payload, headers);
    await this.bus.publish(env);
    return env;
  }

  /** Resolve when every queue is empty and no handler is running. */
  async drain(): Promise<void> {
    await this.bus.drain();
  }

  metrics(): MeshMetrics {
    const agents: Record<string, AgentStats> = {};
    for (const [label, s] of this.stats) {
      const sorted = [...s.latencies].sort((a, b) => a - b);
      agents[label] = {
        processed: s.processed,
        failed: s.failed,
        p50: percentile(sorted, 50),
        p95: percentile(sorted, 95),
        p99: percentile(sorted, 99),
      };
    }
    return {
      agents,
      queues: this.bus.subscriptions().map((s) => ({
        label: s.label,
        pattern: s.pattern,
        depth: s.depth(),
        dropped: s.dropped(),
      })),
    };
  }

  private wire(entry: Registered): void {
    const { agent, retry } = entry;
    let handler = (env: Envelope): void | Promise<void> => agent.handle(env, this.contextFor(env));
    if (retry) handler = withRetryDlq(this.bus, agent.name, handler, retry);
    for (const pattern of agent.subscriptions) {
      this.subs.push(this.bus.subscribe(pattern, handler, { label: agent.name }));
    }
  }

  private contextFor(env: Envelope): AgentContext {
    const bus = this.bus;
    return {
      bus,
      envelope: env,
      async publish<T>(topic: string, payload: T, headers: Record<string, string> = {}): Promise<Envelope<T>> {
        const child = childEnvelope(env, topic, payload, headers);
        await bus.publish(child);
        return child;
      },
      async reply<T>(payload: T, headers: Record<string, string> = {}): Promise<void> {
        const replyTo = env.headers["replyTo"];
        if (!replyTo) throw new Error(`cannot reply: envelope ${env.id} has no replyTo header`);
        await bus.publish(childEnvelope(env, replyTo, payload, headers));
      },
    };
  }

  private statsFor(label: string): { processed: number; failed: number; latencies: number[] } {
    let s = this.stats.get(label);
    if (!s) {
      s = { processed: 0, failed: 0, latencies: [] };
      this.stats.set(label, s);
    }
    return s;
  }
}

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1));
  return sorted[idx];
}
