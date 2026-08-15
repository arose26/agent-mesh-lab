import { randomUUID } from "node:crypto";
import { childEnvelope, createEnvelope, type Envelope } from "./envelope.js";

/**
 * Solace/MQTT-style topic matching over `/`-separated levels.
 *
 *  - `*` matches exactly one level        (`orders/*`  matches `orders/received`, not `orders/us/received`)
 *  - `>` matches one or more remaining levels and must be the final token
 *                                          (`orders/>`  matches `orders/received` and `orders/us/received`, not `orders`)
 *  - anything else matches literally
 *
 * A `>` anywhere but the final position is treated as a literal level.
 */
export function topicMatches(pattern: string, topic: string): boolean {
  const p = pattern.split("/");
  const t = topic.split("/");
  for (let i = 0; i < p.length; i++) {
    if (p[i] === ">" && i === p.length - 1) return t.length > i;
    if (i >= t.length) return false;
    if (p[i] !== "*" && p[i] !== t[i]) return false;
  }
  return t.length === p.length;
}

export type Handler = (env: Envelope) => void | Promise<void>;

export type BackpressurePolicy = "drop-oldest" | "drop-new" | "block";

export interface SubscribeOptions {
  /** Name shown in traces/metrics (usually the agent name). */
  label?: string;
}

export interface Subscription {
  readonly pattern: string;
  readonly label: string;
  /** Messages currently queued for this subscriber. */
  depth(): number;
  /** Messages dropped by backpressure policy. */
  dropped(): number;
  close(): void;
}

/**
 * The broker seam. `InMemoryBus` is the reference implementation; an adapter
 * over a real broker (Solace, MQTT, AMQP) implements this same interface —
 * publish/subscribe with hierarchical wildcard topics and a request/reply
 * convention via a `replyTo` header. Everything above the bus (agents, mesh,
 * patterns, tracing) is broker-agnostic.
 */
export interface Bus {
  /** Publish a pre-built envelope on its topic. Resolves once accepted by every matching subscriber queue. */
  publish(env: Envelope): Promise<void>;
  /** Convenience: wrap a payload in a new root envelope and publish it. */
  emit<T>(topic: string, payload: T, headers?: Record<string, string>): Promise<Envelope<T>>;
  subscribe(pattern: string, handler: Handler, opts?: SubscribeOptions): Subscription;
  /** Request/reply: publish with a unique replyTo topic, resolve with the first reply. */
  request<T = unknown>(
    topic: string,
    payload: unknown,
    opts?: { timeoutMs?: number; parent?: Envelope; headers?: Record<string, string> },
  ): Promise<Envelope<T>>;
}

/** Lifecycle hooks used by the mesh for tracing and metrics. */
export interface BusObserver {
  onPublish?(env: Envelope): void;
  onDeliver?(env: Envelope, label: string): void;
  onAck?(env: Envelope, label: string, latencyMs: number): void;
  onFail?(env: Envelope, label: string, error: unknown): void;
  onDrop?(env: Envelope, label: string, policy: BackpressurePolicy): void;
}

export interface BusOptions {
  /** Max queued envelopes per subscriber before backpressure kicks in. Default 1024. */
  maxQueueDepth?: number;
  /** What to do when a subscriber queue is full. Default "block". */
  backpressure?: BackpressurePolicy;
  observer?: BusObserver;
}

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

class Sub implements Subscription {
  private readonly queue: Envelope[] = [];
  private takers: Array<(env: Envelope | undefined) => void> = [];
  private spaceWaiters: Array<() => void> = [];
  private droppedCount = 0;
  closed = false;

  constructor(
    readonly pattern: string,
    readonly label: string,
    readonly handler: Handler,
  ) {}

  depth(): number {
    return this.queue.length;
  }

  dropped(): number {
    return this.droppedCount;
  }

  async offer(env: Envelope, cap: number, policy: BackpressurePolicy, observer?: BusObserver): Promise<void> {
    if (this.closed) return;
    const taker = this.takers.shift();
    if (taker) {
      taker(env);
      return;
    }
    if (this.queue.length >= cap) {
      if (policy === "drop-new") {
        this.droppedCount++;
        observer?.onDrop?.(env, this.label, policy);
        return;
      }
      if (policy === "drop-oldest") {
        const evicted = this.queue.shift()!;
        this.droppedCount++;
        observer?.onDrop?.(evicted, this.label, policy);
      } else {
        // block: the publisher waits until the consumer makes room
        while (this.queue.length >= cap && !this.closed) {
          await new Promise<void>((r) => this.spaceWaiters.push(r));
        }
        if (this.closed) return;
      }
    }
    this.queue.push(env);
  }

  take(): Promise<Envelope | undefined> {
    if (this.queue.length > 0) {
      const env = this.queue.shift()!;
      this.spaceWaiters.shift()?.();
      return Promise.resolve(env);
    }
    if (this.closed) return Promise.resolve(undefined);
    return new Promise((r) => this.takers.push(r));
  }

  close(): void {
    this.closed = true;
    for (const t of this.takers) t(undefined);
    this.takers = [];
    for (const s of this.spaceWaiters) s();
    this.spaceWaiters = [];
  }
}

/**
 * In-process bus: per-subscriber bounded async queues, one sequential pump
 * per subscriber (a subscriber handles one envelope at a time; distinct
 * subscribers run concurrently), configurable backpressure.
 */
export class InMemoryBus implements Bus {
  private readonly subs: Sub[] = [];
  private readonly cap: number;
  private readonly policy: BackpressurePolicy;
  private readonly observer?: BusObserver;
  private inflight = 0;

  constructor(opts: BusOptions = {}) {
    this.cap = Math.max(1, opts.maxQueueDepth ?? 1024);
    this.policy = opts.backpressure ?? "block";
    this.observer = opts.observer;
  }

  async publish(env: Envelope): Promise<void> {
    this.observer?.onPublish?.(env);
    const matching = this.subs.filter((s) => !s.closed && topicMatches(s.pattern, env.topic));
    await Promise.all(matching.map((s) => s.offer(env, this.cap, this.policy, this.observer)));
  }

  async emit<T>(topic: string, payload: T, headers: Record<string, string> = {}): Promise<Envelope<T>> {
    const env = createEnvelope(topic, payload, headers);
    await this.publish(env);
    return env;
  }

  subscribe(pattern: string, handler: Handler, opts: SubscribeOptions = {}): Subscription {
    const sub = new Sub(pattern, opts.label ?? pattern, handler);
    this.subs.push(sub);
    void this.pump(sub);
    return sub;
  }

  async request<T = unknown>(
    topic: string,
    payload: unknown,
    opts: { timeoutMs?: number; parent?: Envelope; headers?: Record<string, string> } = {},
  ): Promise<Envelope<T>> {
    const timeoutMs = opts.timeoutMs ?? 5000;
    const replyTo = `reply/${randomUUID().slice(0, 8)}`;
    const headers = { ...opts.headers, replyTo };
    const env = opts.parent
      ? childEnvelope(opts.parent, topic, payload, headers)
      : createEnvelope(topic, payload, headers);
    return new Promise<Envelope<T>>((resolve, reject) => {
      const sub = this.subscribe(
        replyTo,
        (reply) => {
          clearTimeout(timer);
          sub.close();
          resolve(reply as Envelope<T>);
        },
        { label: "request" },
      );
      const timer = setTimeout(() => {
        sub.close();
        reject(new Error(`request timed out after ${timeoutMs}ms: ${topic}`));
      }, timeoutMs);
      void this.publish(env).catch(reject);
    });
  }

  /** Snapshot of every live subscription (for metrics). */
  subscriptions(): Subscription[] {
    return this.subs.filter((s) => !s.closed);
  }

  /**
   * Resolve when the bus is quiescent: all queues empty and no handler running.
   * ponytail: polling, not bookkeeping — swap for a completion latch if drain
   * ever sits on a hot path.
   */
  async drain(): Promise<void> {
    for (;;) {
      const busy = () => this.inflight > 0 || this.subs.some((s) => !s.closed && s.depth() > 0);
      if (!busy()) {
        await sleep(4); // let just-published envelopes land in queues
        if (!busy()) return;
      }
      await sleep(4);
    }
  }

  close(): void {
    for (const s of this.subs) s.close();
  }

  private async pump(sub: Sub): Promise<void> {
    for (;;) {
      const env = await sub.take();
      if (env === undefined) return; // closed
      this.observer?.onDeliver?.(env, sub.label);
      this.inflight++;
      const start = performance.now();
      try {
        await sub.handler(env);
        this.observer?.onAck?.(env, sub.label, performance.now() - start);
      } catch (error) {
        this.observer?.onFail?.(env, sub.label, error);
      } finally {
        this.inflight--;
      }
    }
  }
}
