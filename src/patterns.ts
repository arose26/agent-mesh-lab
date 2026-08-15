import { randomUUID } from "node:crypto";
import type { Agent, AgentContext } from "./agent.js";
import { topicMatches, type Bus } from "./bus.js";
import { childEnvelope, createEnvelope, type Envelope } from "./envelope.js";

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

// ---------------------------------------------------------------------------
// Saga / workflow with compensation
// ---------------------------------------------------------------------------

export interface SagaStep<S> {
  name: string;
  run(state: S): S | Promise<S>;
  /** Undo this step. Called in reverse completion order when a later step fails. */
  compensate?(state: S): void | Promise<void>;
}

export type SagaResult<S> =
  | { ok: true; state: S }
  | { ok: false; failedStep: string; error: unknown; compensated: string[] };

/**
 * Run steps in order; on failure, compensate every completed step in reverse
 * order. Pass `emit` (e.g. `(t, p) => ctx.publish(t, p)`) to publish
 * `saga/<name>/<step>/{started,completed,compensated}` progress events onto
 * the bus so sagas show up in traces.
 */
export async function runSaga<S>(
  name: string,
  steps: SagaStep<S>[],
  initial: S,
  emit?: (topic: string, payload: unknown) => unknown,
): Promise<SagaResult<S>> {
  const completed: SagaStep<S>[] = [];
  let state = initial;
  for (const step of steps) {
    try {
      await emit?.(`saga/${name}/${step.name}/started`, {});
      state = await step.run(state);
      completed.push(step);
      await emit?.(`saga/${name}/${step.name}/completed`, {});
    } catch (error) {
      const compensated: string[] = [];
      for (const done of [...completed].reverse()) {
        if (!done.compensate) continue;
        await done.compensate(state);
        compensated.push(done.name);
        await emit?.(`saga/${name}/${done.name}/compensated`, {});
      }
      await emit?.(`saga/${name}/failed`, { step: step.name, error: String(error) });
      return { ok: false, failedStep: step.name, error, compensated };
    }
  }
  return { ok: true, state };
}

// ---------------------------------------------------------------------------
// Scatter-gather
// ---------------------------------------------------------------------------

export interface GatherResult {
  replies: Envelope[];
  /** True if `quorum` replies arrived before the timeout. */
  quorumMet: boolean;
}

/**
 * Fan a request out to every subscriber of `topic` and gather replies on a
 * unique reply topic. Resolves as soon as `quorum` replies arrive, or at the
 * timeout with whatever was collected (`quorumMet: false`).
 */
export async function scatterGather(
  bus: Bus,
  topic: string,
  payload: unknown,
  opts: { quorum: number; timeoutMs: number; parent?: Envelope; headers?: Record<string, string> },
): Promise<GatherResult> {
  const replyTo = `gather/${randomUUID().slice(0, 8)}`;
  const headers = { ...opts.headers, replyTo };
  const env = opts.parent
    ? childEnvelope(opts.parent, topic, payload, headers)
    : createEnvelope(topic, payload, headers);

  const replies: Envelope[] = [];
  return new Promise<GatherResult>((resolve) => {
    const finish = (quorumMet: boolean): void => {
      clearTimeout(timer);
      sub.close();
      resolve({ replies: [...replies], quorumMet });
    };
    const sub = bus.subscribe(
      replyTo,
      (reply) => {
        replies.push(reply);
        if (replies.length >= opts.quorum) finish(true);
      },
      { label: "gather" },
    );
    const timer = setTimeout(() => finish(replies.length >= opts.quorum), opts.timeoutMs);
    void bus.publish(env);
  });
}

// ---------------------------------------------------------------------------
// Retry with exponential backoff, then dead-letter queue
// ---------------------------------------------------------------------------

export interface RetryPolicy {
  maxRetries: number;
  baseDelayMs: number;
  /** Backoff multiplier per attempt. Default 2. */
  factor?: number;
}

/** Delay before each retry: [base, base*f, base*f^2, ...] — length maxRetries. */
export function backoffDelays(policy: RetryPolicy): number[] {
  const f = policy.factor ?? 2;
  return Array.from({ length: policy.maxRetries }, (_, i) => policy.baseDelayMs * f ** i);
}

/**
 * Wrap a handler with retry + DLQ semantics: on failure retry up to
 * `maxRetries` times with exponential backoff; once exhausted, republish the
 * envelope to `dlq/<original topic>` with error metadata headers, then
 * rethrow so the failure is still traced and counted against the agent.
 */
export function withRetryDlq(
  bus: Bus,
  agentName: string,
  handler: (env: Envelope) => void | Promise<void>,
  policy: RetryPolicy,
): (env: Envelope) => Promise<void> {
  const delays = backoffDelays(policy);
  return async (env: Envelope): Promise<void> => {
    let lastError: unknown;
    for (let attempt = 0; attempt <= policy.maxRetries; attempt++) {
      if (attempt > 0) await sleep(delays[attempt - 1]);
      try {
        await handler(env);
        return;
      } catch (error) {
        lastError = error;
      }
    }
    await bus.publish(
      childEnvelope(env, `dlq/${env.topic}`, env.payload, {
        "x-error": String(lastError),
        "x-retries": String(policy.maxRetries),
        "x-failed-agent": agentName,
        "x-original-topic": env.topic,
      }),
    );
    throw lastError;
  };
}

// ---------------------------------------------------------------------------
// Human-in-the-loop approval gate
// ---------------------------------------------------------------------------

export interface ApprovalGateOptions {
  /** Where approved envelopes resume. */
  approvedTopic: string;
  /** Where rejected envelopes go. */
  rejectedTopic: string;
  /** Where envelopes escalate if no decision arrives within timeoutMs. */
  escalationTopic: string;
  timeoutMs: number;
  /** Extract the approval id from a parked envelope (e.g. the order id). */
  idOf(env: Envelope): string;
}

/** Payload expected on `approvals/<id>` decision messages. */
export interface ApprovalDecision {
  approved: boolean;
}

/**
 * Human-in-the-loop gate: envelopes arriving on `inTopic` are parked and an
 * `approvals/requested` event is published. The flow resumes when a human (or
 * any agent) publishes `approvals/<id>` with `{ approved: boolean }` —
 * approved envelopes continue on `approvedTopic`, rejected on
 * `rejectedTopic`, both as children of the parked envelope so the causal
 * chain survives the pause. If no decision arrives within `timeoutMs`, the
 * envelope escalates to `escalationTopic`.
 */
export class ApprovalGate implements Agent {
  readonly subscriptions: readonly string[];
  private readonly inTopic: string;
  private readonly parked = new Map<string, { env: Envelope; ctx: AgentContext; timer: NodeJS.Timeout }>();

  constructor(
    readonly name: string,
    inTopic: string,
    private readonly opts: ApprovalGateOptions,
  ) {
    this.inTopic = inTopic;
    this.subscriptions = [inTopic, "approvals/*"];
  }

  async handle(env: Envelope, ctx: AgentContext): Promise<void> {
    if (topicMatches(this.inTopic, env.topic)) return this.park(env, ctx);
    return this.onDecision(env);
  }

  /** Ids currently awaiting a decision. */
  pending(): string[] {
    return [...this.parked.keys()];
  }

  dispose(): void {
    for (const { timer } of this.parked.values()) clearTimeout(timer);
    this.parked.clear();
  }

  private async park(env: Envelope, ctx: AgentContext): Promise<void> {
    const id = this.opts.idOf(env);
    const timer = setTimeout(() => {
      const entry = this.parked.get(id);
      if (!entry) return;
      this.parked.delete(id);
      void entry.ctx.publish(this.opts.escalationTopic, entry.env.payload, {
        "x-approval-id": id,
        "x-reason": "timeout",
      });
    }, this.opts.timeoutMs);
    this.parked.set(id, { env, ctx, timer });
    await ctx.publish("approvals/requested", { id, topic: env.topic }, { "x-approval-id": id });
  }

  private async onDecision(env: Envelope): Promise<void> {
    const id = env.topic.split("/").pop()!;
    const entry = this.parked.get(id);
    if (!entry) return; // unknown, duplicate, or post-timeout decision: ignore
    clearTimeout(entry.timer);
    this.parked.delete(id);
    const approved = (env.payload as Partial<ApprovalDecision> | undefined)?.approved === true;
    await entry.ctx.publish(approved ? this.opts.approvedTopic : this.opts.rejectedTopic, entry.env.payload, {
      "x-approval-id": id,
      "x-approved-by": env.headers["x-approver"] ?? "unknown",
    });
  }
}
