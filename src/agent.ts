import type { Bus } from "./bus.js";
import type { Envelope } from "./envelope.js";

/**
 * Per-delivery context handed to an agent alongside the envelope. Publishing
 * through the context derives child envelopes, so causality and correlation
 * are preserved without the agent thinking about it.
 */
export interface AgentContext {
  /** The underlying bus, for patterns that need direct access (scatter-gather etc.). */
  readonly bus: Bus;
  /** The envelope currently being handled. */
  readonly envelope: Envelope;
  /** Publish a child envelope caused by the one being handled. Returns the published envelope. */
  publish<T>(topic: string, payload: T, headers?: Record<string, string>): Promise<Envelope<T>>;
  /** Reply to the sender via the inbound envelope's `replyTo` header. Throws if there is none. */
  reply<T>(payload: T, headers?: Record<string, string>): Promise<void>;
}

/**
 * An agent is a named event consumer/producer: it declares topic
 * subscriptions (wildcards allowed) and handles one envelope at a time.
 */
export interface Agent {
  readonly name: string;
  readonly subscriptions: readonly string[];
  handle(env: Envelope, ctx: AgentContext): void | Promise<void>;
  /** Optional cleanup hook called by Mesh.stop() (timers, parked state, ...). */
  dispose?(): void;
}

/** Define an agent from a plain function. */
export function agent(name: string, subscriptions: string[], handle: Agent["handle"]): Agent {
  return { name, subscriptions, handle };
}

/**
 * Wraps a deterministic function as an agent: consume the inbound payload,
 * publish the function's result on `outTopic`.
 */
export class ToolAgent<I, O> implements Agent {
  readonly subscriptions: readonly string[];

  constructor(
    readonly name: string,
    inTopic: string,
    private readonly outTopic: string,
    private readonly fn: (input: I) => O | Promise<O>,
  ) {
    this.subscriptions = [inTopic];
  }

  async handle(env: Envelope, ctx: AgentContext): Promise<void> {
    const output = await this.fn(env.payload as I);
    await ctx.publish(this.outTopic, output);
  }
}

/**
 * Provider seam for LLM-backed agents. A production adapter calls a hosted
 * model; `MockLLM` keeps the runtime deterministic and testable offline.
 */
export interface LLMProvider {
  complete(prompt: string): Promise<string>;
}

/** Scripted provider: first substring key found in the prompt wins. */
export class MockLLM implements LLMProvider {
  constructor(
    private readonly script: Record<string, string>,
    private readonly fallback = "",
  ) {}

  async complete(prompt: string): Promise<string> {
    for (const [needle, response] of Object.entries(this.script)) {
      if (prompt.includes(needle)) return response;
    }
    return this.fallback;
  }
}

/**
 * Skeleton for an LLM-backed agent: build a prompt from the envelope, get a
 * completion from the provider, publish it. Swap `MockLLM` for a real
 * provider adapter and nothing else changes.
 */
export class LLMAgent implements Agent {
  readonly subscriptions: readonly string[];

  constructor(
    readonly name: string,
    inTopic: string,
    private readonly outTopic: string,
    private readonly provider: LLMProvider,
    private readonly buildPrompt: (env: Envelope) => string = (env) => JSON.stringify(env.payload),
  ) {
    this.subscriptions = [inTopic];
  }

  async handle(env: Envelope, ctx: AgentContext): Promise<void> {
    const completion = await this.provider.complete(this.buildPrompt(env));
    await ctx.publish(this.outTopic, { completion });
  }
}
