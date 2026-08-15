import { randomUUID } from "node:crypto";

/**
 * The unit of communication on the mesh. Every message is an immutable
 * envelope carrying identity, causality, and correlation metadata alongside
 * its payload — this is what makes end-to-end tracing possible.
 */
export interface Envelope<T = unknown> {
  /** Unique id of this envelope. */
  readonly id: string;
  /** Topic the envelope was (or will be) published on. */
  readonly topic: string;
  /** Id of the envelope that directly caused this one (parent in the causal tree). */
  readonly causationId?: string;
  /** Stable id shared by every envelope in a causal chain (the root's id). */
  readonly correlationId: string;
  /** Creation time, epoch milliseconds. */
  readonly timestamp: number;
  /** String metadata (replyTo, x-error, x-retries, ...). */
  readonly headers: Readonly<Record<string, string>>;
  readonly payload: T;
}

/** Create a root envelope: it starts a new correlation chain (correlationId = id). */
export function createEnvelope<T>(
  topic: string,
  payload: T,
  headers: Record<string, string> = {},
): Envelope<T> {
  const id = randomUUID();
  return { id, topic, correlationId: id, timestamp: Date.now(), headers, payload };
}

/**
 * Derive a child envelope from a parent: fresh id, causationId = parent.id,
 * correlationId preserved. This is the only sanctioned way to continue a
 * causal chain — use it and `traceOf(correlationId)` can rebuild the tree.
 */
export function childEnvelope<T>(
  parent: Envelope,
  topic: string,
  payload: T,
  headers: Record<string, string> = {},
): Envelope<T> {
  return {
    id: randomUUID(),
    topic,
    causationId: parent.id,
    correlationId: parent.correlationId,
    timestamp: Date.now(),
    headers,
    payload,
  };
}
