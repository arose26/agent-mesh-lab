# Design notes

## Bus-first, not orchestrator-first

Most agent frameworks start from an orchestrator: a central planner that
calls agents like functions and owns the control flow. This runtime starts
from the opposite end: **the bus is the system**. Agents are event consumers
on hierarchical topics; the "orchestration" is emergent from topic wiring
plus a small set of explicit coordination patterns (saga, scatter-gather,
DLQ, HITL gate).

Why bus-first:

- **Loose coupling.** Producers do not know who consumes. Adding a second
  fraud heuristic to the demo means registering one more subscriber to
  `fraud/check` — no other agent changes.
- **Observability for free.** Because every interaction is a published
  envelope, one tap on the bus records the complete history. The causal tree
  in the demo is not instrumentation bolted onto agents; it is the bus's own
  view of the world.
- **Failure is data.** A dead-lettered envelope is just another message on
  `dlq/<topic>` — any agent (a human dashboard, a replayer, an alerter) can
  subscribe to it.
- **The orchestrator stays small.** `Mesh` only registers agents, wires
  subscriptions, wraps handlers (retry/DLQ, metrics, tracing contexts) and
  manages lifecycle. It holds no workflow state; workflow state lives in
  messages.

The cost: control flow is distributed, so you need tooling to see it. That
is why tracing (`trace.ts`) is a core module, not an add-on.

## The envelope and causality

Every message is an `Envelope`: id, topic, `causationId` (immediate parent),
`correlationId` (root of the chain), timestamp, string headers, payload.
Agents never construct envelopes by hand mid-flow — `ctx.publish` derives a
child envelope, so the causal chain survives every hop, including the pause
inside a human-approval gate. `traceOf(correlationId)` then rebuilds the
whole flow as a tree.

## Backpressure

Each subscriber owns a bounded queue and a single sequential pump (one
envelope at a time per subscriber; different subscribers run concurrently).
When a queue is full, the configured policy decides:

- **`block`** (default): the publisher awaits until the consumer makes room.
  End-to-end flow control; the right default for in-process meshes where
  losing messages is worse than slowing down.
- **`drop-oldest`**: keep the freshest data. Right for telemetry-like topics
  where only the latest value matters.
- **`drop-new`**: protect what is already queued. Right when older messages
  carry more value than newer ones (e.g. ordered work already admitted).

Drops are observable: counted per subscription and recorded as `drop` trace
events. Known sharp edge: with `block`, an agent that publishes to its own
full queue deadlocks itself — the same loop you would get with a synchronous
queue in any broker. Keep self-loops off `block` topics or size the queue.

## The broker-adapter seam

`Bus` is deliberately shaped like the subset of a real event broker that the
runtime needs: publish/subscribe over `/`-separated topic hierarchies with
single-level (`*`) and multi-level (`>`) wildcards — the Solace convention;
MQTT's `+`/`#` and AMQP topic-exchange `*`/`#` map onto the same matcher
semantics — plus a request/reply convention via a `replyTo` header, exactly
how reply-to destinations work on real brokers.

`InMemoryBus` is the reference implementation. A broker adapter implements
the same interface by mapping:

| Bus concept          | Solace                    | MQTT 5              | AMQP 0-9-1 (topic exchange) |
| -------------------- | ------------------------- | ------------------- | --------------------------- |
| topic level sep      | `/`                       | `/`                 | `.`                         |
| single-level wildcard| `*`                       | `+`                 | `*`                         |
| multi-level wildcard | `>`                       | `#`                 | `#`                         |
| request/reply        | reply-to destination      | response topic      | `reply_to` property         |
| envelope headers     | user properties           | user properties     | headers                     |

Everything above the bus — agents, mesh, patterns, tracing — never touches
broker specifics, so swapping the transport does not change agent code.

## Patterns as library code, not framework magic

- **Saga**: an ordered list of steps with optional compensations, compensated
  in reverse completion order on failure. Progress is published as
  `saga/<name>/<step>/{started,completed,compensated}` events so sagas appear
  in traces.
- **Scatter-gather**: fan a request to all subscribers of a topic, gather on
  a unique reply topic, resolve on quorum or timeout. The demo's fraud check
  fails closed (treats missing quorum as maximum risk).
- **Retry → DLQ**: exponential backoff, then republish to `dlq/<topic>` with
  `x-error` / `x-retries` / `x-failed-agent` headers — and the failure still
  counts against the agent. The DLQ never launders errors.
- **Human-in-the-loop gate**: first-class, not an afterthought. The
  `ApprovalGate` parks envelopes, announces `approvals/requested`, resumes on
  `approvals/<id>` decisions, and escalates on timeout. Because resume
  publishes a *child of the parked envelope*, the causal chain survives the
  human pause — the trace shows the park, the decision, and the resume as one
  tree. Autonomous flows that can stop and wait for a person are the
  difference between a demo and something an enterprise will deploy.

## The honest LLM seam

There are no API keys in this repo and nothing pretends otherwise.
`LLMProvider` is the seam where a hosted model plugs in; `MockLLM` is a
scripted, deterministic implementation so the runtime, patterns, and tests
all verify offline. An agent built on `LLMAgent` does not change when the
provider becomes real — that is the point of the seam.

## Limitations (deliberate)

- **In-process only.** No persistence, no delivery guarantees across
  restarts. The bus interface is the seam where a real broker adds those.
- **At-most-once delivery** within the process; retries are handler-level,
  not redelivery-level.
- **No topic-level authorization** or schema validation on publish; the
  AsyncAPI document is contract-as-documentation, not enforced at runtime.
- **`drain()` polls** rather than tracking a completion latch — simple and
  fine for tests/demos; a latch would replace it if it ever sat on a hot path.
- **HITL parked state is in-memory** — a restart loses parked envelopes. A
  real deployment would park into a durable store keyed by approval id.
