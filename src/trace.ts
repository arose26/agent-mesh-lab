export type TraceKind = "publish" | "deliver" | "ack" | "fail" | "drop";

export interface TraceEvent {
  seq: number;
  ts: number;
  kind: TraceKind;
  topic: string;
  envelopeId: string;
  correlationId: string;
  causationId?: string;
  /** Consumer label for deliver/ack/fail/drop events. */
  agent?: string;
  error?: string;
}

/**
 * Ring-buffer event trace. Every publish/deliver/ack/fail/drop on the bus is
 * recorded; `traceOf(correlationId)` slices out one causal chain and
 * `renderTree` reconstructs it as an ASCII causal tree.
 */
export class Tracer {
  private buf: TraceEvent[] = [];
  private seq = 0;

  constructor(private readonly capacity = 5000) {}

  record(ev: Omit<TraceEvent, "seq" | "ts">): void {
    this.buf.push({ seq: this.seq++, ts: Date.now(), ...ev });
    if (this.buf.length > this.capacity) this.buf.shift(); // ponytail: O(n) shift; index-based ring if capacity grows large
  }

  events(): readonly TraceEvent[] {
    return this.buf;
  }

  /** All recorded events belonging to one correlation chain, in order. */
  traceOf(correlationId: string): TraceEvent[] {
    return this.buf.filter((e) => e.correlationId === correlationId);
  }

  toJSONL(): string {
    return this.buf.map((e) => JSON.stringify(e)).join("\n");
  }

  /**
   * Render the causal tree of a correlation chain. Nodes are published
   * envelopes (linked child -> parent via causationId); each node is
   * annotated with the agents that consumed it and whether they acked or
   * failed.
   */
  renderTree(correlationId: string): string {
    const events = this.traceOf(correlationId);
    const published = events.filter((e) => e.kind === "publish");
    const ids = new Set(published.map((e) => e.envelopeId));

    const children = new Map<string, TraceEvent[]>();
    const roots: TraceEvent[] = [];
    for (const e of published) {
      if (e.causationId && ids.has(e.causationId)) {
        const list = children.get(e.causationId) ?? [];
        list.push(e);
        children.set(e.causationId, list);
      } else {
        roots.push(e);
      }
    }

    const consumers = new Map<string, string[]>();
    for (const e of events) {
      if (e.kind !== "ack" && e.kind !== "fail") continue;
      const list = consumers.get(e.envelopeId) ?? [];
      list.push(e.kind === "ack" ? `${e.agent} ok` : `${e.agent} FAILED: ${e.error}`);
      consumers.set(e.envelopeId, list);
    }

    const label = (e: TraceEvent): string => {
      const who = consumers.get(e.envelopeId);
      return `${e.topic} #${e.envelopeId.slice(0, 8)}${who ? `  -> [${who.join(", ")}]` : ""}`;
    };

    const lines: string[] = [];
    const walk = (e: TraceEvent, prefix: string): void => {
      const kids = children.get(e.envelopeId) ?? [];
      kids.forEach((kid, i) => {
        const last = i === kids.length - 1;
        lines.push(prefix + (last ? "`- " : "|- ") + label(kid));
        walk(kid, prefix + (last ? "   " : "|  "));
      });
    };

    for (const root of roots) {
      lines.push(label(root));
      walk(root, "");
    }
    return lines.join("\n");
  }
}
