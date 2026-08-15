import { describe, expect, it } from "vitest";
import { LLMAgent, MockLLM, ToolAgent, agent } from "../src/agent.js";
import type { Envelope } from "../src/envelope.js";
import { Mesh } from "../src/mesh.js";

async function collect(mesh: Mesh, pattern: string): Promise<Envelope[]> {
  const out: Envelope[] = [];
  mesh.bus.subscribe(pattern, (env) => void out.push(env), { label: "collect" });
  return out;
}

describe("agents", () => {
  it("ToolAgent consumes its input topic and publishes the function result as a child envelope", async () => {
    const mesh = new Mesh();
    mesh.register(new ToolAgent<number, number>("Doubler", "in", "out", (n) => n * 2));
    const out = await collect(mesh, "out");
    mesh.start();
    const root = await mesh.publish("in", 21);
    await mesh.drain();
    expect(out.length).toBe(1);
    expect(out[0].payload).toBe(42);
    expect(out[0].correlationId).toBe(root.id); // causality preserved by ctx.publish
    await mesh.stop();
  });

  it("function agents receive ctx and can publish onward", async () => {
    const mesh = new Mesh();
    mesh.register(
      agent("Upper", ["words/in"], async (env, ctx) => {
        await ctx.publish("words/out", (env.payload as string).toUpperCase());
      }),
    );
    const out = await collect(mesh, "words/out");
    mesh.start();
    await mesh.publish("words/in", "hello");
    await mesh.drain();
    expect(out.map((e) => e.payload)).toEqual(["HELLO"]);
    await mesh.stop();
  });

  it("MockLLM returns the scripted response for a matching prompt, else the fallback", async () => {
    const llm = new MockLLM({ fraud: "score: high", refund: "approve refund" }, "unknown");
    expect(await llm.complete("please assess fraud risk")).toBe("score: high");
    expect(await llm.complete("customer wants a refund")).toBe("approve refund");
    expect(await llm.complete("unrelated")).toBe("unknown");
  });

  it("LLMAgent publishes the provider completion (deterministic offline seam)", async () => {
    const mesh = new Mesh();
    mesh.register(
      new LLMAgent("Summarizer", "docs/in", "docs/summary", new MockLLM({ laptop: "gadget order" }, "n/a")),
    );
    const out = await collect(mesh, "docs/summary");
    mesh.start();
    await mesh.publish("docs/in", { items: ["laptop"] });
    await mesh.drain();
    expect(out.length).toBe(1);
    expect(out[0].payload).toEqual({ completion: "gadget order" });
    await mesh.stop();
  });
});
