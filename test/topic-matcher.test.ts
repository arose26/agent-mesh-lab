import { describe, expect, it } from "vitest";
import { topicMatches } from "../src/bus.js";

describe("topicMatches", () => {
  const cases: Array<[pattern: string, topic: string, expected: boolean]> = [
    // exact
    ["orders/received", "orders/received", true],
    ["orders/received", "orders/shipped", false],
    ["orders", "orders/received", false],
    ["orders/received", "orders", false],
    // single-level wildcard *
    ["orders/*", "orders/received", true],
    ["orders/*", "orders", false],
    ["orders/*", "orders/us/received", false],
    ["*", "orders", true],
    ["*", "orders/received", false],
    ["*/received", "orders/received", true],
    ["orders/*/shipped", "orders/123/shipped", true],
    ["orders/*/shipped", "orders/123/45/shipped", false],
    // multi-level wildcard >
    ["orders/>", "orders/received", true],
    ["orders/>", "orders/us/received", true],
    ["orders/>", "orders", false], // > requires at least one more level
    [">", "orders", true],
    [">", "orders/us/received", true],
    ["orders/*/>", "orders/123/a/b", true],
    ["orders/*/>", "orders/123", false],
    // > not in final position is a literal level
    ["orders/>/shipped", "orders/123/shipped", false],
    ["orders/>/shipped", "orders/>/shipped", true],
  ];

  it.each(cases)("pattern %s vs topic %s -> %s", (pattern, topic, expected) => {
    expect(topicMatches(pattern, topic)).toBe(expected);
  });
});
