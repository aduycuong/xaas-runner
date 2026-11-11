import assert from "node:assert/strict";
import { test } from "node:test";
import {
  bindingName,
  definitionSchema,
  evaluateCondition,
  judgeNodeOutcome,
  resolveTemplate,
  retryDelay,
  TemplateError,
  walkIfNodes,
} from "./logic";

const scope = {
  input: { region: "vn", urls: ["a", "b"], limit: 5, tag: "spam" },
  context: { n1: { prices: [{ price: 249000 }], ok: false } },
};

test("template: a string that is one template keeps the type of its value", () => {
  assert.equal(resolveTemplate("{{ input.limit }}", scope), 5);
  assert.deepEqual(resolveTemplate("{{input.urls}}", scope), ["a", "b"]);
  assert.equal(resolveTemplate("{{ context.n1.ok }}", scope), false);
});

test("template: inside a longer string values become text", () => {
  assert.equal(resolveTemplate("{{ input.urls.length }} urls in {{ input.region }}", scope), "2 urls in vn");
});

test("template: walks objects and arrays, including array indexes", () => {
  assert.deepEqual(resolveTemplate({ a: ["{{ context.n1.prices.0.price }}", 1], b: "plain" }, scope), {
    a: [249000, 1],
    b: "plain",
  });
});

test("template: a path with no value is an error", () => {
  assert.throws(() => resolveTemplate("{{ input.missing }}", scope), TemplateError);
  assert.throws(() => resolveTemplate("{{ context.n2.x }}", scope), TemplateError);
});

test("template: only input and context can be read, never inherited properties", () => {
  assert.throws(() => resolveTemplate("{{ secrets.TOKEN }}", scope), TemplateError);
  assert.throws(() => resolveTemplate("{{ input.constructor }}", scope), TemplateError);
  assert.throws(() => resolveTemplate("{{ input.__proto__ }}", scope), TemplateError);
});

test("condition: eq, ne and ordering", () => {
  assert.equal(evaluateCondition({ left: "{{ input.tag }}", op: "eq", right: "spam" }, scope), true);
  assert.equal(evaluateCondition({ left: "{{ input.urls }}", op: "eq", right: ["a", "b"] }, scope), true);
  assert.equal(evaluateCondition({ left: "{{ input.limit }}", op: "ne", right: 5 }, scope), false);
  assert.equal(evaluateCondition({ left: "{{ input.limit }}", op: "gt", right: 3 }, scope), true);
  assert.equal(evaluateCondition({ left: "{{ input.limit }}", op: "lte", right: 5 }, scope), true);
  assert.equal(evaluateCondition({ left: "{{ input.limit }}", op: "gt", right: "3" }, scope), false);
});

test("condition: a missing path is falsy for truthy/falsy, an error for other operators", () => {
  assert.equal(evaluateCondition({ left: "{{ input.missing }}", op: "truthy" }, scope), false);
  assert.equal(evaluateCondition({ left: "{{ input.missing }}", op: "falsy" }, scope), true);
  assert.throws(() => evaluateCondition({ left: "{{ input.missing }}", op: "eq", right: 1 }, scope));
});

test("node outcome: 2xx needs an output object", () => {
  assert.deepEqual(judgeNodeOutcome({ kind: "response", status: 200, body: { output: { a: 1 } } }), {
    ok: true,
    output: { a: 1 },
    httpStatus: 200,
  });
  for (const body of [null, {}, { output: "text" }, { output: [1] }]) {
    const verdict = judgeNodeOutcome({ kind: "response", status: 200, body });
    assert.equal(!verdict.ok && !verdict.retryable && verdict.error.code, "INVALID_OUTPUT");
  }
});

test("node outcome: 5xx, 429, timeout and network are retried; other 4xx and a missing node are not", () => {
  const retryable = (outcome: Parameters<typeof judgeNodeOutcome>[0]) => {
    const verdict = judgeNodeOutcome(outcome);
    return !verdict.ok && verdict.retryable;
  };
  assert.equal(retryable({ kind: "response", status: 503, body: null }), true);
  assert.equal(retryable({ kind: "response", status: 429, body: null }), true);
  assert.equal(retryable({ kind: "response", status: 400, body: null }), false);
  assert.equal(retryable({ kind: "timeout" }), true);
  assert.equal(retryable({ kind: "network", message: "reset" }), true);
  assert.equal(retryable({ kind: "unavailable", message: "no binding" }), false);
});

test("node outcome: an error body can ask for a retry and names the error", () => {
  const body = { error: { code: "BUSY", message: "Try later.", retryable: true } };
  assert.deepEqual(judgeNodeOutcome({ kind: "response", status: 409, body }), {
    ok: false,
    retryable: true,
    httpStatus: 409,
    error: { code: "BUSY", message: "Try later." },
  });
});

test("definition: accepts worker and flow.if nodes, rejects bad node ids", () => {
  const nodeVersionId = "018f3b1e-7c2a-7000-8000-000000000001";
  const parsed = definitionSchema.parse({
    entry: "first",
    nodes: [
      { id: "first", type: "worker", nodeVersionId, next: "check" },
      { id: "check", type: "flow.if", branches: [{ when: { left: 1, op: "truthy" }, next: null }], defaultNext: null },
    ],
  });
  assert.deepEqual(parsed.nodes[0].type === "worker" && parsed.nodes[0].input, {});
  const bad = { entry: "a.b", nodes: [{ id: "a.b", type: "worker", nodeVersionId, next: null }] };
  assert.equal(definitionSchema.safeParse(bad).success, false);
});

test("binding name and retry delay", () => {
  assert.equal(bindingName("node-core-echo-v1-0-0"), "NODE_CORE_ECHO_V1_0_0");
  assert.equal(retryDelay("none", 1000), "1000");
  assert.equal(retryDelay("linear", 500), "500 * (1 + retried)");
  assert.equal(retryDelay("exponential", 2000), "2000 * pow(2, retried)");
});

test("walkIfNodes: decides a chain of ifs and stops at the first worker", () => {
  const ifNode = (id: string, left: unknown, yes: string | null, no: string | null) => ({
    id,
    type: "flow.if",
    branches: [{ when: { left, op: "truthy" }, next: yes }],
    defaultNext: no,
  });
  const nodes = new Map<string, { id: string; type: string }>(
    [ifNode("a", "{{ input.limit }}", "b", null), ifNode("b", "", "w", "z"), { id: "w", type: "worker" }].map((n) => [n.id, n]),
  );
  const walk = walkIfNodes(nodes, "a", scope);
  assert.deepEqual(walk.steps, [{ id: "a", next: "b" }, { id: "b", next: "z" }]);
  assert.equal(walk.next, "z");
  assert.equal(walkIfNodes(nodes, "w", scope).next, "w");
  assert.equal(walkIfNodes(nodes, null, scope).next, null);
});

test("walkIfNodes: a loop of ifs fails and an undecidable if reports its node", () => {
  const loop = new Map<string, { id: string; type: string }>([
    ["a", { id: "a", type: "flow.if", branches: [{ when: { left: 1, op: "truthy" }, next: "a" }], defaultNext: null } as never],
  ]);
  assert.equal(walkIfNodes(loop, "a", scope).failure?.error.code, "INVALID_GRAPH");
  const bad = new Map<string, { id: string; type: string }>([
    ["a", { id: "a", type: "flow.if", branches: [{ when: { left: "{{ input.nope }}", op: "eq", right: 1 }, next: null }], defaultNext: null } as never],
  ]);
  assert.equal(walkIfNodes(bad, "a", scope).failure?.error.code, "TEMPLATE_ERROR");
});
