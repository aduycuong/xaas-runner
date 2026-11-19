// Shared with xaas-workflows/lib/workflows/graph.test.ts: keep both copies identical.

import assert from "node:assert/strict";
import { test } from "node:test";
import {
  collectExpressions,
  compare,
  emptyState,
  evaluateIf,
  evaluateSwitch,
  ExpressionError,
  mergeOutput,
  parseExpression,
  planNext,
  resolveInputs,
  resolveValue,
  startState,
  templateParts,
  validateGraph,
  type Graph,
  type NodeStatus,
  type Scope,
} from "./graph";

const action = (id: string, inputs: Record<string, { mode: "fixed" | "expr"; value: unknown }> = {}) => ({
  id,
  type: "action" as const,
  position: { x: 0, y: 0 },
  data: { nodeKey: "core.echo", version: "1.0.0", inputs },
});
const trigger = (id = "t") => ({ id, type: "trigger" as const, data: {} });
const edge = (source: string, target: string, sourceHandle = "out") => ({ source, sourceHandle, target, targetHandle: "in" });

/** trigger → check(if) → true: a, false: b → join(merge) → done */
const branching = (): unknown => ({
  nodes: [
    trigger(),
    { id: "check", type: "if", data: { join: "and", conditions: [{ left: "{{ input.vip }}", op: "equals", right: true }] } },
    action("a", { text: { mode: "expr", value: "{{ input.name }}" } }),
    action("b"),
    { id: "join", type: "merge", data: { mode: "all" } },
    action("done", { first: { mode: "expr", value: "{{ $exists(nodes.join.output.a.echo) ? nodes.join.output.a.echo : \"none\" }}" } }),
  ],
  edges: [edge("t", "check"), edge("check", "a", "true"), edge("check", "b", "false"), edge("a", "join"), edge("b", "join"), edge("join", "done")],
});

const scope: Scope = {
  input: { name: "An", vip: true, tags: ["x", "y"], n: 5 },
  trigger: { body: { who: "hook" }, headers: { "x-event": "push" }, query: {} },
  run: { id: "run_1", no: 7, env: "production" },
  nodes: { a: { output: { items: [{ price: 10 }, { price: 20 }], total: 2 } } },
};

test("validateGraph accepts a good graph and ignores positions", () => {
  const { graph, issues } = validateGraph(branching());
  assert.deepEqual(issues, []);
  assert.equal(graph?.nodes.length, 6);
});

test("validateGraph reports every kind of mistake", () => {
  const messages = (g: unknown) => validateGraph(g).issues.map((i) => i.message);
  const g = branching() as { nodes: unknown[]; edges: unknown[] };

  assert.match(messages({ nodes: [action("a")], edges: [] }).join("|"), /needs a trigger/);
  assert.match(messages({ ...g, nodes: [...g.nodes, action("a")] }).join("|"), /Duplicate node id "a"/);
  assert.match(messages({ ...g, edges: [...g.edges, edge("a", "ghost")] }).join("|"), /unknown node "ghost"/);
  assert.match(messages({ ...g, edges: [...g.edges, edge("check", "done", "maybe")] }).join("|"), /no output "maybe"/);
  assert.match(messages({ ...g, edges: [...g.edges, edge("a", "t")] }).join("|"), /trigger cannot have incoming/);
  assert.match(messages({ ...g, nodes: [...g.nodes, action("lonely")] }).join("|"), /cannot be reached from a trigger/);
  assert.match(messages({ ...g, edges: [...g.edges, edge("done", "a")] }).join("|"), /loop/);
  assert.match(messages({ ...g, edges: g.edges.filter((e) => (e as { source: string }).source !== "b") }).join("|"), /Merge "join" needs at least two/);
});

test("validateGraph checks expressions: syntax, upstream only, no vars or secrets", () => {
  const withInput = (value: string) => ({
    nodes: [trigger(), action("a", { x: { mode: "expr", value } }), action("b")],
    edges: [edge("t", "a"), edge("a", "b")],
  });
  assert.deepEqual(validateGraph(withInput("{{ input.x }}")).issues, []);
  assert.match(validateGraph(withInput("{{ nodes.b.output.y }}")).issues[0].message, /does not come before/);
  assert.match(validateGraph(withInput("{{ nodes.ghost.output.y }}")).issues[0].message, /unknown node "ghost"/);
  assert.match(validateGraph(withInput("{{ secrets.KEY }}")).issues[0].message, /is not an expression/);
  assert.match(validateGraph(withInput("{{ nodes.b.y }}")).issues[0].message, /nodes.<id>.output/);
  assert.match(validateGraph(withInput("{{ foo.bar }}")).issues[0].message, /Unknown source/);
});

test("validateGraph: schema errors and the unsupported merge mode", () => {
  assert.equal(validateGraph({ nodes: [] }).graph, null);
  assert.equal(validateGraph({ nodes: [{ id: "bad id", type: "trigger", data: {} }] }).graph, null);
  const first = { nodes: [trigger(), action("a"), action("b"), { id: "m", type: "merge", data: { mode: "first" } }], edges: [edge("t", "a"), edge("t", "b"), edge("a", "m"), edge("b", "m")] };
  assert.match(validateGraph(first).issues[0].message, /not supported yet/);
});

test("validateGraph: a disabled node cannot be published", () => {
  const node = { ...action("a"), data: { ...action("a").data, disabled: true } };
  assert.match(validateGraph({ nodes: [trigger(), node], edges: [edge("t", "a")] }).issues[0].message, /is disabled/);
});

test("expressions: paths, arrays, whole vs part", async () => {
  assert.equal(await resolveValue("{{ input.name }}", scope), "An");
  assert.equal(await resolveValue("{{ trigger.body.who }}", scope), "hook");
  assert.equal(await resolveValue("{{ trigger.headers.`x-event` }}", scope), "push");
  assert.equal(await resolveValue("{{ trigger.name }}", scope, false), undefined); // the raw call is not the input
  assert.equal(await resolveValue("{{ nodes.a.output.items[1].price }}", scope), 20);
  assert.deepEqual(await resolveValue("{{ input.tags }}", scope), ["x", "y"]);
  assert.equal(await resolveValue("{{ run.no }}", scope), 7);
  assert.equal(await resolveValue("Hi {{ input.name }}, #{{ run.no }}", scope), "Hi An, #7");
  assert.deepEqual(await resolveValue({ a: ["{{ input.n }}"], b: 1 }, scope), { a: [5], b: 1 });
});

test("expressions are JSONata: concatenation, conditions, functions, constructors", async () => {
  assert.equal(await resolveValue('{{ "Hi " & input.name & (input.n > 3 ? "!" : ".") }}', scope), "Hi An!");
  assert.equal(await resolveValue("{{ $count(nodes.a.output.items) }}", scope), 2);
  assert.equal(await resolveValue("{{ $sum(nodes.a.output.items.price) }}", scope), 30);
  assert.equal(await resolveValue('{{ $number("42") + 1 }}', scope), 43);
  assert.equal(await resolveValue('{{ $exists(input.nope) ? input.nope : "n/a" }}', scope), "n/a");
  assert.deepEqual(await resolveValue('{{ {"who": input.name, "tags": $join(input.tags, ",")} }}', scope), { who: "An", tags: "x,y" });
  assert.deepEqual(await resolveValue("{{ nodes.a.output.items[price > 15].price }}", scope), 20);
  assert.equal(await resolveValue('Total: {{ $string($sum(nodes.a.output.items.price)) & " VND" }}', scope), "Total: 30 VND");
});

test("expressions: syntax and evaluation errors, and what an expression reads", async () => {
  assert.throws(() => parseExpression("input.name &"), /cannot be read/);
  assert.throws(() => parseExpression('input.nope | default("n/a")'), /cannot be read/);
  assert.throws(() => parseExpression("trigger.headers.x-event"), /Unknown source "event"/);
  await assert.rejects(resolveValue('{{ $number("abc") }}', scope), /failed/);
  await assert.rejects(resolveValue('{{ ($f := function($x) { $f($x) }; $f(1)) }}', scope), /steps|deeper|failed/);
  assert.deepEqual(parseExpression('$join(nodes.a.output.items[price > 1].name, ", ") & input.who'), [
    { root: "nodes", segments: ["a", "output", "items"] },
    { root: "input", segments: ["who"] },
  ]);
  assert.deepEqual(parseExpression("nodes.`my-node`.output.items[0].id"), [{ root: "nodes", segments: ["my-node", "output", "items", 0, "id"] }]);
  assert.deepEqual(templateParts('a {{ {"x": {"y": 1}} }} b {{ "}}" }}'), [
    { kind: "text", text: "a " },
    { kind: "expr", source: ' {"x": {"y": 1}} ' },
    { kind: "text", text: " b " },
    { kind: "expr", source: ' "}}" ' },
  ]);
});

test("expressions: a missing value is an error in inputs and empty in conditions", async () => {
  await assert.rejects(resolveValue("{{ input.nope }}", scope), ExpressionError);
  await assert.rejects(resolveValue("{{ nodes.zzz.output.x }}", scope), ExpressionError);
  await assert.rejects(resolveValue("{{ secrets.K }}", scope), /is not an expression/);
  assert.equal(await resolveValue("{{ input.nope }}", scope, false), undefined);
  await assert.rejects(resolveInputs({ x: { mode: "expr", value: "{{ input.nope }}" } }, scope), /Input "x"/);
  assert.deepEqual(await resolveInputs({ x: { mode: "fixed", value: "{{ not an expression }}" } }, scope), { x: "{{ not an expression }}" });
  assert.deepEqual(collectExpressions({ a: "{{ input.a }} {{ run.id }}", b: [1, "{{ nodes.x.output.y }}"] }), [" input.a ", " run.id ", " nodes.x.output.y "]);
});

test("compare: every operator", () => {
  assert.equal(compare("equals", { a: 1 }, { a: 1 }), true);
  assert.equal(compare("not_equals", 1, 2), true);
  assert.equal(compare("gt", 5, 3), true);
  assert.equal(compare("gte", 3, 3), true);
  assert.equal(compare("lt", "a", "b"), true);
  assert.equal(compare("lte", 4, 3), false);
  assert.equal(compare("gt", "5", 3), false);
  assert.equal(compare("contains", "hello", "ell"), true);
  assert.equal(compare("contains", ["a", { k: 1 }], { k: 1 }), true);
  assert.equal(compare("starts_with", "mailinator.com", "mail"), true);
  assert.equal(compare("ends_with", "a@mailinator.com", "@mailinator.com"), true);
  assert.equal(compare("is_empty", undefined, undefined), true);
  assert.equal(compare("is_empty", [], undefined), true);
  assert.equal(compare("is_not_empty", "x", undefined), true);
  assert.equal(compare("equals", undefined, 1), false);
});

test("evaluateIf: and / or, and a missing value counts as empty", async () => {
  const node = (join: "and" | "or") => ({
    id: "i", type: "if" as const,
    data: { join, conditions: [{ left: "{{ input.n }}", op: "gt" as const, right: 3 }, { left: "{{ input.name }}", op: "equals" as const, right: "Bo" }] },
  });
  assert.equal(await evaluateIf(node("and"), scope), "false");
  assert.equal(await evaluateIf(node("or"), scope), "true");
  const empty = { id: "i", type: "if" as const, data: { join: "and" as const, conditions: [{ left: "{{ input.nope }}", op: "is_empty" as const }] } };
  assert.equal(await evaluateIf(empty, scope), "true");
});

test("evaluateSwitch: first match, default, or nothing", async () => {
  const node = (fallback: boolean) => ({
    id: "s", type: "switch" as const,
    data: { value: "{{ input.n }}", cases: [{ id: "hot", op: "gte" as const, right: 80 }, { id: "warm", op: "gte" as const, right: 5 }, { id: "also", op: "gte" as const, right: 1 }], fallback },
  });
  assert.equal(await evaluateSwitch(node(true), scope), "warm");
  assert.equal(await evaluateSwitch(node(true), { ...scope, input: { n: 0 } }), "default");
  assert.equal(await evaluateSwitch(node(false), { ...scope, input: { n: 0 } }), null);
});

/** Runs a graph with a fake executor: every action succeeds unless listed in `fail`. Returns statuses in order. */
async function simulate(raw: unknown, trig: unknown, options: { fail?: string[] } = {}) {
  const { graph, issues } = validateGraph(raw);
  assert.deepEqual(issues, []);
  const g = graph as Graph;
  const state = startState(g);
  const outputs: Record<string, unknown> = {};
  const ran: string[] = [];
  const waves: string[][] = [];
  const scopeNow = (): Scope => ({ input: trig, trigger: {}, run: { id: "r", no: 1, env: "production" }, nodes: Object.fromEntries(Object.entries(outputs).map(([k, v]) => [k, { output: v }])) });
  for (let guard = 0; guard < 50; guard++) {
    const { ready } = planNext(g, state);
    if (ready.length === 0) break;
    waves.push([...ready].sort());
    for (const id of ready) {
      const node = g.nodes.find((n) => n.id === id)!;
      let status: NodeStatus = "succeeded";
      if (node.type === "if") state.taken.set(id, await evaluateIf(node, scopeNow()));
      else if (node.type === "switch") state.taken.set(id, await evaluateSwitch(node, scopeNow()));
      else if (node.type === "merge") outputs[id] = mergeOutput(id, g, state, outputs);
      else if (node.type === "action") {
        if (options.fail?.includes(id)) status = "failed";
        else outputs[id] = { echo: await resolveInputs(node.data.inputs, scopeNow()) };
      }
      state.status.set(id, status);
      ran.push(id);
    }
    if ([...state.status.values()].includes("failed")) break;
  }
  return { state, outputs, ran, waves, status: Object.fromEntries(state.status) };
}

test("run: the true branch runs, the false branch is skipped, merge still runs", async () => {
  const r = await simulate(branching(), { name: "An", vip: true });
  assert.deepEqual(r.status, { t: "succeeded", check: "succeeded", a: "succeeded", b: "skipped", join: "succeeded", done: "succeeded" });
  assert.deepEqual(r.outputs.join, { a: { echo: { text: "An" } } });
  assert.deepEqual((r.outputs.done as { echo: unknown }).echo, { first: { text: "An" } });
});

test("run: the false branch runs when the condition fails", async () => {
  const r = await simulate(branching(), { name: "An", vip: false });
  assert.equal(r.status.a, "skipped");
  assert.equal(r.status.b, "succeeded");
  assert.deepEqual(r.outputs.join, { b: { echo: {} } });
});

test("run: parallel branches are one wave and merge waits for both", async () => {
  const g = {
    nodes: [trigger(), action("a"), action("b"), action("c"), { id: "m", type: "merge", data: { mode: "all" } }, action("z")],
    edges: [edge("t", "a"), edge("a", "b"), edge("a", "c"), edge("b", "m"), edge("c", "m"), edge("m", "z")],
  };
  const r = await simulate(g, {});
  assert.deepEqual(r.waves, [["a"], ["b", "c"], ["m"], ["z"]]);
  assert.deepEqual(Object.keys(r.outputs.m as object).sort(), ["b", "c"]);
});

test("run: switch takes the matching case; nodes after other cases are skipped", async () => {
  const g = {
    nodes: [
      trigger(),
      { id: "tier", type: "switch", data: { value: "{{ input.score }}", cases: [{ id: "hot", op: "gte", right: 80 }, { id: "warm", op: "gte", right: 40 }], fallback: true } },
      action("hotNode"), action("warmNode"), action("coldNode"),
    ],
    edges: [edge("t", "tier"), edge("tier", "hotNode", "hot"), edge("tier", "warmNode", "warm"), edge("tier", "coldNode", "default")],
  };
  assert.deepEqual((await simulate(g, { score: 50 })).status, { t: "succeeded", tier: "succeeded", hotNode: "skipped", warmNode: "succeeded", coldNode: "skipped" });
  assert.equal((await simulate(g, { score: 5 })).status.coldNode, "succeeded");
});

test("run: a switch with no match and no default skips everything below it", async () => {
  const g = {
    nodes: [trigger(), { id: "s", type: "switch", data: { value: "{{ input.x }}", cases: [{ id: "one", op: "equals", right: 1 }], fallback: false } }, action("a"), action("after")],
    edges: [edge("t", "s"), edge("s", "a", "one"), edge("a", "after")],
  };
  const r = await simulate(g, { x: 2 });
  assert.deepEqual(r.status, { t: "succeeded", s: "succeeded", a: "skipped", after: "skipped" });
});

test("run: a failed node stops scheduling; every trigger node starts the run", async () => {
  const r = await simulate(branching(), { name: "An", vip: true }, { fail: ["a"] });
  assert.equal(r.status.a, "failed");
  assert.equal(r.status.done, undefined);

  const g = {
    // An older graph with two trigger nodes (one still carrying the dropped `triggerId`): both start.
    nodes: [{ id: "t1", type: "trigger", data: { triggerId: "11111111-1111-4111-8111-111111111111" } }, { id: "t2", type: "trigger", data: {} }, action("a"), action("b")],
    edges: [edge("t1", "a"), edge("t2", "b")],
  };
  assert.deepEqual((await simulate(g, {})).status, { t1: "succeeded", t2: "succeeded", a: "succeeded", b: "succeeded" });
});

test("planNext does nothing for a node that already ended", () => {
  const { graph } = validateGraph({ nodes: [trigger(), action("a")], edges: [edge("t", "a")] });
  const state = startState(graph as Graph);
  assert.deepEqual(planNext(graph as Graph, state).ready, ["a"]);
  state.status.set("a", "succeeded");
  assert.deepEqual(planNext(graph as Graph, state).ready, []);
  assert.equal(emptyState().status.size, 0);
});
