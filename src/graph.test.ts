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
  planNext,
  resolveInputs,
  resolveValue,
  startState,
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
    { id: "check", type: "if", data: { join: "and", conditions: [{ left: "{{ trigger.vip }}", op: "equals", right: true }] } },
    action("a", { text: { mode: "expr", value: "{{ trigger.name }}" } }),
    action("b"),
    { id: "join", type: "merge", data: { mode: "all" } },
    action("done", { first: { mode: "expr", value: "{{ nodes.join.output.a.echo | default(\"none\") }}" } }),
  ],
  edges: [edge("t", "check"), edge("check", "a", "true"), edge("check", "b", "false"), edge("a", "join"), edge("b", "join"), edge("join", "done")],
});

const scope: Scope = {
  trigger: { name: "An", vip: true, tags: ["x", "y"], n: 5 },
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

test("validateGraph checks expressions: syntax, upstream only, no vars or secrets yet", () => {
  const withInput = (value: string) => ({
    nodes: [trigger(), action("a", { x: { mode: "expr", value } }), action("b")],
    edges: [edge("t", "a"), edge("a", "b")],
  });
  assert.deepEqual(validateGraph(withInput("{{ trigger.x }}")).issues, []);
  assert.match(validateGraph(withInput("{{ nodes.b.output.y }}")).issues[0].message, /does not come before/);
  assert.match(validateGraph(withInput("{{ nodes.ghost.output.y }}")).issues[0].message, /unknown node "ghost"/);
  assert.match(validateGraph(withInput("{{ secrets.KEY }}")).issues[0].message, /not available yet/);
  assert.match(validateGraph(withInput("{{ nodes.b.y }}")).issues[0].message, /nodes.<id>.output/);
  assert.match(validateGraph(withInput("{{ foo.bar }}")).issues[0].message, /Unknown source/);
});

test("validateGraph: schema errors and the unsupported merge mode", () => {
  assert.equal(validateGraph({ nodes: [] }).graph, null);
  assert.equal(validateGraph({ nodes: [{ id: "bad id", type: "trigger", data: {} }] }).graph, null);
  const first = { nodes: [trigger(), action("a"), action("b"), { id: "m", type: "merge", data: { mode: "first" } }], edges: [edge("t", "a"), edge("t", "b"), edge("a", "m"), edge("b", "m")] };
  assert.match(validateGraph(first).issues[0].message, /not supported yet/);
});

test("expressions: paths, arrays, length, default, whole vs part", () => {
  assert.equal(resolveValue("{{ trigger.name }}", scope), "An");
  assert.equal(resolveValue("{{ nodes.a.output.items[1].price }}", scope), 20);
  assert.equal(resolveValue("{{ nodes.a.output.items.length }}", scope), 2);
  assert.deepEqual(resolveValue("{{ trigger.tags }}", scope), ["x", "y"]);
  assert.equal(resolveValue("{{ run.no }}", scope), 7);
  assert.equal(resolveValue("{{ trigger.nope | default(\"n/a\") }}", scope), "n/a");
  assert.equal(resolveValue("{{ trigger.nope | default(3) }}", scope), 3);
  assert.equal(resolveValue("Hi {{ trigger.name }}, #{{ run.no }}", scope), "Hi An, #7");
  assert.deepEqual(resolveValue({ a: ["{{ trigger.n }}"], b: 1 }, scope), { a: [5], b: 1 });
});

test("expressions: a missing value is an error in inputs and empty in conditions", () => {
  assert.throws(() => resolveValue("{{ trigger.nope }}", scope), ExpressionError);
  assert.throws(() => resolveValue("{{ nodes.zzz.output.x }}", scope), ExpressionError);
  assert.throws(() => resolveValue("{{ secrets.K }}", scope), /not available yet/);
  assert.equal(resolveValue("{{ trigger.nope }}", scope, false), undefined);
  assert.throws(() => resolveInputs({ x: { mode: "expr", value: "{{ trigger.nope }}" } }, scope), /Input "x"/);
  assert.deepEqual(resolveInputs({ x: { mode: "fixed", value: "{{ not an expression }}" } }, scope), { x: "{{ not an expression }}" });
  assert.deepEqual(collectExpressions({ a: "{{ trigger.a }} {{ run.id }}", b: [1, "{{ nodes.x.output.y }}"] }), [" trigger.a ", " run.id ", " nodes.x.output.y "]);
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

test("evaluateIf: and / or, and a missing value counts as empty", () => {
  const node = (join: "and" | "or") => ({
    id: "i", type: "if" as const,
    data: { join, conditions: [{ left: "{{ trigger.n }}", op: "gt" as const, right: 3 }, { left: "{{ trigger.name }}", op: "equals" as const, right: "Bo" }] },
  });
  assert.equal(evaluateIf(node("and"), scope), "false");
  assert.equal(evaluateIf(node("or"), scope), "true");
  const empty = { id: "i", type: "if" as const, data: { join: "and" as const, conditions: [{ left: "{{ trigger.nope }}", op: "is_empty" as const }] } };
  assert.equal(evaluateIf(empty, scope), "true");
});

test("evaluateSwitch: first match, default, or nothing", () => {
  const node = (fallback: boolean) => ({
    id: "s", type: "switch" as const,
    data: { value: "{{ trigger.n }}", cases: [{ id: "hot", op: "gte" as const, right: 80 }, { id: "warm", op: "gte" as const, right: 5 }, { id: "also", op: "gte" as const, right: 1 }], fallback },
  });
  assert.equal(evaluateSwitch(node(true), scope), "warm");
  assert.equal(evaluateSwitch(node(true), { ...scope, trigger: { n: 0 } }), "default");
  assert.equal(evaluateSwitch(node(false), { ...scope, trigger: { n: 0 } }), null);
});

/** Runs a graph with a fake executor: every action succeeds unless listed in `fail`. Returns statuses in order. */
function simulate(raw: unknown, trig: unknown, options: { fail?: string[]; firedTriggerId?: string | null } = {}) {
  const { graph, issues } = validateGraph(raw);
  assert.deepEqual(issues, []);
  const g = graph as Graph;
  const state = startState(g, options.firedTriggerId ?? null);
  const outputs: Record<string, unknown> = {};
  const ran: string[] = [];
  const waves: string[][] = [];
  const scopeNow = (): Scope => ({ trigger: trig, run: { id: "r", no: 1, env: "production" }, nodes: Object.fromEntries(Object.entries(outputs).map(([k, v]) => [k, { output: v }])) });
  for (let guard = 0; guard < 50; guard++) {
    const { ready } = planNext(g, state);
    if (ready.length === 0) break;
    waves.push([...ready].sort());
    for (const id of ready) {
      const node = g.nodes.find((n) => n.id === id)!;
      let status: NodeStatus = "succeeded";
      if (node.type === "if") state.taken.set(id, evaluateIf(node, scopeNow()));
      else if (node.type === "switch") state.taken.set(id, evaluateSwitch(node, scopeNow()));
      else if (node.type === "merge") outputs[id] = mergeOutput(id, g, state, outputs);
      else if (node.type === "action") {
        if (options.fail?.includes(id)) status = "failed";
        else outputs[id] = { echo: resolveInputs(node.data.inputs, scopeNow()) };
      }
      state.status.set(id, status);
      ran.push(id);
    }
    if ([...state.status.values()].includes("failed")) break;
  }
  return { state, outputs, ran, waves, status: Object.fromEntries(state.status) };
}

test("run: the true branch runs, the false branch is skipped, merge still runs", () => {
  const r = simulate(branching(), { name: "An", vip: true });
  assert.deepEqual(r.status, { t: "succeeded", check: "succeeded", a: "succeeded", b: "skipped", join: "succeeded", done: "succeeded" });
  assert.deepEqual(r.outputs.join, { a: { echo: { text: "An" } } });
  assert.deepEqual((r.outputs.done as { echo: unknown }).echo, { first: { text: "An" } });
});

test("run: the false branch runs when the condition fails", () => {
  const r = simulate(branching(), { name: "An", vip: false });
  assert.equal(r.status.a, "skipped");
  assert.equal(r.status.b, "succeeded");
  assert.deepEqual(r.outputs.join, { b: { echo: {} } });
});

test("run: parallel branches are one wave and merge waits for both", () => {
  const g = {
    nodes: [trigger(), action("a"), action("b"), action("c"), { id: "m", type: "merge", data: { mode: "all" } }, action("z")],
    edges: [edge("t", "a"), edge("a", "b"), edge("a", "c"), edge("b", "m"), edge("c", "m"), edge("m", "z")],
  };
  const r = simulate(g, {});
  assert.deepEqual(r.waves, [["a"], ["b", "c"], ["m"], ["z"]]);
  assert.deepEqual(Object.keys(r.outputs.m as object).sort(), ["b", "c"]);
});

test("run: switch takes the matching case; nodes after other cases are skipped", () => {
  const g = {
    nodes: [
      trigger(),
      { id: "tier", type: "switch", data: { value: "{{ trigger.score }}", cases: [{ id: "hot", op: "gte", right: 80 }, { id: "warm", op: "gte", right: 40 }], fallback: true } },
      action("hotNode"), action("warmNode"), action("coldNode"),
    ],
    edges: [edge("t", "tier"), edge("tier", "hotNode", "hot"), edge("tier", "warmNode", "warm"), edge("tier", "coldNode", "default")],
  };
  assert.deepEqual(simulate(g, { score: 50 }).status, { t: "succeeded", tier: "succeeded", hotNode: "skipped", warmNode: "succeeded", coldNode: "skipped" });
  assert.equal(simulate(g, { score: 5 }).status.coldNode, "succeeded");
});

test("run: a switch with no match and no default skips everything below it", () => {
  const g = {
    nodes: [trigger(), { id: "s", type: "switch", data: { value: "{{ trigger.x }}", cases: [{ id: "one", op: "equals", right: 1 }], fallback: false } }, action("a"), action("after")],
    edges: [edge("t", "s"), edge("s", "a", "one"), edge("a", "after")],
  };
  const r = simulate(g, { x: 2 });
  assert.deepEqual(r.status, { t: "succeeded", s: "succeeded", a: "skipped", after: "skipped" });
});

test("run: a failed node stops scheduling, and a trigger that did not fire is skipped", () => {
  const r = simulate(branching(), { name: "An", vip: true }, { fail: ["a"] });
  assert.equal(r.status.a, "failed");
  assert.equal(r.status.done, undefined);

  const g = {
    nodes: [{ id: "t1", type: "trigger", data: { triggerId: "11111111-1111-4111-8111-111111111111" } }, { id: "t2", type: "trigger", data: { triggerId: "22222222-2222-4222-8222-222222222222" } }, action("a"), action("b")],
    edges: [edge("t1", "a"), edge("t2", "b")],
  };
  assert.deepEqual(simulate(g, {}, { firedTriggerId: "22222222-2222-4222-8222-222222222222" }).status, { t1: "skipped", t2: "succeeded", a: "skipped", b: "succeeded" });
  assert.equal(simulate(g, {}).status.a, "succeeded");
});

test("planNext does nothing for a node that already ended", () => {
  const { graph } = validateGraph({ nodes: [trigger(), action("a")], edges: [edge("t", "a")] });
  const state = startState(graph as Graph, null);
  assert.deepEqual(planNext(graph as Graph, state).ready, ["a"]);
  state.status.set("a", "succeeded");
  assert.deepEqual(planNext(graph as Graph, state).ready, []);
  assert.equal(emptyState().status.size, 0);
});
