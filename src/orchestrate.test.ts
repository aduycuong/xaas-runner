import assert from "node:assert/strict";
import { test } from "node:test";
import { decideBuiltIns, fromWire, type RunPlan, type StepResult } from "./engine";
import { validateGraph, type Graph } from "./graph";
import { orchestrate, type Deps } from "./orchestrate";

const action = (id: string, extra: Record<string, unknown> = {}) => ({
  id,
  type: "action",
  data: { nodeKey: "core.echo", version: "1.0.0", inputs: {}, ...extra },
});
const trigger = { id: "t", type: "trigger", data: {} };
const edge = (source: string, target: string, sourceHandle = "out") => ({ source, sourceHandle, target });

function planOf(raw: unknown): Extract<RunPlan, { ok: true }> {
  const { graph, issues } = validateGraph(raw);
  assert.deepEqual(issues, []);
  const nodes = Object.fromEntries((graph as Graph).nodes.map((n) => [n.id, { retries: 2, retryDelay: "1000" }]));
  return { ok: true, graph: graph as Graph, triggerId: null, nodes, info: { workspaceId: "w", workflowId: "f", no: 1 } };
}

/** Fake steps: records their names, and lets a test decide which actions fail. */
function harness(options: { fail?: string[] } = {}) {
  const log: string[] = [];
  const finished: { status: string; skips: string[] }[] = [];
  const deps: Deps = {
    async step(name, fn) {
      log.push(name);
      return fn();
    },
    async executeAction(id): Promise<StepResult> {
      if (options.fail?.includes(id)) return { ends: [{ id, status: "failed" }], finished: true, failed: { code: "X", message: "boom" } };
      return { ends: [{ id, status: "succeeded" }], finished: false };
    },
    async runBuiltIns(wire, skips) {
      // The graph is not available here, so tests give built-in decisions through `decide`.
      void skips;
      return decide ? decide(wire) : { ends: [], finished: false };
    },
    async finish(status, skips) {
      finished.push({ status, skips });
    },
  };
  let decide: ((wire: Parameters<Deps["runBuiltIns"]>[0]) => StepResult) | null = null;
  return { log, finished, deps, setDecide: (fn: typeof decide) => (decide = fn) };
}

test("orchestrate: a chain is one step per node, then finish", async () => {
  const h = harness();
  await orchestrate(planOf({ nodes: [trigger, action("a"), action("b"), action("c")], edges: [edge("t", "a"), edge("a", "b"), edge("b", "c")] }), h.deps);
  assert.deepEqual(h.log, ["node:a", "node:b", "node:c", "finish"]);
  assert.deepEqual(h.finished, [{ status: "succeeded", skips: [] }]);
});

test("orchestrate: nodes of one wave are separate steps started together", async () => {
  const h = harness();
  const started: string[] = [];
  const order: string[] = [];
  h.deps.step = async (name, fn) => {
    started.push(name);
    await Promise.resolve();
    order.push(name);
    return fn();
  };
  const plan = planOf({ nodes: [trigger, action("a"), action("b"), action("c")], edges: [edge("t", "a"), edge("t", "b"), edge("t", "c")] });
  await orchestrate(plan, h.deps);
  assert.deepEqual(started.slice(0, 3), ["node:a", "node:b", "node:c"]);
});

test("orchestrate: an if is decided in a flow step and the skipped branch is recorded by the next step", async () => {
  const raw = {
    nodes: [trigger, { id: "i", type: "if", data: { join: "and", conditions: [{ left: 1, op: "equals", right: 1 }] } }, action("yes"), action("no")],
    edges: [edge("t", "i"), edge("i", "yes", "true"), edge("i", "no", "false")],
  };
  const plan = planOf(raw);
  const h = harness();
  const scope = { trigger: {}, run: { id: "r", no: 1, env: "production" }, nodes: {} };
  h.setDecide((wire) => {
    const decided = decideBuiltIns(plan.graph, fromWire(wire), scope, {});
    return { ends: decided.ends, finished: false };
  });
  await orchestrate(plan, h.deps);
  assert.deepEqual(h.log, ["flow:0", "node:yes", "finish"]);
  // "no" was found skipped after the flow step; the next node step records it, so finish has nothing left.
  assert.deepEqual(h.finished[0].skips, []);
});

test("orchestrate: a failed node ends the run without a finish step", async () => {
  const h = harness({ fail: ["b"] });
  await orchestrate(planOf({ nodes: [trigger, action("a"), action("b"), action("c")], edges: [edge("t", "a"), edge("a", "b"), edge("b", "c")] }), h.deps);
  assert.deepEqual(h.log, ["node:a", "node:b"]);
  assert.deepEqual(h.finished, []);
});

test("orchestrate: a plan that failed to load is finished as failed", async () => {
  const h = harness();
  await orchestrate({ ok: false, error: { code: "INVALID_DEFINITION", message: "bad" } }, h.deps);
  assert.deepEqual(h.log, ["finish"]);
  assert.equal(h.finished[0].status, "failed");
});
