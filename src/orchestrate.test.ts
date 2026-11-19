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
  return { ok: true, graph: graph as Graph, nodes, info: { workspaceId: "w", workflowId: "f", no: 1, test: false } };
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
    async sleep(name) {
      log.push(name);
    },
    async waitForEvent(name) {
      log.push(name);
      return { timeout: false };
    },
    async startAsync(id) {
      return { kind: "ended", result: { ends: [{ id, status: "succeeded" }], finished: false } };
    },
    async completeAsync(id) {
      return { kind: "ended", result: { ends: [{ id, status: "succeeded" }], finished: false } };
    },
    async pollAsync(id) {
      return { kind: "ended", result: { ends: [{ id, status: "succeeded" }], finished: false } };
    },
  };
  let decide: ((wire: Parameters<Deps["runBuiltIns"]>[0]) => StepResult | Promise<StepResult>) | null = null;
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
  const scope = { input: {}, trigger: {}, run: { id: "r", no: 1, env: "production" }, nodes: {} };
  h.setDecide(async (wire) => {
    const decided = await decideBuiltIns(plan.graph, fromWire(wire), scope, {});
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

// ---------------------------------------------------------------------------
// Async nodes
// ---------------------------------------------------------------------------

const callbackPlan = { completion: "callback" as const, maxAttempts: 3, timeoutMs: 30_000, maxWaitMs: 600_000, backoff: "none", baseDelayMs: 2_000, callbackHandler: false };
const pollingPlan = { ...callbackPlan, completion: "polling" as const, poll: { intervalMs: 1_000, maxIntervalMs: 2_000, maxPolls: 10 } };

function asyncPlanOf(raw: unknown, asyncNodes: Record<string, typeof callbackPlan | typeof pollingPlan>) {
  const plan = planOf(raw);
  for (const [id, config] of Object.entries(asyncNodes)) plan.nodes[id] = { ...plan.nodes[id], async: config };
  return plan;
}

const ok = (id: string): StepResult => ({ ends: [{ id, status: "succeeded" }], finished: false });

test("orchestrate: an async callback node sends, waits for its event, then reads the callback", async () => {
  const h = harness();
  const waited: string[] = [];
  h.deps.startAsync = async () => ({ kind: "accepted", nodeRunId: "nr1" });
  h.deps.waitForEvent = async (name, eventId) => {
    h.log.push(name);
    waited.push(eventId);
    return { timeout: false };
  };
  h.deps.completeAsync = async (id) => ({ kind: "ended", result: ok(id) });
  await orchestrate(asyncPlanOf({ nodes: [trigger, action("a"), action("b")], edges: [edge("t", "a"), edge("a", "b")] }, { a: callbackPlan }), h.deps);
  assert.deepEqual(h.log, ["node:a:1", "wait:a:1", "done:a:1", "node:b", "finish"]);
  assert.deepEqual(waited, ["callback-nr1-1"]);
});

test("orchestrate: a polling node sleeps longer each time, and follows retry_after_ms", async () => {
  const h = harness();
  const sleeps: [string, number][] = [];
  h.deps.sleep = async (name, ms) => {
    h.log.push(name);
    sleeps.push([name, ms]);
  };
  h.deps.startAsync = async () => ({ kind: "accepted", nodeRunId: "nr1" });
  let polls = 0;
  h.deps.pollAsync = async (id) => {
    polls++;
    if (polls === 1) return { kind: "running", errors: 0 };
    if (polls === 2) return { kind: "running", errors: 0 };
    if (polls === 3) return { kind: "running", errors: 1, retryAfterMs: 5_000 };
    return { kind: "ended", result: ok(id) };
  };
  await orchestrate(asyncPlanOf({ nodes: [trigger, action("a")], edges: [edge("t", "a")] }, { a: pollingPlan }), h.deps);
  assert.deepEqual(h.log, ["node:a:1", "poll:a:1:1", "check:a:1:1", "poll:a:1:2", "check:a:1:2", "poll:a:1:3", "check:a:1:3", "poll:a:1:4", "check:a:1:4", "finish"]);
  assert.deepEqual(sleeps.map(([, ms]) => ms), [1_000, 1_500, 2_000, 5_000]);
});

test("orchestrate: a retryable async failure sleeps, then the next attempt has its own steps", async () => {
  const h = harness();
  const attempts: number[] = [];
  h.deps.startAsync = async (_id, attempt) => {
    attempts.push(attempt);
    return attempt === 1 ? { kind: "accepted", nodeRunId: "nr1" } : { kind: "accepted", nodeRunId: "nr1" };
  };
  h.deps.completeAsync = async (id, attempt) => (attempt === 1 ? { kind: "retry", delayMs: 2_000 } : { kind: "ended", result: ok(id) });
  await orchestrate(asyncPlanOf({ nodes: [trigger, action("a")], edges: [edge("t", "a")] }, { a: callbackPlan }), h.deps);
  assert.deepEqual(h.log, ["node:a:1", "wait:a:1", "done:a:1", "backoff:a:1", "node:a:2", "wait:a:2", "done:a:2", "finish"]);
  assert.deepEqual(attempts, [1, 2]);
});

test("orchestrate: an async node runs alongside a sync node of the same wave, and only the first step records skips", async () => {
  const h = harness();
  const skipsSeen: string[][] = [];
  h.deps.startAsync = async (_id, attempt, _plan, options) => {
    skipsSeen.push(options.skips);
    return attempt === 1 ? { kind: "retry", delayMs: 1_000 } : { kind: "accepted", nodeRunId: "nr1" };
  };
  h.deps.completeAsync = async (id) => ({ kind: "ended", result: ok(id) });
  await orchestrate(
    asyncPlanOf({ nodes: [trigger, action("a"), action("b"), action("c")], edges: [edge("t", "a"), edge("t", "b"), edge("a", "c"), edge("b", "c")] }, { a: callbackPlan }),
    h.deps,
  );
  assert.ok(h.log.includes("node:b"));
  assert.deepEqual(h.log.filter((name) => name.endsWith(":a:1") || name.endsWith(":a:2")), ["node:a:1", "backoff:a:1", "node:a:2", "wait:a:2", "done:a:2"]);
  assert.equal(h.log.at(-2), "node:c");
  assert.deepEqual(skipsSeen.slice(1), [[]]);
});
