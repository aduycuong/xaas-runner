// The function body of a run, with the Upstash and database parts passed in so it can be tested
// without either. Upstash runs this body again from the top for every step, so everything here
// must give the same answer each time: no clock, no database, only what the steps returned.

import { planNext, startState, type Graph, type NodeStatus, type StoredError } from "./graph";
import { ASYNC_STEP, toWire, type AsyncPlan, type AsyncStep, type NodeEnd, type PlanNode, type RunPlan, type StepResult, type WireState } from "./engine";
import { nextPollWait } from "./limits";

export type StepOptions = { wire: WireState; solo: boolean; skips: string[] };

export type Deps = {
  /** `context.run` of Upstash: runs `fn` once and remembers its result under `name`. */
  step<T>(name: string, fn: () => Promise<T>, settings?: PlanNode): Promise<T>;
  executeAction(nodeId: string, options: StepOptions): Promise<StepResult>;
  runBuiltIns(wire: WireState, skips: string[]): Promise<StepResult>;
  finish(status: "succeeded" | "failed", skips: string[], error?: StoredError): Promise<void>;
  /** `context.sleep` of Upstash: no Worker runs while it waits. */
  sleep(name: string, ms: number): Promise<void>;
  /** `context.waitForEvent` of Upstash: resolves when the event comes (`timeout: false`) or after `timeoutMs`. */
  waitForEvent(name: string, eventId: string, timeoutMs: number): Promise<{ timeout: boolean }>;
  /** Steps of an async node (engine.ts): send the job, read the callback, poll the job. */
  startAsync(nodeId: string, attempt: number, plan: AsyncPlan, options: StepOptions): Promise<AsyncStep>;
  completeAsync(nodeId: string, attempt: number, plan: AsyncPlan, timedOut: boolean, options: StepOptions): Promise<AsyncStep>;
  pollAsync(nodeId: string, attempt: number, poll: number, errors: number, plan: AsyncPlan, options: StepOptions): Promise<AsyncStep>;
};

/**
 * The attempts of one async node. Each attempt sends the job (`node:<id>:<n>`), then either waits for the callback
 * (`wait:<id>:<n>`) and reads it (`done:<id>:<n>`), or sleeps and polls (`poll:<id>:<n>:<k>` + `check:<id>:<n>:<k>`).
 * A failure that may be retried sleeps (`backoff:<id>:<n>`) before the next attempt.
 */
async function runAsyncNode(deps: Deps, id: string, plan: AsyncPlan, first: StepOptions): Promise<StepResult> {
  for (let attempt = 1; ; attempt++) {
    // Skipped nodes are recorded by the first step only.
    const options = attempt === 1 ? first : { ...first, skips: [] };
    let result: AsyncStep = await deps.step(`node:${id}:${attempt}`, () => deps.startAsync(id, attempt, plan, options), ASYNC_STEP);
    if (result.kind === "accepted") {
      if (plan.completion === "callback") {
        const event = await deps.waitForEvent(`wait:${id}:${attempt}`, `callback-${result.nodeRunId}-${attempt}`, plan.maxWaitMs);
        result = await deps.step(`done:${id}:${attempt}`, () => deps.completeAsync(id, attempt, plan, event.timeout, options), ASYNC_STEP);
      } else {
        const poll = plan.poll ?? { intervalMs: 5_000, maxIntervalMs: 60_000, maxPolls: 200 };
        let wait = poll.intervalMs;
        let errors = 0;
        for (let k = 1; ; k++) {
          await deps.sleep(`poll:${id}:${attempt}:${k}`, wait);
          const pollErrors = errors;
          result = await deps.step(`check:${id}:${attempt}:${k}`, () => deps.pollAsync(id, attempt, k, pollErrors, plan, options), ASYNC_STEP);
          if (result.kind !== "running") break;
          errors = result.errors;
          wait = nextPollWait(wait, poll.maxIntervalMs, result.retryAfterMs);
        }
      }
    }
    if (result.kind === "ended") return result.result;
    if (result.kind === "retry") {
      await deps.sleep(`backoff:${id}:${attempt}`, result.delayMs);
      continue;
    }
    throw new Error(`Unexpected step result "${result.kind}" for async node "${id}".`);
  }
}

function apply(state: ReturnType<typeof startState>, ends: NodeEnd[]) {
  for (const end of ends) {
    state.status.set(end.id, end.status as NodeStatus);
    if (end.taken !== undefined) state.taken.set(end.id, end.taken);
  }
}

/** Walks the graph wave by wave. Returns the names of the steps it ran (handy in tests). */
export async function orchestrate(plan: Extract<RunPlan, { ok: true }> | { ok: false; error: StoredError }, deps: Deps): Promise<void> {
  if (!plan.ok) {
    await deps.step("finish", () => deps.finish("failed", [], plan.error));
    return;
  }

  const graph: Graph = plan.graph;
  const byId = new Map(graph.nodes.map((node) => [node.id, node]));
  const state = startState(graph);
  let pendingSkips: string[] = [];
  let flows = 0;
  let finished = false;
  let failed: StoredError | undefined;

  while (!finished) {
    const next = planNext(graph, state);
    pendingSkips.push(...next.skipped);

    const builtIns = next.ready.filter((id) => byId.get(id)?.type !== "action");
    if (builtIns.length) {
      const skips = pendingSkips;
      pendingSkips = [];
      const wire = toWire(state);
      const result = await deps.step(`flow:${flows++}`, () => deps.runBuiltIns(wire, skips));
      apply(state, result.ends);
      continue;
    }

    const actions = next.ready;
    if (actions.length === 0) break;

    const solo = actions.length === 1;
    const wire = toWire(state);
    const skips = pendingSkips;
    pendingSkips = [];
    const results = await Promise.all(
      actions.map((id, index) => {
        const options = { wire, solo, skips: index === 0 ? skips : [] };
        const asyncPlan = plan.nodes[id]?.async;
        if (asyncPlan) return runAsyncNode(deps, id, asyncPlan, options);
        return deps.step(`node:${id}`, () => deps.executeAction(id, options), plan.nodes[id]);
      }),
    );
    for (const result of results) {
      apply(state, result.ends);
      if (result.finished) finished = true;
      if (result.failed) failed = result.failed;
    }
    // A failure ends the run inside the failing step; the others only report.
    if (failed) finished = true;
  }

  if (!finished) await deps.step("finish", () => deps.finish("succeeded", pendingSkips));
}
