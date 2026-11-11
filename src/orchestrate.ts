// The function body of a run, with the Upstash and database parts passed in so it can be tested
// without either. Upstash runs this body again from the top for every step, so everything here
// must give the same answer each time: no clock, no database, only what the steps returned.

import { planNext, startState, type Graph, type NodeStatus, type StoredError } from "./graph";
import { toWire, type NodeEnd, type PlanNode, type RunPlan, type StepResult, type WireState } from "./engine";

export type StepOptions = { wire: WireState; solo: boolean; skips: string[] };

export type Deps = {
  /** `context.run` of Upstash: runs `fn` once and remembers its result under `name`. */
  step<T>(name: string, fn: () => Promise<T>, settings?: PlanNode): Promise<T>;
  executeAction(nodeId: string, options: StepOptions): Promise<StepResult>;
  runBuiltIns(wire: WireState, skips: string[]): Promise<StepResult>;
  finish(status: "succeeded" | "failed", skips: string[], error?: StoredError): Promise<void>;
};

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
  const state = startState(graph, plan.triggerId);
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
      actions.map((id, index) =>
        deps.step(`node:${id}`, () => deps.executeAction(id, { wire, solo, skips: index === 0 ? skips : [] }), plan.nodes[id]),
      ),
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
