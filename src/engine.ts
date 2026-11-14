// The body of each Upstash step. All database access of the runner is here, as plain SQL:
// the tables are owned and migrated by the xaas-workflows app.
//
// A run is: `load`, then waves. A wave is every node that is ready at once; the nodes of a wave
// run as parallel steps (`node:<id>`). `if`, `switch` and `merge` run inside the runner: a wave
// with one node decides the built-ins that follow it in the same step (the common case), otherwise
// a `flow:<n>` step does it. A step that leaves nothing more to do ends the run itself.

import type { Sql } from "./db";
import {
  emptyState,
  graphSchema,
  evaluateIf,
  evaluateSwitch,
  ExpressionError,
  mergeOutput,
  planNext,
  resolveInputs,
  validateGraph,
  type ActionNode,
  type Graph,
  type GraphNode,
  type NodeStatus,
  type RunState,
  type Scope,
  type StoredError,
} from "./graph";
import { judgeNodeOutcome, retryDelay, uuidv7, type NodeOutcome } from "./logic";
import { newResourceToken, RESOURCE_TOKEN_GRACE_MS } from "./resource-token";

export type { Sql };

/** Upstash settings of the step that runs one action node. */
export type PlanNode = { retries: number; retryDelay: string };

/**
 * Result of the `load` step. Upstash sends it back on every later request of the run, so it holds
 * only what the code between steps needs.
 */
export type RunPlan =
  | { ok: true; graph: Graph; triggerId: string | null; nodes: Record<string, PlanNode>; info: { workspaceId: string; workflowId: string; no: number; test: boolean } }
  | { ok: false; error: StoredError };

export type NodeCall = {
  scriptName: string;
  transport: string;
  headers: Record<string, string>;
  body: string;
  timeoutMs: number;
};
export type CallNode = (call: NodeCall) => Promise<NodeOutcome>;

/** A node attempt failed and may be tried again. Thrown so that Upstash retries the step. */
export class NodeAttemptError extends Error {}

/** Upstash retries on top of a node's own attempts, for failures around the call (database, network). */
const INFRA_RETRIES = 2;
const BUILT_IN_STEP: PlanNode = { retries: 3, retryDelay: "1000 * (1 + retried)" };
/** Largest response kept in `attempts.response`; a bigger one is replaced by a note. */
const MAX_STORED_BYTES = 64 * 1024;

// ---------------------------------------------------------------------------
// State that travels between steps (small, JSON)
// ---------------------------------------------------------------------------

export type WireState = { status: [string, NodeStatus][]; taken: [string, string | null][] };

export function toWire(state: RunState): WireState {
  return { status: [...state.status], taken: [...state.taken] };
}

export function fromWire(wire: WireState): RunState {
  return { status: new Map(wire.status), taken: new Map(wire.taken) };
}

/** What one node ended as, sent back to the function body. `taken` is set for `if` / `switch`. */
export type NodeEnd = { id: string; status: NodeStatus; taken?: string | null };

/** Result of a `node:<id>` or `flow:<n>` step. */
export type StepResult = {
  ends: NodeEnd[];
  /** The step also ended the run, so no `finish` step follows. */
  finished: boolean;
  failed?: StoredError;
};

// ---------------------------------------------------------------------------
// Test runs (started from the editor on the draft)
// ---------------------------------------------------------------------------

/** `runs.test`: set for a run started from the editor. `nodeId` limits it to one node; `outputs` is what the nodes before it produced. */
export type TestSpec = { nodeId?: string; outputs?: Record<string, unknown> } | null;

/** One pinned output of the draft: `workflow_versions.pins[nodeId]`. */
export type Pins = Record<string, { output?: unknown } | undefined>;

/**
 * The graph a run executes. A real run and a whole-workflow test run the stored definition. A test of one node runs a
 * graph of the trigger and that node only: the rest of the draft may be unfinished and is not even validated.
 */
export function graphOfRun(definition: unknown, test: TestSpec): { graph: Graph | null; message?: string } {
  if (!test?.nodeId) {
    const { graph, issues } = validateGraph(definition);
    return graph && issues.length === 0 ? { graph } : { graph: null, message: issues[0]?.message ?? "unknown error" };
  }
  const parsed = graphSchema.safeParse(definition);
  if (!parsed.success) return { graph: null, message: "The draft could not be read." };
  const target = parsed.data.nodes.find((node) => node.id === test.nodeId);
  const trigger = parsed.data.nodes.find((node) => node.type === "trigger");
  if (!target || target.type === "trigger") return { graph: null, message: `Node "${test.nodeId}" is not in the draft.` };
  const nodes = trigger ? [trigger, target] : [target];
  const reduced = { nodes, edges: trigger ? [{ source: trigger.id, sourceHandle: "out", target: target.id, targetHandle: "in" }] : [] };
  // Not validated as a whole: the node reads nodes that are not in this graph, their outputs come with the run (`test.outputs`).
  return { graph: graphSchema.parse(reduced) };
}

// ---------------------------------------------------------------------------
// load
// ---------------------------------------------------------------------------

/** First step of a run: marks it running and returns the graph and the Upstash settings of each action. */
export async function loadRun(sql: Sql, runId: string): Promise<RunPlan> {
  const [run] = await sql`
    with started as (
      update runs set status = 'running', started_at = coalesce(started_at, now())
      where id = ${runId} and status = 'queued'
      returning id
    ), logged as (
      insert into run_logs (id, run_id, level, message) select ${uuidv7()}::uuid, id, 'info', 'Run started' from started
    )
    select wv.definition, r.test, r.trigger_id, r.workspace_id, r.workflow_id, r.no
    from runs r join workflow_versions wv on wv.id = r.workflow_version_id
    where r.id = ${runId}`;
  if (!run) return { ok: false, error: { code: "RUN_NOT_FOUND", message: `Run ${runId} does not exist.` } };

  const { graph, message } = graphOfRun(run.definition, (run.test as TestSpec) ?? null);
  if (!graph) return { ok: false, error: { code: "INVALID_DEFINITION", message: `Invalid workflow definition: ${message}` } };

  const actions = graph.nodes.filter((node): node is Extract<GraphNode, { type: "action" }> => node.type === "action");
  const keys = [...new Set(actions.map((node) => node.data.nodeKey))];
  const versions = keys.length
    ? await sql`
        select n.key, nv.version, nv.retry_max_attempts, nv.retry_backoff, nv.retry_base_delay_ms
        from node_versions nv join nodes n on n.id = nv.node_id
        where n.key = any(${keys}::text[])`
    : [];
  const versionOf = new Map(versions.map((row) => [`${row.key}@${row.version}`, row]));

  const nodes: Record<string, PlanNode> = {};
  for (const node of graph.nodes) {
    if (node.type !== "action") {
      nodes[node.id] = BUILT_IN_STEP;
      continue;
    }
    const version = versionOf.get(`${node.data.nodeKey}@${node.data.version}`);
    if (!version) {
      return {
        ok: false,
        error: { code: "NODE_VERSION_NOT_FOUND", message: `Node "${node.id}" uses ${node.data.nodeKey}@${node.data.version}, which is not registered.` },
      };
    }
    const attempts = node.data.settings.maxAttempts ?? version.retry_max_attempts;
    nodes[node.id] = { retries: Math.max(0, attempts - 1) + INFRA_RETRIES, retryDelay: retryDelay(version.retry_backoff, version.retry_base_delay_ms) };
  }
  return { ok: true, graph, triggerId: run.trigger_id ?? null, nodes, info: { workspaceId: run.workspace_id, workflowId: run.workflow_id, no: Number(run.no), test: run.test != null } };
}

// ---------------------------------------------------------------------------
// Reading what a step needs
// ---------------------------------------------------------------------------

type RunRow = {
  workspace_id: string;
  input: unknown;
  trigger_data: unknown;
  no: number;
  env: string;
  status: string;
  test: TestSpec;
  pins: Pins;
  workflow_slug: string;
  workflow_version: number;
  definition: unknown;
};
type NodeRunRow = { node_id: string; status: string; output: unknown; error: StoredError | null };
type VersionRow = { id: string; key: string; version: string; script_name: string; transport: string; timeout_ms: number; retry_max_attempts: number };

async function readRun(sql: Sql, runId: string): Promise<{ run: RunRow; nodeRuns: NodeRunRow[] } | null> {
  const [runs, nodeRuns] = await sql.transaction([
    sql`select r.workspace_id, r.input, r.trigger_data, r.no, r.env, r.status, r.test, wv.pins, w.slug as workflow_slug, wv.version as workflow_version, wv.definition
        from runs r join workflows w on w.id = r.workflow_id join workflow_versions wv on wv.id = r.workflow_version_id
        where r.id = ${runId}`,
    sql`select node_id, status, output, error from node_runs where run_id = ${runId}`,
  ]);
  if (!runs[0]) return null;
  return { run: runs[0] as unknown as RunRow, nodeRuns: nodeRuns as unknown as NodeRunRow[] };
}

function buildScope(run: RunRow, runId: string, nodeRuns: NodeRunRow[], extra: Record<string, unknown> = {}): { scope: Scope; outputs: Record<string, unknown> } {
  const outputs: Record<string, unknown> = {};
  for (const row of nodeRuns) if (row.status === "succeeded" || row.status === "warning") outputs[row.node_id] = row.output;
  Object.assign(outputs, run.test?.outputs ?? {}, extra);
  return {
    outputs,
    scope: {
      input: run.input,
      // The raw call that started the run (`trigger.body`, `trigger.headers`, …). Runs started before it was kept, and runs started from the app, have none.
      trigger: run.trigger_data ?? {},
      run: { id: runId, no: run.no, env: run.env },
      nodes: Object.fromEntries(Object.entries(outputs).map(([id, output]) => [id, { output }])),
    },
  };
}

const json = (value: unknown) => JSON.stringify(value ?? null);

function capped(value: unknown): unknown {
  const text = JSON.stringify(value ?? null);
  return text.length > MAX_STORED_BYTES ? { truncated: true, bytes: text.length } : value ?? null;
}

// ---------------------------------------------------------------------------
// Built-in nodes and skipped nodes
// ---------------------------------------------------------------------------

type Written = { id: string; type: string; label: string | null; status: NodeStatus; output?: unknown };

/** Inserts or updates node runs that did not call a node Worker: if / switch / merge, skipped, cancelled. */
function writeNodeRuns(sql: Sql, runId: string, rows: Written[]) {
  return rows.map(
    (row) => sql`
      insert into node_runs (id, run_id, node_id, node_type, label, status, output, finished_at)
      values (${uuidv7()}, ${runId}, ${row.id}, ${row.type}, ${row.label}, ${row.status}, ${row.output === undefined ? null : json(row.output)}::jsonb, now())
      on conflict (run_id, node_id) do update
        set status = excluded.status, output = excluded.output, finished_at = now()`,
  );
}

function labelOf(node: GraphNode): string | null {
  return ("label" in node.data && node.data.label) || null;
}

function rowsForSkipped(graph: Graph, ids: string[]): Written[] {
  const byId = new Map(graph.nodes.map((node) => [node.id, node]));
  return ids.flatMap((id) => {
    const node = byId.get(id);
    return node && node.type !== "trigger" ? [{ id, type: node.type, label: labelOf(node), status: "skipped" as const }] : [];
  });
}

/**
 * Decides every built-in node that is ready, and the built-ins that become ready after them,
 * without touching the database. Returns what to write and what to tell the function body.
 */
export function decideBuiltIns(graph: Graph, state: RunState, scope: Scope, outputs: Record<string, unknown>) {
  const byId = new Map(graph.nodes.map((node) => [node.id, node]));
  const ends: NodeEnd[] = [];
  const writes: Written[] = [];
  const skipped: string[] = [];
  for (let guard = 0; guard < graph.nodes.length + 1; guard++) {
    const plan = planNext(graph, state);
    skipped.push(...plan.skipped);
    const builtIns = plan.ready.map((id) => byId.get(id) as GraphNode).filter((node) => node.type !== "action");
    if (builtIns.length === 0) return { ends, writes, skipped, readyActions: plan.ready.filter((id) => byId.get(id)?.type === "action") };
    for (const node of builtIns) {
      let output: unknown;
      let taken: string | null | undefined;
      if (node.type === "if") {
        taken = evaluateIf(node, scope);
        output = { taken };
      } else if (node.type === "switch") {
        taken = evaluateSwitch(node, scope);
        output = { taken };
      } else {
        output = mergeOutput(node.id, graph, state, outputs);
        outputs[node.id] = output;
        scope.nodes[node.id] = { output };
      }
      state.status.set(node.id, "succeeded");
      if (taken !== undefined) state.taken.set(node.id, taken);
      ends.push({ id: node.id, status: "succeeded", ...(taken !== undefined ? { taken } : {}) });
      writes.push({ id: node.id, type: node.type, label: labelOf(node), status: "succeeded", output });
    }
  }
  throw new Error("Built-in nodes did not settle.");
}

/** A `flow:<n>` step: decides the ready built-ins and records them and the skipped nodes. */
export async function runBuiltIns(sql: Sql, runId: string, wire: WireState, pendingSkips: string[]): Promise<StepResult> {
  const read = await readRun(sql, runId);
  if (!read) return { ends: [], finished: true, failed: { code: "RUN_NOT_FOUND", message: `Run ${runId} does not exist.` } };
  const { graph } = graphOfRun(read.run.definition, read.run.test);
  if (!graph) throw new Error("The definition was valid at load time.");

  const state = fromWire(wire);
  const { scope, outputs } = buildScope(read.run, runId, read.nodeRuns);
  const decided = decideBuiltIns(graph, state, scope, outputs);
  const rows = [...rowsForSkipped(graph, [...pendingSkips, ...decided.skipped]), ...decided.writes];
  const queries = writeNodeRuns(sql, runId, rows);
  if (queries.length) await sql.transaction(queries);
  return { ends: decided.ends, finished: false };
}

// ---------------------------------------------------------------------------
// Action nodes
// ---------------------------------------------------------------------------

async function loadVersion(sql: Sql, node: ActionNode): Promise<VersionRow | null> {
  const [row] = await sql`
    select nv.id, n.key, nv.version, nv.script_name, nv.transport, nv.timeout_ms, nv.retry_max_attempts
    from node_versions nv join nodes n on n.id = nv.node_id
    where n.key = ${node.data.nodeKey} and nv.version = ${node.data.version}`;
  return (row as unknown as VersionRow) ?? null;
}

type ActionOptions = {
  /** State of the run before this wave. */
  wire: WireState;
  /** The only node of its wave: the step may decide the built-ins after it and end the run. */
  solo: boolean;
  /** Nodes already found skipped, to be recorded by this step. */
  skips: string[];
};

/** Records a failure that ends the run: the node run, the run, and node runs still running. */
async function failRunAt(sql: Sql, runId: string, nodeRunId: string | null, error: StoredError) {
  const stored = json(error);
  const queries = [
    ...(nodeRunId ? [sql`update node_runs set status = 'failed', error = ${stored}::jsonb, finished_at = now() where id = ${nodeRunId}`] : []),
    sql`update node_runs set status = 'cancelled', finished_at = now() where run_id = ${runId} and status = 'running' and id is distinct from ${nodeRunId}::uuid`,
    sql`update runs set status = 'failed', error = ${stored}::jsonb, output = 'null'::jsonb, finished_at = now() where id = ${runId} and status in ('queued', 'running')`,
    sql`insert into run_logs (id, run_id, level, message) values (${uuidv7()}, ${runId}, 'error', ${error.message})`,
  ];
  await sql.transaction(queries);
}

/**
 * Runs one action node; the body of the `node:<id>` step. A failed attempt that may be tried again
 * throws, so Upstash retries the step. A final failure ends the run and is returned, not thrown.
 */
export async function executeAction(sql: Sql, runId: string, nodeId: string, callNode: CallNode, options: ActionOptions): Promise<StepResult> {
  const read = await readRun(sql, runId);
  if (!read) return { ends: [], finished: true, failed: { code: "RUN_NOT_FOUND", message: `Run ${runId} does not exist.` } };
  const { run, nodeRuns } = read;
  const { graph } = graphOfRun(run.definition, run.test);
  if (!graph) throw new Error("The definition was valid at load time.");
  const node = graph.nodes.find((candidate) => candidate.id === nodeId);
  if (!node || node.type !== "action") {
    const error = { code: "NODE_NOT_FOUND", message: `Node "${nodeId}" is not an action of the workflow.` };
    await failRunAt(sql, runId, null, error);
    return { ends: [{ id: nodeId, status: "failed" }], finished: true, failed: error };
  }

  const skipRows = writeNodeRuns(sql, runId, rowsForSkipped(graph, options.skips));
  const label = node.data.label ?? node.id;

  // A sibling branch failed while this step was waiting to start: do not call the node.
  if (run.status === "failed" || run.status === "cancelled") {
    await sql.transaction([...skipRows, ...writeNodeRuns(sql, runId, [{ id: nodeId, type: "action", label, status: "cancelled" }])]);
    return { ends: [{ id: nodeId, status: "cancelled" }], finished: true };
  }

  const existing = nodeRuns.find((row) => row.node_id === nodeId);
  let end: NodeEnd | null = null;
  let nodeOutput: unknown;
  let failure: StoredError | null = null;

  if (existing && (existing.status === "succeeded" || existing.status === "warning")) {
    // The step ran again after its work was done but before Upstash stored the result.
    end = { id: nodeId, status: existing.status as NodeStatus };
    nodeOutput = existing.output;
  } else if (existing?.status === "failed") {
    failure = existing.error ?? { code: "NODE_FAILED", message: "The node run failed." };
  } else if (!run.test?.nodeId && run.test && run.pins?.[nodeId] !== undefined) {
    // A test of the whole draft does not call a pinned node: the pinned output stands in for it.
    const pinned = run.pins[nodeId]?.output ?? null;
    const { scope } = buildScope(run, runId, nodeRuns);
    let input: Record<string, unknown> | null = null;
    try {
      input = resolveInputs(node.data.inputs, scope);
    } catch {
      input = null;
    }
    await sql.transaction([
      ...skipRows,
      sql`insert into node_runs (id, run_id, node_id, node_type, label, status, input, output, attempt_count, finished_at)
          values (${uuidv7()}, ${runId}, ${nodeId}, 'action', ${label}, 'succeeded', ${json(input)}::jsonb, ${json(pinned)}::jsonb, 0, now())
          on conflict (run_id, node_id) do update set status = 'succeeded', output = excluded.output, finished_at = now()`,
      sql`insert into run_logs (id, run_id, level, message) values (${uuidv7()}, ${runId}, 'info', ${`Node "${label}" is pinned: its pinned output was used, the node was not called.`})`,
    ]);
    end = { id: nodeId, status: "succeeded" };
    nodeOutput = pinned;
  } else {
    const version = await loadVersion(sql, node);
    if (!version) {
      failure = { code: "NODE_VERSION_NOT_FOUND", message: `Node "${nodeId}" uses ${node.data.nodeKey}@${node.data.version}, which is not registered.` };
      await sql.transaction([...skipRows, ...writeNodeRuns(sql, runId, [{ id: nodeId, type: "action", label, status: "failed" }])]);
      await failRunAt(sql, runId, null, failure);
      return { ends: [{ id: nodeId, status: "failed" }], finished: true, failed: failure };
    }

    const { scope } = buildScope(run, runId, nodeRuns);
    let input: Record<string, unknown>;
    try {
      input = resolveInputs(node.data.inputs, scope);
    } catch (error) {
      if (!(error instanceof ExpressionError)) throw error;
      failure = { code: "EXPRESSION_ERROR", message: `Node "${nodeId}": ${error.message}` };
      await sql.transaction([...skipRows, ...writeNodeRuns(sql, runId, [{ id: nodeId, type: "action", label, status: "failed" }])]);
      return finishAfterFailure(sql, runId, node, failure);
    }

    const maxAttempts = node.data.settings.maxAttempts ?? version.retry_max_attempts;
    const timeoutMs = node.data.settings.timeout ?? version.timeout_ms;
    // The run token of this attempt: the node shows it to xaas-resource-broker. Only its hash is stored (with the attempt).
    const resourceToken = await newResourceToken();
    const [nodeRun] = await sql`
      with nr as (
        insert into node_runs (id, run_id, node_id, node_type, label, node_version_id, attempt_count, input)
        values (${uuidv7()}, ${runId}, ${nodeId}, 'action', ${label}, ${version.id}, 1, ${json(input)}::jsonb)
        on conflict (run_id, node_id) do update set attempt_count = node_runs.attempt_count + 1
        returning id, attempt_count
      ), at as (
        insert into attempts (id, node_run_id, attempt, resource_token_hash, resource_token_expires_at)
        select ${uuidv7()}::uuid, nr.id, nr.attempt_count, ${resourceToken.hash}, now() + ${timeoutMs + RESOURCE_TOKEN_GRACE_MS} * interval '1 millisecond'
        from nr where nr.attempt_count <= ${maxAttempts}::int
        on conflict (node_run_id, attempt) do nothing
      )
      select id, attempt_count from nr`;
    const nodeRunId: string = nodeRun.id;
    const attempt: number = nodeRun.attempt_count;

    if (attempt > maxAttempts) {
      failure = { code: "ATTEMPTS_EXHAUSTED", message: `The node failed ${maxAttempts} time(s).` };
      await sql`update node_runs set status = 'failed', error = ${json(failure)}::jsonb, attempt_count = ${maxAttempts}, finished_at = now() where id = ${nodeRunId}`;
      return finishAfterFailure(sql, runId, node, failure);
    }

    const headers = {
      "Content-Type": "application/json",
      "X-XaaS-Run-Id": runId,
      "X-XaaS-Node-Run-Id": nodeRunId,
      "X-XaaS-Attempt": String(attempt),
      // The same for every attempt, so the node can skip work it already did.
      "Idempotency-Key": nodeRunId,
    };
    // Sent to the node, never stored: `headers` (what `attempts.request` keeps) leaves it out.
    const sentHeaders = { ...headers, "X-XAAS-Resource-Token": resourceToken.token };
    const envelope = {
      input,
      // Secrets, variables and connections never travel here: the node reads what is mapped to it through the resource broker.
      context: {},
      run: { id: runId, no: run.no, workspace: run.workspace_id, workflow: run.workflow_slug, version: run.workflow_version, env: run.env },
      node: { key: version.key, version: version.version, label },
    };
    const outcome = await callNode({ scriptName: version.script_name, transport: version.transport, timeoutMs, headers: sentHeaders, body: JSON.stringify(envelope) });
    const verdict = judgeNodeOutcome(outcome);
    const stored = { request: json({ headers, body: envelope }), response: json(capped(outcome.kind === "response" ? { status: outcome.status, body: outcome.body } : { kind: outcome.kind })) };

    if (verdict.ok) {
      const status: NodeStatus = attempt > 1 ? "warning" : "succeeded";
      await sql.transaction([
        ...skipRows,
        sql`update attempts set status = 'succeeded', http_status = ${verdict.httpStatus}, request = ${stored.request}::jsonb, response = ${stored.response}::jsonb, finished_at = now()
            where node_run_id = ${nodeRunId} and attempt = ${attempt}`,
        sql`update node_runs set status = ${status}, output = ${json(verdict.output)}::jsonb, finished_at = now() where id = ${nodeRunId}`,
        sql`insert into run_logs (id, run_id, node_run_id, level, message) values (${uuidv7()}, ${runId}, ${nodeRunId}, 'info', ${attempt > 1 ? `Succeeded on attempt ${attempt}` : "Succeeded"})`,
      ]);
      end = { id: nodeId, status };
      nodeOutput = verdict.output;
    } else {
      const error = json(verdict.error);
      const attemptRow = sql`
        update attempts set status = 'failed', http_status = ${verdict.httpStatus}, error = ${error}::jsonb, request = ${stored.request}::jsonb, response = ${stored.response}::jsonb, finished_at = now()
        where node_run_id = ${nodeRunId} and attempt = ${attempt}`;
      const log = sql`insert into run_logs (id, run_id, node_run_id, level, message) values (${uuidv7()}, ${runId}, ${nodeRunId}, 'warn', ${`Attempt ${attempt} failed: ${verdict.error.message}`})`;
      if (verdict.retryable && attempt < maxAttempts) {
        await sql.transaction([attemptRow, log]);
        throw new NodeAttemptError(`${verdict.error.code}: ${verdict.error.message}`);
      }
      failure = verdict.error;
      if (node.data.settings.onError === "continue") {
        // The run goes on with this node's output as null.
        await sql.transaction([
          ...skipRows,
          attemptRow,
          log,
          sql`update node_runs set status = 'warning', error = ${error}::jsonb, output = 'null'::jsonb, finished_at = now() where id = ${nodeRunId}`,
        ]);
        end = { id: nodeId, status: "warning" };
        nodeOutput = null;
        failure = null;
      } else {
        await sql.transaction([...skipRows, attemptRow, log]);
        return finishAfterFailure(sql, runId, node, verdict.error, nodeRunId);
      }
    }
  }

  if (failure && !end) return finishAfterFailure(sql, runId, node, failure);
  if (!end) throw new Error("Unreachable: a node run without an end.");

  // Succeeded. Alone in its wave, the step also decides what follows and may end the run.
  const ends: NodeEnd[] = [end];
  if (!options.solo) return { ends, finished: false };

  const state = fromWire(options.wire);
  state.status.set(nodeId, end.status);
  const { scope, outputs } = buildScope(run, runId, nodeRuns, { [nodeId]: nodeOutput });
  const decided = decideBuiltIns(graph, state, scope, outputs);
  ends.push(...decided.ends);
  const rows = [...rowsForSkipped(graph, decided.skipped), ...decided.writes];
  const queries = writeNodeRuns(sql, runId, rows);
  if (queries.length) await sql.transaction(queries);

  if (decided.readyActions.length === 0) {
    await finishRun(sql, runId, "succeeded");
    return { ends, finished: true };
  }
  return { ends, finished: false };
}

/** A node failed for good: the run fails with it. */
async function finishAfterFailure(sql: Sql, runId: string, node: ActionNode, error: StoredError, nodeRunId?: string): Promise<StepResult> {
  if (node.data.settings.onError === "continue") {
    // An expression error or similar before the node ran: continue with a null output.
    await sql.transaction(writeNodeRuns(sql, runId, [{ id: node.id, type: "action", label: node.data.label ?? node.id, status: "warning", output: null }]));
    return { ends: [{ id: node.id, status: "warning" }], finished: false };
  }
  await failRunAt(sql, runId, nodeRunId ?? null, error);
  return { ends: [{ id: node.id, status: "failed" }], finished: true, failed: error };
}

// ---------------------------------------------------------------------------
// finish
// ---------------------------------------------------------------------------

/**
 * Stores the final status and output of a run. The output of a run that succeeded is the output
 * of the last action node that ended well. Does nothing if the run already ended.
 */
export async function finishRun(sql: Sql, runId: string, status: "succeeded" | "failed", error?: StoredError): Promise<void> {
  await sql`
    with ended as (
      update runs set status = ${status},
        output = case when ${status}::text = 'succeeded' then coalesce((
          select output from node_runs
          where run_id = ${runId} and status in ('succeeded', 'warning') and node_type = 'action'
          order by finished_at desc limit 1), 'null'::jsonb) else 'null'::jsonb end,
        error = ${error ? json(error) : null}::jsonb, finished_at = now()
      where id = ${runId} and status in ('queued', 'running')
      returning id
    )
    insert into run_logs (id, run_id, level, message) select ${uuidv7()}::uuid, id, ${status === "failed" ? "error" : "info"}, ${status === "failed" ? "Run failed" : "Run succeeded"} from ended`;
}

/** Last step when no node step ended the run: records leftover skipped nodes, then finishes. */
export async function finishWithSkips(sql: Sql, runId: string, status: "succeeded" | "failed", pendingSkips: string[], error?: StoredError): Promise<void> {
  if (pendingSkips.length) {
    const read = await readRun(sql, runId);
    if (read) {
      const { graph } = graphOfRun(read.run.definition, read.run.test);
      if (graph) {
        const queries = writeNodeRuns(sql, runId, rowsForSkipped(graph, pendingSkips));
        if (queries.length) await sql.transaction(queries);
      }
    }
  }
  await finishRun(sql, runId, status, error);
}

/** Ends a run the engine gave up on (Upstash ran out of retries), with any node run still running. */
export async function failRun(sql: Sql, runId: string, error: StoredError): Promise<void> {
  await failRunAt(sql, runId, null, error);
}

export { emptyState };
