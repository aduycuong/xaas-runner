// The body of each Upstash step. All database access of the runner is here, as plain SQL:
// the tables are owned and migrated by the xaas-workflows app.
//
// A run is: `load`, then waves. A wave is every node that is ready at once; the nodes of a wave
// run as parallel steps (`node:<id>`). `if`, `switch` and `merge` run inside the runner: a wave
// with one node decides the built-ins that follow it in the same step (the common case), otherwise
// a `flow:<n>` step does it. A step that leaves nothing more to do ends the run itself.

import { callbackUrl } from "./callback-url";
import type { Query, Sql } from "./db";
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
import { CACHE_KEY_TIMEOUT_MS, clampToRange, MAX_ATTEMPTS, MAX_CACHE_KEY_CHARS, MAX_CACHE_TTL_S, MAX_WAIT_MS, POLL_ERRORS_IN_A_ROW, TIMEOUT_MS } from "./limits";
import {
  cacheWindow,
  hashCacheKey,
  judgeCallbackBody,
  judgeNodeOutcome,
  judgePollOutcome,
  readCacheKey,
  retryDelay,
  retryDelayMs,
  uuidv7,
  type NodeOutcome,
  type NodeVerdict,
} from "./logic";
import { derivedResourceToken, newResourceToken, RESOURCE_TOKEN_GRACE_MS } from "./resource-token";

export type { Sql };

/** How an async node's attempts run: its manifest's settings with the workflow's, clamped to the shared ranges. */
export type AsyncPlan = {
  completion: "callback" | "polling";
  maxAttempts: number;
  timeoutMs: number;
  maxWaitMs: number;
  backoff: string;
  baseDelayMs: number;
  callbackHandler: boolean;
  poll?: { intervalMs: number; maxIntervalMs: number; maxPolls: number };
};

/** Upstash settings of the step that runs one action node; `async` for an async node. */
export type PlanNode = { retries: number; retryDelay: string; async?: AsyncPlan };

/** Upstash settings of every step of an async node: its attempts are counted by the runner, these only cover failures around the call. */
export const ASYNC_STEP: PlanNode = { retries: 2, retryDelay: "2000 * (1 + retried)" };

/** A callback URL stays valid this long after the attempt's max wait, for a callback that arrives at the last moment. */
const CALLBACK_URL_GRACE_S = 3600;

/**
 * Result of the `load` step. Upstash sends it back on every later request of the run, so it holds
 * only what the code between steps needs.
 */
export type RunPlan =
  | {
      ok: true;
      graph: Graph;
      nodes: Record<string, PlanNode>;
      /**
       * `reportsNodes`: someone follows which node the run is at (a status callback of level `nodes` that takes this run,
       * or the URL the call named), so the runner reports each node it starts (`publishRunNode`). Read once, at load.
       */
      info: { workspaceId: string; workflowId: string; no: number; test: boolean; reportsNodes?: boolean };
    }
  | { ok: false; error: StoredError };

export type NodeCall = {
  scriptName: string;
  transport: string;
  headers: Record<string, string>;
  body?: string;
  timeoutMs: number;
  /** The node's own path; `/` (run the node) when not given. */
  path?: string;
  method?: "GET" | "POST";
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
    select wv.definition, r.test, r.workspace_id, r.workflow_id, r.no,
      (r.test is null and (
        exists (select 1 from workflow_callbacks c where c.workflow_id = r.workflow_id and c.enabled and c.level = 'nodes'
          and (c.only_authentication_id is null or c.only_authentication_id = r.authentication_id))
        or exists (select 1 from run_deliveries d where d.run_id = r.id and d.callback_id is null)
      )) as reports_nodes
    from runs r join workflow_versions wv on wv.id = r.workflow_version_id
    where r.id = ${runId}`;
  if (!run) return { ok: false, error: { code: "RUN_NOT_FOUND", message: `Run ${runId} does not exist.` } };

  const { graph, message } = graphOfRun(run.definition, (run.test as TestSpec) ?? null);
  if (!graph) return { ok: false, error: { code: "INVALID_DEFINITION", message: `Invalid workflow definition: ${message}` } };

  const actions = graph.nodes.filter((node): node is Extract<GraphNode, { type: "action" }> => node.type === "action");
  const keys = [...new Set(actions.map((node) => node.data.nodeKey))];
  const versions = keys.length
    ? await sql`
        select n.key, nv.version, nv.retry_max_attempts, nv.retry_backoff, nv.retry_base_delay_ms, nv.timeout_ms, nv.mode, nv.async
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
    const attempts = clampToRange(node.data.settings.maxAttempts ?? version.retry_max_attempts, MAX_ATTEMPTS);
    if (version.mode === "async" && version.async) {
      const config = version.async as { completion: "callback" | "polling"; maxWaitMs: number; callbackHandler?: boolean; poll?: AsyncPlan["poll"] };
      nodes[node.id] = {
        ...ASYNC_STEP,
        async: {
          completion: config.completion,
          maxAttempts: attempts,
          timeoutMs: clampToRange(node.data.settings.timeout ?? version.timeout_ms, TIMEOUT_MS),
          maxWaitMs: clampToRange(node.data.settings.maxWait ?? config.maxWaitMs, MAX_WAIT_MS),
          backoff: version.retry_backoff,
          baseDelayMs: version.retry_base_delay_ms,
          callbackHandler: config.callbackHandler === true,
          ...(config.completion === "polling" && config.poll ? { poll: config.poll } : {}),
        },
      };
      continue;
    }
    nodes[node.id] = { retries: Math.max(0, attempts - 1) + INFRA_RETRIES, retryDelay: retryDelay(version.retry_backoff, version.retry_base_delay_ms) };
  }
  return { ok: true, graph, nodes, info: { workspaceId: run.workspace_id, workflowId: run.workflow_id, no: Number(run.no), test: run.test != null, reportsNodes: Boolean(run.reports_nodes) } };
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
type VersionRow = { id: string; key: string; version: string; script_name: string; transport: string; timeout_ms: number; retry_max_attempts: number; cache: boolean };

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

/** `error`: why a node failed before its Worker was called (an input expression, an unknown version); kept when absent. */
type Written = { id: string; type: string; label: string | null; status: NodeStatus; output?: unknown; error?: StoredError };

/** Inserts or updates node runs that did not call a node Worker: if / switch / merge, skipped, cancelled. */
function writeNodeRuns(sql: Sql, runId: string, rows: Written[]) {
  return rows.map(
    (row) => sql`
      insert into node_runs (id, run_id, node_id, node_type, label, status, output, error, finished_at)
      values (${uuidv7()}, ${runId}, ${row.id}, ${row.type}, ${row.label}, ${row.status}, ${row.output === undefined ? null : json(row.output)}::jsonb, ${row.error ? json(row.error) : null}::jsonb, now())
      on conflict (run_id, node_id) do update
        set status = excluded.status, output = excluded.output, error = coalesce(excluded.error, node_runs.error), finished_at = now()`,
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
export async function decideBuiltIns(graph: Graph, state: RunState, scope: Scope, outputs: Record<string, unknown>) {
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
        taken = await evaluateIf(node, scope);
        output = { taken };
      } else if (node.type === "switch") {
        taken = await evaluateSwitch(node, scope);
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
  const decided = await decideBuiltIns(graph, state, scope, outputs);
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
    select nv.id, n.key, nv.version, nv.script_name, nv.transport, nv.timeout_ms, nv.retry_max_attempts, nv.cache
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

/** What every step of one action node reads first. */
type ActionScope = {
  runId: string;
  run: RunRow;
  nodeRuns: NodeRunRow[];
  graph: Graph;
  node: ActionNode;
  label: string;
  /** Skipped nodes this step records with its first write. */
  skipRows: Query[];
};

/** Reads the run and the node; ends the step when there is nothing to do (run gone, node gone, run over). */
async function beginAction(sql: Sql, runId: string, nodeId: string, skips: string[]): Promise<{ ended: StepResult } | { scope: ActionScope }> {
  const read = await readRun(sql, runId);
  if (!read) return { ended: { ends: [], finished: true, failed: { code: "RUN_NOT_FOUND", message: `Run ${runId} does not exist.` } } };
  const { run, nodeRuns } = read;
  const { graph } = graphOfRun(run.definition, run.test);
  if (!graph) throw new Error("The definition was valid at load time.");
  const node = graph.nodes.find((candidate) => candidate.id === nodeId);
  if (!node || node.type !== "action") {
    const error = { code: "NODE_NOT_FOUND", message: `Node "${nodeId}" is not an action of the workflow.` };
    await failRunAt(sql, runId, null, error);
    return { ended: { ends: [{ id: nodeId, status: "failed" }], finished: true, failed: error } };
  }
  const skipRows = writeNodeRuns(sql, runId, rowsForSkipped(graph, skips));
  const label = node.data.label ?? node.id;
  // A sibling branch failed (or the run was cancelled) while this step was waiting to start: do not call the node.
  if (run.status === "failed" || run.status === "cancelled") {
    await sql.transaction([...skipRows, ...writeNodeRuns(sql, runId, [{ id: nodeId, type: "action", label, status: "cancelled" }])]);
    return { ended: { ends: [{ id: nodeId, status: "cancelled" }], finished: true } };
  }
  return { scope: { runId, run, nodeRuns, graph, node, label, skipRows } };
}

/** How one attempt ended, once recorded. */
type Settled = { kind: "ok"; end: NodeEnd; nodeOutput: unknown } | { kind: "retry" } | { kind: "ended"; result: StepResult };

/**
 * Records the verdict on an attempt: the attempt, and when it is the last word, the node run. A retryable failure with
 * attempts left only records the attempt. `stored` replaces what the attempt keeps of the request and response.
 */
async function settleAttempt(
  sql: Sql,
  scope: ActionScope,
  nodeRunId: string,
  attempt: number,
  maxAttempts: number,
  verdict: NodeVerdict,
  stored: { request?: string; response?: string },
): Promise<Settled> {
  const { runId, node, skipRows } = scope;
  const request = stored.request ?? null;
  const response = stored.response ?? null;
  if (verdict.ok) {
    const status: NodeStatus = attempt > 1 ? "warning" : "succeeded";
    await sql.transaction([
      ...skipRows,
      sql`update attempts set status = 'succeeded', http_status = coalesce(${verdict.httpStatus}::int, http_status), request = coalesce(${request}::jsonb, request),
            response = coalesce(${response}::jsonb, response), finished_at = now()
          where node_run_id = ${nodeRunId} and attempt = ${attempt}`,
      succeedNodeRun(sql, scope, nodeRunId, status, verdict.output, attempt),
      sql`insert into run_logs (id, run_id, node_run_id, level, message) values (${uuidv7()}, ${runId}, ${nodeRunId}, 'info', ${attempt > 1 ? `Succeeded on attempt ${attempt}` : "Succeeded"})`,
    ]);
    return { kind: "ok", end: { id: node.id, status }, nodeOutput: verdict.output };
  }
  const error = json(verdict.error);
  const attemptRow = sql`
    update attempts set status = 'failed', http_status = coalesce(${verdict.httpStatus}::int, http_status), error = ${error}::jsonb,
      request = coalesce(${request}::jsonb, request), response = coalesce(${response}::jsonb, response), finished_at = now()
    where node_run_id = ${nodeRunId} and attempt = ${attempt}`;
  const log = sql`insert into run_logs (id, run_id, node_run_id, level, message) values (${uuidv7()}, ${runId}, ${nodeRunId}, 'warn', ${`Attempt ${attempt} failed: ${verdict.error.message}`})`;
  if (verdict.retryable && attempt < maxAttempts) {
    await sql.transaction([...skipRows, attemptRow, log]);
    return { kind: "retry" };
  }
  if (node.data.settings.onError === "continue") {
    // The run goes on with this node's output as null.
    await sql.transaction([
      ...skipRows,
      attemptRow,
      log,
      sql`update node_runs set status = 'warning', error = ${error}::jsonb, output = 'null'::jsonb, attempt_count = ${attempt}, finished_at = now() where id = ${nodeRunId}`,
    ]);
    return { kind: "ok", end: { id: node.id, status: "warning" }, nodeOutput: null };
  }
  await sql.transaction([...skipRows, attemptRow, log]);
  return { kind: "ended", result: await finishAfterFailure(sql, scope.runId, node, verdict.error, nodeRunId) };
}

// ---------------------------------------------------------------------------
// Node cache (`data.settings.cacheTtl` of a node whose version has `cache`)
// ---------------------------------------------------------------------------

/**
 * Ends a node run that succeeded. When attempt 1 got a cache key (`node_runs.cache_key`), the output is also stored in
 * `node_cache` under it, for the workspace, node version and environment of the run, until the run's TTL is over
 * (`expires_at = 'infinity'` when the node caches forever).
 */
function succeedNodeRun(sql: Sql, scope: ActionScope, nodeRunId: string, status: NodeStatus, output: unknown, attempt: number): Query {
  const window = cacheWindow(scope.node.data.settings.cacheTtl, MAX_CACHE_TTL_S);
  const seconds = window.on ? window.seconds : null;
  return sql`
    with nr as (
      update node_runs set status = ${status}, output = ${json(output)}::jsonb, attempt_count = ${attempt}, finished_at = now() where id = ${nodeRunId}
      returning id, cache_key, output
    )
    insert into node_cache (workspace_id, node_key, node_version, env, key, output, node_run_id, expires_at)
    select ${scope.run.workspace_id}::uuid, ${scope.node.data.nodeKey}, ${scope.node.data.version}, ${scope.run.env}, nr.cache_key, nr.output, nr.id,
      case when ${seconds}::float8 is null then 'infinity'::timestamptz else now() + ${seconds}::float8 * interval '1 second' end
    from nr where nr.cache_key is not null and ${window.on}::boolean
    on conflict (workspace_id, node_key, node_version, env, key) do update
      set output = excluded.output, node_run_id = excluded.node_run_id, created_at = now(), expires_at = excluded.expires_at`;
}

/** How long attempt 1 of this node may spend on `POST /cache-key`; 0 when the node is not cached. */
function cacheKeyTimeoutMs(scope: ActionScope, version: VersionRow, timeoutMs: number): number {
  return version.cache && cacheWindow(scope.node.data.settings.cacheTtl, MAX_CACHE_TTL_S).on ? Math.min(timeoutMs, CACHE_KEY_TIMEOUT_MS) : 0;
}

type CacheLookup = { kind: "hit"; key: string; output: unknown; storedAt: string } | { kind: "miss" };

/**
 * Attempt 1 of a node the workflow caches: asks the node for the key of its input (`POST /cache-key`, with the
 * attempt's headers and run token), keeps its sha256 on the node run and looks it up. An entry younger than the
 * node's TTL is a hit. A node that gives no key, or fails to, is a miss: it runs as usual.
 */
async function lookUpCache(
  sql: Sql,
  callNode: CallNode,
  scope: ActionScope,
  version: VersionRow,
  input: Record<string, unknown>,
  nodeRunId: string,
  attempt: number,
  headers: Record<string, string>,
  timeoutMs: number,
): Promise<CacheLookup> {
  const keyTimeoutMs = cacheKeyTimeoutMs(scope, version, timeoutMs);
  if (!keyTimeoutMs || attempt !== 1) return { kind: "miss" };
  const { run, runId, node } = scope;
  const outcome = await callNode({
    scriptName: version.script_name,
    transport: version.transport,
    timeoutMs: keyTimeoutMs,
    headers,
    body: JSON.stringify(envelopeFor(scope, version, input)),
    path: "/cache-key",
    method: "POST",
  });
  const answer = readCacheKey(outcome, MAX_CACHE_KEY_CHARS);
  if (answer.kind !== "key") {
    const message = answer.kind === "none" ? "The node gave no cache key for this input: it runs." : `No cache key, ${answer.message}: the node runs.`;
    await sql`insert into run_logs (id, run_id, node_run_id, level, message) values (${uuidv7()}, ${runId}, ${nodeRunId}, ${answer.kind === "none" ? "info" : "warn"}, ${message})`;
    return { kind: "miss" };
  }
  const key = await hashCacheKey(answer.key);
  const window = cacheWindow(node.data.settings.cacheTtl, MAX_CACHE_TTL_S);
  // Forever (-1): any entry of this key; otherwise one younger than the TTL.
  const seconds = window.on ? window.seconds : null;
  const [, found] = await sql.transaction([
    sql`update node_runs set cache_key = ${key} where id = ${nodeRunId}`,
    sql`select output, created_at from node_cache
        where workspace_id = ${run.workspace_id}::uuid and node_key = ${node.data.nodeKey} and node_version = ${node.data.version} and env = ${run.env} and key = ${key}
          and (${seconds}::float8 is null or created_at > now() - ${seconds}::float8 * interval '1 second')
        limit 1`,
  ]);
  const row = (found as unknown as { output: unknown; created_at: Date | string }[])[0];
  return row ? { kind: "hit", key, output: row.output, storedAt: new Date(row.created_at).toISOString() } : { kind: "miss" };
}

/** A cache hit ends attempt 1 and the node run with the stored output; the node is not run. */
async function settleCacheHit(sql: Sql, scope: ActionScope, nodeRunId: string, attempt: number, hit: Extract<CacheLookup, { kind: "hit" }>, request: string) {
  const { runId, node, skipRows } = scope;
  await sql.transaction([
    ...skipRows,
    sql`update attempts set status = 'succeeded', http_status = 200, request = ${request}::jsonb,
          response = ${json({ cache: "hit", key: hit.key, stored_at: hit.storedAt })}::jsonb, finished_at = now()
        where node_run_id = ${nodeRunId} and attempt = ${attempt}`,
    sql`update node_runs set status = 'succeeded', output = ${json(hit.output)}::jsonb, cache_hit = true, attempt_count = ${attempt}, finished_at = now() where id = ${nodeRunId}`,
    sql`insert into run_logs (id, run_id, node_run_id, level, message) values (${uuidv7()}, ${runId}, ${nodeRunId}, 'info', ${`Used the cached output stored at ${hit.storedAt}: the node was not run.`})`,
  ]);
  const end: NodeEnd = { id: node.id, status: "succeeded" };
  return { end, nodeOutput: hit.output };
}

/** After a node ended well: alone in its wave, the step also decides what follows and may end the run. */
async function afterNodeEnd(sql: Sql, scope: ActionScope, end: NodeEnd, nodeOutput: unknown, options: ActionOptions): Promise<StepResult> {
  const ends: NodeEnd[] = [end];
  if (!options.solo) return { ends, finished: false };

  const state = fromWire(options.wire);
  state.status.set(scope.node.id, end.status);
  const { scope: exprScope, outputs } = buildScope(scope.run, scope.runId, scope.nodeRuns, { [scope.node.id]: nodeOutput });
  const decided = await decideBuiltIns(scope.graph, state, exprScope, outputs);
  ends.push(...decided.ends);
  const rows = [...rowsForSkipped(scope.graph, decided.skipped), ...decided.writes];
  const queries = writeNodeRuns(sql, scope.runId, rows);
  if (queries.length) await sql.transaction(queries);

  if (decided.readyActions.length === 0) {
    await finishRun(sql, scope.runId, "succeeded");
    return { ends, finished: true };
  }
  return { ends, finished: false };
}

/** What happens before a node is called, the same for sync and async: done already, pinned, or what to call it with. */
type Prepared =
  | { kind: "end"; end: NodeEnd; nodeOutput: unknown }
  | { kind: "failure"; failure: StoredError }
  | { kind: "ended"; result: StepResult }
  | { kind: "call"; version: VersionRow; input: Record<string, unknown> };

async function prepareCall(sql: Sql, scope: ActionScope): Promise<Prepared> {
  const { runId, run, nodeRuns, node, label, skipRows } = scope;
  const nodeId = node.id;
  const existing = nodeRuns.find((row) => row.node_id === nodeId);
  if (existing && (existing.status === "succeeded" || existing.status === "warning")) {
    // The step ran again after its work was done but before Upstash stored the result.
    return { kind: "end", end: { id: nodeId, status: existing.status as NodeStatus }, nodeOutput: existing.output };
  }
  if (existing?.status === "failed") return { kind: "failure", failure: existing.error ?? { code: "NODE_FAILED", message: "The node run failed." } };
  if (!run.test?.nodeId && run.test && run.pins?.[nodeId] !== undefined) {
    // A test of the whole draft does not call a pinned node: the pinned output stands in for it.
    const pinned = run.pins[nodeId]?.output ?? null;
    const { scope: exprScope } = buildScope(run, runId, nodeRuns);
    let input: Record<string, unknown> | null = null;
    try {
      input = await resolveInputs(node.data.inputs, exprScope);
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
    return { kind: "end", end: { id: nodeId, status: "succeeded" }, nodeOutput: pinned };
  }
  const version = await loadVersion(sql, node);
  if (!version) {
    const failure = { code: "NODE_VERSION_NOT_FOUND", message: `Node "${nodeId}" uses ${node.data.nodeKey}@${node.data.version}, which is not registered.` };
    await sql.transaction([...skipRows, ...writeNodeRuns(sql, runId, [{ id: nodeId, type: "action", label, status: "failed", error: failure }])]);
    await failRunAt(sql, runId, null, failure);
    return { kind: "ended", result: { ends: [{ id: nodeId, status: "failed" }], finished: true, failed: failure } };
  }
  const { scope: exprScope } = buildScope(run, runId, nodeRuns);
  try {
    return { kind: "call", version, input: await resolveInputs(node.data.inputs, exprScope) };
  } catch (error) {
    if (!(error instanceof ExpressionError)) throw error;
    const failure = { code: "EXPRESSION_ERROR", message: `Node "${nodeId}": ${error.message}` };
    await sql.transaction([...skipRows, ...writeNodeRuns(sql, runId, [{ id: nodeId, type: "action", label, status: "failed", error: failure }])]);
    return { kind: "ended", result: await finishAfterFailure(sql, runId, node, failure) };
  }
}

/** Ends the step for a `Prepared` that needs no call. */
async function endPrepared(sql: Sql, scope: ActionScope, prepared: Exclude<Prepared, { kind: "call" }>, options: ActionOptions): Promise<StepResult> {
  if (prepared.kind === "ended") return prepared.result;
  if (prepared.kind === "failure") return finishAfterFailure(sql, scope.runId, scope.node, prepared.failure);
  return afterNodeEnd(sql, scope, prepared.end, prepared.nodeOutput, options);
}

function headersFor(runId: string, nodeRunId: string, attempt: number): Record<string, string> {
  return {
    "Content-Type": "application/json",
    "X-XaaS-Run-Id": runId,
    "X-XaaS-Node-Run-Id": nodeRunId,
    "X-XaaS-Attempt": String(attempt),
    // The same for every attempt, so the node can skip work it already did.
    "Idempotency-Key": nodeRunId,
  };
}

function envelopeFor(scope: ActionScope, version: VersionRow, input: Record<string, unknown>): Record<string, unknown> {
  const { run, runId, label } = scope;
  return {
    input,
    // Secrets, variables and connections never travel here: the node reads what is mapped to it through the resource broker.
    context: {},
    run: { id: runId, no: run.no, workspace: run.workspace_id, workflow: run.workflow_slug, version: run.workflow_version, env: run.env },
    node: { key: version.key, version: version.version, label },
  };
}

/**
 * Runs one sync action node; the body of the `node:<id>` step. A failed attempt that may be tried again
 * throws, so Upstash retries the step. A final failure ends the run and is returned, not thrown.
 */
export async function executeAction(sql: Sql, runId: string, nodeId: string, callNode: CallNode, options: ActionOptions): Promise<StepResult> {
  const begun = await beginAction(sql, runId, nodeId, options.skips);
  if ("ended" in begun) return begun.ended;
  const { scope } = begun;
  const { node, label } = scope;

  const prepared = await prepareCall(sql, scope);
  if (prepared.kind !== "call") return endPrepared(sql, scope, prepared, options);
  const { version, input } = prepared;

  const maxAttempts = clampToRange(node.data.settings.maxAttempts ?? version.retry_max_attempts, MAX_ATTEMPTS);
  const timeoutMs = clampToRange(node.data.settings.timeout ?? version.timeout_ms, TIMEOUT_MS);
  // Attempt 1 of a cached node also asks for its cache key, with the same token.
  const keyTimeoutMs = cacheKeyTimeoutMs(scope, version, timeoutMs);
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
      select ${uuidv7()}::uuid, nr.id, nr.attempt_count, ${resourceToken.hash}, now() + ${timeoutMs + keyTimeoutMs + RESOURCE_TOKEN_GRACE_MS} * interval '1 millisecond'
      from nr where nr.attempt_count <= ${maxAttempts}::int
      on conflict (node_run_id, attempt) do nothing
    )
    select id, attempt_count from nr`;
  const nodeRunId: string = nodeRun.id;
  const attempt: number = nodeRun.attempt_count;

  if (attempt > maxAttempts) {
    const failure = { code: "ATTEMPTS_EXHAUSTED", message: `The node failed ${maxAttempts} time(s).` };
    await sql`update node_runs set status = 'failed', error = ${json(failure)}::jsonb, attempt_count = ${maxAttempts}, finished_at = now() where id = ${nodeRunId}`;
    return finishAfterFailure(sql, runId, node, failure);
  }

  const headers = headersFor(runId, nodeRunId, attempt);
  // Sent to the node, never stored: `headers` (what `attempts.request` keeps) leaves it out.
  const sentHeaders = { ...headers, "X-XAAS-Resource-Token": resourceToken.token };
  const envelope = envelopeFor(scope, version, input);
  const cached = await lookUpCache(sql, callNode, scope, version, input, nodeRunId, attempt, sentHeaders, timeoutMs);
  if (cached.kind === "hit") {
    const done = await settleCacheHit(sql, scope, nodeRunId, attempt, cached, json({ headers, path: "/cache-key", body: envelope }));
    return afterNodeEnd(sql, scope, done.end, done.nodeOutput, options);
  }
  const outcome = await callNode({ scriptName: version.script_name, transport: version.transport, timeoutMs, headers: sentHeaders, body: JSON.stringify(envelope) });
  const verdict = judgeNodeOutcome(outcome);
  const stored = { request: json({ headers, body: envelope }), response: json(capped(outcome.kind === "response" ? { status: outcome.status, body: outcome.body } : { kind: outcome.kind })) };

  const settled = await settleAttempt(sql, scope, nodeRunId, attempt, maxAttempts, verdict, stored);
  if (settled.kind === "retry") throw new NodeAttemptError(verdict.ok ? "retry" : `${verdict.error.code}: ${verdict.error.message}`);
  if (settled.kind === "ended") return settled.result;
  return afterNodeEnd(sql, scope, settled.end, settled.nodeOutput, options);
}

// ---------------------------------------------------------------------------
// Async action nodes (callback or polling)
// ---------------------------------------------------------------------------

/** What the steps of an async node need besides the database: the secrets of callbacks and run tokens. */
export type AsyncEnv = { ingressUrl: string; callbackSigningKey: string; resourceTokenSecret: string };

/** Result of a step of an async node, read by the function body (orchestrate). */
export type AsyncStep =
  | { kind: "ended"; result: StepResult }
  /** The node took the job (202); wait for its callback or poll it. */
  | { kind: "accepted"; nodeRunId: string }
  /** The attempt failed and another may follow, after `delayMs`. */
  | { kind: "retry"; delayMs: number }
  /** Polling: the job still runs. `errors`: failed status calls in a row so far. */
  | { kind: "running"; errors: number; retryAfterMs?: number };

type AttemptRow = {
  id: string;
  node_run_id: string;
  accepted_at: Date | null;
  job_id: string | null;
  status_url: string | null;
  callback_at: Date | null;
  callback: { body: unknown; headers: Record<string, string> } | null;
  finished_at: Date | null;
  status: string;
};

async function readAttempt(sql: Sql, runId: string, nodeId: string, attempt: number): Promise<AttemptRow | null> {
  const [row] = await sql`
    select a.id, a.node_run_id, a.accepted_at, a.job_id, a.status_url, a.callback_at, a.callback, a.finished_at, a.status
    from attempts a join node_runs nr on nr.id = a.node_run_id
    where nr.run_id = ${runId} and nr.node_id = ${nodeId} and a.attempt = ${attempt}`;
  return (row as unknown as AttemptRow) ?? null;
}

/** Turns a settled attempt into the answer of an async step. */
async function asyncAnswer(sql: Sql, scope: ActionScope, settled: Settled, plan: AsyncPlan, attempt: number, options: ActionOptions): Promise<AsyncStep> {
  if (settled.kind === "retry") return { kind: "retry", delayMs: retryDelayMs(plan.backoff, plan.baseDelayMs, attempt) };
  if (settled.kind === "ended") return { kind: "ended", result: settled.result };
  return { kind: "ended", result: await afterNodeEnd(sql, scope, settled.end, settled.nodeOutput, options) };
}

/** An async step that ran again after its attempt was recorded: answer from what the database holds. */
async function replayedAttempt(sql: Sql, scope: ActionScope, row: AttemptRow, plan: AsyncPlan, attempt: number, options: ActionOptions): Promise<AsyncStep | null> {
  if (!row.finished_at) return null;
  const prepared = await prepareCall(sql, scope);
  if (prepared.kind !== "call") return { kind: "ended", result: await endPrepared(sql, scope, prepared, options) };
  // The attempt failed and the node run goes on: the next attempt follows.
  return { kind: "retry", delayMs: retryDelayMs(plan.backoff, plan.baseDelayMs, attempt) };
}

/** Calls the node again within an attempt (`POST /callback`, the status route), with the attempt's own token. */
async function callAgain(callNode: CallNode, env: AsyncEnv, scope: ActionScope, version: VersionRow, row: AttemptRow, attempt: number, plan: AsyncPlan, path: string, method: "GET" | "POST", body?: unknown) {
  const token = await derivedResourceToken(env.resourceTokenSecret, row.id);
  const headers = { ...headersFor(scope.runId, row.node_run_id, attempt), "X-XAAS-Resource-Token": token.token };
  return callNode({ scriptName: version.script_name, transport: version.transport, timeoutMs: plan.timeoutMs, headers, body: body === undefined ? undefined : JSON.stringify(body), path, method });
}

/**
 * First step of attempt `attempt` of an async node (`node:<id>:<attempt>`): sends the job. The node answers 202 (wait for
 * its callback, or poll it), its output right away (as a sync node would), or an error.
 */
export async function startAsyncAttempt(sql: Sql, env: AsyncEnv, runId: string, nodeId: string, attempt: number, plan: AsyncPlan, callNode: CallNode, options: ActionOptions): Promise<AsyncStep> {
  const begun = await beginAction(sql, runId, nodeId, options.skips);
  if ("ended" in begun) return { kind: "ended", result: begun.ended };
  const { scope } = begun;
  const prepared = await prepareCall(sql, scope);
  if (prepared.kind !== "call") return { kind: "ended", result: await endPrepared(sql, scope, prepared, options) };
  const { version, input } = prepared;

  // The token is derived from the attempt id, so a step that runs again makes the same one.
  const newId = uuidv7();
  const newToken = await derivedResourceToken(env.resourceTokenSecret, newId);
  const tokenMs = plan.maxWaitMs + plan.timeoutMs + cacheKeyTimeoutMs(scope, version, plan.timeoutMs) + RESOURCE_TOKEN_GRACE_MS;
  await sql`
    with nr as (
      insert into node_runs (id, run_id, node_id, node_type, label, node_version_id, attempt_count, input)
      values (${uuidv7()}, ${runId}, ${nodeId}, 'action', ${scope.label}, ${version.id}, ${attempt}, ${json(input)}::jsonb)
      on conflict (run_id, node_id) do update set attempt_count = greatest(node_runs.attempt_count, excluded.attempt_count)
      returning id
    )
    insert into attempts (id, node_run_id, attempt, resource_token_hash, resource_token_expires_at)
    select ${newId}::uuid, nr.id, ${attempt}, ${newToken.hash}, now() + ${tokenMs} * interval '1 millisecond' from nr
    on conflict (node_run_id, attempt) do nothing`;
  const row = await readAttempt(sql, runId, nodeId, attempt);
  if (!row) throw new Error("The attempt was just written.");
  if (row.accepted_at) return { kind: "accepted", nodeRunId: row.node_run_id };
  const replayed = await replayedAttempt(sql, scope, row, plan, attempt, options);
  if (replayed) return replayed;

  const token = row.id === newId ? newToken : await derivedResourceToken(env.resourceTokenSecret, row.id);
  const headers = headersFor(runId, row.node_run_id, attempt);
  const cached = await lookUpCache(sql, callNode, scope, version, input, row.node_run_id, attempt, { ...headers, "X-XAAS-Resource-Token": token.token }, plan.timeoutMs);
  if (cached.kind === "hit") {
    const done = await settleCacheHit(sql, scope, row.node_run_id, attempt, cached, json({ headers, path: "/cache-key", body: envelopeFor(scope, version, input) }));
    return { kind: "ended", result: await afterNodeEnd(sql, scope, done.end, done.nodeOutput, options) };
  }
  const body = envelopeFor(scope, version, input);
  if (plan.completion === "callback") {
    const exp = Math.floor((Date.now() + plan.maxWaitMs) / 1000) + CALLBACK_URL_GRACE_S;
    body.callback_url = await callbackUrl(env.ingressUrl, env.callbackSigningKey, row.node_run_id, attempt, exp);
  }
  const outcome = await callNode({
    scriptName: version.script_name,
    transport: version.transport,
    timeoutMs: plan.timeoutMs,
    headers: { ...headers, "X-XAAS-Resource-Token": token.token },
    body: JSON.stringify(body),
  });
  // What the attempt keeps: the callback URL works as a credential, so only its path.
  const keptBody = typeof body.callback_url === "string" ? { ...body, callback_url: body.callback_url.split("?")[0] } : body;
  const stored = { request: json({ headers, body: keptBody }), response: json(capped(outcome.kind === "response" ? { status: outcome.status, body: outcome.body } : { kind: outcome.kind })) };

  if (outcome.kind === "response" && outcome.status === 202) {
    const accepted = isRecord(outcome.body) ? outcome.body : {};
    const jobId = typeof accepted.job_id === "string" ? accepted.job_id.slice(0, 500) : null;
    const statusUrl = typeof accepted.status_url === "string" ? accepted.status_url : null;
    if (plan.completion === "polling" && !isNodePath(statusUrl)) {
      const verdict: NodeVerdict = {
        ok: false,
        retryable: false,
        httpStatus: 202,
        error: { code: "INVALID_ACCEPT", message: 'A polling node must answer 202 with a "status_url" path of its own (starting with "/").' },
      };
      return asyncAnswer(sql, scope, await settleAttempt(sql, scope, row.node_run_id, attempt, plan.maxAttempts, verdict, stored), plan, attempt, options);
    }
    const waiting = plan.completion === "callback" ? "waiting for its callback" : "polling it";
    await sql.transaction([
      ...scope.skipRows,
      sql`update attempts set http_status = 202, request = ${stored.request}::jsonb, response = ${stored.response}::jsonb, accepted_at = now(), job_id = ${jobId}, status_url = ${statusUrl}
          where id = ${row.id}`,
      sql`insert into run_logs (id, run_id, node_run_id, level, message) values (${uuidv7()}, ${runId}, ${row.node_run_id}, 'info', ${`Attempt ${attempt} accepted (202), ${waiting}`})`,
    ]);
    return { kind: "accepted", nodeRunId: row.node_run_id };
  }
  const settled = await settleAttempt(sql, scope, row.node_run_id, attempt, plan.maxAttempts, judgeNodeOutcome(outcome), stored);
  return asyncAnswer(sql, scope, settled, plan, attempt, options);
}

/**
 * After the wait for a callback (`done:<id>:<attempt>`): reads what the ingress recorded (through the node's
 * `POST /callback` when it reads its own callbacks), or the timeout, and ends the attempt.
 */
export async function completeAsyncAttempt(
  sql: Sql,
  env: AsyncEnv,
  runId: string,
  nodeId: string,
  attempt: number,
  plan: AsyncPlan,
  callNode: CallNode,
  timedOut: boolean,
  options: ActionOptions,
): Promise<AsyncStep> {
  const begun = await beginAction(sql, runId, nodeId, []);
  if ("ended" in begun) return { kind: "ended", result: begun.ended };
  const { scope } = begun;
  const row = await readAttempt(sql, runId, nodeId, attempt);
  if (!row) throw new Error(`Attempt ${attempt} of "${nodeId}" is missing.`);
  const replayed = await replayedAttempt(sql, scope, row, plan, attempt, options);
  if (replayed) return replayed;

  let verdict: NodeVerdict;
  if (!row.callback_at || !row.callback) {
    verdict = timedOut
      ? { ok: false, retryable: true, httpStatus: null, error: { code: "TIMEOUT", message: `No callback came within ${Math.round(plan.maxWaitMs / 1000)} s.` } }
      : { ok: false, retryable: true, httpStatus: null, error: { code: "CALLBACK_MISSING", message: "The run was woken without a recorded callback." } };
  } else if (plan.callbackHandler) {
    const version = await loadVersion(sql, scope.node);
    if (!version) throw new Error("The node version was there when the attempt started.");
    const [nodeRun] = await sql`select input from node_runs where id = ${row.node_run_id}`;
    const outcome = await callAgain(callNode, env, scope, version, row, attempt, plan, "/callback", "POST", {
      job_id: row.job_id,
      payload: row.callback.body,
      headers: row.callback.headers,
      input: nodeRun?.input ?? {},
    });
    verdict = judgeNodeOutcome(outcome);
  } else {
    verdict = judgeCallbackBody(row.callback.body);
  }
  const settled = await settleAttempt(sql, scope, row.node_run_id, attempt, plan.maxAttempts, verdict, {});
  return asyncAnswer(sql, scope, settled, plan, attempt, options);
}

/** One poll of a polling node (`check:<id>:<attempt>:<poll>`): asks its status route and ends the attempt once the job is done. */
export async function pollAsyncAttempt(
  sql: Sql,
  env: AsyncEnv,
  runId: string,
  nodeId: string,
  attempt: number,
  poll: number,
  errors: number,
  plan: AsyncPlan,
  callNode: CallNode,
  options: ActionOptions,
): Promise<AsyncStep> {
  const begun = await beginAction(sql, runId, nodeId, []);
  if ("ended" in begun) return { kind: "ended", result: begun.ended };
  const { scope } = begun;
  const row = await readAttempt(sql, runId, nodeId, attempt);
  if (!row || !row.status_url || !row.accepted_at) throw new Error(`Attempt ${attempt} of "${nodeId}" was not accepted for polling.`);
  const replayed = await replayedAttempt(sql, scope, row, plan, attempt, options);
  if (replayed) return replayed;
  const maxPolls = plan.poll?.maxPolls ?? 1;
  const settle = async (verdict: NodeVerdict) =>
    asyncAnswer(sql, scope, await settleAttempt(sql, scope, row.node_run_id, attempt, plan.maxAttempts, verdict, {}), plan, attempt, options);
  const timeout = (message: string): NodeVerdict => ({ ok: false, retryable: true, httpStatus: null, error: { code: "TIMEOUT", message } });

  const version = await loadVersion(sql, scope.node);
  if (!version) throw new Error("The node version was there when the attempt started.");
  const outcome = await callAgain(callNode, env, scope, version, row, attempt, plan, row.status_url, "GET");
  await sql`update attempts set poll_count = poll_count + 1, last_polled_at = now() where id = ${row.id}`;
  const judged = judgePollOutcome(outcome);
  // The node is asked even after its max wait: the branches of a wave move together, so a poll can come late
  // because a sibling was waiting; a job that ended in time still counts. Only a job still running is a TIMEOUT.
  if (judged.kind === "ended") return settle(judged.verdict);
  if (new Date(row.accepted_at).getTime() + plan.maxWaitMs <= Date.now()) return settle(timeout(`The job did not end within ${Math.round(plan.maxWaitMs / 1000)} s.`));
  if (judged.kind === "unreachable" && errors + 1 >= POLL_ERRORS_IN_A_ROW) {
    return settle({ ok: false, retryable: true, httpStatus: null, error: { code: "POLL_FAILED", message: `The status route failed ${errors + 1} times in a row: ${judged.error.message}` } });
  }
  if (poll >= maxPolls) return settle(timeout(`The job did not end within ${maxPolls} polls.`));
  if (judged.kind === "unreachable") return { kind: "running", errors: errors + 1 };
  return { kind: "running", errors: 0, ...(judged.retryAfterMs !== undefined ? { retryAfterMs: judged.retryAfterMs } : {}) };
}

/** A path of the node itself, never a URL elsewhere: the runner only calls it through the node's binding. */
function isNodePath(value: string | null): value is string {
  return typeof value === "string" && value.length <= 500 && value.startsWith("/") && !value.startsWith("//") && !/[\s\\]/.test(value);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/** A node failed for good: the run fails with it. */
async function finishAfterFailure(sql: Sql, runId: string, node: ActionNode, error: StoredError, nodeRunId?: string): Promise<StepResult> {
  if (node.data.settings.onError === "continue") {
    // An expression error or similar before the node ran: continue with a null output.
    await sql.transaction(writeNodeRuns(sql, runId, [{ id: node.id, type: "action", label: node.data.label ?? node.id, status: "warning", output: null, error }]));
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
