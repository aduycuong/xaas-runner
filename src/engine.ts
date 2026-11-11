// The body of each Upstash step. All database access of the runner is here, as plain SQL:
// the tables are owned and migrated by the xaas-workflows app.

import type { NeonQueryFunction } from "@neondatabase/serverless";
import {
  definitionSchema,
  judgeNodeOutcome,
  resolveTemplate,
  retryDelay,
  TemplateError,
  uuidv7,
  walkIfNodes,
  type NodeOutcome,
  type Scope,
  type StoredError,
  type WorkerNode,
} from "./logic";

export type Sql = NeonQueryFunction<false, false>;

/** Upstash settings of the step that runs one node. */
export type PlanNode = { retries: number; retryDelay: string };

/**
 * Result of the `load` step. Upstash sends it back on every later request of the run, so it
 * holds only what the code between steps needs.
 */
export type RunPlan =
  | { ok: true; entry: string; nodes: Record<string, PlanNode> }
  | { ok: false; error: StoredError };

/**
 * Result of a `node:<id>` step. Kept small: outputs live in the database. `finished` means the
 * step also ended the run, so no `finish` step follows.
 */
export type NodeStepResult =
  | { status: "succeeded"; next: string | null; finished: boolean }
  | { status: "failed"; error: StoredError; finished: true };

/** What running one node gives back to its step, before the step decides whether the run ended. */
type NodeResult =
  | { status: "succeeded"; next: string | null; output?: unknown }
  | { status: "failed"; error: StoredError };

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

/**
 * Upstash retries on top of a node's own attempts. They cover failures around the node call
 * (database, network); `executeNode` still caps real calls at the node's max attempts.
 */
const INFRA_RETRIES = 2;
const BUILT_IN_STEP: PlanNode = { retries: 3, retryDelay: "1000 * (1 + retried)" };

/**
 * First step of a run: marks it running and returns what the runner needs between steps. The run
 * is marked in the same statement that reads the definition, to save a round trip.
 */
export async function loadRunPlan(sql: Sql, runId: string): Promise<RunPlan> {
  const [run] = await sql`
    with started as (
      update runs set status = 'running', started_at = coalesce(started_at, now())
      where id = ${runId} and status = 'queued'
    )
    select wv.definition
    from runs r join workflow_versions wv on wv.id = r.workflow_version_id
    where r.id = ${runId}`;
  if (!run) return { ok: false, error: { code: "RUN_NOT_FOUND", message: `Run ${runId} does not exist.` } };

  const parsed = definitionSchema.safeParse(run.definition);
  if (!parsed.success) {
    const message = parsed.error.issues[0]?.message ?? "unknown error";
    return { ok: false, error: { code: "INVALID_DEFINITION", message: `Invalid workflow definition: ${message}` } };
  }
  const definition = parsed.data;

  const versionIds = [...new Set(definition.nodes.flatMap((node) => (node.type === "worker" ? [node.nodeVersionId] : [])))];
  const versions = versionIds.length
    ? await sql`
        select id, retry_max_attempts, retry_backoff, retry_base_delay_ms
        from node_versions where id = any(${versionIds}::uuid[])`
    : [];
  const versionById = new Map(versions.map((version) => [version.id as string, version]));

  const nodes: Record<string, PlanNode> = {};
  for (const node of definition.nodes) {
    if (node.type !== "worker") {
      nodes[node.id] = BUILT_IN_STEP;
      continue;
    }
    const version = versionById.get(node.nodeVersionId);
    if (!version) {
      return {
        ok: false,
        error: { code: "NODE_VERSION_NOT_FOUND", message: `Node "${node.id}" uses a node version that does not exist.` },
      };
    }
    nodes[node.id] = {
      retries: Math.max(0, version.retry_max_attempts - 1) + INFRA_RETRIES,
      retryDelay: retryDelay(version.retry_backoff, version.retry_base_delay_ms),
    };
  }

  return { ok: true, entry: definition.entry, nodes };
}

/** Records a node run that ended without a usable attempt, and returns the result for it. */
async function failNodeRun(
  sql: Sql,
  runId: string,
  nodeId: string,
  nodeVersionId: string | null,
  error: StoredError,
): Promise<NodeResult> {
  await sql`
    insert into node_runs (id, run_id, node_id, node_version_id, status, error, finished_at)
    values (${uuidv7()}, ${runId}, ${nodeId}, ${nodeVersionId}, 'failed', ${JSON.stringify(error)}::jsonb, now())
    on conflict (run_id, node_id) do update
      set status = 'failed', error = excluded.error, finished_at = now()`;
  return { status: "failed", error };
}

/** Everything a step reads in one query: the run, this node's earlier run and its node version. */
type StepRow = {
  input: unknown;
  context: Record<string, unknown>;
  env: string;
  workflow_slug: string;
  workflow_version: number;
  definition: unknown;
  node_run_status: string | null;
  node_run_error: StoredError | null;
  node_run_output: unknown;
  nv_id: string | null;
  node_key: string | null;
  nv_version: string | null;
  script_name: string | null;
  transport: string | null;
  timeout_ms: number | null;
  retry_max_attempts: number | null;
};

async function executeWorkerNode(
  sql: Sql,
  runId: string,
  node: WorkerNode,
  row: StepRow,
  scope: Scope,
  callNode: CallNode,
): Promise<NodeResult> {
  // The step may run again after its work was done but before Upstash stored the result.
  if (row.node_run_status === "succeeded") return { status: "succeeded", next: node.next, output: row.node_run_output };
  if (row.node_run_status === "failed") {
    return { status: "failed", error: row.node_run_error ?? { code: "NODE_FAILED", message: "The node run failed." } };
  }

  if (!row.nv_id) {
    return failNodeRun(sql, runId, node.id, null, {
      code: "NODE_VERSION_NOT_FOUND",
      message: `Node "${node.id}" uses a node version that does not exist.`,
    });
  }
  const maxAttempts = row.retry_max_attempts ?? 1;

  let input: unknown;
  try {
    input = resolveTemplate(node.input, scope);
  } catch (error) {
    if (!(error instanceof TemplateError)) throw error;
    return failNodeRun(sql, runId, node.id, row.nv_id, { code: "TEMPLATE_ERROR", message: error.message });
  }

  // One statement: counts the attempt and, while the node has attempts left, records it.
  const [nodeRun] = await sql`
    with nr as (
      insert into node_runs (id, run_id, node_id, node_version_id, attempt_count, input)
      values (${uuidv7()}, ${runId}, ${node.id}, ${row.nv_id}, 1, ${JSON.stringify(input)}::jsonb)
      on conflict (run_id, node_id) do update set attempt_count = node_runs.attempt_count + 1
      returning id, attempt_count
    ), at as (
      insert into attempts (id, node_run_id, attempt)
      select ${uuidv7()}::uuid, nr.id, nr.attempt_count from nr where nr.attempt_count <= ${maxAttempts}::int
      on conflict (node_run_id, attempt) do nothing
    )
    select id, attempt_count from nr`;
  const nodeRunId: string = nodeRun.id;
  const attempt: number = nodeRun.attempt_count;

  if (attempt > maxAttempts) {
    // Upstash retried for a reason of its own after the node had used all its attempts.
    const error = { code: "ATTEMPTS_EXHAUSTED", message: `The node failed ${maxAttempts} time(s).` };
    await sql`
      update node_runs set status = 'failed', error = ${JSON.stringify(error)}::jsonb,
        attempt_count = ${maxAttempts}, finished_at = now()
      where id = ${nodeRunId}`;
    return { status: "failed", error };
  }

  const outcome = await callNode({
    scriptName: row.script_name as string,
    transport: row.transport as string,
    timeoutMs: row.timeout_ms as number,
    headers: {
      "Content-Type": "application/json",
      "X-XaaS-Run-Id": runId,
      "X-XaaS-Node-Run-Id": nodeRunId,
      "X-XaaS-Attempt": String(attempt),
      // The same for every attempt, so the node can skip work it already did.
      "Idempotency-Key": nodeRunId,
    },
    body: JSON.stringify({
      input,
      context: { vars: {} },
      run: { id: runId, workflow: row.workflow_slug, version: row.workflow_version, env: row.env },
      node: { key: row.node_key, version: row.nv_version, label: node.label ?? node.id },
    }),
  });
  const verdict = judgeNodeOutcome(outcome);

  if (verdict.ok) {
    const output = JSON.stringify(verdict.output);
    await sql.transaction([
      sql`update attempts set status = 'succeeded', http_status = ${verdict.httpStatus}, finished_at = now()
          where node_run_id = ${nodeRunId} and attempt = ${attempt}`,
      sql`update node_runs set status = 'succeeded', output = ${output}::jsonb, finished_at = now()
          where id = ${nodeRunId}`,
      sql`update runs set context = context || ${JSON.stringify({ [node.id]: verdict.output })}::jsonb
          where id = ${runId}`,
    ]);
    return { status: "succeeded", next: node.next, output: verdict.output };
  }

  const error = JSON.stringify(verdict.error);
  const failAttempt = sql`
    update attempts set status = 'failed', http_status = ${verdict.httpStatus}, error = ${error}::jsonb, finished_at = now()
    where node_run_id = ${nodeRunId} and attempt = ${attempt}`;

  if (verdict.retryable && attempt < maxAttempts) {
    await failAttempt;
    throw new NodeAttemptError(`${verdict.error.code}: ${verdict.error.message}`);
  }

  await sql.transaction([
    failAttempt,
    sql`update node_runs set status = 'failed', error = ${error}::jsonb, finished_at = now() where id = ${nodeRunId}`,
  ]);
  return { status: "failed", error: verdict.error };
}

/**
 * Runs one node of a run; the body of the `node:<id>` step. A failed attempt that may be tried
 * again throws, so Upstash retries the step. A final failure is returned, not thrown.
 *
 * Each Upstash step is a round trip through QStash, which costs far more than a query, so the
 * step also decides the `flow.if` nodes that follow the node and ends the run when nothing
 * follows or the node failed. That step is then the last one: no `finish` step is needed.
 */
export async function executeNode(sql: Sql, runId: string, nodeId: string, callNode: CallNode): Promise<NodeStepResult> {
  const [row] = (await sql`
    select r.input, r.context, r.env, w.slug as workflow_slug, wv.version as workflow_version, wv.definition,
      nr.status as node_run_status, nr.error as node_run_error, nr.output as node_run_output,
      nv.id as nv_id, n.key as node_key, nv.version as nv_version, nv.script_name, nv.transport,
      nv.timeout_ms, nv.retry_max_attempts
    from runs r
      join workflows w on w.id = r.workflow_id
      join workflow_versions wv on wv.id = r.workflow_version_id
      left join node_runs nr on nr.run_id = r.id and nr.node_id = ${nodeId}
      left join node_versions nv on nv.id = (
        select (e->>'nodeVersionId')::uuid from jsonb_array_elements(wv.definition->'nodes') e
        where e->>'id' = ${nodeId} limit 1)
      left join nodes n on n.id = nv.node_id
    where r.id = ${runId}`) as unknown as StepRow[];
  if (!row) {
    return { status: "failed", finished: true, error: { code: "RUN_NOT_FOUND", message: `Run ${runId} does not exist.` } };
  }

  // The `load` step already checked the definition; a failure here is a bug and is retried.
  const definition = definitionSchema.parse(row.definition);
  const byId = new Map(definition.nodes.map((candidate) => [candidate.id, candidate]));
  const node = byId.get(nodeId);

  let scope: Scope = { input: row.input, context: row.context };
  let result: NodeResult;
  let walkFrom: string | null = null;
  if (!node) {
    result = await failNodeRun(sql, runId, nodeId, null, { code: "NODE_NOT_FOUND", message: `Node "${nodeId}" is not in the workflow.` });
  } else if (node.type === "worker") {
    result = await executeWorkerNode(sql, runId, node, row, scope, callNode);
    if (result.status === "succeeded") {
      scope = { input: row.input, context: { ...row.context, [node.id]: result.output } };
      walkFrom = result.next;
    }
  } else {
    // A `flow.if` that opens the graph (or follows a node that ended the run's previous step).
    result = { status: "succeeded", next: nodeId };
    walkFrom = nodeId;
  }

  if (result.status === "succeeded") {
    const walk = walkIfNodes(byId, walkFrom, scope);
    if (walk.steps.length || walk.failure) await recordIfNodes(sql, runId, walk);
    result = walk.failure ? { status: "failed", error: walk.failure.error } : { status: "succeeded", next: walk.next };
  }

  if (result.status === "failed") {
    await finishRun(sql, runId, "failed", result.error);
    return { status: "failed", error: result.error, finished: true };
  }
  if (result.next === null) {
    await finishRun(sql, runId, "succeeded");
    return { status: "succeeded", next: null, finished: true };
  }
  return { status: "succeeded", next: result.next, finished: false };
}

/** Writes the node runs of the `flow.if` nodes a step decided, in one round trip. */
async function recordIfNodes(sql: Sql, runId: string, walk: ReturnType<typeof walkIfNodes>): Promise<void> {
  const queries = walk.steps.map(
    (step) => sql`
      insert into node_runs (id, run_id, node_id, status, output, finished_at)
      values (${uuidv7()}, ${runId}, ${step.id}, 'succeeded', ${JSON.stringify({ next: step.next })}::jsonb, now())
      on conflict (run_id, node_id) do update
        set status = 'succeeded', output = excluded.output, finished_at = now()`,
  );
  if (walk.failure) {
    queries.push(sql`
      insert into node_runs (id, run_id, node_id, status, error, finished_at)
      values (${uuidv7()}, ${runId}, ${walk.failure.nodeId}, 'failed', ${JSON.stringify(walk.failure.error)}::jsonb, now())
      on conflict (run_id, node_id) do update
        set status = 'failed', error = excluded.error, finished_at = now()`);
  }
  await sql.transaction(queries);
}

/**
 * Stores the final status and output of a run. The output of a run that succeeded is the output
 * of the last node Worker that ran. Does nothing if the run already ended.
 */
export async function finishRun(sql: Sql, runId: string, status: "succeeded" | "failed", error?: StoredError): Promise<void> {
  await sql`
    update runs set status = ${status},
      output = case when ${status}::text = 'succeeded' then coalesce((
        select output from node_runs
        where run_id = ${runId} and status = 'succeeded' and node_version_id is not null
        order by finished_at desc limit 1), 'null'::jsonb) else 'null'::jsonb end,
      error = ${error ? JSON.stringify(error) : null}::jsonb, finished_at = now()
    where id = ${runId} and status in ('queued', 'running')`;
}

/** Ends a run the engine gave up on (Upstash ran out of retries), with any node run still running. */
export async function failRun(sql: Sql, runId: string, error: StoredError): Promise<void> {
  const json = JSON.stringify(error);
  await sql.transaction([
    sql`update runs set status = 'failed', error = ${json}::jsonb, finished_at = now()
        where id = ${runId} and status in ('queued', 'running')`,
    sql`update node_runs set status = 'failed', error = ${json}::jsonb, finished_at = now()
        where run_id = ${runId} and status = 'running'`,
  ]);
}
