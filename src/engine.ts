// The body of each Upstash step. All database access of the runner is here, as plain SQL:
// the tables are owned and migrated by the xaas-workflows app.

import type { NeonQueryFunction } from "@neondatabase/serverless";
import {
  definitionSchema,
  evaluateCondition,
  judgeNodeOutcome,
  resolveTemplate,
  retryDelay,
  TemplateError,
  uuidv7,
  type IfNode,
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

/** Result of a `node:<id>` step. Kept small: outputs live in the database. */
export type NodeStepResult =
  | { status: "succeeded"; next: string | null }
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

/** First step of a run: marks it running and returns what the runner needs between steps. */
export async function loadRunPlan(sql: Sql, runId: string): Promise<RunPlan> {
  const [run] = await sql`
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

  await sql`
    update runs set status = 'running', started_at = coalesce(started_at, now())
    where id = ${runId} and status = 'queued'`;

  return { ok: true, entry: definition.entry, nodes };
}

/** Records a node run that ended without a usable attempt, and returns the step result for it. */
async function failNodeRun(
  sql: Sql,
  runId: string,
  nodeId: string,
  nodeVersionId: string | null,
  error: StoredError,
): Promise<NodeStepResult> {
  await sql`
    insert into node_runs (id, run_id, node_id, node_version_id, status, error, finished_at)
    values (${uuidv7()}, ${runId}, ${nodeId}, ${nodeVersionId}, 'failed', ${JSON.stringify(error)}::jsonb, now())
    on conflict (run_id, node_id) do update
      set status = 'failed', error = excluded.error, finished_at = now()`;
  return { status: "failed", error };
}

async function executeIfNode(sql: Sql, runId: string, node: IfNode, scope: Scope): Promise<NodeStepResult> {
  let next: string | null;
  try {
    const branch = node.branches.find((candidate) => evaluateCondition(candidate.when, scope));
    next = branch ? branch.next : node.defaultNext;
  } catch (error) {
    if (!(error instanceof TemplateError)) throw error;
    return failNodeRun(sql, runId, node.id, null, { code: "TEMPLATE_ERROR", message: error.message });
  }

  await sql`
    insert into node_runs (id, run_id, node_id, status, output, finished_at)
    values (${uuidv7()}, ${runId}, ${node.id}, 'succeeded', ${JSON.stringify({ next })}::jsonb, now())
    on conflict (run_id, node_id) do update
      set status = 'succeeded', output = excluded.output, finished_at = now()`;
  return { status: "succeeded", next };
}

type RunRow = { env: string; workflow_slug: string; workflow_version: number };

async function executeWorkerNode(
  sql: Sql,
  runId: string,
  node: WorkerNode,
  run: RunRow,
  scope: Scope,
  callNode: CallNode,
): Promise<NodeStepResult> {
  // The step may run again after its work was done but before Upstash stored the result.
  const [existing] = await sql`select status, error from node_runs where run_id = ${runId} and node_id = ${node.id}`;
  if (existing?.status === "succeeded") return { status: "succeeded", next: node.next };
  if (existing?.status === "failed") {
    return { status: "failed", error: existing.error ?? { code: "NODE_FAILED", message: "The node run failed." } };
  }

  const [version] = await sql`
    select nv.id, n.key, nv.version, nv.script_name, nv.transport, nv.timeout_ms, nv.retry_max_attempts
    from node_versions nv join nodes n on n.id = nv.node_id
    where nv.id = ${node.nodeVersionId}`;
  if (!version) {
    return failNodeRun(sql, runId, node.id, null, {
      code: "NODE_VERSION_NOT_FOUND",
      message: `Node "${node.id}" uses a node version that does not exist.`,
    });
  }

  let input: unknown;
  try {
    input = resolveTemplate(node.input, scope);
  } catch (error) {
    if (!(error instanceof TemplateError)) throw error;
    return failNodeRun(sql, runId, node.id, version.id, { code: "TEMPLATE_ERROR", message: error.message });
  }

  const [nodeRun] = await sql`
    insert into node_runs (id, run_id, node_id, node_version_id, attempt_count, input)
    values (${uuidv7()}, ${runId}, ${node.id}, ${version.id}, 1, ${JSON.stringify(input)}::jsonb)
    on conflict (run_id, node_id) do update set attempt_count = node_runs.attempt_count + 1
    returning id, attempt_count`;
  const nodeRunId: string = nodeRun.id;
  const attempt: number = nodeRun.attempt_count;
  const maxAttempts: number = version.retry_max_attempts;

  if (attempt > maxAttempts) {
    // Upstash retried for a reason of its own after the node had used all its attempts.
    const error = { code: "ATTEMPTS_EXHAUSTED", message: `The node failed ${maxAttempts} time(s).` };
    await sql`
      update node_runs set status = 'failed', error = ${JSON.stringify(error)}::jsonb,
        attempt_count = ${maxAttempts}, finished_at = now()
      where id = ${nodeRunId}`;
    return { status: "failed", error };
  }

  await sql`
    insert into attempts (id, node_run_id, attempt) values (${uuidv7()}, ${nodeRunId}, ${attempt})
    on conflict (node_run_id, attempt) do nothing`;

  const outcome = await callNode({
    scriptName: version.script_name,
    transport: version.transport,
    timeoutMs: version.timeout_ms,
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
      run: { id: runId, workflow: run.workflow_slug, version: run.workflow_version, env: run.env },
      node: { key: version.key, version: version.version, label: node.label ?? node.id },
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
    return { status: "succeeded", next: node.next };
  }

  const error = JSON.stringify(verdict.error);
  await sql`
    update attempts set status = 'failed', http_status = ${verdict.httpStatus}, error = ${error}::jsonb, finished_at = now()
    where node_run_id = ${nodeRunId} and attempt = ${attempt}`;

  if (verdict.retryable && attempt < maxAttempts) {
    throw new NodeAttemptError(`${verdict.error.code}: ${verdict.error.message}`);
  }

  await sql`update node_runs set status = 'failed', error = ${error}::jsonb, finished_at = now() where id = ${nodeRunId}`;
  return { status: "failed", error: verdict.error };
}

/**
 * Runs one node of a run; the body of the `node:<id>` step. A failed attempt that may be tried
 * again throws, so Upstash retries the step. A final failure is returned, not thrown, so the
 * runner can finish the run itself.
 */
export async function executeNode(sql: Sql, runId: string, nodeId: string, callNode: CallNode): Promise<NodeStepResult> {
  const [run] = await sql`
    select r.input, r.context, r.env, w.slug as workflow_slug, wv.version as workflow_version, wv.definition
    from runs r
      join workflows w on w.id = r.workflow_id
      join workflow_versions wv on wv.id = r.workflow_version_id
    where r.id = ${runId}`;
  if (!run) return { status: "failed", error: { code: "RUN_NOT_FOUND", message: `Run ${runId} does not exist.` } };

  // The `load` step already checked the definition; a failure here is a bug and is retried.
  const node = definitionSchema.parse(run.definition).nodes.find((candidate) => candidate.id === nodeId);
  if (!node) {
    return failNodeRun(sql, runId, nodeId, null, { code: "NODE_NOT_FOUND", message: `Node "${nodeId}" is not in the workflow.` });
  }

  const scope: Scope = { input: run.input, context: run.context };
  if (node.type === "flow.if") return executeIfNode(sql, runId, node, scope);
  return executeWorkerNode(sql, runId, node, run as RunRow, scope, callNode);
}

/**
 * Last step of a run: stores its final status and output. The output of a run that succeeded is
 * the output of the last node Worker that ran. Does nothing if the run already ended.
 */
export async function finishRun(sql: Sql, runId: string, status: "succeeded" | "failed", error?: StoredError): Promise<void> {
  let output: unknown = null;
  if (status === "succeeded") {
    const [last] = await sql`
      select output from node_runs
      where run_id = ${runId} and status = 'succeeded' and node_version_id is not null
      order by finished_at desc limit 1`;
    output = last?.output ?? null;
  }
  await sql`
    update runs set status = ${status}, output = ${JSON.stringify(output)}::jsonb,
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
