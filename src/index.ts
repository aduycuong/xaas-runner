import { neon } from "@neondatabase/serverless";
import { serve, type WorkflowBindings } from "@upstash/workflow/hono";
import { Hono } from "hono";
import { executeAction, failRun, finishWithSkips, loadRun, runBuiltIns, type CallNode } from "./engine";
import { bindingName } from "./logic";
import { orchestrate } from "./orchestrate";
import { createRealtime, runEvents, type RtdbEnv } from "./rtdb";

/** Secrets and vars. The service bindings to node Workers are read by name in `createCallNode`. */
type Bindings = WorkflowBindings & RtdbEnv & {
  DATABASE_URL: string;
  /** Bearer token of `GET /nodes/:script/health`. The route answers 503 while it is not set. */
  RUNNER_ADMIN_TOKEN?: string;
};

type NodeFetcher = { fetch: (input: string, init?: RequestInit) => Promise<Response> };

/**
 * The one place that decides how a node Worker is reached. Moving to a dispatch namespace
 * (Workers for Platforms) only changes this function.
 */
function createCallNode(env: Record<string, unknown>): CallNode {
  return async (call) => {
    const binding = call.transport === "service_binding" ? env[bindingName(call.scriptName)] : undefined;
    if (!binding || typeof (binding as NodeFetcher).fetch !== "function") {
      return { kind: "unavailable", message: `The runner has no ${call.transport} for node Worker "${call.scriptName}".` };
    }

    let response: Response;
    try {
      // The host is ignored: a service binding always reaches the bound Worker.
      response = await (binding as NodeFetcher).fetch("https://node.internal/", {
        method: "POST",
        headers: call.headers,
        body: call.body,
        signal: AbortSignal.timeout(call.timeoutMs),
      });
    } catch (error) {
      const name = error instanceof Error ? error.name : "";
      if (name === "TimeoutError" || name === "AbortError") return { kind: "timeout" };
      return { kind: "network", message: error instanceof Error ? error.message : "The node could not be reached." };
    }
    return { kind: "response", status: response.status, body: await response.json().catch(() => null) };
  };
}

const app = new Hono<{ Bindings: Bindings; Variables: object }>();

app.get("/health", (c) => c.json({ ok: true }));

/**
 * Health of a node Worker, asked through its binding: a node has no URL of its own. Used by the
 * deploy script to confirm that a node is deployed and bound. Needs the admin bearer token.
 */
app.get("/nodes/:script/health", async (c) => {
  const token = c.env.RUNNER_ADMIN_TOKEN;
  if (!token) return c.json({ error: "admin_token_not_configured" }, 503);
  if (c.req.header("authorization") !== `Bearer ${token}`) return c.json({ error: "unauthorized" }, 401);

  const script = c.req.param("script");
  const binding = (c.env as unknown as Record<string, unknown>)[bindingName(script)] as NodeFetcher | undefined;
  if (!binding || typeof binding.fetch !== "function") return c.json({ error: "node_not_bound", script }, 404);

  try {
    const response = await binding.fetch("https://node.internal/health", { signal: AbortSignal.timeout(10_000) });
    const body: unknown = await response.json().catch(() => null);
    return c.json({ script, status: response.status, health: body }, response.ok ? 200 : 502);
  } catch (error) {
    return c.json({ script, error: error instanceof Error ? error.message : "The node could not be reached." }, 502);
  }
});

/**
 * Runs one workflow run. Upstash calls this route once per step and the function below runs
 * again from the top each time (see `orchestrate`).
 *
 * Step names and their order are a contract with runs in flight. A change to either needs a
 * new route (`/run/v4`), with this one kept until its runs have ended.
 *
 * v3 runs the workflow graph: `load`, then waves of `node:<id>` steps (parallel inside a wave),
 * `flow:<n>` for if / switch / merge that need a step of their own, and `finish` when no node step
 * ended the run.
 */
app.post("/run/v3", async (c, next) => {
  // The Upstash SDK answers an unsigned call with a 500 and a stack trace; refuse it plainly first.
  if (!c.req.header("upstash-signature")) return c.json({ error: "missing_signature" }, 401);
  await next();
});

app.post("/run/v3", (c) => {
  const sql = neon(c.env.DATABASE_URL);
  const callNode = createCallNode(c.env as unknown as Record<string, unknown>);
  // Realtime writes happen in the background; they never slow a step or fail it.
  const rt = createRealtime(c.env, (promise) => c.executionCtx.waitUntil(promise));

  const handler = serve<{ runId: string }, Bindings>(
    async (context) => {
      const { runId } = context.requestPayload;
      const plan = await context.run("load", async () => {
        const loaded = await loadRun(sql, runId);
        if (loaded.ok) runEvents(rt, { runId, ...loaded.info }).started();
        return loaded;
      });
      const events = plan.ok ? runEvents(rt, { runId, ...plan.info }) : null;
      const endRun = (status: "succeeded" | "failed") => events?.runEnded(status);
      const skipped = (ids: string[]) => ids.forEach((id) => events?.nodeEnded(id, "skipped"));
      const ended = (ends: { id: string; status: string }[]) => ends.forEach((end) => events?.nodeEnded(end.id, end.status));

      await orchestrate(plan, {
        step: (name, fn, settings) => context.run(name, fn, settings),
        executeAction: async (nodeId, options) => {
          events?.nodeRunning(nodeId);
          skipped(options.skips);
          const result = await executeAction(sql, runId, nodeId, callNode, options);
          ended(result.ends);
          if (result.failed) endRun("failed");
          else if (result.finished && !result.ends.some((end) => end.status === "cancelled")) endRun("succeeded");
          return result;
        },
        runBuiltIns: async (wire, skips) => {
          skipped(skips);
          const result = await runBuiltIns(sql, runId, wire, skips);
          ended(result.ends);
          return result;
        },
        finish: async (status, skips, error) => {
          skipped(skips);
          await finishWithSkips(sql, runId, status, skips, error);
          endRun(status);
        },
      });
    },
    {
      // Reached only when Upstash runs out of retries, i.e. for failures around the nodes.
      failureFunction: async ({ context, failStatus, failResponse }) => {
        const message = `${failStatus}: ${failResponse}`.slice(0, 500);
        await failRun(sql, context.requestPayload.runId, { code: "ENGINE_FAILURE", message });
      },
    },
  );

  return handler(c);
});

export default app;
