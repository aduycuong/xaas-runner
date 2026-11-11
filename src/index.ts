import { neon } from "@neondatabase/serverless";
import { serve, type WorkflowBindings } from "@upstash/workflow/hono";
import { Hono } from "hono";
import { executeAction, failRun, finishWithSkips, loadRun, runBuiltIns, type CallNode } from "./engine";
import { bindingName } from "./logic";
import { orchestrate } from "./orchestrate";

/** Secrets and vars. The service bindings to node Workers are read by name in `createCallNode`. */
type Bindings = WorkflowBindings & {
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

  const handler = serve<{ runId: string }, Bindings>(
    async (context) => {
      const { runId } = context.requestPayload;
      const plan = await context.run("load", () => loadRun(sql, runId));
      await orchestrate(plan, {
        step: (name, fn, settings) => context.run(name, fn, settings),
        executeAction: (nodeId, options) => executeAction(sql, runId, nodeId, callNode, options),
        runBuiltIns: (wire, skips) => runBuiltIns(sql, runId, wire, skips),
        finish: (status, skips, error) => finishWithSkips(sql, runId, status, skips, error),
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
