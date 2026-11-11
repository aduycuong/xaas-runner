import { neon } from "@neondatabase/serverless";
import { serve, type WorkflowBindings } from "@upstash/workflow/hono";
import { Hono } from "hono";
import { executeNode, failRun, finishRun, loadRunPlan, type CallNode, type NodeStepResult, type PlanNode } from "./engine";
import { bindingName, type StoredError } from "./logic";

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
 * again from the top each time, so everything outside `context.run` must give the same answer
 * on every call: no database reads, no clock, only step results.
 *
 * Step names and their order are a contract with runs in flight. A change to either needs a
 * new route (`/run/v2`), with this one kept until its runs have ended.
 */
app.post("/run/v1", (c) => {
  const sql = neon(c.env.DATABASE_URL);
  const callNode = createCallNode(c.env as unknown as Record<string, unknown>);

  const handler = serve<{ runId: string }, Bindings>(
    async (context) => {
      const { runId } = context.requestPayload;

      const plan = await context.run("load", () => loadRunPlan(sql, runId));

      let status: "succeeded" | "failed" = "succeeded";
      let error: StoredError | undefined;

      if (!plan.ok) {
        status = "failed";
        error = plan.error;
      } else {
        const visited = new Set<string>();
        let cursor: string | null = plan.entry;

        while (cursor) {
          const nodeId: string = cursor;
          const settings: PlanNode | undefined = plan.nodes[nodeId];
          if (!settings || visited.has(nodeId)) {
            // A step name may be used once per run, so a graph that loops cannot go on.
            status = "failed";
            error = { code: "INVALID_GRAPH", message: `Node "${nodeId}" is unknown or was reached twice.` };
            break;
          }
          visited.add(nodeId);

          const result: NodeStepResult = await context.run(
            `node:${nodeId}`,
            () => executeNode(sql, runId, nodeId, callNode),
            settings,
          );
          if (result.status === "failed") {
            status = "failed";
            error = result.error;
            break;
          }
          cursor = result.next;
        }
      }

      await context.run("finish", () => finishRun(sql, runId, status, error));
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
