import { serve, type WorkflowBindings } from "@upstash/workflow/hono";
import { Hono } from "hono";
import { connect, databaseUrl } from "./db";
import {
  completeAsyncAttempt,
  executeAction,
  failRun,
  finishWithSkips,
  loadRun,
  pollAsyncAttempt,
  runBuiltIns,
  startAsyncAttempt,
  type AsyncEnv,
  type AsyncStep,
  type CallNode,
} from "./engine";
import { emitEvent, RUNNER_NODES } from "./emit-event";
import { bindingName } from "./logic";
import { publishRunFinished, publishRunNode } from "./notify";
import { orchestrate } from "./orchestrate";
import { createRealtime, runEvents, type RtdbEnv } from "./rtdb";
import { createTiming } from "./timing";

/** Secrets and vars. The service bindings to node Workers are read by name in `createCallNode`. */
type Bindings = WorkflowBindings & RtdbEnv & {
  /** Public URL of the ingress Worker; the runner tells it when a run ends. Optional. */
  INGRESS_URL?: string;
  /** Direct database URL; not needed when the HYPERDRIVE binding is set. */
  DATABASE_URL?: string;
  /** Optional Hyperdrive in front of the database: warm connections near the Worker. */
  HYPERDRIVE?: Hyperdrive;
  /** Bearer token of `GET /nodes/:script/health`. The route answers 503 while it is not set. */
  RUNNER_ADMIN_TOKEN?: string;
  /** Signs the callback URLs of async nodes; the same value as the ingress (which checks them). */
  CALLBACK_SIGNING_KEY?: string;
  /** Derives the run tokens of async attempts (engine.ts, derivedResourceToken). Runner only. */
  RESOURCE_TOKEN_SECRET?: string;
};

/** What async nodes need; a run that reaches one without it fails with a clear message. */
function asyncEnv(env: Bindings): AsyncEnv {
  if (!env.INGRESS_URL || !env.CALLBACK_SIGNING_KEY || !env.RESOURCE_TOKEN_SECRET) {
    throw new Error("Async nodes need INGRESS_URL, CALLBACK_SIGNING_KEY and RESOURCE_TOKEN_SECRET on the runner.");
  }
  return { ingressUrl: env.INGRESS_URL, callbackSigningKey: env.CALLBACK_SIGNING_KEY, resourceTokenSecret: env.RESOURCE_TOKEN_SECRET };
}

type NodeFetcher = { fetch: (input: string, init?: RequestInit) => Promise<Response> };

/**
 * The one place that decides how a node Worker is reached. Moving to a dispatch namespace
 * (Workers for Platforms) only changes this function. A node of transport `runner` has no Worker:
 * the runner does its work here (`emit-event.ts`).
 */
function createCallNode(env: Record<string, unknown>): CallNode {
  return async (call) => {
    if (call.transport === "runner") {
      if (call.scriptName === "runner-core-emit-event-v1-0-0" && (call.path ?? "/") === "/") return emitEvent(env as Bindings, call);
      return { kind: "unavailable", message: `The runner does not know node "${call.scriptName}".` };
    }
    const binding = call.transport === "service_binding" ? env[bindingName(call.scriptName)] : undefined;
    if (!binding || typeof (binding as NodeFetcher).fetch !== "function") {
      return { kind: "unavailable", message: `The runner has no ${call.transport} for node Worker "${call.scriptName}".` };
    }

    let response: Response;
    try {
      // The host is ignored: a service binding always reaches the bound Worker.
      response = await (binding as NodeFetcher).fetch(`https://node.internal${call.path ?? "/"}`, {
        method: call.method ?? "POST",
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
  // A node the runner does itself is healthy when the runner is.
  if (RUNNER_NODES.has(script)) return c.json({ script, status: 200, health: { ok: true, runner: true } });
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
 * new route (`/run/v5`), with this one kept until its runs have ended.
 *
 * v4 runs the workflow graph: `load`, then waves of node steps (parallel inside a wave), `flow:<n>`
 * for if / switch / merge that need a step of their own, and `finish` when no node step ended the run.
 * A sync node is one step `node:<id>`. An async node runs attempts: `node:<id>:<n>`, then `wait:<id>:<n>`
 * + `done:<id>:<n>` (callback) or `poll:<id>:<n>:<k>` + `check:<id>:<n>:<k>` (polling), and
 * `backoff:<id>:<n>` before a retry. (v3 was the same without async nodes.)
 */
app.post("/run/v4", async (c, next) => {
  // The Upstash SDK answers an unsigned call with a 500 and a stack trace; refuse it plainly first.
  if (!c.req.header("upstash-signature")) return c.json({ error: "missing_signature" }, 401);
  await next();
});

app.post("/run/v4", async (c) => {
  const timing = createTiming();
  const sql = timing.sql(connect(databaseUrl(c.env)));
  const callNode = timing.callNode(createCallNode(c.env as unknown as Record<string, unknown>));
  // Realtime writes happen in the background; they never slow a step or fail it.
  const rt = createRealtime(c.env, (promise) => c.executionCtx.waitUntil(promise));

  const handler = serve<{ runId: string }, Bindings>(
    async (context) => {
      timing.functionStarted();
      const { runId } = context.requestPayload;
      const plan = await context.run("load", () => timing.step("load", async () => {
        const loaded = await loadRun(sql, runId);
        if (loaded.ok) runEvents(rt, { runId, ...loaded.info }).started();
        return loaded;
      }));
      const events = plan.ok ? runEvents(rt, { runId, ...plan.info }) : null;
      const endRun = (status: "succeeded" | "failed") => {
        events?.runEnded(status);
        // A test run starts nothing else: no queue to advance, no event trigger to fire.
        if (!plan.ok || !plan.info.test) c.executionCtx.waitUntil(publishRunFinished(c.env, runId));
      };
      // A node starts (or an async node starts waiting): the status callbacks that follow nodes hear of it, at most every 5 s.
      const nodeStarted = () => {
        if (plan.ok && plan.info.reportsNodes) c.executionCtx.waitUntil(publishRunNode(c.env, runId));
      };
      const skipped = (ids: string[]) => ids.forEach((id) => events?.nodeEnded(id, "skipped"));
      const ended = (ends: { id: string; status: string }[]) => ends.forEach((end) => events?.nodeEnded(end.id, end.status));
      /** Realtime updates once an async node's step ended it. */
      const asyncEnded = (step: AsyncStep): AsyncStep => {
        if (step.kind !== "ended") return step;
        ended(step.result.ends);
        if (step.result.failed) endRun("failed");
        else if (step.result.finished && !step.result.ends.some((end) => end.status === "cancelled")) endRun("succeeded");
        return step;
      };

      await orchestrate(plan, {
        step: (name, fn, settings) => context.run(name, () => timing.step(name, fn), settings),
        executeAction: async (nodeId, options) => {
          events?.nodeRunning(nodeId);
          nodeStarted();
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
        sleep: (name, ms) => context.sleep(name, Math.max(1, Math.ceil(ms / 1000))),
        waitForEvent: async (name, eventId, timeoutMs) => {
          const event = await context.waitForEvent(name, eventId, { timeout: Math.max(1, Math.ceil(timeoutMs / 1000)) });
          return { timeout: event.timeout };
        },
        startAsync: async (nodeId, attempt, asyncPlan, options) => {
          if (attempt === 1) {
            events?.nodeRunning(nodeId);
            nodeStarted();
            skipped(options.skips);
          }
          return asyncEnded(await startAsyncAttempt(sql, asyncEnv(c.env), runId, nodeId, attempt, asyncPlan, callNode, options));
        },
        completeAsync: async (nodeId, attempt, asyncPlan, timedOut, options) =>
          asyncEnded(await completeAsyncAttempt(sql, asyncEnv(c.env), runId, nodeId, attempt, asyncPlan, callNode, timedOut, options)),
        pollAsync: async (nodeId, attempt, poll, errors, asyncPlan, options) =>
          asyncEnded(await pollAsyncAttempt(sql, asyncEnv(c.env), runId, nodeId, attempt, poll, errors, asyncPlan, callNode, options)),
      });
    },
    {
      // Reached only when Upstash runs out of retries, i.e. for failures around the nodes.
      failureFunction: async ({ context, failStatus, failResponse }) => {
        const message = `${failStatus}: ${failResponse}`.slice(0, 500);
        await failRun(sql, context.requestPayload.runId, { code: "ENGINE_FAILURE", message });
        await publishRunFinished(c.env, context.requestPayload.runId);
      },
    },
  );

  let status: number | null = null;
  try {
    const response = await handler(c);
    status = response.status;
    return response;
  } finally {
    const cf = c.req.raw.cf as { colo?: string } | undefined;
    timing.done({ status, colo: cf?.colo ?? null, messageId: c.req.header("upstash-message-id") ?? null, retried: c.req.header("upstash-retried") ?? null });
    // A Worker cannot reuse a socket across requests: close this step's connections once it is answered.
    c.executionCtx.waitUntil(sql.end());
  }
});

export default app;
