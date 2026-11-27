import { Client } from "@upstash/qstash";

type NotifyEnv = { QSTASH_TOKEN?: string; QSTASH_URL?: string; INGRESS_URL?: string };

/**
 * Tells the ingress a run has ended, through QStash (signed, retried): it then starts the next run
 * waiting for the trigger's slot and fires `event` triggers. Best effort; the app's sweep catches
 * a queue this message did not move.
 */
/** One `run.node` report per run in each window of this length; it carries the state at the end of the window. */
export const RUN_NODE_WINDOW_MS = 5_000;

/**
 * The window a report made at `now` falls in, and how long QStash waits before delivering it (at the window's end).
 * Reports in the same window share one deduplication id, so QStash delivers one. Pure.
 */
export function runNodeSlot(now: number): { slot: number; delaySeconds: number } {
  const slot = Math.floor(now / RUN_NODE_WINDOW_MS) + 1;
  return { slot, delaySeconds: Math.max(1, Math.ceil((slot * RUN_NODE_WINDOW_MS - now) / 1000)) };
}

/**
 * Tells the ingress the run reached another node (or started waiting), for the `run.node` event of its status
 * callbacks: at most one message per run every 5 s; the ingress reads where the run is when it arrives. Best effort.
 */
export async function publishRunNode(env: NotifyEnv, runId: string, now = Date.now()): Promise<void> {
  if (!env.QSTASH_TOKEN || !env.INGRESS_URL) return;
  const { slot, delaySeconds } = runNodeSlot(now);
  try {
    await new Client({ token: env.QSTASH_TOKEN, baseUrl: env.QSTASH_URL || undefined }).publishJSON({
      url: `${env.INGRESS_URL.replace(/\/$/, "")}/internal/events/run-node`,
      body: { runId },
      retries: 2,
      delay: delaySeconds,
      deduplicationId: `run-node-${runId}-${slot}`,
    });
  } catch (error) {
    console.warn("[notify] could not report the node of a run", error instanceof Error ? error.message : error);
  }
}

export async function publishRunFinished(env: NotifyEnv, runId: string): Promise<void> {
  if (!env.QSTASH_TOKEN || !env.INGRESS_URL) return;
  try {
    await new Client({ token: env.QSTASH_TOKEN, baseUrl: env.QSTASH_URL || undefined }).publishJSON({
      url: `${env.INGRESS_URL.replace(/\/$/, "")}/internal/events/run-finished`,
      body: { runId },
      retries: 3,
    });
  } catch (error) {
    console.warn("[notify] could not announce the end of a run", error instanceof Error ? error.message : error);
  }
}
