import { Client } from "@upstash/qstash";

type NotifyEnv = { QSTASH_TOKEN?: string; QSTASH_URL?: string; INGRESS_URL?: string };

/**
 * Tells the ingress a run has ended, through QStash (signed, retried): it then starts the next run
 * waiting for the trigger's slot and fires `event` triggers. Best effort; the app's sweep catches
 * a queue this message did not move.
 */
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
