// Node `core.emit_event` (milestone 20): a node the runner does itself (transport `runner`), with no Worker. It hands the
// event to the ingress (`POST /internal/events/emit`, signed with CALLBACK_SIGNING_KEY), which starts every `custom`
// trigger of the workspace listening to the name in the run's environment and answers with what each one did; that
// answer is the node's output. Its manifest and docs: `runner/nodes/core-emit-event/`.
//
// The ingress reads the workspace, the environment and the chain of runs from the database by `runId` and `nodeRunId`:
// only the name and the payload come from the workflow. `nodeRunId` is also the id of the emit, the same for every
// attempt, so a retry does not start a trigger's run twice.

import type { NodeCall } from "./engine";
import type { NodeOutcome } from "./logic";

/** Names of the nodes the runner does itself (`node_versions.script_name` with transport `runner`). */
export const RUNNER_NODES = new Set(["runner-core-emit-event-v1-0-0"]);

/** Largest payload, as JSON: each run it starts keeps it in `runs.trigger_data`. */
export const MAX_PAYLOAD_BYTES = 256 * 1024;

/** What a `custom` trigger's `event_name` may be (the app checks it the same way, `eventNameSchema`). */
const EVENT_NAME = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,99}$/;

const encoder = new TextEncoder();

export type EmitInput = { event: string; payload: Record<string, unknown> };

const isRecord = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === "object" && !Array.isArray(value);

/** The node's input, checked: a valid event name and a JSON object payload (empty when not given) within the size limit. Pure. */
export function readEmitInput(input: unknown): { ok: true; value: EmitInput } | { ok: false; message: string } {
  if (!isRecord(input)) return { ok: false, message: "The input must be an object with event and payload." };
  const event = typeof input.event === "string" ? input.event.trim() : "";
  if (!EVENT_NAME.test(event)) return { ok: false, message: `"event" must be a name of letters, digits and . _ : - (at most 100), e.g. task.created; got ${JSON.stringify(input.event ?? null)}.` };
  const payload = input.payload === undefined || input.payload === null ? {} : input.payload;
  if (!isRecord(payload)) return { ok: false, message: '"payload" must be a JSON object.' };
  const size = encoder.encode(JSON.stringify(payload)).length;
  if (size > MAX_PAYLOAD_BYTES) return { ok: false, message: `"payload" is ${Math.ceil(size / 1024)} KB; at most ${MAX_PAYLOAD_BYTES / 1024} KB.` };
  return { ok: true, value: { event, payload } };
}

function base64Url(bytes: ArrayBuffer): string {
  let text = "";
  for (const byte of new Uint8Array(bytes)) text += String.fromCharCode(byte);
  return btoa(text).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

/** `HMAC-SHA256(key, "<timestamp>.<body>")`, base64url: what the ingress checks (`ingress/src/emit.ts`, same code). */
export async function signEmit(key: string, timestamp: number, body: string): Promise<string> {
  const cryptoKey = await crypto.subtle.importKey("raw", encoder.encode(key), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  return base64Url(await crypto.subtle.sign("HMAC", cryptoKey, encoder.encode(`${timestamp}.${body}`)));
}

const failed = (status: number, code: string, message: string, retryable = false): NodeOutcome => ({ kind: "response", status, body: { error: { code, message, retryable } } });

export type EmitEnv = { INGRESS_URL?: string; CALLBACK_SIGNING_KEY?: string };

/** Runs `core.emit_event` for one attempt: the envelope the runner built (`envelopeFor`) in, a node answer out. */
export async function emitEvent(env: EmitEnv, call: NodeCall, fetcher: typeof fetch = fetch): Promise<NodeOutcome> {
  if (!env.INGRESS_URL || !env.CALLBACK_SIGNING_KEY) return { kind: "unavailable", message: "Emit event needs INGRESS_URL and CALLBACK_SIGNING_KEY on the runner." };
  let envelope: unknown;
  try {
    envelope = JSON.parse(call.body ?? "null");
  } catch {
    envelope = null;
  }
  const runId = isRecord(envelope) && isRecord(envelope.run) && typeof envelope.run.id === "string" ? envelope.run.id : null;
  const nodeRunId = call.headers["X-XaaS-Node-Run-Id"] ?? null;
  if (!runId || !nodeRunId) return failed(400, "INVALID_CALL", "The runner did not say which run and node run emit.");
  const input = readEmitInput(isRecord(envelope) ? envelope.input : null);
  if (!input.ok) return failed(400, "INVALID_INPUT", input.message);

  const body = JSON.stringify({ runId, nodeRunId, event: input.value.event, payload: input.value.payload });
  const timestamp = Math.floor(Date.now() / 1000);
  let response: Response;
  try {
    response = await fetcher(`${env.INGRESS_URL.replace(/\/$/, "")}/internal/events/emit`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Xaas-Timestamp": String(timestamp), "X-Xaas-Signature": await signEmit(env.CALLBACK_SIGNING_KEY, timestamp, body) },
      body,
      signal: AbortSignal.timeout(call.timeoutMs),
    });
  } catch (error) {
    const name = error instanceof Error ? error.name : "";
    if (name === "TimeoutError" || name === "AbortError") return { kind: "timeout" };
    return { kind: "network", message: error instanceof Error ? error.message : "The ingress could not be reached." };
  }
  const answer: unknown = await response.json().catch(() => null);
  if (response.ok && isRecord(answer)) return { kind: "response", status: 200, body: { output: answer } };
  const reported = isRecord(answer) && typeof answer.error === "string" ? answer.error : `HTTP_${response.status}`;
  // The ingress failing (5xx, a lost answer) is worth another attempt; a refusal (4xx) is not.
  return failed(response.ok ? 502 : response.status,`INGRESS_${reported.toUpperCase()}`, `The ingress answered ${response.status} (${reported}).`, response.status >= 500 || response.status === 429);
}
