// Pure helpers of the runner: no I/O, covered by logic.test.ts.

import type { StoredError } from "./graph";
export type { StoredError };

// ---------------------------------------------------------------------------
// Calling a node Worker
// ---------------------------------------------------------------------------

/** What came back from a node Worker, before it is judged. */
export type NodeOutcome =
  | { kind: "response"; status: number; body: unknown }
  | { kind: "timeout" }
  | { kind: "network"; message: string }
  /** The node cannot be reached at all, e.g. its binding is missing. Retrying will not help. */
  | { kind: "unavailable"; message: string };

export type NodeVerdict =
  | { ok: true; output: Record<string, unknown>; httpStatus: number }
  | { ok: false; retryable: boolean; error: StoredError; httpStatus: number | null };

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/**
 * Applies the XaaS node protocol to what a node returned.
 * Retryable: 5xx, 429, timeout, network error, or an error body with `retryable: true`.
 */
export function judgeNodeOutcome(outcome: NodeOutcome): NodeVerdict {
  if (outcome.kind === "timeout") {
    return { ok: false, retryable: true, httpStatus: null, error: { code: "TIMEOUT", message: "The node did not answer in time." } };
  }
  if (outcome.kind === "network") {
    return { ok: false, retryable: true, httpStatus: null, error: { code: "NETWORK", message: outcome.message } };
  }
  if (outcome.kind === "unavailable") {
    return { ok: false, retryable: false, httpStatus: null, error: { code: "NODE_UNAVAILABLE", message: outcome.message } };
  }

  const { status, body } = outcome;
  if (status >= 200 && status < 300) {
    if (isRecord(body) && isRecord(body.output)) return { ok: true, output: body.output, httpStatus: status };
    return {
      ok: false,
      retryable: false,
      httpStatus: status,
      error: { code: "INVALID_OUTPUT", message: 'The node answered without an "output" object.' },
    };
  }

  const reported = isRecord(body) && isRecord(body.error) ? body.error : {};
  return {
    ok: false,
    retryable: status >= 500 || status === 429 || reported.retryable === true,
    httpStatus: status,
    error: {
      code: typeof reported.code === "string" ? reported.code : `HTTP_${status}`,
      message: typeof reported.message === "string" ? reported.message : `The node answered ${status}.`,
    },
  };
}

/**
 * Name of the runner's service binding for a node Worker: `node-core-echo-v1-0-0` gives
 * `NODE_CORE_ECHO_V1_0_0`. wrangler.jsonc must declare the binding with this name.
 */
export function bindingName(scriptName: string): string {
  return scriptName.toUpperCase().replace(/[^A-Z0-9]/g, "_");
}

/** Upstash `retryDelay` expression in milliseconds; `retried` is the number of retries so far. */
export function retryDelay(backoff: string, baseDelayMs: number): string {
  if (backoff === "linear") return `${baseDelayMs} * (1 + retried)`;
  if (backoff === "exponential") return `${baseDelayMs} * pow(2, retried)`;
  return String(baseDelayMs);
}

/** UUIDv7: ids sort by creation time. */
export function uuidv7(now: number = Date.now()): string {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  bytes[0] = Math.floor(now / 0x10000000000) & 0xff;
  bytes[1] = Math.floor(now / 0x100000000) & 0xff;
  bytes[2] = Math.floor(now / 0x1000000) & 0xff;
  bytes[3] = Math.floor(now / 0x10000) & 0xff;
  bytes[4] = Math.floor(now / 0x100) & 0xff;
  bytes[5] = now & 0xff;
  bytes[6] = (bytes[6] & 0x0f) | 0x70;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
