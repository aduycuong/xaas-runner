// Pure helpers of the runner: no I/O, covered by logic.test.ts.

import type { StoredError } from "./graph";
import { CACHE_FOREVER } from "./limits";
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
  | { ok: true; output: Record<string, unknown>; httpStatus: number | null }
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

function reportedError(value: unknown, fallbackCode: string, retryableByDefault: boolean): { retryable: boolean; error: StoredError } {
  const reported = isRecord(value) ? value : {};
  return {
    retryable: typeof reported.retryable === "boolean" ? reported.retryable : retryableByDefault,
    error: {
      code: typeof reported.code === "string" ? reported.code : fallbackCode,
      message: typeof reported.message === "string" ? reported.message : "The node reported an error.",
    },
  };
}

/** A callback body without a handler on the node: `{ output }` or `{ error }`. A reported error is final unless it says `retryable`. */
export function judgeCallbackBody(body: unknown): NodeVerdict {
  if (isRecord(body) && isRecord(body.output)) return { ok: true, output: body.output, httpStatus: null };
  if (isRecord(body) && isRecord(body.error)) return { ok: false, httpStatus: null, ...reportedError(body.error, "NODE_FAILED", false) };
  return { ok: false, retryable: false, httpStatus: null, error: { code: "INVALID_CALLBACK", message: 'The callback had neither an "output" nor an "error" object.' } };
}

/** What a polling node's status route said. */
export type PollVerdict = { kind: "running"; retryAfterMs?: number } | { kind: "ended"; verdict: NodeVerdict } | { kind: "unreachable"; error: StoredError };

/** Applies the protocol to one answer of a status route: `{ status: "running" | "succeeded" | "failed", … }`. */
export function judgePollOutcome(outcome: NodeOutcome): PollVerdict {
  if (outcome.kind !== "response" || outcome.status < 200 || outcome.status >= 300) {
    const verdict = judgeNodeOutcome(outcome);
    if (!verdict.ok && verdict.retryable) return { kind: "unreachable", error: verdict.error };
    if (!verdict.ok) return { kind: "ended", verdict };
  }
  const body = outcome.kind === "response" ? outcome.body : null;
  const status = isRecord(body) ? body.status : undefined;
  if (status === "running") {
    const retryAfter = isRecord(body) && typeof body.retry_after_ms === "number" && body.retry_after_ms > 0 ? body.retry_after_ms : undefined;
    return { kind: "running", ...(retryAfter !== undefined ? { retryAfterMs: retryAfter } : {}) };
  }
  if (status === "succeeded" && isRecord(body) && isRecord(body.output)) return { kind: "ended", verdict: { ok: true, output: body.output, httpStatus: 200 } };
  if (status === "failed" && isRecord(body)) return { kind: "ended", verdict: { ok: false, httpStatus: 200, ...reportedError(body.error, "NODE_FAILED", false) } };
  return { kind: "ended", verdict: { ok: false, retryable: false, httpStatus: 200, error: { code: "INVALID_STATUS", message: 'The status route answered without a valid "status".' } } };
}

/** What a node answered to `POST /cache-key`: a key for its input, no key (do not cache this input), or a failure. */
export type CacheKeyAnswer = { kind: "key"; key: string } | { kind: "none" } | { kind: "failed"; message: string };

/** Reads the answer to `POST /cache-key`: `200 { key: string }`, or `{ key: null }` for an input the node does not cache. */
export function readCacheKey(outcome: NodeOutcome, maxChars: number): CacheKeyAnswer {
  if (outcome.kind !== "response") return { kind: "failed", message: outcome.kind === "timeout" ? "it did not answer in time" : outcome.message };
  const { status, body } = outcome;
  if (status < 200 || status >= 300) {
    const reported = isRecord(body) && isRecord(body.error) && typeof body.error.message === "string" ? `: ${body.error.message}` : "";
    return { kind: "failed", message: `it answered ${status}${reported}` };
  }
  if (!isRecord(body) || !("key" in body)) return { kind: "failed", message: 'it answered without a "key"' };
  if (body.key === null) return { kind: "none" };
  if (typeof body.key !== "string" || body.key.length === 0) return { kind: "failed", message: '"key" is not a non-empty string' };
  if (body.key.length > maxChars) return { kind: "failed", message: `"key" is longer than ${maxChars} characters` };
  return { kind: "key", key: body.key };
}

/**
 * What a node's `cacheTtl` means: no cache (0, or anything below -1), a number of seconds (cut to `maxSeconds`), or
 * forever (-1: `seconds` is null).
 */
export function cacheWindow(cacheTtl: number, maxSeconds: number): { on: false } | { on: true; seconds: number | null } {
  if (cacheTtl === CACHE_FOREVER) return { on: true, seconds: null };
  if (!(cacheTtl > 0)) return { on: false };
  return { on: true, seconds: Math.min(cacheTtl, maxSeconds) };
}

/** sha256 (hex) of a cache key: what `node_runs.cache_key` and `node_cache.key` hold. */
export async function hashCacheKey(key: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(key));
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
}

/** Milliseconds before attempt `attempt + 1`, after attempt `attempt` failed (the same as the Upstash `retryDelay` of sync nodes). */
export function retryDelayMs(backoff: string, baseDelayMs: number, attempt: number): number {
  if (backoff === "linear") return baseDelayMs * attempt;
  if (backoff === "exponential") return baseDelayMs * 2 ** (attempt - 1);
  return baseDelayMs;
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
