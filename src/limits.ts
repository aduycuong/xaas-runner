// Ranges a node's execution settings must fall in: a copy of xaas-workflows/lib/nodes/constants.ts (change both
// together). The app refuses to publish values outside them; the runner clamps, for definitions published before.

export type Range = { min: number; max: number };

export const TIMEOUT_MS: Range = { min: 1_500, max: 30_000 };
export const MAX_ATTEMPTS: Range = { min: 1, max: 10 };
export const MAX_WAIT_MS: Range = { min: 10_000, max: 7 * 24 * 60 * 60 * 1000 };
export const POLL_INTERVAL_MS: Range = { min: 1_000, max: 10 * 60 * 1000 };
export const POLL_MAX_POLLS: Range = { min: 1, max: 200 };
/** Each wait between polls is this many times the previous one, up to the node's `max_interval_ms`. */
export const POLL_GROWTH = 1.5;
/** A polling attempt fails (POLL_FAILED, retryable) after this many failed status calls in a row. */
export const POLL_ERRORS_IN_A_ROW = 3;

/** Time the node has to answer `POST /cache-key` (at most its own timeout). */
export const CACHE_KEY_TIMEOUT_MS = 10_000;
/** A cache TTL longer than this is cut to it when the entry is stored (`expires_at` must stay a valid time). */
export const MAX_CACHE_TTL_S = 100 * 365 * 86_400;
/** A cache key longer than this is refused (the runner stores only its sha256). */
export const MAX_CACHE_KEY_CHARS = 100_000;

export function clampToRange(value: number, range: Range): number {
  return Math.min(range.max, Math.max(range.min, Math.round(value)));
}

/** The wait before the next poll: the node's `retry_after_ms` when it gave one, otherwise the last wait grown. */
export function nextPollWait(lastWaitMs: number, maxIntervalMs: number, retryAfterMs?: number): number {
  if (retryAfterMs !== undefined) return clampToRange(retryAfterMs, POLL_INTERVAL_MS);
  return Math.min(maxIntervalMs, Math.round(lastWaitMs * POLL_GROWTH));
}
