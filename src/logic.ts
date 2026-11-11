// Pure helpers of the runner: no I/O, covered by logic.test.ts.

import { z } from "zod";

export type StoredError = { code: string; message: string };

// ---------------------------------------------------------------------------
// Workflow definition (the `workflow_versions.definition` column)
// ---------------------------------------------------------------------------

/** Node ids end up in Upstash step names and in `{{ context.<id> }}` paths. */
const nodeId = z.string().regex(/^[A-Za-z0-9_-]{1,64}$/);

const condition = z.object({
  left: z.unknown(),
  op: z.enum(["eq", "ne", "gt", "gte", "lt", "lte", "truthy", "falsy"]),
  right: z.unknown().optional(),
});

const workerNode = z.object({
  id: nodeId,
  type: z.literal("worker"),
  label: z.string().optional(),
  nodeVersionId: z.uuid(),
  input: z.record(z.string(), z.unknown()).default({}),
  next: nodeId.nullable(),
});

const ifNode = z.object({
  id: nodeId,
  type: z.literal("flow.if"),
  label: z.string().optional(),
  branches: z.array(z.object({ when: condition, next: nodeId.nullable() })).min(1),
  defaultNext: nodeId.nullable(),
});

export const definitionSchema = z.object({
  entry: nodeId,
  nodes: z.array(z.discriminatedUnion("type", [workerNode, ifNode])).min(1),
});

export type Condition = z.infer<typeof condition>;
export type Definition = z.infer<typeof definitionSchema>;
export type WorkerNode = z.infer<typeof workerNode>;
export type IfNode = z.infer<typeof ifNode>;

// ---------------------------------------------------------------------------
// Templates: `{{ input.x }}` and `{{ context.<nodeId>.x }}`
// ---------------------------------------------------------------------------

export type Scope = { input: unknown; context: unknown };

/** A template names a path that has no value. Retrying cannot fix it. */
export class TemplateError extends Error {}

const WHOLE = /^\s*\{\{\s*([^{}]+?)\s*\}\}\s*$/;
const PART = /\{\{\s*([^{}]+?)\s*\}\}/g;

function lookup(path: string, scope: Scope): unknown {
  const segments = path.split(".");
  if (segments[0] !== "input" && segments[0] !== "context") {
    throw new TemplateError(`Unknown template root in "{{ ${path} }}".`);
  }
  let current: unknown = scope;
  for (const segment of segments) {
    if (Array.isArray(current) && segment === "length") {
      current = current.length;
    } else if (current !== null && typeof current === "object" && Object.hasOwn(current, segment)) {
      current = (current as Record<string, unknown>)[segment];
    } else {
      throw new TemplateError(`"{{ ${path} }}" has no value.`);
    }
  }
  return current;
}

/**
 * Replaces templates in a JSON value. A string that is one template alone becomes the value it
 * names, keeping its type; inside a longer string the value is turned into text.
 */
export function resolveTemplate(value: unknown, scope: Scope): unknown {
  if (typeof value === "string") {
    const whole = WHOLE.exec(value);
    if (whole) return lookup(whole[1], scope);
    return value.replace(PART, (_match, path: string) => {
      const found = lookup(path, scope);
      return typeof found === "string" ? found : JSON.stringify(found);
    });
  }
  if (Array.isArray(value)) return value.map((item) => resolveTemplate(item, scope));
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, resolveTemplate(item, scope)]));
  }
  return value;
}

/** Evaluates a `flow.if` condition. Ordering operators are false when the two sides differ in type. */
export function evaluateCondition(cond: Condition, scope: Scope): boolean {
  if (cond.op === "truthy" || cond.op === "falsy") {
    // A path with no value counts as falsy here, so an optional field can be tested.
    let left: unknown;
    try {
      left = resolveTemplate(cond.left, scope);
    } catch (error) {
      if (!(error instanceof TemplateError)) throw error;
    }
    return cond.op === "truthy" ? Boolean(left) : !left;
  }

  const left = resolveTemplate(cond.left, scope);
  const right = resolveTemplate(cond.right, scope);
  if (cond.op === "eq") return JSON.stringify(left) === JSON.stringify(right);
  if (cond.op === "ne") return JSON.stringify(left) !== JSON.stringify(right);

  let order: number;
  if (typeof left === "number" && typeof right === "number") order = left - right;
  else if (typeof left === "string" && typeof right === "string") order = left < right ? -1 : left > right ? 1 : 0;
  else return false;

  if (cond.op === "gt") return order > 0;
  if (cond.op === "gte") return order >= 0;
  if (cond.op === "lt") return order < 0;
  return order <= 0;
}

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
