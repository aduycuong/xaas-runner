// Workflow graph: format, validation, expressions, conditions and scheduling. Pure: no I/O.
//
// This file is declared three times, byte for byte: here, in xaas-workflows/lib/workflows/graph.ts
// (the app validates and previews with it) and in xaas-ingress/src/graph.ts (webhook filters and
// input mappings use its expressions and conditions). Change all together; the tests
// (graph.test.ts) are copied to the app and the runner.

import { z } from "zod";

export type StoredError = { code: string; message: string };

// ---------------------------------------------------------------------------
// Format
// ---------------------------------------------------------------------------

/** Node ids end up in Upstash step names and in `{{ nodes.<id>.output }}` paths. */
export const nodeIdSchema = z.string().regex(/^[A-Za-z0-9_-]{1,64}$/, { error: "Node id must be 1-64 letters, digits, _ or -." });

export const operators = [
  "equals", "not_equals", "gt", "gte", "lt", "lte", "contains", "starts_with", "ends_with", "is_empty", "is_not_empty",
] as const;
export type Operator = (typeof operators)[number];

const conditionSchema = z.object({
  /** An expression (`{{ … }}`) or a literal. */
  left: z.unknown(),
  op: z.enum(operators),
  right: z.unknown().optional(),
});
export type Condition = z.infer<typeof conditionSchema>;

const positionSchema = z.object({ x: z.number(), y: z.number() }).optional();

const inputSchema = z.object({ mode: z.enum(["fixed", "expr"]), value: z.unknown() });

/**
 * What a node resource is chosen to be, by the resource's name: a secret or variable name, or a connection id (string);
 * an item of a connection (Drive file or folder, channel, chat, Page); a path in workspace files.
 */
const resourceChoiceSchema = z.union([
  z.string().min(1).max(64),
  z.object({ connectionId: z.uuid(), id: z.string().min(1).max(200) }),
  z.object({ path: z.string().min(1).max(500) }),
]);

/** Reads the first shape (`{ connections, secrets, variables }` of maps) as the flat one. */
function flattenResources(value: unknown): unknown {
  if (value == null) return {};
  if (typeof value !== "object" || Array.isArray(value)) return value;
  const groups = ["connections", "secrets", "variables"];
  const entries = Object.entries(value as Record<string, unknown>);
  const isOld = entries.length > 0 && entries.every(([key, inner]) => groups.includes(key) && inner !== null && typeof inner === "object" && !("connectionId" in inner) && !("path" in inner));
  return isOld ? Object.assign({}, ...entries.map(([, inner]) => inner)) : value;
}

const resourcesSchema = z.preprocess(flattenResources, z.record(z.string(), resourceChoiceSchema));
export type ResourceChoice = z.infer<typeof resourceChoiceSchema>;

const settingsSchema = z
  .object({
    onError: z.enum(["stop", "continue"]).default("stop"),
    /** Milliseconds; null inherits the node definition. */
    timeout: z.int().positive().nullable().default(null),
    /** Max attempts; null inherits the node definition. */
    maxAttempts: z.int().min(1).max(10).nullable().default(null),
  })
  .default({ onError: "stop", timeout: null, maxAttempts: null });

const triggerNode = z.object({
  id: nodeIdSchema,
  type: z.literal("trigger"),
  position: positionSchema,
  data: z.object({ label: z.string().optional(), triggerId: z.uuid().optional() }).default({}),
});

const actionNode = z.object({
  id: nodeIdSchema,
  type: z.literal("action"),
  position: positionSchema,
  data: z.object({
    label: z.string().optional(),
    nodeKey: z.string().min(1),
    /** A concrete semver: "latest" is turned into a version when the workflow is published. */
    version: z.string().regex(/^\d+\.\d+\.\d+$/, { error: "Node version must be semver, e.g. 1.0.0." }),
    inputs: z.record(z.string(), inputSchema).default({}),
    settings: settingsSchema,
    disabled: z.boolean().default(false),
    /**
     * What the node may use from the workspace, keyed by the names of its `resources`: a secret or variable name, a
     * connection id, an item of a connection, a path in workspace files. Never values, and no environment: secrets and
     * variables take the value of the run's environment.
     */
    resources: resourcesSchema,
  }),
});

const ifNode = z.object({
  id: nodeIdSchema,
  type: z.literal("if"),
  position: positionSchema,
  data: z.object({ label: z.string().optional(), join: z.enum(["and", "or"]).default("and"), conditions: z.array(conditionSchema).min(1) }),
});

const switchNode = z.object({
  id: nodeIdSchema,
  type: z.literal("switch"),
  position: positionSchema,
  data: z.object({
    label: z.string().optional(),
    value: z.unknown(),
    cases: z.array(z.object({ id: nodeIdSchema, op: z.enum(operators), right: z.unknown().optional() })),
    /** Adds the `default` output, taken when no case matches. */
    fallback: z.boolean().default(true),
  }),
});

const mergeNode = z.object({
  id: nodeIdSchema,
  type: z.literal("merge"),
  position: positionSchema,
  data: z.object({ label: z.string().optional(), mode: z.enum(["all", "first"]).default("all") }).default({ mode: "all" }),
});

const nodeSchema = z.discriminatedUnion("type", [triggerNode, actionNode, ifNode, switchNode, mergeNode]);

const edgeSchema = z.object({
  id: z.string().optional(),
  source: nodeIdSchema,
  sourceHandle: z.string().default("out"),
  target: nodeIdSchema,
  targetHandle: z.string().default("in"),
});

/** Shape of `workflow_versions.definition`. `position` and other editor data are ignored by the runner. */
export const graphSchema = z.object({ nodes: z.array(nodeSchema).min(1), edges: z.array(edgeSchema).default([]) });

export type Graph = z.infer<typeof graphSchema>;
export type GraphNode = Graph["nodes"][number];
export type GraphEdge = Graph["edges"][number];
export type ActionNode = z.infer<typeof actionNode>;
export type IfNode = z.infer<typeof ifNode>;
export type SwitchNode = z.infer<typeof switchNode>;
export type MergeNode = z.infer<typeof mergeNode>;

export type Issue = { nodeId: string | null; path: string; message: string };

// ---------------------------------------------------------------------------
// Expressions: {{ input.x }} · {{ trigger.body.x }} · {{ nodes.<id>.output.items[0].name }} · {{ run.id }} · {{ x | default("n/a") }}
// ---------------------------------------------------------------------------

export type Scope = {
  /** The input of the run: what the trigger made of the call (its mapping, or the whole body). */
  input: unknown;
  /** The raw data of the call that started the run: `{ body, headers, query }`, plus `scheduledAt` for a schedule. */
  trigger: unknown;
  run: { id: string; no: number | null; env: string };
  nodes: Record<string, { output: unknown }>;
};

/** An expression names a value that is missing, or uses something not available. Retrying cannot fix it. */
export class ExpressionError extends Error {}

const WHOLE = /^\s*\{\{([\s\S]+?)\}\}\s*$/;
const PART = /\{\{([\s\S]+?)\}\}/g;
const SEGMENT = /^[A-Za-z_][A-Za-z0-9_-]*/;

type Path = { root: string; segments: (string | number)[]; fallback: { value: unknown } | null };

/** Parses the inside of one `{{ … }}`. */
export function parseExpression(source: string): Path {
  let text = source.trim();
  let fallback: Path["fallback"] = null;
  const withDefault = /^([\s\S]*?)\s*\|\s*default\(([\s\S]*)\)$/.exec(text);
  if (withDefault) {
    try {
      fallback = { value: JSON.parse(withDefault[2]) };
    } catch {
      throw new ExpressionError(`default(...) needs a JSON value in "{{ ${text} }}".`);
    }
    text = withDefault[1].trim();
  }

  const root = SEGMENT.exec(text)?.[0];
  if (!root) throw new ExpressionError(`"{{ ${text} }}" does not start with a name.`);
  let rest = text.slice(root.length);
  const segments: (string | number)[] = [];
  while (rest.length) {
    const index = /^\[(\d+)\]/.exec(rest);
    if (index) {
      segments.push(Number(index[1]));
      rest = rest.slice(index[0].length);
      continue;
    }
    const name = rest.startsWith(".") ? SEGMENT.exec(rest.slice(1))?.[0] : undefined;
    if (!name) throw new ExpressionError(`"{{ ${text} }}" has a path that cannot be read.`);
    segments.push(name);
    rest = rest.slice(1 + name.length);
  }

  if (root === "vars" || root === "secrets") throw new ExpressionError(`"${root}" is not an expression: a node reads secrets and variables by name, in the environment of the run.`);
  if (root === "files") throw new ExpressionError(`"files" is not an expression: choose the file or its folder in the node's Resources tab.`);
  if (!["input", "trigger", "nodes", "run"].includes(root)) throw new ExpressionError(`Unknown source "${root}" in "{{ ${text} }}". Use input, trigger, nodes or run.`);
  if (root === "nodes" && (typeof segments[0] !== "string" || segments[1] !== "output")) {
    throw new ExpressionError(`"{{ ${text} }}" must look like nodes.<id>.output...`);
  }
  return { root, segments, fallback };
}

/** Value at a parsed path, or `undefined` when something on the way is missing. */
function readPath(path: Path, scope: Scope): unknown {
  let current: unknown;
  let segments = path.segments;
  if (path.root === "input") current = scope.input;
  else if (path.root === "trigger") current = scope.trigger;
  else if (path.root === "run") current = scope.run;
  else {
    const entry = scope.nodes[segments[0] as string];
    if (!entry) return undefined;
    current = entry.output;
    segments = segments.slice(2);
  }
  for (const segment of segments) {
    if (Array.isArray(current) && segment === "length") current = current.length;
    else if (Array.isArray(current) && typeof segment === "number") current = current[segment];
    else if (current !== null && typeof current === "object" && typeof segment === "string" && Object.hasOwn(current, segment)) {
      current = (current as Record<string, unknown>)[segment];
    } else return undefined;
  }
  return current;
}

function evaluateToken(source: string, scope: Scope, strict: boolean): unknown {
  const path = parseExpression(source);
  const value = readPath(path, scope);
  if (value !== undefined) return value;
  if (path.fallback) return path.fallback.value;
  if (strict) throw new ExpressionError(`"{{ ${source.trim()} }}" has no value.`);
  return undefined;
}

/**
 * Replaces expressions in a JSON value. A string that is one expression keeps the type of its
 * value; inside a longer string values become text. `strict` makes a missing value an error
 * (action inputs); lenient gives `undefined` (conditions).
 */
export function resolveValue(value: unknown, scope: Scope, strict = true): unknown {
  if (typeof value === "string") {
    const whole = WHOLE.exec(value);
    if (whole && !/\}\}[\s\S]*\{\{/.test(value)) return evaluateToken(whole[1], scope, strict);
    return value.replace(PART, (_match, source: string) => {
      const found = evaluateToken(source, scope, strict);
      return found === undefined ? "" : typeof found === "string" ? found : JSON.stringify(found);
    });
  }
  if (Array.isArray(value)) return value.map((item) => resolveValue(item, scope, strict));
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, resolveValue(item, scope, strict)]));
  }
  return value;
}

/** Every `{{ … }}` source inside a value, for checking references without running anything. */
export function collectExpressions(value: unknown): string[] {
  if (typeof value === "string") return [...value.matchAll(PART)].map((match) => match[1]);
  if (Array.isArray(value)) return value.flatMap(collectExpressions);
  if (value !== null && typeof value === "object") return Object.values(value).flatMap(collectExpressions);
  return [];
}

/** Resolves the `inputs` of an action: `fixed` values are used as written, `expr` values are expressions. */
export function resolveInputs(inputs: ActionNode["data"]["inputs"], scope: Scope): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [name, input] of Object.entries(inputs)) {
    try {
      out[name] = input.mode === "expr" ? resolveValue(input.value, scope, true) : input.value;
    } catch (error) {
      if (error instanceof ExpressionError) throw new ExpressionError(`Input "${name}": ${error.message}`);
      throw error;
    }
  }
  return out;
}

// ---------------------------------------------------------------------------
// Conditions
// ---------------------------------------------------------------------------

const same = (a: unknown, b: unknown) => JSON.stringify(a) === JSON.stringify(b);

function isEmpty(value: unknown): boolean {
  if (value === undefined || value === null || value === "") return true;
  if (Array.isArray(value)) return value.length === 0;
  if (typeof value === "object") return Object.keys(value as object).length === 0;
  return false;
}

/** Compares two resolved values. Ordering operators are false when the types differ. */
export function compare(op: Operator, left: unknown, right: unknown): boolean {
  switch (op) {
    case "equals": return same(left, right);
    case "not_equals": return !same(left, right);
    case "is_empty": return isEmpty(left);
    case "is_not_empty": return !isEmpty(left);
    case "contains":
      if (typeof left === "string") return typeof right === "string" && left.includes(right);
      return Array.isArray(left) && left.some((item) => same(item, right));
    case "starts_with": return typeof left === "string" && typeof right === "string" && left.startsWith(right);
    case "ends_with": return typeof left === "string" && typeof right === "string" && left.endsWith(right);
    default: {
      let order: number;
      if (typeof left === "number" && typeof right === "number") order = left - right;
      else if (typeof left === "string" && typeof right === "string") order = left < right ? -1 : left > right ? 1 : 0;
      else return false;
      if (op === "gt") return order > 0;
      if (op === "gte") return order >= 0;
      if (op === "lt") return order < 0;
      return order <= 0;
    }
  }
}

/** A missing value counts as empty here: `is_empty` is true and comparisons are false. */
function resolveLenient(value: unknown, scope: Scope): unknown {
  return resolveValue(value, scope, false);
}

/** Handle (`true` / `false`) an `if` node takes. */
export function evaluateIf(node: IfNode, scope: Scope): "true" | "false" {
  const results = node.data.conditions.map((c) => compare(c.op, resolveLenient(c.left, scope), resolveLenient(c.right, scope)));
  return (node.data.join === "and" ? results.every(Boolean) : results.some(Boolean)) ? "true" : "false";
}

/** Handle a `switch` node takes: the first matching case, `default`, or null when nothing matches and there is no default. */
export function evaluateSwitch(node: SwitchNode, scope: Scope): string | null {
  const value = resolveLenient(node.data.value, scope);
  const match = node.data.cases.find((c) => compare(c.op, value, resolveLenient(c.right, scope)));
  return match ? match.id : node.data.fallback ? "default" : null;
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/** Handles a node offers on its right side. */
export function outputHandles(node: GraphNode): string[] {
  if (node.type === "if") return ["true", "false"];
  if (node.type === "switch") return [...node.data.cases.map((c) => c.id), ...(node.data.fallback ? ["default"] : [])];
  return ["out"];
}

function expressionsOf(node: GraphNode): unknown[] {
  if (node.type === "action") return Object.values(node.data.inputs).filter((i) => i.mode === "expr").map((i) => i.value);
  if (node.type === "if") return node.data.conditions.flatMap((c) => [c.left, c.right]);
  if (node.type === "switch") return [node.data.value, ...node.data.cases.map((c) => c.right)];
  return [];
}

/** Everything wrong with a graph, in the order found. Empty means it is valid. Same result in app and runner. */
export function validateGraph(input: unknown): { graph: Graph | null; issues: Issue[] } {
  const parsed = graphSchema.safeParse(input);
  if (!parsed.success) {
    return {
      graph: null,
      issues: parsed.error.issues.map((issue) => ({ nodeId: null, path: issue.path.join("."), message: issue.message })),
    };
  }
  const graph = parsed.data;
  const issues: Issue[] = [];
  const add = (nodeId: string | null, path: string, message: string) => issues.push({ nodeId, path, message });

  const byId = new Map<string, GraphNode>();
  for (const node of graph.nodes) {
    if (byId.has(node.id)) add(node.id, "nodes", `Duplicate node id "${node.id}".`);
    byId.set(node.id, node);
  }

  const incoming = new Map<string, GraphEdge[]>();
  const outgoing = new Map<string, GraphEdge[]>();
  graph.edges.forEach((edge, index) => {
    const path = `edges.${index}`;
    const source = byId.get(edge.source);
    const target = byId.get(edge.target);
    if (!source) add(null, path, `Edge starts at unknown node "${edge.source}".`);
    if (!target) add(null, path, `Edge ends at unknown node "${edge.target}".`);
    if (!source || !target) return;
    if (!outputHandles(source).includes(edge.sourceHandle)) add(source.id, path, `Node "${source.id}" has no output "${edge.sourceHandle}".`);
    if (edge.targetHandle !== "in") add(target.id, path, `Node "${target.id}" has no input "${edge.targetHandle}".`);
    if (target.type === "trigger") add(target.id, path, `A trigger cannot have incoming edges ("${target.id}").`);
    incoming.set(target.id, [...(incoming.get(target.id) ?? []), edge]);
    outgoing.set(source.id, [...(outgoing.get(source.id) ?? []), edge]);
  });

  const triggers = graph.nodes.filter((node) => node.type === "trigger");
  if (triggers.length === 0) add(null, "nodes", "The workflow needs a trigger.");

  for (const node of graph.nodes) {
    if (node.type === "action" && node.data.disabled) {
      add(node.id, "nodes", `Node "${node.id}" is disabled: enable it or remove it before publishing.`);
    }
    if (node.type === "merge") {
      if ((incoming.get(node.id) ?? []).length < 2) add(node.id, "nodes", `Merge "${node.id}" needs at least two incoming edges.`);
      if (node.data.mode === "first") add(node.id, "nodes", `Merge "${node.id}": "first to finish" is not supported yet.`);
    }
  }

  // Reachability and cycles (Kahn's algorithm over the edges that point at real nodes).
  const indegree = new Map(graph.nodes.map((node) => [node.id, (incoming.get(node.id) ?? []).filter((edge) => byId.has(edge.source)).length]));
  const queue = graph.nodes.filter((node) => indegree.get(node.id) === 0).map((node) => node.id);
  const order: string[] = [];
  while (queue.length) {
    const id = queue.shift() as string;
    order.push(id);
    for (const edge of outgoing.get(id) ?? []) {
      const left = (indegree.get(edge.target) ?? 0) - 1;
      indegree.set(edge.target, left);
      if (left === 0) queue.push(edge.target);
    }
  }
  if (order.length < graph.nodes.length) {
    const stuck = graph.nodes.filter((node) => !order.includes(node.id)).map((node) => node.id);
    add(stuck[0], "edges", `The graph has a loop through ${stuck.map((id) => `"${id}"`).join(", ")}.`);
  }

  const reached = new Set<string>(triggers.map((node) => node.id));
  const stack = [...reached];
  while (stack.length) {
    for (const edge of outgoing.get(stack.pop() as string) ?? []) {
      if (!reached.has(edge.target)) {
        reached.add(edge.target);
        stack.push(edge.target);
      }
    }
  }
  for (const node of graph.nodes) if (!reached.has(node.id)) add(node.id, "nodes", `Node "${node.id}" cannot be reached from a trigger.`);

  // Expressions: valid syntax, no vars/secrets (nodes read them by name), and only nodes that come before.
  const ancestors = (id: string): Set<string> => {
    const seen = new Set<string>();
    const walk = [id];
    while (walk.length) {
      for (const edge of incoming.get(walk.pop() as string) ?? []) {
        if (!seen.has(edge.source)) {
          seen.add(edge.source);
          walk.push(edge.source);
        }
      }
    }
    return seen;
  };
  for (const node of graph.nodes) {
    const before = ancestors(node.id);
    for (const source of expressionsOf(node).flatMap(collectExpressions)) {
      try {
        const path = parseExpression(source);
        const ref = path.root === "nodes" ? (path.segments[0] as string) : null;
        if (ref !== null && !byId.has(ref)) add(node.id, "data", `"{{ ${source.trim()} }}" refers to unknown node "${ref}".`);
        else if (ref !== null && !before.has(ref)) add(node.id, "data", `"${ref}" does not come before "${node.id}", so "{{ ${source.trim()} }}" cannot be used.`);
      } catch (error) {
        if (!(error instanceof ExpressionError)) throw error;
        add(node.id, "data", error.message);
      }
    }
  }
  return { graph, issues };
}

// ---------------------------------------------------------------------------
// Scheduling
// ---------------------------------------------------------------------------

/** What a node ended as. `warning` is success after a retry, or a failure the node let the run continue past. */
export type NodeStatus = "succeeded" | "warning" | "failed" | "skipped" | "cancelled";

export type RunState = {
  status: Map<string, NodeStatus>;
  /** Handle an `if` / `switch` took; null when a switch matched nothing. Other nodes take "out". */
  taken: Map<string, string | null>;
};

export const emptyState = (): RunState => ({ status: new Map(), taken: new Map() });

function edgeActive(edge: GraphEdge, node: GraphNode, state: RunState): boolean | undefined {
  const status = state.status.get(edge.source);
  if (status === undefined) return undefined;
  if (status !== "succeeded" && status !== "warning") return false;
  if (node.type === "if" || node.type === "switch") return state.taken.get(edge.source) === edge.sourceHandle;
  return true;
}

export type Plan = {
  /** Nodes whose every incoming edge is settled and at least one is active: run them. */
  ready: string[];
  /** Nodes that will never run because no incoming edge is active. Already marked in the state. */
  skipped: string[];
};

/**
 * Decides what happens next from the nodes that have ended, and marks nodes with no active way in
 * as skipped (repeatedly, so a skipped branch skips what hangs off it). A `merge` is not special
 * here: it waits for all its incoming edges to settle and runs if any is active, so a skipped
 * branch never makes it wait.
 */
export function planNext(graph: Graph, state: RunState): Plan {
  const byId = new Map(graph.nodes.map((node) => [node.id, node]));
  const incoming = new Map<string, GraphEdge[]>();
  for (const edge of graph.edges) incoming.set(edge.target, [...(incoming.get(edge.target) ?? []), edge]);

  const ready: string[] = [];
  const skipped: string[] = [];
  let changed = true;
  while (changed) {
    changed = false;
    ready.length = 0;
    for (const node of graph.nodes) {
      if (node.type === "trigger" || state.status.has(node.id)) continue;
      const edges = incoming.get(node.id) ?? [];
      const results = edges.map((edge) => edgeActive(edge, byId.get(edge.source) as GraphNode, state));
      if (results.some((result) => result === undefined)) continue;
      if (results.some((result) => result === true)) ready.push(node.id);
      else {
        state.status.set(node.id, "skipped");
        skipped.push(node.id);
        changed = true;
      }
    }
  }
  return { ready, skipped };
}

/** Which trigger nodes start a run: the ones tied to the trigger that fired, or all when none is. */
export function startState(graph: Graph, firedTriggerId: string | null): RunState {
  const state = emptyState();
  const triggers = graph.nodes.filter((node) => node.type === "trigger");
  const matching = triggers.filter((node) => firedTriggerId !== null && node.data.triggerId === firedTriggerId);
  const active = matching.length ? matching : triggers;
  for (const node of triggers) state.status.set(node.id, active.includes(node) ? "succeeded" : "skipped");
  return state;
}

/** Output of a `merge`: one entry per branch that arrived, keyed by the id of the node it came from. */
export function mergeOutput(nodeId: string, graph: Graph, state: RunState, outputs: Record<string, unknown>): Record<string, unknown> {
  const byId = new Map(graph.nodes.map((node) => [node.id, node]));
  const result: Record<string, unknown> = {};
  for (const edge of graph.edges) {
    if (edge.target !== nodeId) continue;
    if (edgeActive(edge, byId.get(edge.source) as GraphNode, state)) result[edge.source] = outputs[edge.source] ?? null;
  }
  return result;
}
