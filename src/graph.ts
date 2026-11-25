// Workflow graph: format, validation, expressions, conditions and scheduling. Pure: no I/O.
//
// This file is declared three times, byte for byte: in workers/runner/src/graph.ts, in
// xaas-workflows/lib/workflows/graph.ts (the app validates and previews with it) and in
// workers/ingress/src/graph.ts (webhook filters and input mappings use its expressions and
// conditions). Change all together; the tests (graph.test.ts) are copied to the app and the runner.

import jsonata from "jsonata";
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
 * an item of a connection (Drive file or folder, channel, chat, Page); a path in workspace files (`""` is the workspace
 * root, a folder only).
 */
const resourceChoiceSchema = z.union([
  z.string().min(1).max(64),
  z.object({ connectionId: z.uuid(), id: z.string().min(1).max(200) }),
  z.object({ path: z.string().max(500) }),
]);

/** The choice of each resource by name; a node without resources has `{}`. */
const resourcesSchema = z.preprocess((value) => value ?? {}, z.record(z.string(), resourceChoiceSchema));
export type ResourceChoice = z.infer<typeof resourceChoiceSchema>;

const settingsSchema = z
  .object({
    onError: z.enum(["stop", "continue"]).default("stop"),
    /** Milliseconds; null inherits the node definition. */
    timeout: z.int().positive().nullable().default(null),
    /** Max attempts; null inherits the node definition. */
    maxAttempts: z.int().min(1).max(10).nullable().default(null),
    /** Async nodes: milliseconds from the 202 to the result; null inherits the node definition. Ignored for sync nodes. */
    maxWait: z.int().positive().nullable().default(null),
    /** Seconds a run may reuse the output of an earlier run of this node with the same cache key; 0 does not cache. Nodes with manifest `cache` only. */
    cacheTtl: z.int().min(0).default(0),
  })
  .default({ onError: "stop", timeout: null, maxAttempts: null, maxWait: null, cacheTtl: 0 });

const triggerNode = z.object({
  id: nodeIdSchema,
  type: z.literal("trigger"),
  position: positionSchema,
  /** A workflow has one trigger node; its triggers live on the Triggers page. Older graphs may still carry `triggerId`: it is dropped. */
  data: z.object({ label: z.string().optional() }).default({}),
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

/**
 * The input schema of a workflow: a JSON Schema (draft 2020-12) of the run input, what nodes read as `{{ input.… }}`. Every
 * trigger's mapping must make it; Run, Test, Fire now and webhook calls are checked against it. A workspace file or folder
 * is a string with `format: "workspace-file"` / `"workspace-folder"` (its path).
 */
export const inputSchemaSchema = z
  .record(z.string(), z.unknown())
  .refine((schema) => schema.type === "object", { error: 'The input schema must describe an object ("type": "object").' })
  .refine((schema) => JSON.stringify(schema).length <= 64 * 1024, { error: "The input schema is too large (64 KB at most)." });

/** Shape of `workflow_versions.definition`. `position` and other editor data are ignored by the runner. */
export const graphSchema = z.object({
  nodes: z.array(nodeSchema).min(1),
  edges: z.array(edgeSchema).default([]),
  /** The input schema of the workflow (see `inputSchemaSchema`); without it any input is accepted. */
  inputSchema: inputSchemaSchema.optional(),
});

export type Graph = z.infer<typeof graphSchema>;
export type GraphNode = Graph["nodes"][number];
export type GraphEdge = Graph["edges"][number];
export type ActionNode = z.infer<typeof actionNode>;
export type IfNode = z.infer<typeof ifNode>;
export type SwitchNode = z.infer<typeof switchNode>;
export type MergeNode = z.infer<typeof mergeNode>;

export type Issue = { nodeId: string | null; path: string; message: string };

// ---------------------------------------------------------------------------
// Expressions: the inside of each `{{ … }}` is JSONata (https://jsonata.org), read against the scope
// `{ input, trigger, nodes, run }`: {{ input.x }} · {{ trigger.body.x }} · {{ nodes.<id>.output.items[0].name }}
// · {{ "Hi " & input.name }} · {{ input.vip ? "VIP" : "guest" }} · {{ $number(input.age) }} · {{ $count(nodes.a.output.items) }}.
// A node id or field that is not a plain name goes in backticks: {{ nodes.`my-node`.output }}.
// ---------------------------------------------------------------------------

export type Scope = {
  /** The input of the run: what the trigger made of the call (its mapping, or the whole body). */
  input: unknown;
  /** The raw data of the call that started the run: `{ body, headers, query }`, plus `scheduledAt` for a schedule. */
  trigger: unknown;
  run: { id: string; no: number | null; env: string };
  nodes: Record<string, { output: unknown }>;
};

/** An expression cannot be read, names a value that is missing, fails, or uses something not available. Retrying cannot fix it. */
export class ExpressionError extends Error {}

/** A path an expression reads from the scope: `nodes.a.output.items[0]` is root `nodes`, segments `["a", "output", "items", 0]`. */
export type ExpressionRef = { root: string; segments: (string | number)[] };

/** One piece of a text: plain text, or the inside of one `{{ … }}`. */
export type TemplatePart = { kind: "text"; text: string } | { kind: "expr"; source: string };

const ROOTS = ["input", "trigger", "nodes", "run"];
/** Evaluation steps and nesting allowed for one expression (Workers stop the clock during CPU work, so steps, not time). */
const MAX_STEPS = 100_000;
const MAX_DEPTH = 200;
const ENTRY_HOOK = Symbol.for("jsonata.__evaluate_entry");
const EXIT_HOOK = Symbol.for("jsonata.__evaluate_exit");

type Ast = { type?: string; value?: unknown; steps?: Ast[]; stages?: Ast[]; expr?: Ast; [key: string]: unknown };
type JsonataError = { code?: string; message?: string; position?: number; token?: string };

function describe(error: unknown): string {
  const { message, position, token } = (error ?? {}) as JsonataError;
  const where = typeof position === "number" ? ` (at ${position}${token ? `, near "${token}"` : ""})` : "";
  return `${message ?? String(error)}${where}`;
}

/**
 * Splits a text into plain text and `{{ … }}` expressions. Braces and quoted strings inside an expression are
 * balanced, so `{{ {"a": {"b": 1}} }}` is one expression. An unclosed `{{` is plain text.
 */
export function templateParts(text: string): TemplatePart[] {
  const parts: TemplatePart[] = [];
  let from = 0;
  let open = text.indexOf("{{");
  while (open >= 0) {
    let depth = 0;
    let quote: string | null = null;
    let end = -1;
    for (let i = open + 2; i < text.length; i++) {
      const c = text[i];
      if (quote) {
        if (c === "\\" && quote !== "`") i++;
        else if (c === quote) quote = null;
      } else if (c === '"' || c === "'" || c === "`") quote = c;
      else if (c === "{") depth++;
      else if (c === "}") {
        if (depth > 0) depth--;
        else if (text[i + 1] === "}") {
          end = i;
          break;
        }
      }
    }
    if (end < 0) break;
    if (open > from) parts.push({ kind: "text", text: text.slice(from, open) });
    parts.push({ kind: "expr", source: text.slice(open + 2, end) });
    from = end + 2;
    open = text.indexOf("{{", from);
  }
  if (from < text.length) parts.push({ kind: "text", text: text.slice(from) });
  return parts;
}

/** The leading names and fixed indexes of a path that starts at a root name. */
function refOf(steps: Ast[]): ExpressionRef {
  const segments: (string | number)[] = [];
  for (let i = 0; i < steps.length; i++) {
    const step = steps[i];
    if (step.type !== "name" || typeof step.value !== "string") break;
    if (i > 0) segments.push(step.value);
    const stages = step.stages ?? [];
    const index = stages.length === 1 && stages[0].type === "filter" && stages[0].expr?.type === "number" ? stages[0].expr.value : undefined;
    if (stages.length > 0) {
      if (typeof index !== "number" || !Number.isInteger(index) || index < 0) break;
      segments.push(index);
    }
    if (step.group || step.focus || step.index || step.tuple) break;
  }
  return { root: steps[0].value as string, segments };
}

/**
 * Collects the paths an expression reads from the scope. Names at the top level (and in function arguments,
 * conditions, blocks, constructors) start at the scope; names inside a filter or after a step are relative.
 */
function collectRefs(node: unknown, top: boolean, refs: ExpressionRef[]): void {
  if (Array.isArray(node)) {
    for (const item of node) collectRefs(item, top, refs);
    return;
  }
  if (node === null || typeof node !== "object") return;
  const ast = node as Ast;
  if (ast.type === "path" && Array.isArray(ast.steps) && ast.steps.length > 0) {
    const [first, ...rest] = ast.steps;
    if (top && first.type === "name") {
      refs.push(refOf(ast.steps));
      for (const [key, value] of Object.entries(first)) if (key !== "value" && key !== "type") collectRefs(value, false, refs);
    } else collectRefs(first, top, refs);
    collectRefs(rest, false, refs);
    for (const [key, value] of Object.entries(ast)) if (key !== "steps") collectRefs(value, false, refs);
    return;
  }
  if (top && ast.type === "name" && typeof ast.value === "string") {
    refs.push({ root: ast.value, segments: [] });
    return;
  }
  for (const [key, value] of Object.entries(ast)) if (key !== "position") collectRefs(value, top, refs);
}

function compile(source: string): { expression: ReturnType<typeof jsonata>; refs: ExpressionRef[] } {
  const text = source.trim();
  if (!text) throw new ExpressionError(`"{{ }}" is empty.`);
  let expression: ReturnType<typeof jsonata>;
  try {
    expression = jsonata(text);
  } catch (error) {
    throw new ExpressionError(`"{{ ${text} }}" cannot be read: ${describe(error)}.`);
  }
  const refs: ExpressionRef[] = [];
  collectRefs(expression.ast(), true, refs);
  for (const { root, segments } of refs) {
    if (root === "vars" || root === "secrets") throw new ExpressionError(`"${root}" is not an expression: a node reads secrets and variables by name, in the environment of the run.`);
    if (root === "files") throw new ExpressionError(`"files" is not an expression: choose the file or its folder in the node's Resources tab.`);
    if (!ROOTS.includes(root)) throw new ExpressionError(`Unknown source "${root}" in "{{ ${text} }}". Use input, trigger, nodes or run.`);
    if (root === "nodes" && (typeof segments[0] !== "string" || segments[1] !== "output")) {
      throw new ExpressionError(`"{{ ${text} }}" must look like nodes.<id>.output...`);
    }
  }
  return { expression, refs };
}

/** Checks the inside of one `{{ … }}` (JSONata syntax, known sources) and lists the paths it reads. */
export function parseExpression(source: string): ExpressionRef[] {
  return compile(source).refs;
}

async function evaluateToken(source: string, scope: Scope, strict: boolean): Promise<unknown> {
  const { expression } = compile(source);
  let steps = 0;
  let depth = 0;
  expression.assign(ENTRY_HOOK as unknown as string, () => {
    steps++;
    depth++;
    if (steps > MAX_STEPS) throw { code: "BUDGET", message: `The expression took more than ${MAX_STEPS} steps.` };
    if (depth > MAX_DEPTH) throw { code: "BUDGET", message: `The expression nested deeper than ${MAX_DEPTH} levels.` };
  });
  expression.assign(EXIT_HOOK as unknown as string, () => {
    depth--;
  });
  let value: unknown;
  try {
    value = await expression.evaluate(scope);
  } catch (error) {
    if (!strict) return undefined;
    throw new ExpressionError(`"{{ ${source.trim()} }}" failed: ${describe(error)}.`);
  }
  // JSONata sequences carry hidden flags and a function is not data: a JSON round trip gives plain values.
  const json = value === undefined ? undefined : JSON.stringify(value);
  if (json !== undefined) return JSON.parse(json);
  if (strict) throw new ExpressionError(`"{{ ${source.trim()} }}" has no value.`);
  return undefined;
}

/**
 * Replaces expressions in a JSON value. A string that is one expression keeps the type of its
 * value; inside a longer string values become text. `strict` makes a missing value or a failed
 * evaluation an error (action inputs); lenient gives `undefined` (conditions). An expression that
 * cannot be read is an error either way.
 */
export async function resolveValue(value: unknown, scope: Scope, strict = true): Promise<unknown> {
  if (typeof value === "string") {
    const parts = templateParts(value);
    const expressions = parts.filter((part) => part.kind === "expr");
    if (expressions.length === 0) return value;
    if (expressions.length === 1 && parts.every((part) => part.kind === "expr" || part.text.trim() === "")) {
      return evaluateToken(expressions[0].source, scope, strict);
    }
    let out = "";
    for (const part of parts) {
      if (part.kind === "text") out += part.text;
      else {
        const found = await evaluateToken(part.source, scope, strict);
        out += found === undefined ? "" : typeof found === "string" ? found : JSON.stringify(found);
      }
    }
    return out;
  }
  if (Array.isArray(value)) return Promise.all(value.map((item) => resolveValue(item, scope, strict)));
  if (value !== null && typeof value === "object") {
    const entries = await Promise.all(Object.entries(value).map(async ([key, item]) => [key, await resolveValue(item, scope, strict)] as const));
    return Object.fromEntries(entries);
  }
  return value;
}

/** Every `{{ … }}` source inside a value, for checking references without running anything. */
export function collectExpressions(value: unknown): string[] {
  if (typeof value === "string") return templateParts(value).flatMap((part) => (part.kind === "expr" ? [part.source] : []));
  if (Array.isArray(value)) return value.flatMap(collectExpressions);
  if (value !== null && typeof value === "object") return Object.values(value).flatMap(collectExpressions);
  return [];
}

/** Resolves the `inputs` of an action: `fixed` values are used as written, `expr` values are expressions. */
export async function resolveInputs(inputs: ActionNode["data"]["inputs"], scope: Scope): Promise<Record<string, unknown>> {
  const out: Record<string, unknown> = {};
  for (const [name, input] of Object.entries(inputs)) {
    try {
      out[name] = input.mode === "expr" ? await resolveValue(input.value, scope, true) : input.value;
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

/** A missing value, or one whose expression fails, counts as empty here: `is_empty` is true and comparisons are false. */
async function resolveLenient(value: unknown, scope: Scope): Promise<unknown> {
  try {
    return await resolveValue(value, scope, false);
  } catch (error) {
    if (error instanceof ExpressionError) return undefined;
    throw error;
  }
}

/** Handle (`true` / `false`) an `if` node takes. */
export async function evaluateIf(node: IfNode, scope: Scope): Promise<"true" | "false"> {
  const results: boolean[] = [];
  for (const c of node.data.conditions) results.push(compare(c.op, await resolveLenient(c.left, scope), await resolveLenient(c.right, scope)));
  return (node.data.join === "and" ? results.every(Boolean) : results.some(Boolean)) ? "true" : "false";
}

/** Handle a `switch` node takes: the first matching case, `default`, or null when nothing matches and there is no default. */
export async function evaluateSwitch(node: SwitchNode, scope: Scope): Promise<string | null> {
  const value = await resolveLenient(node.data.value, scope);
  for (const c of node.data.cases) if (compare(c.op, value, await resolveLenient(c.right, scope))) return c.id;
  return node.data.fallback ? "default" : null;
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
        for (const path of parseExpression(source)) {
          const ref = path.root === "nodes" ? (path.segments[0] as string) : null;
          if (ref !== null && !byId.has(ref)) add(node.id, "data", `"{{ ${source.trim()} }}" refers to unknown node "${ref}".`);
          else if (ref !== null && !before.has(ref)) add(node.id, "data", `"${ref}" does not come before "${node.id}", so "{{ ${source.trim()} }}" cannot be used.`);
        }
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

/** The state a run starts from: the trigger node has fired, whatever started the run (older graphs with several: all of them). */
export function startState(graph: Graph): RunState {
  const state = emptyState();
  for (const node of graph.nodes) if (node.type === "trigger") state.status.set(node.id, "succeeded");
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
