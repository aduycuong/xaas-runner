import assert from "node:assert/strict";
import { test } from "node:test";
import { emitEvent, MAX_PAYLOAD_BYTES, readEmitInput, signEmit } from "./emit-event";
import { judgeNodeOutcome } from "./logic";

const env = { INGRESS_URL: "https://ingress.test/", CALLBACK_SIGNING_KEY: "k" };
const call = (input: unknown) => ({
  scriptName: "runner-core-emit-event-v1-0-0",
  transport: "runner",
  timeoutMs: 1000,
  headers: { "X-XaaS-Node-Run-Id": "nr-1" },
  body: JSON.stringify({ input, run: { id: "run-1", env: "staging" } }),
});

test("input: a valid name and an object payload; payload defaults to {}", () => {
  assert.deepEqual(readEmitInput({ event: " task.created " }), { ok: true, value: { event: "task.created", payload: {} } });
  assert.deepEqual(readEmitInput({ event: "a:b-c_d", payload: { x: 1 } }), { ok: true, value: { event: "a:b-c_d", payload: { x: 1 } } });
  for (const input of [null, {}, { event: "" }, { event: ".x" }, { event: "a b" }, { event: "x".repeat(101) }, { event: "x", payload: [1] }, { event: "x", payload: "s" }]) {
    assert.equal(readEmitInput(input).ok, false, JSON.stringify(input));
  }
  assert.equal(readEmitInput({ event: "x", payload: { big: "y".repeat(MAX_PAYLOAD_BYTES) } }).ok, false);
});

test("signature: HMAC of <timestamp>.<body>, base64url, stable", async () => {
  // The same vector is in `ingress/src/emit.test.ts`: both sides sign alike.
  assert.equal(await signEmit("secret", 1764310000, '{"a":1}'), "bW-mNGI8Bdj6V8wJPlge65DTe9JZRcb_DpfB4MLXTGY");
  const a = await signEmit("k", 100, "{}");
  assert.equal(a, await signEmit("k", 100, "{}"));
  assert.notEqual(a, await signEmit("k", 101, "{}"));
  assert.match(a, /^[A-Za-z0-9_-]+$/);
});

test("emit: sends run, node run, name and payload, signed; the ingress answer is the output", async () => {
  let sent: { url: string; init: RequestInit } | null = null;
  const fetcher = (async (url: string, init: RequestInit) => {
    sent = { url, init };
    return new Response(JSON.stringify({ event: "task.created", started: 1, triggers: [] }), { status: 200 });
  }) as unknown as typeof fetch;
  const outcome = await emitEvent(env, call({ event: "task.created", payload: { a: 1 } }), fetcher);
  assert.deepEqual(judgeNodeOutcome(outcome), { ok: true, output: { event: "task.created", started: 1, triggers: [] }, httpStatus: 200 });
  assert.ok(sent);
  const { url, init } = sent as { url: string; init: RequestInit };
  assert.equal(url, "https://ingress.test/internal/events/emit");
  assert.deepEqual(JSON.parse(String(init.body)), { runId: "run-1", nodeRunId: "nr-1", event: "task.created", payload: { a: 1 } });
  const headers = init.headers as Record<string, string>;
  assert.equal(headers["X-Xaas-Signature"], await signEmit("k", Number(headers["X-Xaas-Timestamp"]), String(init.body)));
});

test("emit: a bad input or a refusal is final, an ingress failure is retried", async () => {
  const answering = (status: number) => (async () => new Response(JSON.stringify({ error: "nope" }), { status })) as unknown as typeof fetch;
  const bad = judgeNodeOutcome(await emitEvent(env, call({ event: "" }), answering(200)));
  assert.equal(bad.ok, false);
  assert.equal(!bad.ok && bad.retryable, false);
  assert.equal(!bad.ok && bad.error.code, "INVALID_INPUT");
  const refused = judgeNodeOutcome(await emitEvent(env, call({ event: "x" }), answering(401)));
  assert.equal(!refused.ok && refused.retryable, false);
  const down = judgeNodeOutcome(await emitEvent(env, call({ event: "x" }), answering(503)));
  assert.equal(!down.ok && down.retryable, true);
  const offline = await emitEvent(env, call({ event: "x" }), (async () => { throw new TypeError("fetch failed"); }) as unknown as typeof fetch);
  assert.equal(offline.kind, "network");
  assert.equal((await emitEvent({}, call({ event: "x" }))).kind, "unavailable");
});
