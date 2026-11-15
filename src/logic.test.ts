import assert from "node:assert/strict";
import { test } from "node:test";
import { bindingName, judgeNodeOutcome, retryDelay } from "./logic";

test("node outcome: 2xx needs an output object", () => {
  assert.deepEqual(judgeNodeOutcome({ kind: "response", status: 200, body: { output: { a: 1 } } }), {
    ok: true,
    output: { a: 1 },
    httpStatus: 200,
  });
  for (const body of [null, {}, { output: "text" }, { output: [1] }]) {
    const verdict = judgeNodeOutcome({ kind: "response", status: 200, body });
    assert.equal(!verdict.ok && !verdict.retryable && verdict.error.code, "INVALID_OUTPUT");
  }
});

test("node outcome: 5xx, 429, timeout and network are retried; other 4xx and a missing node are not", () => {
  const retryable = (outcome: Parameters<typeof judgeNodeOutcome>[0]) => {
    const verdict = judgeNodeOutcome(outcome);
    return !verdict.ok && verdict.retryable;
  };
  assert.equal(retryable({ kind: "response", status: 503, body: null }), true);
  assert.equal(retryable({ kind: "response", status: 429, body: null }), true);
  assert.equal(retryable({ kind: "response", status: 400, body: null }), false);
  assert.equal(retryable({ kind: "timeout" }), true);
  assert.equal(retryable({ kind: "network", message: "reset" }), true);
  assert.equal(retryable({ kind: "unavailable", message: "no binding" }), false);
});

test("node outcome: an error body can ask for a retry and names the error", () => {
  const body = { error: { code: "BUSY", message: "Try later.", retryable: true } };
  assert.deepEqual(judgeNodeOutcome({ kind: "response", status: 409, body }), {
    ok: false,
    retryable: true,
    httpStatus: 409,
    error: { code: "BUSY", message: "Try later." },
  });
});

test("binding name and retry delay", () => {
  assert.equal(bindingName("node-core-echo-v1-0-0"), "NODE_CORE_ECHO_V1_0_0");
  assert.equal(retryDelay("none", 1000), "1000");
  assert.equal(retryDelay("linear", 500), "500 * (1 + retried)");
  assert.equal(retryDelay("exponential", 2000), "2000 * pow(2, retried)");
});

import { judgeCallbackBody, judgePollOutcome, retryDelayMs } from "./logic";
import { nextPollWait } from "./limits";
import { derivedResourceToken } from "./resource-token";

test("judgeCallbackBody: output, a reported error (final unless retryable), or neither", () => {
  assert.deepEqual(judgeCallbackBody({ output: { a: 1 } }), { ok: true, output: { a: 1 }, httpStatus: null });
  const failed = judgeCallbackBody({ error: { code: "RENDER", message: "no" } });
  assert.ok(!failed.ok && !failed.retryable && failed.error.code === "RENDER");
  const retry = judgeCallbackBody({ error: { code: "BUSY", message: "later", retryable: true } });
  assert.ok(!retry.ok && retry.retryable);
  const invalid = judgeCallbackBody("hello");
  assert.ok(!invalid.ok && invalid.error.code === "INVALID_CALLBACK");
});

test("judgePollOutcome: running (with retry_after_ms), succeeded, failed, unreachable", () => {
  assert.deepEqual(judgePollOutcome({ kind: "response", status: 200, body: { status: "running", retry_after_ms: 3000 } }), { kind: "running", retryAfterMs: 3000 });
  const done = judgePollOutcome({ kind: "response", status: 200, body: { status: "succeeded", output: { x: 1 } } });
  assert.ok(done.kind === "ended" && done.verdict.ok);
  const failed = judgePollOutcome({ kind: "response", status: 200, body: { status: "failed", error: { code: "E", message: "m" } } });
  assert.ok(failed.kind === "ended" && !failed.verdict.ok && failed.verdict.error.code === "E");
  assert.equal(judgePollOutcome({ kind: "response", status: 503, body: null }).kind, "unreachable");
  assert.equal(judgePollOutcome({ kind: "timeout" }).kind, "unreachable");
  const notFound = judgePollOutcome({ kind: "response", status: 404, body: null });
  assert.ok(notFound.kind === "ended" && !notFound.verdict.ok);
  const invalid = judgePollOutcome({ kind: "response", status: 200, body: { status: "weird" } });
  assert.ok(invalid.kind === "ended" && !invalid.verdict.ok && invalid.verdict.error.code === "INVALID_STATUS");
});

test("retryDelayMs matches the Upstash retryDelay of sync nodes", () => {
  assert.equal(retryDelayMs("none", 2000, 3), 2000);
  assert.equal(retryDelayMs("linear", 2000, 3), 6000);
  assert.equal(retryDelayMs("exponential", 2000, 3), 8000);
});

test("nextPollWait grows ×1.5 up to the ceiling, or follows retry_after_ms within the range", () => {
  assert.equal(nextPollWait(1000, 60_000), 1500);
  assert.equal(nextPollWait(50_000, 60_000), 60_000);
  assert.equal(nextPollWait(1000, 60_000, 200), 1000);
  assert.equal(nextPollWait(1000, 60_000, 45_000), 45_000);
});

test("derivedResourceToken is the same for one attempt and differs between attempts", async () => {
  const a = await derivedResourceToken("secret", "attempt-1");
  assert.deepEqual(await derivedResourceToken("secret", "attempt-1"), a);
  assert.notEqual((await derivedResourceToken("secret", "attempt-2")).token, a.token);
  assert.notEqual((await derivedResourceToken("other", "attempt-1")).token, a.token);
  assert.match(a.token, /^xrb_/);
});
