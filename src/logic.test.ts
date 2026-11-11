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
