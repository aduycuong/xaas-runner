import assert from "node:assert/strict";
import { test } from "node:test";
import { callbackUrl, validCallbackSignature } from "./callback-url";

const nodeRunId = "0193a7c2-0000-7000-8000-000000000001";

test("a callback URL carries a signature the same key accepts", async () => {
  const url = new URL(await callbackUrl("https://ingress.example/", "key-1", nodeRunId, 2, 1_800_000_000));
  assert.equal(url.pathname, `/callbacks/${nodeRunId}/2`);
  const sig = url.searchParams.get("sig") ?? "";
  assert.equal(await validCallbackSignature("key-1", nodeRunId, 2, 1_800_000_000, sig), true);
});

test("another key, attempt or expiry does not accept it", async () => {
  const sig = new URL(await callbackUrl("https://ingress.example", "key-1", nodeRunId, 2, 1_800_000_000)).searchParams.get("sig") ?? "";
  assert.equal(await validCallbackSignature("key-2", nodeRunId, 2, 1_800_000_000, sig), false);
  assert.equal(await validCallbackSignature("key-1", nodeRunId, 3, 1_800_000_000, sig), false);
  assert.equal(await validCallbackSignature("key-1", nodeRunId, 2, 1_900_000_000, sig), false);
  assert.equal(await validCallbackSignature("key-1", nodeRunId, 2, 1_800_000_000, sig.slice(1)), false);
});
