import assert from "node:assert/strict";
import { test } from "node:test";
import { hashResourceToken, newResourceToken } from "./resource-token";

test("a run token is random, URL-safe, and only its hash is kept", async () => {
  const a = await newResourceToken();
  const b = await newResourceToken();
  assert.match(a.token, /^xrb_[A-Za-z0-9_-]{43}$/);
  assert.notEqual(a.token, b.token);
  assert.match(a.hash, /^[0-9a-f]{64}$/);
  assert.equal(await hashResourceToken(a.token), a.hash);
  assert.ok(!a.hash.includes(a.token.slice(4, 20)));
});
