import assert from "node:assert/strict";
import { test } from "node:test";
import { createRealtime, noRealtime, pemToDer, signServiceAccountJwt } from "./rtdb";

async function makeKey() {
  const pair = (await crypto.subtle.generateKey({ name: "RSASSA-PKCS1-v1_5", modulusLength: 2048, publicExponent: new Uint8Array([1, 0, 1]), hash: "SHA-256" }, true, ["sign", "verify"])) as CryptoKeyPair;
  const der = new Uint8Array((await crypto.subtle.exportKey("pkcs8", pair.privateKey)) as ArrayBuffer);
  let binary = "";
  for (const byte of der) binary += String.fromCharCode(byte);
  const pem = `-----BEGIN PRIVATE KEY-----\n${btoa(binary).match(/.{1,64}/g)!.join("\n")}\n-----END PRIVATE KEY-----\n`;
  return { pair, pem };
}

test("pemToDer accepts real newlines and a secret pasted with literal \\n", async () => {
  const { pem } = await makeKey();
  const a = pemToDer(pem);
  const b = pemToDer(pem.replace(/\n/g, "\\n"));
  assert.deepEqual(a, b);
  assert.ok(a.length > 1000);
});

test("signServiceAccountJwt makes a token that verifies and carries the scopes", async () => {
  const { pair, pem } = await makeKey();
  const jwt = await signServiceAccountJwt("svc@project.iam.gserviceaccount.com", pem, 1_800_000_000);
  const [header, claims, signature] = jwt.split(".");
  const fromB64 = (text: string) => Uint8Array.from(atob(text.replace(/-/g, "+").replace(/_/g, "/")), (c) => c.charCodeAt(0));
  const ok = await crypto.subtle.verify("RSASSA-PKCS1-v1_5", pair.publicKey, fromB64(signature), new TextEncoder().encode(`${header}.${claims}`));
  assert.equal(ok, true);
  const body = JSON.parse(new TextDecoder().decode(fromB64(claims)));
  assert.equal(body.iss, "svc@project.iam.gserviceaccount.com");
  assert.equal(body.exp - body.iat, 3600);
  assert.match(body.scope, /firebase\.database/);
  assert.equal(JSON.parse(new TextDecoder().decode(fromB64(header))).alg, "RS256");
});

test("without the secrets realtime does nothing and never throws", () => {
  const rt = createRealtime({}, () => assert.fail("no write should be scheduled"));
  assert.equal(rt, noRealtime);
  rt.set("runs/x", {});
  rt.update("runs/x", {});
  rt.remove("runs/x");
});

test("runEvents: start, node and end events go to the documented paths", async () => {
  const { runEvents } = await import("./rtdb");
  const calls: string[] = [];
  const rt = {
    set: (path: string, value: unknown) => calls.push(`set ${path} ${(value as { status: string }).status}`),
    update: (path: string, value: Record<string, unknown>) => calls.push(`update ${path} ${value.status}`),
    remove: (path: string) => calls.push(`remove ${path}`),
  };
  const events = runEvents(rt, { runId: "r1", workspaceId: "w1", workflowId: "f1", no: 7 });
  events.started();
  events.nodeRunning("a");
  events.nodeEnded("a", "succeeded");
  events.runEnded("failed");
  assert.deepEqual(calls, [
    "update runs/r1 running",
    "set liveRuns/w1/r1 running",
    "update runs/r1/nodes/a running",
    "update runs/r1/nodes/a succeeded",
    "update runs/r1 failed",
    "remove liveRuns/w1/r1",
  ]);
});
