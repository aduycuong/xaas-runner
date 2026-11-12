// Best-effort writes to the Firebase Realtime Database over REST. Postgres is the source of truth:
// a write that fails is logged and forgotten, and the UI is right again after a reload.
//
// A Worker cannot use firebase-admin, so the runner signs a service-account JWT itself (WebCrypto)
// and trades it for an OAuth access token, kept until shortly before it expires.

export type RtdbEnv = { FIREBASE_CLIENT_EMAIL?: string; FIREBASE_PRIVATE_KEY?: string; FIREBASE_DATABASE_URL?: string };

/** What the engine calls. Every method returns at once; the write happens in the background. */
export type Realtime = {
  set(path: string, value: unknown): void;
  update(path: string, value: Record<string, unknown>): void;
  remove(path: string): void;
};

export const noRealtime: Realtime = { set() {}, update() {}, remove() {} };

const SCOPES = "https://www.googleapis.com/auth/firebase.database https://www.googleapis.com/auth/userinfo.email";
const TOKEN_URL = "https://oauth2.googleapis.com/token";
const WRITE_TIMEOUT_MS = 4000;

const encoder = new TextEncoder();

function base64Url(bytes: ArrayBuffer | Uint8Array | string): string {
  const data = typeof bytes === "string" ? encoder.encode(bytes) : new Uint8Array(bytes);
  let binary = "";
  for (const byte of data) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

/** PEM (PKCS8) text to the DER bytes WebCrypto imports. A secret pasted with literal `\n` is accepted. */
export function pemToDer(pem: string): Uint8Array {
  const body = pem.replace(/\\n/g, "\n").replace(/-----[A-Z ]+-----/g, "").replace(/\s+/g, "");
  return Uint8Array.from(atob(body), (char) => char.charCodeAt(0));
}

/** A signed RS256 JWT asking for `SCOPES` on behalf of the service account. */
export async function signServiceAccountJwt(clientEmail: string, privateKeyPem: string, nowSeconds: number): Promise<string> {
  const key = await crypto.subtle.importKey("pkcs8", pemToDer(privateKeyPem), { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" }, false, ["sign"]);
  const header = base64Url(JSON.stringify({ alg: "RS256", typ: "JWT" }));
  const claims = base64Url(JSON.stringify({ iss: clientEmail, scope: SCOPES, aud: TOKEN_URL, iat: nowSeconds, exp: nowSeconds + 3600 }));
  const signature = await crypto.subtle.sign("RSASSA-PKCS1-v1_5", key, encoder.encode(`${header}.${claims}`));
  return `${header}.${claims}.${base64Url(signature)}`;
}

let cached: { token: string; expiresAt: number } | null = null;

async function accessToken(env: Required<RtdbEnv>): Promise<string> {
  const now = Date.now();
  if (cached && cached.expiresAt - 60_000 > now) return cached.token;
  const assertion = await signServiceAccountJwt(env.FIREBASE_CLIENT_EMAIL, env.FIREBASE_PRIVATE_KEY, Math.floor(now / 1000));
  const response = await fetch(TOKEN_URL, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer", assertion }),
    signal: AbortSignal.timeout(WRITE_TIMEOUT_MS),
  });
  if (!response.ok) throw new Error(`Token request answered ${response.status}`);
  const body = (await response.json()) as { access_token: string; expires_in: number };
  cached = { token: body.access_token, expiresAt: now + body.expires_in * 1000 };
  return cached.token;
}

/**
 * Realtime writes for one request. Without the three secrets it does nothing. `waitUntil` keeps the
 * Worker alive for the writes without making the step wait for them.
 */
export function createRealtime(env: RtdbEnv, waitUntil: (promise: Promise<unknown>) => void): Realtime {
  if (!env.FIREBASE_CLIENT_EMAIL || !env.FIREBASE_PRIVATE_KEY || !env.FIREBASE_DATABASE_URL) return noRealtime;
  const full = env as Required<RtdbEnv>;
  const base = env.FIREBASE_DATABASE_URL.replace(/\/$/, "");

  const send = (method: "PUT" | "PATCH" | "DELETE", path: string, value?: unknown) => {
    waitUntil(
      (async () => {
        try {
          const token = await accessToken(full);
          const response = await fetch(`${base}/${path}.json?access_token=${encodeURIComponent(token)}`, {
            method,
            headers: { "Content-Type": "application/json" },
            body: value === undefined ? undefined : JSON.stringify(value),
            signal: AbortSignal.timeout(WRITE_TIMEOUT_MS),
          });
          if (!response.ok) console.warn(`[rtdb] ${method} ${path} answered ${response.status}`);
        } catch (error) {
          console.warn(`[rtdb] ${method} ${path} failed`, error instanceof Error ? error.message : error);
        }
      })(),
    );
  };
  return {
    set: (path, value) => send("PUT", path, value),
    update: (path, value) => send("PATCH", path, value),
    remove: (path) => send("DELETE", path),
  };
}

// ---------------------------------------------------------------------------
// What the runner publishes (see xaas-workflows/.ai/rtdb-rules.md)
// ---------------------------------------------------------------------------
//   runs/<runId>                    { status, updatedAt, nodes: { <nodeId>: { status, updatedAt } } }
//   liveRuns/<workspaceId>/<runId>  { workflowId, no, status, updatedAt }  while the run is queued or running

export const runPath = (runId: string) => `runs/${runId}`;
export const livePath = (workspaceId: string, runId: string) => `liveRuns/${workspaceId}/${runId}`;

export const nodePath = (runId: string, nodeId: string) => `runs/${runId}/nodes/${nodeId}`;

/** `test` runs (started from the editor) are not listed among the live runs of the workspace. */
export type RunInfo = { runId: string; workspaceId: string; workflowId: string; no: number; test?: boolean };

/** The few events the UI follows, turned into writes. Built on top of `Realtime`, so it never throws. */
export function runEvents(rt: Realtime, run: RunInfo) {
  const now = () => Date.now();
  return {
    started() {
      rt.update(runPath(run.runId), { status: "running", updatedAt: now() });
      if (!run.test) rt.set(livePath(run.workspaceId, run.runId), { workflowId: run.workflowId, no: run.no, status: "running", updatedAt: now() });
    },
    nodeRunning(nodeId: string) {
      rt.update(nodePath(run.runId, nodeId), { status: "running", updatedAt: now() });
    },
    nodeEnded(nodeId: string, status: string) {
      rt.update(nodePath(run.runId, nodeId), { status, updatedAt: now() });
    },
    runEnded(status: "succeeded" | "failed" | "cancelled") {
      rt.update(runPath(run.runId), { status, updatedAt: now() });
      rt.remove(livePath(run.workspaceId, run.runId));
    },
  };
}
