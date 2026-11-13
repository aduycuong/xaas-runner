// The run token a node shows to xaas-resource-broker: 32 random bytes, new for every attempt. The runner keeps only its
// sha256 (in `attempts.resource_token_hash`, with an expiry of the node's timeout + RESOURCE_TOKEN_GRACE_MS); the broker
// hashes what it receives and finds the attempt, so a copy of the database holds nothing usable.
// Architecture: xaas-workflows/docs/kien-truc-upstash-cloudflare-hono.md §5.2.

/** Time a token outlives the node's timeout, for the node's last calls and clock drift. */
export const RESOURCE_TOKEN_GRACE_MS = 60_000;

const toBase64Url = (bytes: Uint8Array) => btoa(String.fromCharCode(...bytes)).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
const toHex = (bytes: Uint8Array) => [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");

/** sha256 (hex) of a token: what is stored and looked up. */
export async function hashResourceToken(token: string): Promise<string> {
  return toHex(new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(token))));
}

export async function newResourceToken(): Promise<{ token: string; hash: string }> {
  const token = `xrb_${toBase64Url(crypto.getRandomValues(new Uint8Array(32)))}`;
  return { token, hash: await hashResourceToken(token) };
}
