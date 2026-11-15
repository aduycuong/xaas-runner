// The signed URL an async node calls back with its result. The runner makes it, the ingress checks it.
// The same file is in ingress and runner (Workers share no code); change both together.
//
//   https://<ingress>/callbacks/<node_run_id>/<attempt>?exp=<unix seconds>&sig=<base64url>
//   sig = HMAC-SHA256(CALLBACK_SIGNING_KEY, "<node_run_id>.<attempt>.<exp>")

const encoder = new TextEncoder();

function base64Url(bytes: ArrayBuffer): string {
  let text = "";
  for (const byte of new Uint8Array(bytes)) text += String.fromCharCode(byte);
  return btoa(text).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

async function sign(key: string, nodeRunId: string, attempt: number, exp: number): Promise<string> {
  const cryptoKey = await crypto.subtle.importKey("raw", encoder.encode(key), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  return base64Url(await crypto.subtle.sign("HMAC", cryptoKey, encoder.encode(`${nodeRunId}.${attempt}.${exp}`)));
}

/** The callback URL of one attempt, valid until `exp` (unix seconds). */
export async function callbackUrl(ingressUrl: string, key: string, nodeRunId: string, attempt: number, exp: number): Promise<string> {
  const sig = await sign(key, nodeRunId, attempt, exp);
  return `${ingressUrl.replace(/\/+$/, "")}/callbacks/${nodeRunId}/${attempt}?exp=${exp}&sig=${sig}`;
}

/** Whether `sig` is the signature of this attempt and `exp` (compared in constant time). */
export async function validCallbackSignature(key: string, nodeRunId: string, attempt: number, exp: number, sig: string): Promise<boolean> {
  const expected = await sign(key, nodeRunId, attempt, exp);
  if (expected.length !== sig.length) return false;
  let diff = 0;
  for (let index = 0; index < expected.length; index++) diff |= expected.charCodeAt(index) ^ sig.charCodeAt(index);
  return diff === 0;
}
