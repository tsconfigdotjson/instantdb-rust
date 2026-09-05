// A mock OpenID Connect provider for tests: discovery, JWKS, authorize (an
// immediate redirect back with a code), token (a signed id_token for the
// well-known code), userinfo, and a test-only `/__mint` that signs arbitrary
// claims so a test can hand a server id_tokens with any nonce / audience /
// issuer / algorithm.
//
// Every URL in the discovery document is built from the request's Host
// header, so the same process serves a server that reaches it as
// `localhost:9377` and one that reaches it as `mock-oidc:9377` (the
// differential harness runs it in a container beside the legacy server).
//
// Env: MOCK_OIDC_PORT (default 9377), MOCK_OIDC_HOST (bind, default 0.0.0.0)
//
// Codes the token endpoint accepts:
//   mock-code            → id_token for mock-sub-42 / oauth-user@example.com (verified)
//   mock-code-unverified → the same user with email_verified false
//   anything else        → 400 {error: invalid_grant, error_description: "Malformed auth code"}

import http from "node:http";
import crypto from "node:crypto";
import path from "node:path";
import { fileURLToPath } from "node:url";

const port = Number(process.env.MOCK_OIDC_PORT || 9377);
const host = process.env.MOCK_OIDC_HOST || "0.0.0.0";

const { publicKey, privateKey } = crypto.generateKeyPairSync("rsa", { modulusLength: 2048 });
const jwk = { ...publicKey.export({ format: "jwk" }), kid: "k1", use: "sig", alg: "RS256" };
const b64 = (o) => Buffer.from(JSON.stringify(o)).toString("base64url");
const HASH = { RS256: "sha256", RS384: "sha384", RS512: "sha512" };
export const signJwt = (claims, alg = "RS256") => {
  const input = `${b64({ alg, typ: "JWT", kid: "k1" })}.${b64(claims)}`;
  const sig = crypto.sign(HASH[alg] || "sha256", Buffer.from(input), privateKey).toString("base64url");
  return `${input}.${sig}`;
};

const readBody = (req) =>
  new Promise((resolve) => {
    let data = "";
    req.on("data", (c) => (data += c));
    req.on("end", () => resolve(data));
  });
const json = (res, status, body) => {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify(body));
};

export const server = http.createServer(async (req, res) => {
  const origin = `http://${req.headers.host}`;
  const url = new URL(req.url, origin);
  const now = Math.floor(Date.now() / 1000);
  if (url.pathname === "/.well-known/openid-configuration") {
    return json(res, 200, {
      issuer: origin,
      authorization_endpoint: `${origin}/authorize`,
      token_endpoint: `${origin}/token`,
      userinfo_endpoint: `${origin}/userinfo`,
      jwks_uri: `${origin}/jwks`,
      id_token_signing_alg_values_supported: ["RS256"],
      response_types_supported: ["code"],
      subject_types_supported: ["public"],
    });
  }
  if (url.pathname === "/jwks") return json(res, 200, { keys: [jwk] });
  if (url.pathname === "/authorize") {
    // the "user consents" instantly: back to the redirect_uri with a code
    const back = new URL(url.searchParams.get("redirect_uri"));
    back.searchParams.set("code", "mock-code");
    if (url.searchParams.get("state")) back.searchParams.set("state", url.searchParams.get("state"));
    res.writeHead(302, { location: back.toString() });
    return res.end();
  }
  if (url.pathname === "/token") {
    const params = new URLSearchParams(await readBody(req));
    const code = params.get("code");
    if (code !== "mock-code" && code !== "mock-code-unverified") {
      return json(res, 400, { error: "invalid_grant", error_description: "Malformed auth code" });
    }
    const claims = {
      iss: origin,
      sub: "mock-sub-42",
      email: "oauth-user@example.com",
      email_verified: code === "mock-code",
      picture: "https://example.com/pic.png",
      aud: params.get("client_id") || "mock-client-id",
      iat: now,
      exp: now + 3600,
    };
    return json(res, 200, { id_token: signJwt(claims), access_token: "mock-access-token", token_type: "Bearer" });
  }
  if (url.pathname === "/userinfo") {
    return json(res, 200, { sub: "mock-sub-42", email: "oauth-user@example.com", email_verified: true, picture: "https://example.com/pic.png" });
  }
  if (url.pathname === "/__mint" && req.method === "POST") {
    // test helper: {claims, alg?, forge?} → {id_token}
    const { claims, alg, forge } = JSON.parse((await readBody(req)) || "{}");
    const merged = { iat: now, exp: now + 3600, ...claims };
    // a null claim means "leave it out"
    for (const k of Object.keys(merged)) if (merged[k] === null) delete merged[k];
    let token = signJwt(merged, alg || "RS256");
    if (forge) token = token.replace(/\.[^.]+$/, ".AAAA");
    return json(res, 200, { id_token: token });
  }
  if (url.pathname === "/health") return json(res, 200, { ok: true });
  res.writeHead(404);
  res.end("nope");
});

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  server.listen(port, host, () => console.log(`mock oidc provider listening on ${host}:${port}`));
}
