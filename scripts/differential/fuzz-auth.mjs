// Seeded auth-flow fuzz layer of the differential harness.
//
// Random sequences over the auth surface a shipped client uses
// (@instantdb/core / react / admin), run live against both servers:
//   - guest sign-in over /runtime/auth/sign_in_guest and /admin/sign_in_guest;
//   - magic-code send over /runtime/auth/send_magic_code, codes read through
//     /admin/magic_code, and verify over /runtime/auth/verify_magic_code and
//     /admin/verify_magic_code, including wrong, reused and expired-shaped
//     codes and guest -> signed-in upgrades;
//   - /runtime/auth/verify_refresh_token and both sign-out routes
//     (/runtime/signout, /admin/sign_out by email / id / refresh_token);
//   - /admin/refresh_tokens creation by email / id / both, and
//     /admin/users GET+DELETE by email / id / refresh_token, including missing,
//     malformed and unknown parameters;
//   - extra-fields signups against generated $users attrs and generated
//     $users.allow.create / $users.allow.view rules;
//   - socket init with every resulting token, comparing the auth identity in
//     init-ok and the $users query result under the generated view rule.
//
// The fixed prefix guarantees every surface and the security-relevant
// sequences run for every seed: a signed-out token, a deleted user's token,
// a reused code, an expired code and malformed parameters. The random tail
// explores mixtures of the same actions. A create rule that allows anonymous
// guests and @fuzz.example emails (with a seed-chosen extra condition) keeps
// the generated signup path exercising real tokens, while @denied.example
// signups exercise the denial path unless the seed chose the open rule.
//
// Compared: HTTP status, type / message / hint / key set and the whole
// normalized response body; socket init-ok auth identity / admin flag; and the
// folded $users query result. Only server-chosen ids, tokens, codes and
// timestamps are normalized; fixed ids stay raw. Codes are read through the
// admin route (one per server), so their random values never need to match.
//
// Usage: node fuzz-auth.mjs <app-id-legacy> <app-id-rust> <admin-token> [seed] [rounds]
// Env: LEGACY_URL, RUST_URL, LEGACY_DATABASE_URL, RUST_DATABASE_URL

import { createHash } from "node:crypto";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  canon,
  connect,
  describeAllowVerdict,
  DIVERGENCE_PREFIXES,
  firstDifference,
  isFixedId,
  loadAllowlist,
  makeIdFactory,
  psql,
  uuid,
} from "./lib.mjs";

const here = path.dirname(fileURLToPath(import.meta.url));

const appIdLegacy = process.argv[2];
const appIdRust = process.argv[3];
const adminToken = process.argv[4];
const seed = Number(process.argv[5] ?? 501);
const rounds = Number(process.argv[6] ?? 40);
if (!appIdLegacy || !appIdRust || !adminToken)
  throw new Error("usage: node fuzz-auth.mjs <app-id-legacy> <app-id-rust> <admin-token> [seed] [rounds]");

const SERVERS = {
  legacy: {
    url: process.env.LEGACY_URL || "http://localhost:8891",
    db: process.env.LEGACY_DATABASE_URL || "postgres://instant:instant@localhost:8890/instant",
    appId: appIdLegacy,
  },
  rust: {
    url: process.env.RUST_URL || "http://localhost:8888",
    db: process.env.RUST_DATABASE_URL || "postgres://instant:instant@localhost:5432/instant",
    appId: appIdRust,
  },
};

// legacy loads rules off the WAL feed (the other seeded layers use the same)
const RULES_SETTLE_MS = 1500;
const UUID_RE = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi;
const ISO_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/;
const VOLATILE_KEYS = new Set([
  "trace-id",
  "duration-ms",
  "session-id",
  "client-event-id",
  "instaql-query-hash",
  "isn",
  "processed-isn",
  "tx-id",
  "processed-tx-id",
]);

function prng(a) {
  return () => {
    a |= 0; a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const addBlob = (id, label, extra = {}) => [
  "add-attr",
  {
    id,
    "forward-identity": [id, "$users", label],
    "value-type": "blob",
    cardinality: "one",
    "unique?": false,
    "index?": false,
    isUnsynced: true,
    ...extra,
  },
];

function buildPlan() {
  const rand = prng(seed * 104729 + 991);
  const pick = (arr) => arr[Math.floor(rand() * arr.length)];
  const chance = (p) => rand() < p;
  const mk = makeIdFactory(appIdLegacy);
  const attrIds = { username: mk(), displayName: mk(), tier: mk() };
  const fixedIds = { U1: mk(), U2: mk(), U3: mk(), U4: mk() };
  const emailFor = (name) => `authfuzz-${seed}-${name}@fuzz.example`;
  const emails = {
    U1: emailFor("u1"),
    U2: emailFor("u2"),
    U3: emailFor("u3"),
    U4: emailFor("u4"),
    M1: emailFor("m1"),
    M2: emailFor("m2"),
    M3: emailFor("m3"),
    M4: emailFor("m4"),
    blocked: `authfuzz-${seed}-blocked@denied.example`,
    malformed: "not an email",
  };

  // Generated $users schema: signup extra-fields must name one of these attrs.
  const schema = [
    addBlob(attrIds.username, "username"),
    addBlob(attrIds.displayName, "displayName"),
    addBlob(attrIds.tier, "tier"),
  ];

  // Both rules are generated from the seed. Every create rule lets guests
  // (no email) and @fuzz.example signups through, so the fuzz always has real
  // tokens to exercise; @denied.example only passes when the open rule wins.
  const createRule = pick([
    "data.email == null || data.email.endsWith('@fuzz.example')",
    "newData.email == null || newData.email.endsWith('@fuzz.example')",
    "true",
    "data.username != null || data.email == null || data.email.endsWith('@fuzz.example')",
    "data.email == null || auth.email != null && auth.email.endsWith('@fuzz.example')",
  ]);
  const viewRule = pick([
    "auth.id == data.id || (data.linkedPrimaryUser != null && auth.id == data.linkedPrimaryUser)",
    "auth.id == data.id",
    "true",
    "data.email != null",
    "auth.email != null",
    "auth.id == data.id || data.email == auth.email",
    "data.username != null || auth.id == data.id",
    "false",
  ]);
  const rules = { $users: { allow: { create: createRule, view: viewRule } } };

  const script = [];
  const planUsers = {};
  const planTokens = {};
  const planCodes = {};
  let seq = 0;
  let dyn = 0;

  const push = (action) => {
    script.push({ name: `${action.kind}-${seq++}`, ...action });
  };
  const setUser = (label, { id = null, email = null, deleted = false } = {}) => {
    const prev = planUsers[label] ?? {};
    planUsers[label] = { id: id ?? prev.id ?? null, email: email ?? prev.email ?? null, deleted };
  };
  const addToken = (label, user) => {
    planTokens[label] = { user, active: true };
  };
  const markUserTokens = (user, active = false) => {
    for (const [label, t] of Object.entries(planTokens)) {
      if (t.user === user) t.active = active;
    }
  };
  const activeTokens = () =>
    Object.keys(planTokens).filter((label) => {
      const t = planTokens[label];
      return t.active && !planUsers[t.user]?.deleted;
    });
  const activeUsers = () => Object.keys(planUsers).filter((label) => !planUsers[label].deleted);
  const nextToken = () => `t${dyn++}`;
  const freshEmail = () => emailFor(`r${dyn}`);
  const validExtra = () =>
    pick([
      { username: `u${Math.floor(rand() * 1000)}` },
      { displayName: `Display ${Math.floor(rand() * 1000)}` },
      { tier: Math.floor(rand() * 5) },
      {
        username: `u${Math.floor(rand() * 1000)}`,
        displayName: `Display ${Math.floor(rand() * 1000)}`,
        tier: Math.floor(rand() * 5),
      },
    ]);

  const adminRefresh = ({ email, id, idLabel, extraFields, assignUser, assignToken }) => {
    push({ kind: "admin_refresh", email, id, idLabel, extraFields, assignUser, assignToken });
    if (assignUser) setUser(assignUser, { email: email ?? planUsers[assignUser]?.email ?? null, id });
    if (assignToken && assignUser) addToken(assignToken, assignUser);
    return assignToken;
  };
  const guest = ({ admin = false, extraFields, assignUser, assignToken }) => {
    push({ kind: guestKind(admin), extraFields, assignUser, assignToken });
    if (assignUser) setUser(assignUser, {});
    if (assignUser && assignToken) addToken(assignToken, assignUser);
  };
  const guestKind = (admin) => (admin ? "admin_guest" : "guest");
  const adminCode = (email, codeLabel) => {
    push({ kind: "admin_code", email, codeLabel });
    planCodes[codeLabel] = { email, used: false, expired: false };
  };
  const adminSendCode = (email, codeLabel) => {
    push({ kind: "admin_send_code", email, codeLabel });
    planCodes[codeLabel] = { email, used: false, expired: false };
  };
  const expireCode = (codeLabel, email) => {
    push({ kind: "expire_code", codeLabel, email });
    if (planCodes[codeLabel]) planCodes[codeLabel].expired = true;
  };
  const verify = ({
    admin = false,
    email,
    codeLabel,
    literalCode,
    guestToken,
    extraFields,
    assignUser,
    assignToken,
    consumes = true,
  }) => {
    push({
      kind: admin ? "admin_verify_code" : "verify_code",
      email,
      codeLabel,
      literalCode,
      guestToken,
      extraFields,
      assignUser,
      assignToken,
    });
    if (codeLabel && consumes && planCodes[codeLabel]) planCodes[codeLabel].used = true;
    if (assignUser) {
      setUser(assignUser, { email });
      if (assignToken) addToken(assignToken, assignUser);
      // a guest upgrading to a fresh email keeps its id and its old tokens
      if (guestToken && planTokens[guestToken]) planTokens[guestToken].user = assignUser;
    }
  };
  const runtimeSendCode = ({ email, missingEmail = false }) => push({ kind: "runtime_send_code", email, missingEmail });
  const verifyRefresh = ({ tokenLabel, literalToken, missing = false }) =>
    push({ kind: "verify_refresh", tokenLabel, literalToken, missing });
  const runtimeSignout = ({ tokenLabel, literalToken, missingApp = false, missingToken = false }) => {
    push({ kind: "runtime_signout", tokenLabel, literalToken, missingApp, missingToken });
    if (tokenLabel && planTokens[tokenLabel]) planTokens[tokenLabel].active = false;
  };
  const adminSignout = ({ by, user, token, literal }) => {
    push({ kind: "admin_signout", by, user, token, literal });
    if (by === "email" || by === "id") markUserTokens(user);
    if (by === "refresh_token" && planTokens[token]) planTokens[token].active = false;
  };
  const usersGet = ({ by, user, token, rawQuery, none = false }) => push({ kind: "users_get", by, user, token, rawQuery, none });
  const usersDelete = ({ by, user, token, rawQuery, none = false }) => {
    push({ kind: "users_delete", by, user, token, rawQuery, none });
    if (by === "email" || by === "id") {
      markUserTokens(user);
      if (planUsers[user]) planUsers[user].deleted = true;
    } else if (by === "refresh_token") {
      const owner = planTokens[token]?.user;
      if (owner) {
        markUserTokens(owner);
        planUsers[owner].deleted = true;
      }
    }
  };
  const init = ({ tokenLabel, literalToken, admin = false, query = false, anonymous = false, name }) =>
    push({ kind: "init", tokenLabel, literalToken, admin, query, anonymous, ...(name ? { name } : {}) });

  // ---- fixed prefix: every surface, security sequence and malformed shape
  adminRefresh({
    email: emails.U1,
    id: fixedIds.U1,
    extraFields: { username: "u1", displayName: "User One", tier: 1 },
    assignUser: "U1",
    assignToken: "tU1",
  });
  verifyRefresh({ tokenLabel: "tU1" });
  usersGet({ by: "email", user: "U1" });
  usersGet({ by: "id", user: "U1" });
  usersGet({ by: "refresh_token", token: "tU1" });
  init({ tokenLabel: "tU1", query: true });

  guest({ extraFields: { username: "guest1" }, assignUser: "G1", assignToken: "tG1" });
  verifyRefresh({ tokenLabel: "tG1" });
  usersGet({ by: "refresh_token", token: "tG1" });
  init({ tokenLabel: "tG1", query: true });

  // guest upgrade with a fresh email keeps the guest id and writes new extra-fields
  adminCode(emails.M1, "cM1");
  verify({
    email: emails.M1,
    codeLabel: "cM1",
    guestToken: "tG1",
    extraFields: { username: "m1", tier: 2 },
    assignUser: "M1",
    assignToken: "tM1",
  });
  verifyRefresh({ tokenLabel: "tM1" });
  init({ tokenLabel: "tM1", query: true });

  // runtime send + an admin-read code for an existing user: a returning signup
  // ignores invalid extra-fields instead of failing
  runtimeSendCode({ email: emails.M1 });
  adminCode(emails.M1, "cM1b");
  verify({
    email: emails.M1,
    codeLabel: "cM1b",
    extraFields: { unknown: "ignored" },
    assignUser: "M1",
    assignToken: "tM1b",
  });

  // wrong then correct then reused
  adminCode(emails.M2, "cM2");
  verify({ email: emails.M2, literalCode: "000000", consumes: false });
  verify({
    email: emails.M2,
    codeLabel: "cM2",
    extraFields: { username: "m2" },
    assignUser: "M2",
    assignToken: "tM2",
  });
  verify({ email: emails.M2, codeLabel: "cM2" });

  // expired-shaped code
  adminCode(emails.M3, "cM3");
  expireCode("cM3", emails.M3);
  verify({ email: emails.M3, codeLabel: "cM3" });

  // admin send / admin verify (the admin SDK's magic-code routes)
  adminSendCode(emails.M4, "cM4");
  verify({
    admin: true,
    email: emails.M4,
    codeLabel: "cM4",
    extraFields: { username: "admin-m4" },
    assignUser: "M4",
    assignToken: "tM4",
  });
  verifyRefresh({ tokenLabel: "tM4" });
  init({ tokenLabel: "tM4", query: true });
  verify({ admin: true, email: emails.U1 });
  verify({ admin: true, email: emails.malformed, literalCode: "000000" });

  // denied-domain signup (open rule seeds allow it; gated seeds compare denial)
  adminCode(emails.blocked, "cBlocked");
  verify({ email: emails.blocked, codeLabel: "cBlocked", consumes: false });

  // existing admin user: create=false and extra-fields are ignored
  adminRefresh({ email: emails.U1, extraFields: { username: "ignored" }, assignUser: "U1", assignToken: "tU1b" });
  // new user by id only
  adminRefresh({ id: fixedIds.U2, extraFields: { username: "u2" }, assignUser: "U2", assignToken: "tU2" });
  // admin guest, then link it to an existing primary by upgrading with its email
  guest({ admin: true, assignUser: "G2", assignToken: "tG2" });
  adminCode(emails.U1, "cU1");
  verify({ email: emails.U1, codeLabel: "cU1", guestToken: "tG2", assignUser: "U1", assignToken: "tU1c" });

  // sign out by email invalidates every token for that user
  adminSignout({ by: "email", user: "U1" });
  verifyRefresh({ tokenLabel: "tU1" });
  init({ tokenLabel: "tU1" });

  // delete by refresh token deletes the user and all of its tokens
  adminRefresh({ email: emails.U3, id: fixedIds.U3, assignUser: "U3", assignToken: "tU3" });
  adminRefresh({ email: emails.U3, assignUser: "U3", assignToken: "tU3b" });
  usersDelete({ by: "refresh_token", token: "tU3b" });
  verifyRefresh({ tokenLabel: "tU3" });
  usersGet({ by: "id", user: "U3" });

  // runtime signout, then again (idempotent), and the token stays rejected
  runtimeSignout({ tokenLabel: "tU2" });
  verifyRefresh({ tokenLabel: "tU2" });
  runtimeSignout({ tokenLabel: "tU2" });

  // admin signout by id and by refresh token
  adminSignout({ by: "id", user: "M1" });
  verifyRefresh({ tokenLabel: "tM1" });
  verifyRefresh({ tokenLabel: "tM1b" });
  adminSignout({ by: "refresh_token", token: "tM2" });
  verifyRefresh({ tokenLabel: "tM2" });

  // missing / malformed parameters across the auth surface
  verifyRefresh({ literalToken: "nope" });
  verifyRefresh({ missing: true });
  usersGet({ none: true });
  usersDelete({ none: true });
  usersGet({ rawQuery: "?id=not-a-uuid" });
  usersDelete({ rawQuery: "?email=not%20an%20email" });
  usersGet({ rawQuery: "?refresh_token=nope" });
  usersGet({ rawQuery: "?foo=bar" });
  adminSignout({ by: "none" });
  adminSignout({ by: "id", literal: "nope" });
  adminSignout({ by: "email", literal: emails.malformed });
  adminSignout({ by: "refresh_token", literal: "nope" });
  adminRefresh({});
  adminRefresh({ email: emails.malformed });
  adminRefresh({ id: "not-a-uuid" });
  runtimeSignout({ tokenLabel: "tG2", missingApp: true });
  planTokens.tG2.active = false;
  runtimeSignout({ missingToken: true });
  runtimeSignout({ literalToken: "nope" });
  runtimeSendCode({ email: emails.malformed });
  runtimeSendCode({ missingEmail: true });
  verify({ email: emails.U1 });
  verify({ email: emails.U1, literalCode: "nope", consumes: false });
  verify({ email: emails.malformed, literalCode: "000000", consumes: false });
  adminCode(emails.malformed, "cBadEmail");
  guest({ extraFields: { unknown: "x" } });
  guest({ admin: true, extraFields: { email: "evil@fuzz.example" } });
  adminRefresh({ email: freshEmail(), extraFields: { unknown: "x" } });
  init({ literalToken: "nope", name: "init-malformed-token" });
  init({ literalToken: uuid(), name: "init-unknown-token" });

  // a fresh socket session still sees the generated view rule
  adminRefresh({ email: emails.U4, id: fixedIds.U4, assignUser: "U4", assignToken: "tU4" });
  init({ tokenLabel: "tU4", query: true });
  usersGet({ by: "email", user: "U4" });
  usersDelete({ by: "email", user: "U4" });
  verifyRefresh({ tokenLabel: "tU4" });
  init({ tokenLabel: "tU4" });

  // ---- random tail: mixtures and repeat visits
  for (let i = 0; i < rounds; i++) {
    const r = rand();
    if (r < 0.17) {
      const label = `rndU${dyn}`;
      const token = nextToken();
      adminRefresh({ email: freshEmail(), extraFields: chance(0.65) ? validExtra() : undefined, assignUser: label, assignToken: token });
    } else if (r < 0.31) {
      const label = pick(activeUsers());
      if (!label) continue;
      const u = planUsers[label];
      const token = nextToken();
      adminRefresh({
        ...(u.email ? { email: u.email } : { idLabel: label }),
        extraFields: chance(0.5) ? validExtra() : undefined,
        assignUser: label,
        assignToken: token,
      });
    } else if (r < 0.45) {
      const label = `rndG${dyn}`;
      guest({ admin: chance(0.3), extraFields: chance(0.6) ? validExtra() : undefined, assignUser: label, assignToken: nextToken() });
    } else if (r < 0.61) {
      const label = `rndM${dyn}`;
      const email = freshEmail();
      const code = `c${dyn}`;
      const guests = activeTokens();
      const guestToken = chance(0.35) && guests.length ? pick(guests) : undefined;
      adminCode(email, code);
      verify({
        email,
        codeLabel: code,
        guestToken,
        extraFields: chance(0.65) ? validExtra() : undefined,
        assignUser: label,
        assignToken: nextToken(),
      });
    } else if (r < 0.68) {
      const email = freshEmail();
      runtimeSendCode({ email });
      if (chance(0.5)) {
        const code = `s${dyn}`;
        adminCode(email, code);
        verify({ email, codeLabel: code, extraFields: chance(0.5) ? validExtra() : undefined });
      }
    } else if (r < 0.76) {
      if (chance(0.35)) {
        const email = freshEmail();
        const code = `w${dyn}`;
        adminCode(email, code);
        verify({ email, literalCode: "000000", consumes: false });
        if (chance(0.5)) verify({ email, codeLabel: code, extraFields: chance(0.5) ? validExtra() : undefined });
      } else if (chance(0.5)) {
        const email = freshEmail();
        const code = `r${dyn}`;
        adminCode(email, code);
        verify({ email, codeLabel: code, extraFields: chance(0.5) ? validExtra() : undefined });
        verify({ email, codeLabel: code });
      } else {
        const email = freshEmail();
        const code = `e${dyn}`;
        adminCode(email, code);
        expireCode(code, email);
        verify({ email, codeLabel: code });
      }
    } else if (r < 0.84) {
      const all = Object.keys(planTokens);
      const token = all.length ? pick(all) : null;
      if (token) verifyRefresh({ tokenLabel: token });
    } else if (r < 0.88) {
      const tokens = activeTokens();
      if (tokens.length) runtimeSignout({ tokenLabel: pick(tokens) });
    } else if (r < 0.92) {
      const tokens = activeTokens();
      const by = pick(["email", "id", "refresh_token"]);
      if (by === "refresh_token") {
        if (tokens.length) adminSignout({ by, token: pick(tokens) });
      } else {
        const users = activeUsers();
        if (users.length) adminSignout({ by, user: pick(users) });
      }
    } else if (r < 0.96) {
      const by = pick(["email", "id", "refresh_token", "malformed", "missing", "unknown"]);
      if (by === "malformed") {
        (chance(0.5) ? usersGet : usersDelete)({ rawQuery: pick(["?id=nope", "?email=nope", "?refresh_token=nope"]) });
      } else if (by === "missing") {
        (chance(0.5) ? usersGet : usersDelete)({ none: true });
      } else if (by === "unknown") {
        (chance(0.5) ? usersGet : usersDelete)({ rawQuery: "?foo=bar" });
      } else if (by === "refresh_token") {
        const tokens = activeTokens();
        if (tokens.length) {
          const token = pick(tokens);
          const del = chance(0.5);
          if (del) usersDelete({ by, token });
          else usersGet({ by, token });
        }
      } else {
        const users = activeUsers();
        if (users.length) {
          const user = pick(users);
          const del = chance(0.5);
          if (del) usersDelete({ by, user });
          else usersGet({ by, user });
        }
      }
    } else {
      const tokens = activeTokens();
      if (tokens.length) init({ tokenLabel: pick(tokens), query: chance(0.5) });
    }
  }

  return { schema, rules, script };
}

// ---------------------------------------------------------------------------
// comparison helpers

function isFixed(id) {
  return typeof id === "string" && isFixedId(id);
}

function normalizer(state) {
  return (value, key = null) => {
    const ids = new Map();
    const tokenIds = new Map();
    const codeIds = new Map();
    for (const [label, u] of Object.entries(state.users)) {
      if (u.id) ids.set(String(u.id).toLowerCase(), `<user:${label}>`);
    }
    for (const [label, t] of Object.entries(state.tokens)) {
      if (t.token) tokenIds.set(String(t.token).toLowerCase(), `<token:${label}>`);
    }
    for (const [label, c] of Object.entries(state.codes)) {
      if (c.code) codeIds.set(String(c.code), `<code:${label}>`);
    }
    const norm = (value, key = null) => {
      if (typeof value === "string") {
        const lower = value.toLowerCase();
        if (ids.has(lower)) return ids.get(lower);
        if (tokenIds.has(lower)) return tokenIds.get(lower);
        if (codeIds.has(value)) return codeIds.get(value);
        if (key === "code" && /^\d{6}$/.test(value)) return "<code>";
        if (VOLATILE_KEYS.has(key)) return "<volatile>";
        if (key === "created_at" || key === "createdAt" || ISO_RE.test(value)) return "<ts>";
        return value.replace(UUID_RE, (m) => (isFixed(m) ? m : "<uuid>"));
      }
      if (typeof value === "number") {
        if (key === "created_at" || key === "createdAt") return "<ts>";
        return value > 1e12 ? "<ts>" : value;
      }
      if (Array.isArray(value)) return value.map((v) => norm(v));
      if (value !== null && typeof value === "object") {
        const out = {};
        for (const [k, v] of Object.entries(value)) out[k] = norm(v, k);
        return out;
      }
      return value;
    };
    return norm(value, key);
  };
}

function projectSocketResult(result, norm) {
  const triples = [];
  const walk = (nodes) => {
    for (const node of nodes ?? []) {
      for (const rows of node?.data?.["datalog-result"]?.["join-rows"] ?? []) {
        for (const t of rows) triples.push(norm(t));
      }
      walk(node?.["child-nodes"]);
    }
  };
  walk(result);
  const sorted = triples.map((t) => norm(t));
  sorted.sort((x, y) => (canon(x) < canon(y) ? -1 : 1));
  const deduped = [...new Map(sorted.map((t) => [canon(t), t])).values()];
  return {
    triples: deduped,
    "page-info": norm(result?.[0]?.data?.["page-info"] ?? null),
    aggregate: norm(result?.[0]?.data?.aggregate ?? null),
  };
}

function httpOutcome(res, norm) {
  const body = res.body;
  const object = body && typeof body === "object" && !Array.isArray(body) ? body : null;
  return {
    kind: "http",
    status: res.status,
    type: object?.type ?? null,
    message: object?.message ?? object?.error ?? null,
    hint: norm(object?.hint ?? null),
    keys: object ? Object.keys(object).sort() : ["<non-json>"],
    body: object ? norm(object) : { "<non-json>": String(res.text ?? "").slice(0, 300) },
  };
}

// ---------------------------------------------------------------------------
// one run of the plan against one server

async function httpJson(url, method, pathName, { body, headers = {} } = {}) {
  const h = { ...headers };
  if (body !== undefined) h["content-type"] = "application/json";
  const res = await fetch(url + pathName, {
    method,
    headers: h,
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  let parsed = null;
  try {
    parsed = JSON.parse(text);
  } catch {
    parsed = null;
  }
  return { status: res.status, body: parsed, text };
}

async function setupSchema(url, appId, schema) {
  const c = connect(url, appId, "authfuzz:setup");
  await c.open;
  c.send({ "client-event-id": uuid(), op: "init", "app-id": appId, "__admin-token": adminToken, versions: { "@instantdb/core": "v0.21.0" } });
  const ok = await c.waitFor((m) => m.op === "init-ok" || m.op === "error");
  if (ok.op !== "init-ok") throw new Error(`schema setup init failed: ${JSON.stringify(ok).slice(0, 400)}`);
  c.takeNewFrames();
  const ceid = c.send({ "client-event-id": uuid(), op: "transact", "tx-steps": schema });
  const reply = await c.waitFor((m) => m["client-event-id"] === ceid && (m.op === "transact-ok" || m.op === "error"));
  c.close();
  if (reply.op !== "transact-ok") throw new Error(`schema setup failed: ${JSON.stringify(reply).slice(0, 400)}`);
}

async function runOn(serverName, plan) {
  const { url, db, appId } = SERVERS[serverName];
  await setupSchema(url, appId, plan.schema);
  psql(db, `UPDATE rules SET code = $rules$${JSON.stringify(plan.rules)}$rules$::jsonb WHERE app_id = '${appId}'; INSERT INTO rules (app_id, code) SELECT '${appId}', $rules$${JSON.stringify(plan.rules)}$rules$::jsonb WHERE NOT EXISTS (SELECT 1 FROM rules WHERE app_id = '${appId}')`);
  await new Promise((r) => setTimeout(r, RULES_SETTLE_MS));

  const state = { users: {}, tokens: {}, codes: {} };
  const adminHeaders = { "app-id": appId, authorization: `Bearer ${adminToken}` };
  const norm = normalizer(state);
  const outcomes = [];

  const tokenOf = (action) => {
    if (action.literalToken !== undefined) return action.literalToken;
    if (!action.tokenLabel) return undefined;
    const t = state.tokens[action.tokenLabel];
    if (!t) throw new Error(`[${serverName}] token state ${action.tokenLabel} missing`);
    return t.token;
  };
  const userOf = (label) => {
    const u = state.users[label];
    if (!u) throw new Error(`[${serverName}] user state ${label} missing`);
    return u;
  };
  const rememberToken = (label, user, token) => {
    if (label) state.tokens[label] = { token, user };
  };
  const rememberUser = (label, user) => {
    if (label) state.users[label] = { id: user?.id ?? state.users[label]?.id ?? null, email: user?.email ?? state.users[label]?.email ?? null };
  };
  const rememberCode = (label, code) => {
    if (label) state.codes[label] = { code, email: state.codes[label]?.email };
  };
  const markUserTokens = (label) => {
    for (const t of Object.values(state.tokens)) if (t.user === label) t.active = false;
  };

  for (const action of plan.script) {
    let outcome;
    switch (action.kind) {
      case "admin_refresh": {
        const body = {};
        if (action.email !== undefined) body.email = action.email;
        if (action.id !== undefined) body.id = action.id;
        if (action.idLabel) body.id = userOf(action.idLabel).id;
        if (action.extraFields !== undefined) body["extra-fields"] = action.extraFields;
        const res = await httpJson(url, "POST", "/admin/refresh_tokens", { body, headers: adminHeaders });
        if (res.body?.user) {
          rememberUser(action.assignUser, res.body.user);
          rememberToken(action.assignToken, action.assignUser, res.body.user.refresh_token);
        }
        outcome = httpOutcome(res, norm);
        break;
      }
      case "guest":
      case "admin_guest": {
        const body = { "app-id": appId };
        if (action.extraFields !== undefined) body["extra-fields"] = action.extraFields;
        const res = await httpJson(
          url,
          "POST",
          action.kind === "guest" ? "/runtime/auth/sign_in_guest" : "/admin/sign_in_guest",
          { body, headers: action.kind === "guest" ? {} : adminHeaders },
        );
        if (res.body?.user) {
          rememberUser(action.assignUser, res.body.user);
          rememberToken(action.assignToken, action.assignUser, res.body.user.refresh_token);
        }
        outcome = httpOutcome(res, norm);
        break;
      }
      case "admin_code":
      case "admin_send_code": {
        const pathName = action.kind === "admin_code" ? "/admin/magic_code" : "/admin/send_magic_code";
        const res = await httpJson(url, "POST", pathName, { body: { email: action.email }, headers: adminHeaders });
        if (typeof res.body?.code === "string") rememberCode(action.codeLabel, res.body.code);
        outcome = httpOutcome(res, norm);
        break;
      }
      case "expire_code": {
        const code = state.codes[action.codeLabel]?.code;
        if (!code) throw new Error(`[${serverName}] code state ${action.codeLabel} missing`);
        const hash = createHash("sha256").update(code).digest("hex");
        const email = String(action.email).replaceAll("'", "''");
        psql(
          db,
          `UPDATE triples SET created_at = created_at - 3 * 24 * 60 * 60 * 1000 WHERE app_id = '${appId}' AND entity_id IN (SELECT a.entity_id FROM triples a JOIN triples b ON b.app_id = a.app_id AND b.entity_id = a.entity_id WHERE a.app_id = '${appId}' AND a.value = to_jsonb('${email}'::text) AND b.value = to_jsonb('${hash}'::text))`,
        );
        outcome = { kind: "expire", code: `<code:${action.codeLabel}>`, email: action.email };
        break;
      }
      case "runtime_send_code": {
        const body = {};
        if (!action.missingEmail) body.email = action.email;
        body["app-id"] = appId;
        const res = await httpJson(url, "POST", "/runtime/auth/send_magic_code", { body });
        outcome = httpOutcome(res, norm);
        break;
      }
      case "verify_code":
      case "admin_verify_code": {
        const body = { email: action.email };
        if (action.codeLabel) body.code = state.codes[action.codeLabel]?.code;
        else if (action.literalCode !== undefined) body.code = action.literalCode;
        if (action.guestToken) body["refresh-token"] = tokenOf({ tokenLabel: action.guestToken });
        if (action.extraFields !== undefined) body["extra-fields"] = action.extraFields;
        if (action.kind === "verify_code") body["app-id"] = appId;
        const res = await httpJson(
          url,
          "POST",
          action.kind === "verify_code" ? "/runtime/auth/verify_magic_code" : "/admin/verify_magic_code",
          { body, headers: action.kind === "verify_code" ? {} : adminHeaders },
        );
        if (res.body?.user) {
          rememberUser(action.assignUser, res.body.user);
          rememberToken(action.assignToken, action.assignUser, res.body.user.refresh_token);
        }
        outcome = httpOutcome(res, norm);
        break;
      }
      case "verify_refresh": {
        const body = { "app-id": appId };
        if (!action.missing) body["refresh-token"] = tokenOf(action);
        const res = await httpJson(url, "POST", "/runtime/auth/verify_refresh_token", { body });
        outcome = httpOutcome(res, norm);
        break;
      }
      case "runtime_signout": {
        const body = {};
        if (!action.missingApp) body.app_id = appId;
        if (!action.missingToken) body.refresh_token = tokenOf(action);
        const res = await httpJson(url, "POST", "/runtime/signout", { body });
        outcome = httpOutcome(res, norm);
        break;
      }
      case "admin_signout": {
        const body = {};
        if (action.literal !== undefined) {
          body[action.by] = action.literal;
        } else if (action.by === "email") body.email = state.users[action.user]?.email;
        else if (action.by === "id") body.id = state.users[action.user]?.id;
        else if (action.by === "refresh_token") body.refresh_token = state.tokens[action.token]?.token;
        const res = await httpJson(url, "POST", "/admin/sign_out", { body, headers: adminHeaders });
        outcome = httpOutcome(res, norm);
        break;
      }
      case "users_get":
      case "users_delete": {
        let query = "";
        if (action.rawQuery) query = action.rawQuery;
        else if (action.by === "email") query = `?email=${encodeURIComponent(state.users[action.user]?.email ?? "")}`;
        else if (action.by === "id") query = `?id=${encodeURIComponent(state.users[action.user]?.id ?? "")}`;
        else if (action.by === "refresh_token") query = `?refresh_token=${encodeURIComponent(state.tokens[action.token]?.token ?? "")}`;
        const res = await httpJson(url, action.kind === "users_get" ? "GET" : "DELETE", `/admin/users${query}`, { headers: adminHeaders });
        outcome = httpOutcome(res, norm);
        break;
      }
      case "init": {
        const c = connect(url, appId, `${serverName}:authfuzz:init:${action.name}`);
        await c.open;
        const msg = { "client-event-id": uuid(), op: "init", "app-id": appId, versions: { "@instantdb/core": "v0.21.0" } };
        if (action.tokenLabel) msg["refresh-token"] = tokenOf(action);
        else if (action.literalToken !== undefined) msg["refresh-token"] = action.literalToken;
        if (action.admin) msg["__admin-token"] = adminToken;
        c.send(msg);
        const reply = await c.waitFor((m) => m.op === "init-ok" || m.op === "error");
        if (reply.op === "error") {
          outcome = {
            kind: "init",
            status: reply.status,
            type: reply.type,
            message: reply.message,
            hint: norm(reply.hint ?? null),
            keys: Object.keys(reply).sort(),
          };
        } else {
          const auth = reply.auth ?? {};
          outcome = {
            kind: "init",
            authKeys: Object.keys(auth).sort(),
            // legacy leaves admin? nil when no token was sent, rust sets
            // false: both mean "not an admin" to the client
            admin: Boolean(auth["admin?"] ?? false),
            app: norm(auth.app?.id ?? null),
            user: auth.user
              ? { id: norm(auth.user.id ?? null), email: auth.user.email ?? null }
              : null,
          };
          if (action.query) {
            const ceid = c.send({ "client-event-id": uuid(), op: "add-query", q: { $users: {} } });
            const result = await c.waitFor((m) => m["client-event-id"] === ceid && (m.op === "add-query-ok" || m.op === "error"));
            outcome.query =
              result.op === "add-query-ok"
                ? projectSocketResult(result.result, norm)
                : {
                    kind: "query-error",
                    status: result.status,
                    type: result.type,
                    message: result.message,
                    hint: norm(result.hint ?? null),
                  };
          }
        }
        c.close();
        break;
      }
      default:
        throw new Error(`unknown auth fuzz action ${action.kind}`);
    }
    outcomes.push({ name: action.name, ...outcome });
  }
  return { outcomes };
}

// ---------------------------------------------------------------------------
// both servers + comparison

const plan = buildPlan();
console.log(
  `fuzz-auth: seed=${seed} rounds=${rounds} actions=${plan.script.length} create=${JSON.stringify(plan.rules.$users.allow.create)} view=${JSON.stringify(plan.rules.$users.allow.view)}`,
);
console.log("running against legacy…");
const legacy = await runOn("legacy", plan);
console.log("running against rust…");
const rust = await runOn("rust", plan);

const allowlist = loadAllowlist(path.join(here, "allowed-divergences.json"), {
  prefixes: DIVERGENCE_PREFIXES.authfuzz,
});
let mismatches = 0;
let allowed = 0;

function compare(p, legacyValue, rustValue) {
  if (canon(legacyValue) === canon(rustValue)) return;
  const verdict = allowlist.check(p, legacyValue, rustValue);
  if (verdict.entry) {
    allowed++;
    console.log(`[ALLOWED] ${p}: ${verdict.differed.join(", ") || "(whole value)"}`);
    return;
  }
  mismatches++;
  const fd = firstDifference(legacyValue, rustValue);
  console.error(`DIVERGENCE ${p}`);
  if (verdict.rejected.length) for (const line of describeAllowVerdict(verdict)) console.error(line);
  console.error(`  at ${fd?.p || "(whole value)"}:`);
  console.error(`    legacy ${JSON.stringify(fd?.a)?.slice(0, 1200)}`);
  console.error(`    rust   ${JSON.stringify(fd?.b)?.slice(0, 1200)}`);
}

const n = Math.max(legacy.outcomes.length, rust.outcomes.length);
for (let i = 0; i < n; i++) {
  const l = legacy.outcomes[i];
  const r = rust.outcomes[i];
  const action = plan.script[i];
  if (l?.name !== r?.name) {
    mismatches++;
    console.error(`OUTCOME NAME MISMATCH at ${i}: legacy ${JSON.stringify(l?.name)} rust ${JSON.stringify(r?.name)}`);
    continue;
  }
  compare(`authfuzz:${action?.name ?? `outcome-${i}`}`, l, r);
}

const stale = allowlist.reportStale();
if (mismatches || stale) {
  console.error(`FUZZ-AUTH FAILED: ${mismatches} mismatches, ${stale} stale allowlist entries (seed ${seed})`);
  process.exit(1);
}
const httpActions = plan.script.filter((a) => !["init", "expire_code"].includes(a.kind)).length;
const inits = plan.script.filter((a) => a.kind === "init").length;
console.log(
  `FUZZ-AUTH PASSED: ${plan.script.length} actions (${httpActions} HTTP, ${inits} socket inits), ${allowed} allowed divergences (seed ${seed})`,
);
