// Regression test for the nightly fuzz-perms seed 9 failure (run 45620798435,
// op 73): legacy sent session U1 a typeless 500 refresh error because another
// session's shared datalog computation was cancelled (lib.mjs
// legacyCancelledRefresh). Pins what the rule excuses and, above all, what it
// must keep failing: any rust refresh error, a rust session that missed a
// refresh, other legacy errors, and subscription differences legacy's own
// fresh probe does not explain.
import assert from "node:assert/strict";
import { isLegacyCancelledRefreshError, legacyCancelledRefresh } from "./lib.mjs";

const SESSIONS = ["G", "U1", "U2", "ADM"];
const YIKES = "Yikes, something broke on our end! Sorry about that.";
const messages = new WeakMap();
const err = (type, status, message) => {
  const e = { type, status };
  messages.set(e, message);
  return e;
};
const messageOf = (e) => messages.get(e);

const KEY = '{"pz":{}}';
const OLD = { triples: [["e1", "p1", "a"]] };
const NEW = { triples: [["e1", "p1", "b"]] };
// one checkpoint: every session holds KEY; `over` patches one session
const checkpoint = (over = {}) => ({
  i: 73,
  kind: "checkpoint",
  state: Object.fromEntries(SESSIONS.map((s) => [s, { subs: { [KEY]: NEW }, refreshErrors: [], ...(over[s] ?? {}) }])),
});
const probes = (over = {}) => Object.fromEntries(SESSIONS.map((s) => [s, { [KEY]: over[s] ?? NEW }]));
const check = (l, r, lf = probes(), rf = probes()) => legacyCancelledRefresh(l, r, lf, rf, messageOf, SESSIONS);

// the seed 9 shape: legacy U1 got the typeless 500 and its subscription
// stayed behind; legacy's fresh probe and rust both have the new result
const seed9 = checkpoint({ U1: { subs: { [KEY]: OLD }, refreshErrors: [err(null, 500, YIKES)] } });
assert.equal(check(seed9, checkpoint()), true, "seed 9 shape is excused");
// the refresh failed but the session's result had not changed yet
assert.equal(check(checkpoint({ U1: { refreshErrors: [err(null, 500, YIKES)] } }), checkpoint()), true);
// the typed flavours of the same cancellation
for (const [type, msg] of [["sql-exception", "SQL Exception: connection-failure"], ["timeout", "The query took too long to complete."]]) {
  assert.equal(check(checkpoint({ U2: { refreshErrors: [err(type, 500, msg)] } }), checkpoint()), true, type);
}
// a matching error pair that both servers sent stays a match next to it
assert.equal(
  check(
    checkpoint({ U1: { refreshErrors: [err("permission-denied", 400, "x"), err(null, 500, YIKES)] } }),
    checkpoint({ U1: { refreshErrors: [err("permission-denied", 400, "x")] } }),
  ),
  true,
);
console.log("cancelled-refresh: seed 9 shape excused");

// rust's own refresh errors are never excused: a rust typeless 500, timeout
// or handler timeout (a deadlocked refresh) still fails
for (const re of [err(null, 500, YIKES), err("timeout", 500, "The query took too long to complete."), err("operation-timed-out", 500, "Operation timed out: handle-receive")]) {
  assert.equal(check(checkpoint(), checkpoint({ U1: { refreshErrors: [re] } })), false, `rust ${re.type}`);
  assert.equal(check(seed9, checkpoint({ U1: { refreshErrors: [re] } })), false, `rust ${re.type} beside legacy's`);
}
// other legacy errors are not this bug
for (const le of [
  err(null, 500, "Something else broke"),
  err(null, 503, YIKES),
  err("operation-timed-out", 500, "Operation timed out: handle-receive"),
  err("sql-exception", 500, "SQL Exception: unique-violation"),
  err("permission-denied", 400, "Permission denied"),
  err(null, 500, undefined),
]) {
  assert.equal(check(checkpoint({ U1: { refreshErrors: [le] } }), checkpoint()), false, `legacy ${le.type} ${le.status} ${messageOf(le)}`);
}
assert.equal(isLegacyCancelledRefreshError({ type: null, status: 500 }, YIKES), true);
assert.equal(isLegacyCancelledRefreshError({ type: undefined, status: 500 }, YIKES), true);
// rust's session missed the refresh legacy failed: rust's fresh probe moved on
assert.equal(check(seed9, checkpoint({ U1: { subs: { [KEY]: OLD } } }), probes(), probes({ U1: NEW })), false, "stale rust session");
assert.equal(check(seed9, checkpoint(), probes(), probes({ U1: OLD })), false, "rust probe disagrees with rust session");
// legacy's stale subscription is not explained by its own fresh probe
assert.equal(check(seed9, checkpoint(), probes({ U1: OLD })), false, "legacy probe disagrees with rust");
// a difference on another session must still be legacy staleness
assert.equal(check(checkpoint({ U1: { refreshErrors: [err(null, 500, YIKES)] }, U2: { subs: { [KEY]: OLD } } }), checkpoint(), probes({ U2: OLD })), false);
// different subscription sets
assert.equal(check(checkpoint({ U1: { subs: {}, refreshErrors: [err(null, 500, YIKES)] } }), checkpoint()), false, "missing sub");
// nothing to excuse: not this rule's shape
assert.equal(check(checkpoint(), checkpoint()), false);
// only checkpoints
assert.equal(check({ ...seed9, kind: "query" }, { ...checkpoint(), kind: "query" }), false);
console.log("cancelled-refresh: rust errors, stale rust sessions and other differences still fail");
