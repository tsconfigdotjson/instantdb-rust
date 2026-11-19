// Regression test for the presence seed 110 harness race: waiting for a
// re-join's full snapshot must ignore matching frames from before the join.
import assert from "node:assert/strict";
import { annotateSyncChanges, firstFrameAfter, sortFramesByKey, syncTargets } from "./lib.mjs";

const oldSnapshot = { op: "refresh-presence", "room-id": "" };
const other = { op: "join-room-ok", "room-id": "" };
const newSnapshot = { op: "refresh-presence", "room-id": "" };
const frames = [oldSnapshot, other, newSnapshot];
const isSnapshot = (m) => m.op === "refresh-presence" && m["room-id"] === "";

assert.equal(firstFrameAfter(frames, 0, isSnapshot), oldSnapshot);
assert.equal(firstFrameAfter(frames, 1, isSnapshot), newSnapshot);
assert.equal(firstFrameAfter(frames, 3, isSnapshot), undefined);
assert.equal(firstFrameAfter(frames, 99, isSnapshot), undefined);
console.log("wait-frames: fresh frame selection passed");

// Sync frames for different subscriptions are independent; their relative
// order is not client-visible. Canonicalizing by subscription must keep each
// subscription's own frame order (seed 308 had two live subs with different
// frozen attr sets, so frame 1 could be either sub on either server).
const syncFrames = [
  { sub: "b", seq: 1 },
  { sub: "a", seq: 1 },
  { sub: "b", seq: 2 },
  { sub: "a", seq: 2 },
];
assert.deepEqual(
  sortFramesByKey(syncFrames, (f) => f.sub),
  [
    { sub: "a", seq: 1 },
    { sub: "a", seq: 2 },
    { sub: "b", seq: 1 },
    { sub: "b", seq: 2 },
  ],
);
assert.deepEqual(sortFramesByKey([], () => "x"), []);
console.log("wait-frames: per-subscription frame grouping passed");

// Committed sync tx entries carry an `etypes` array. The old target filter
// read `c.etype` (always undefined), so the burst never waited for its docs
// subscriptions and compared whatever happened to arrive within 180ms.
const committed = [
  { txId: 1, etypes: ["docs"] },
  { txId: 2, etypes: ["notes"] },
  { txId: 3, etypes: ["docs", "notes"] },
  { txId: 4, etypes: [] },
];
const docsSub = { active: true, etype: "docs", txId: 0 };
const notesSub = { active: true, etype: "notes", txId: 2 };
const otherSub = { active: true, etype: "other", txId: 0 };
const inactiveDocs = { active: false, etype: "docs", txId: 0 };
assert.deepEqual(
  syncTargets(committed, [docsSub, notesSub, otherSub, inactiveDocs], ["docs"]),
  [{ sub: docsSub, target: 3 }],
);
assert.deepEqual(
  syncTargets(committed, [docsSub, notesSub, otherSub]),
  [
    { sub: docsSub, target: 3 },
    { sub: notesSub, target: 3 },
  ],
);
const burstCommitted = Array.from({ length: 130 }, (_, i) => ({ txId: i + 1, etypes: ["docs"] }));
assert.deepEqual(syncTargets(burstCommitted, [docsSub], ["docs"]), [{ sub: docsSub, target: 130 }]);
console.log("wait-frames: sync target namespace matching passed");

// A no-op tx and a tx that only changes a many attr outside the sync topic
// produce no frame, so neither may become a wait target. The title change
// after them does.
const syncedDocsSub = { active: true, etype: "docs", txId: 0, syncAttrs: ["id", "title"] };
const attrCommitted = [
  { txId: 5, etypes: ["docs"], attrs: ["tags"] },
  { txId: 6, etypes: ["docs"], attrs: [] },
  { txId: 7, etypes: ["docs"], attrs: ["title"] },
];
assert.deepEqual(syncTargets(attrCommitted, [syncedDocsSub], ["docs"]), [
  { sub: syncedDocsSub, target: 7 },
]);

// Seed 308 tx-51-type shape: after the first retraction, repeating it and
// setting the same title/rank changes nothing.
const plan = [
  {
    kind: "tx",
    etypes: ["docs"],
    steps: [
      ["add-attr", { id: "id" }],
      ["add-attr", { id: "title" }],
      ["add-attr", { id: "tags" }],
      ["add-triple", "e", "id", "e"],
      ["add-triple", "e", "title", "Zed"],
      ["add-triple", "e", "rank", -3],
      ["add-triple", "e", "due", 1704067200000],
    ],
  },
  { kind: "start", session: "S1" },
  {
    kind: "tx",
    etypes: ["docs"],
    steps: [
      ["add-triple", "e", "title", "Zed"],
      ["add-triple", "e", "rank", -3],
      ["retract-triple", "e", "due", 1704067200000],
    ],
  },
  {
    kind: "tx",
    etypes: ["docs"],
    steps: [
      ["add-triple", "e", "title", "Zed"],
      ["add-triple", "e", "rank", -3],
      ["retract-triple", "e", "due", 1704067200000],
    ],
  },
  { kind: "tx", etypes: ["docs"], steps: [["add-triple", "e", "tags", "tag-1"]] },
];
annotateSyncChanges(plan, ["tags"]);
assert.deepEqual(plan[1].syncAttrs.sort(), ["id", "title"]);
assert.deepEqual(plan[2].changedAttrs.sort(), ["due"]);
assert.deepEqual(plan[3].changedAttrs, []);
assert.deepEqual(plan[4].changedAttrs, ["tags"]);
console.log("wait-frames: no-op sync targets passed");
