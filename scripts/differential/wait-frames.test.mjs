// Regression test for the presence seed 110 harness race: waiting for a
// re-join's full snapshot must ignore matching frames from before the join.
import assert from "node:assert/strict";
import { firstFrameAfter, sortFramesByKey } from "./lib.mjs";

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
