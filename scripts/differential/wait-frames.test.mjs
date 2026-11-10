// Regression test for the presence seed 110 harness race: waiting for a
// re-join's full snapshot must ignore matching frames from before the join.
import assert from "node:assert/strict";
import { firstFrameAfter } from "./lib.mjs";

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
