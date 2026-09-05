// Aggregates the coverage lines written by coverage-hook.mjs (COVERAGE_FILE)
// against surface.json and enforces no regression against the committed
// coverage-baseline.json.
//
//   node coverage.mjs --report <file>   per-group coverage + the uncovered list
//   node coverage.mjs --check <file>    also fail if a baseline-covered id is missing
//   node coverage.mjs --write <file>    rewrite coverage-baseline.json from this run
//
// Groups `demo`, `health`, `ws-internal` and `cel-internal` are listed in the
// manifest but never counted (dev tooling, server-enqueued ops, optimizer
// internals), and the hosted-only items of out-of-scope.json (billing,
// backups, sunset, Postmark, the operators' reports) are counted separately.

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const mode = process.argv[2];
const file = process.argv[3];
if (!mode || !file) throw new Error("usage: node coverage.mjs --report|--check|--write <coverage-file>");

const manifest = JSON.parse(fs.readFileSync(path.join(here, "surface.json"), "utf8"));
const NOT_COUNTED = new Set(["demo", "health", "ws-internal", "cel-internal"]);
const covered = new Set();
const lines = fs.existsSync(file) ? fs.readFileSync(file, "utf8").split("\n").filter(Boolean) : [];
for (const l of lines) for (const id of JSON.parse(l).covered) covered.add(id);

const outOfScope = JSON.parse(fs.readFileSync(path.join(here, "out-of-scope.json"), "utf8")).items;
const outOfScopeIds = new Set(outOfScope.map((i) => i.id));
const manifestIds = new Set(manifest.items.map((i) => i.id));
const unknown = outOfScope.filter((i) => !manifestIds.has(i.id));
if (unknown.length) {
  console.error(`out-of-scope.json names ids that are not in surface.json: ${unknown.map((i) => i.id).join(", ")}`);
  process.exit(1);
}
const counted = manifest.items.filter((i) => !NOT_COUNTED.has(i.group) && !outOfScopeIds.has(i.id));
const byGroup = {};
for (const i of counted) {
  const g = (byGroup[i.group] ??= { total: 0, covered: 0, missing: [] });
  g.total++;
  if (covered.has(i.id)) g.covered++;
  else g.missing.push(i.id);
}
const total = counted.length;
const hit = counted.filter((i) => covered.has(i.id)).length;
console.log(`legacy surface coverage: ${hit}/${total} (${((100 * hit) / total).toFixed(1)}%); ${outOfScopeIds.size} hosted-only items out of scope (out-of-scope.json)`);
const exercisedOutOfScope = [...outOfScopeIds].filter((id) => covered.has(id));
if (exercisedOutOfScope.length) console.log(`  out-of-scope items a run exercised (drop them from out-of-scope.json): ${exercisedOutOfScope.join(", ")}`);
for (const [g, v] of Object.entries(byGroup).sort()) {
  console.log(`  ${g.padEnd(11)} ${String(v.covered).padStart(3)}/${String(v.total).padEnd(3)}${v.missing.length ? "  missing: " + v.missing.join(", ") : ""}`);
}

// the covered set on one line, so a CI log can seed coverage-baseline.json
console.log(`COVERED_IDS ${JSON.stringify([...covered].sort())}`);

const baselinePath = path.join(here, "coverage-baseline.json");
if (mode === "--write") {
  fs.writeFileSync(baselinePath, JSON.stringify({ covered: [...covered].sort() }, null, 1) + "\n");
  console.log(`wrote ${covered.size} covered ids to coverage-baseline.json`);
} else if (mode === "--check") {
  const baseline = fs.existsSync(baselinePath) ? JSON.parse(fs.readFileSync(baselinePath, "utf8")).covered : [];
  const lost = baseline.filter((id) => !covered.has(id));
  const gained = [...covered].filter((id) => !baseline.includes(id));
  if (gained.length) console.log(`newly covered (add to the baseline with --write): ${gained.join(", ")}`);
  if (lost.length) {
    console.error(`COVERAGE REGRESSION: ${lost.length} baseline-covered surface items were not exercised: ${lost.join(", ")}`);
    process.exit(1);
  }
  console.log(`coverage baseline holds (${baseline.length} ids)`);
}
