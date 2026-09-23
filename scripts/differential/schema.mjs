// Schema parity: the rust server runs on a database built from the vendored
// legacy migrations (scripts/apply-migrations.sh), the legacy image migrates
// its own database with golang-migrate from whatever `main` was at image
// build time. This diffs the two live catalogs (tables, columns, constraints,
// indexes, enums, functions, triggers) so an upstream migration the vendored
// copy lacks shows up in CI instead of as a runtime error, and so nothing the
// rust side adds drifts away from the legacy shape. `schema-allowed.json`
// lists the accepted differences.
//
// Usage: node schema.mjs
// Env: LEGACY_DATABASE_URL, RUST_DATABASE_URL

import fs from "node:fs";
import path from "node:path";
import { execSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const DBS = {
  legacy: process.env.LEGACY_DATABASE_URL || "postgres://instant:instant@localhost:8890/instant",
  rust: process.env.RUST_DATABASE_URL || "postgres://instant:instant@localhost:5432/instant",
};

const CATALOG = {
  tables: `select table_name || ' ' || table_type from information_schema.tables where table_schema = 'public' order by 1`,
  columns: `select table_name || '.' || column_name || ' ' || udt_name || ' null=' || is_nullable || ' default=' || coalesce(column_default, '') from information_schema.columns where table_schema = 'public' order by 1`,
  constraints: `select conrelid::regclass || ' ' || conname || ' ' || pg_get_constraintdef(oid) from pg_constraint where connamespace = 'public'::regnamespace order by 1`,
  indexes: `select tablename || ' ' || indexname || ' ' || indexdef from pg_indexes where schemaname = 'public' order by 1`,
  enums: `select t.typname || ' ' || e.enumsortorder || ' ' || e.enumlabel from pg_type t join pg_enum e on e.enumtypid = t.oid join pg_namespace n on n.oid = t.typnamespace where n.nspname = 'public' order by 1`,
  functions: `select proname || '(' || pg_get_function_identity_arguments(oid) || ') ' || prorettype::regtype from pg_proc where pronamespace = 'public'::regnamespace order by 1`,
  triggers: `select event_object_table || ' ' || trigger_name || ' ' || action_timing || ' ' || event_manipulation || ' ' || action_statement from information_schema.triggers where trigger_schema = 'public' order by 1`,
};

function rows(db, sql) {
  const out = execSync(`psql "${db}" -At -v ON_ERROR_STOP=1 -c ${JSON.stringify(sql)}`, { encoding: "utf8" });
  return out.split("\n").filter(Boolean);
}

const allowed = fs.existsSync(path.join(here, "schema-allowed.json"))
  ? JSON.parse(fs.readFileSync(path.join(here, "schema-allowed.json"), "utf8"))
  : [];
const isAllowed = (category, line) => allowed.find((a) => a.category === category && new RegExp(a.pattern).test(line));

let failures = 0;
let allowedHits = 0;
let total = 0;
for (const [category, sql] of Object.entries(CATALOG)) {
  const legacy = new Set(rows(DBS.legacy, sql));
  const rust = new Set(rows(DBS.rust, sql));
  total += legacy.size;
  const onlyLegacy = [...legacy].filter((l) => !rust.has(l));
  const onlyRust = [...rust].filter((l) => !legacy.has(l));
  for (const [side, lines] of [["only legacy", onlyLegacy], ["only rust", onlyRust]]) {
    for (const line of lines) {
      if (isAllowed(category, line)) {
        allowedHits++;
        continue;
      }
      failures++;
      console.error(`SCHEMA DIFF ${category} ${side}: ${line}`);
    }
  }
  console.log(`${category.padEnd(12)} legacy=${legacy.size} rust=${rust.size} onlyLegacy=${onlyLegacy.length} onlyRust=${onlyRust.length}`);
}
if (failures) {
  console.error(`SCHEMA PARITY FAILED: ${failures} unexplained differences (add a citation to schema-allowed.json or port the migration)`);
  process.exit(1);
}
console.log(`SCHEMA PARITY PASSED: ${total} catalog rows match (${allowedHits} allowed differences)`);
