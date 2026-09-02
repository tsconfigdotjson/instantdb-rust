// End-to-end test of the official `instant-cli` against this server
// (issue #6): `push` (schema + perms), idempotent re-push, `pull` into a
// fresh project, and the failing-indexing-job path (duplicate values under a
// new unique constraint, missing values under a new required constraint).
//
// Usage: node scripts/cli-test.mjs <app-id> <admin-token>
// Env:   API_URL (default http://localhost:8888)
// Needs the legacy CLI built once:
//   cd LEGACY/client && npx pnpm@10.2.0 install --frozen-lockfile --filter 'instant-cli...' \
//     && npx pnpm@10.2.0 --filter '@instantdb/version' --filter '@instantdb/core' \
//          --filter '@instantdb/platform' --filter 'instant-cli' run build

import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const appId = process.argv[2];
const adminToken = process.argv[3];
if (!appId || !adminToken) throw new Error("usage: node cli-test.mjs <app-id> <admin-token>");
const API = process.env.API_URL || "http://localhost:8888";

const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const CLI = path.join(repo, "LEGACY/client/packages/cli/bin/index.js");
const CORE = path.join(repo, "LEGACY/client/packages/core");
if (!fs.existsSync(path.join(repo, "LEGACY/client/packages/cli/dist/index.js")))
  throw new Error("instant-cli is not built; see the header of this script");

let passed = 0;
const assert = (c, m) => {
  if (!c) throw new Error("ASSERT FAILED: " + m);
  passed++;
};
const group = (name) => console.log(`\n== ${name} ==`);

// a throwaway project directory with @instantdb/core resolvable
function project(name) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), `instant-cli-${name}-`));
  fs.mkdirSync(path.join(dir, "node_modules/@instantdb"), { recursive: true });
  fs.symlinkSync(CORE, path.join(dir, "node_modules/@instantdb/core"));
  fs.writeFileSync(
    path.join(dir, "package.json"),
    JSON.stringify({ name, type: "module", dependencies: { "@instantdb/core": "*" } }),
  );
  return dir;
}

function cli(dir, args) {
  let out;
  let status = 0;
  try {
    out = execFileSync("node", [CLI, ...args, "-y"], {
      cwd: dir,
      env: {
        ...process.env,
        INSTANT_CLI_API_URI: API,
        INSTANT_APP_ID: appId,
        INSTANT_APP_ADMIN_TOKEN: adminToken,
        FORCE_COLOR: "0",
        CI: "1",
      },
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    });
  } catch (e) {
    out = `${e.stdout ?? ""}${e.stderr ?? ""}`;
    status = e.status ?? 1;
  }
  // strip spinner redraws / ansi
  out = out.replace(/\x1b\[[0-9;?]*[A-Za-z]/g, "");
  return { out, status };
}

const schemaTs = (titleUnique, priorityRequired) => `
import { i } from "@instantdb/core";

const _schema = i.schema({
  entities: {
    $users: i.entity({
      email: i.string().unique().indexed().optional(),
      nickname: i.string().optional(),
    }),
    todos: i.entity({
      title: i.string()${titleUnique ? ".unique()" : ""}.indexed(),
      done: i.boolean(),
      priority: i.number()${priorityRequired ? "" : ".optional()"},
      slug: i.string().unique().optional(),
    }),
    projects: i.entity({
      name: i.string(),
    }),
  },
  links: {
    todosOwner: {
      forward: { on: "todos", has: "one", label: "owner" },
      reverse: { on: "$users", has: "many", label: "todos" },
    },
    projectTodos: {
      forward: { on: "projects", has: "many", label: "todos" },
      reverse: { on: "todos", has: "one", label: "project", onDelete: "cascade" },
    },
  },
  rooms: {},
});

type _AppSchema = typeof _schema;
interface AppSchema extends _AppSchema {}
const schema: AppSchema = _schema;

export type { AppSchema };
export default schema;
`;

const permsTs = `
import type { InstantRules } from "@instantdb/core";

const rules = {
  todos: {
    bind: ["isOwner", "auth.id != null && auth.id in data.ref('owner.id')"],
    allow: { view: "isOwner", create: "auth.id != null", update: "isOwner", delete: "isOwner" },
  },
  $users: { allow: { view: "auth.id == data.id" } },
} satisfies InstantRules;

export default rules;
`;

async function dash(p) {
  const res = await fetch(`${API}/dash/apps/${appId}${p}`, {
    headers: { authorization: `Bearer ${adminToken}` },
  });
  return res.json();
}

// ---------------------------------------------------------------------------

group("push schema + perms");
const src = project("src");
fs.writeFileSync(path.join(src, "instant.schema.ts"), schemaTs(false, false));
fs.writeFileSync(path.join(src, "instant.perms.ts"), permsTs);
let r = cli(src, ["push"]);
assert(r.status === 0, `push exits 0: ${r.out.slice(-800)}`);
assert(/Schema updated!/.test(r.out), "schema updated");
assert(/Permissions updated!/.test(r.out), "perms updated");
assert(/Finished adding uniqueness constraint to todos\.slug/.test(r.out), "unique job reported");
assert(/Finished adding index to todos\.title/.test(r.out), "index job reported");
assert(/Finished adding required constraint to todos\.done/.test(r.out), "required job reported");
assert(!/Some steps failed/.test(r.out), "no failed steps");

const pulled = await dash("/schema/pull");
const todos = pulled.schema.blobs.todos;
assert(todos.title["index?"] === true && todos.title["checked-data-type"] === "string", "todos.title indexed string");
assert(todos.slug["unique?"] === true && todos.slug["required?"] === false, "todos.slug unique optional");
assert(todos.done["required?"] === true && todos.done["checked-data-type"] === "boolean", "todos.done required boolean");
const link = pulled.schema.refs['["projects" "todos" "todos" "project"]'];
assert(link && link.cardinality === "many" && link["unique?"] === true && link["on-delete-reverse"] === "cascade", "projects.todos link");
assert(pulled.schema.blobs.$users.nickname, "$users.nickname added");
const perms = await dash("/perms/pull");
assert(perms.perms?.todos?.allow?.update === "isOwner", "rules stored");

group("re-push is a no-op");
r = cli(src, ["push"]);
assert(r.status === 0, "re-push exits 0");
assert(/No schema changes to apply!/.test(r.out), "no schema changes");
assert(/No perms changes to apply!/.test(r.out), "no perms changes");

group("pull into a fresh project");
const dst = project("dst");
r = cli(dst, ["pull"]);
assert(r.status === 0, `pull exits 0: ${r.out.slice(-800)}`);
const pulledSchema = fs.readFileSync(path.join(dst, "instant.schema.ts"), "utf8");
const pulledPerms = fs.readFileSync(path.join(dst, "instant.perms.ts"), "utf8");
assert(/todos: i\.entity\(\{/.test(pulledSchema), "schema file has todos");
assert(/slug: i\.string\(\)\.unique\(\)\.optional\(\)/.test(pulledSchema), "slug def round-trips");
assert(/title: i\.string\(\)\.indexed\(\)/.test(pulledSchema), "title def round-trips");
assert(/projectsTodos: \{/.test(pulledSchema), "link round-trips");
assert(/onDelete: "cascade"/.test(pulledSchema), "cascade round-trips");
assert(/isOwner/.test(pulledPerms), "perms file round-trips");
// pushing the pulled files back is a no-op too
r = cli(dst, ["push"]);
assert(/No schema changes to apply!/.test(r.out) && /No perms changes to apply!/.test(r.out), "pulled files push as no-op");

group("failing indexing jobs are reported");
const seed = await fetch(`${API}/admin/transact`, {
  method: "POST",
  headers: { "content-type": "application/json", "app-id": appId, authorization: `Bearer ${adminToken}` },
  body: JSON.stringify({
    steps: [
      ["update", "todos", "11111111-1111-4111-8111-111111111111", { title: "dup", done: false }],
      ["update", "todos", "22222222-2222-4222-8222-222222222222", { title: "dup", done: true, priority: 3 }],
    ],
  }),
});
assert(seed.status === 200, "seeded todos");
fs.writeFileSync(path.join(src, "instant.schema.ts"), schemaTs(true, true));
r = cli(src, ["push", "schema"]);
assert(r.status !== 0, "push with conflicting data exits non-zero");
assert(/INVALID DATA adding uniqueness constraint to todos\.title/.test(r.out), "unique failure reported");
assert(/Found multiple entities with value "dup"/.test(r.out), "duplicate value shown");
assert(/INVALID DATA adding required constraint to todos\.priority/.test(r.out), "required failure reported");
assert(/11111111-1111-4111-8111-111111111111/.test(r.out), "offending entity listed");
assert(/Some steps failed while updating schema/.test(r.out), "failure summary");
const after = await dash("/schema/pull");
assert(after.schema.blobs.todos.title["unique?"] === false, "failed unique job left the attr non-unique");
assert(after.schema.blobs.todos.priority["required?"] === false, "failed required job left the attr optional");

console.log(`\nCLI TEST PASSED (${passed} assertions)`);
