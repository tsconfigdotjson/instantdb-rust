#!/usr/bin/env node
// Compares the official SDK suites' vitest JSON reports between the legacy
// server and the rust server (written by run-suite.sh as
// <dir>/<suite>-<server>.json, plus <suite>-<server>.exit with vitest's exit
// status).
//
//   node compare.mjs [--dir sdk-suite-reports] [--suites core,cli]
//                    [--allowed scripts/sdk-suites/allowed.json]
//
// Lists every test with its outcome on each server and fails when
//   - a report is missing, unreadable, or has no tests,
//   - a test's outcome differs between legacy and rust (including a test
//     that exists on only one side),
//   - a test fails (or never finished) on both servers — a failure that
//     legacy shares is still a broken SDK path worth surfacing,
//   - vitest exited non-zero with no failing test in its report (unhandled
//     errors are not recorded in the JSON report),
// unless the test is listed in the allowlist file as
// {"suite": "...", "test": "<id as printed>", "reason": "..."}. An entry with
// "server": "legacy" (or "rust") only excuses that server failing while the
// other passes — e.g. an upstream race in legacy that rust must not share.
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const SERVERS = ['legacy', 'rust'];

function parseArgs(argv) {
  const opts = {
    dir: path.resolve(HERE, '../../sdk-suite-reports'),
    suites: ['core', 'cli'],
    allowed: path.join(HERE, 'allowed.json'),
  };
  for (let i = 0; i < argv.length; i++) {
    const [flag, inline] = argv[i].split(/=(.*)/s, 2);
    const value = () => (inline !== undefined ? inline : argv[++i]);
    if (flag === '--dir') opts.dir = path.resolve(value());
    else if (flag === '--suites')
      opts.suites = value().split(',').map((s) => s.trim()).filter(Boolean);
    else if (flag === '--allowed') opts.allowed = path.resolve(value());
    else {
      console.error(`unknown argument ${argv[i]}`);
      process.exit(2);
    }
  }
  return opts;
}

function loadAllowlist(file) {
  if (!fs.existsSync(file)) return [];
  const list = JSON.parse(fs.readFileSync(file, 'utf8'));
  if (!Array.isArray(list)) throw new Error(`${file}: expected a JSON array`);
  for (const e of list) {
    if (!e || typeof e.suite !== 'string' || typeof e.test !== 'string' ||
        typeof e.reason !== 'string' || !e.reason.trim()) {
      throw new Error(
        `${file}: every entry needs string "suite", "test" and a non-empty "reason": ${JSON.stringify(e)}`,
      );
    }
    if (e.server !== undefined && !SERVERS.includes(e.server)) {
      throw new Error(`${file}: "server" must be one of ${SERVERS.join(', ')}: ${JSON.stringify(e)}`);
    }
  }
  return list;
}

// passed | failed | skipped | incomplete (vitest's "pending": never finished)
function outcome(status) {
  switch (status) {
    case 'passed':
      return 'passed';
    case 'failed':
      return 'failed';
    case 'skipped':
    case 'todo':
    case 'disabled':
      return 'skipped';
    default:
      return 'incomplete';
  }
}

const isBad = (o) => o === 'failed' || o === 'incomplete';

// file paths are absolute; key tests by the path below the package dir
function relFile(abs) {
  const norm = abs.replace(/\\/g, '/');
  const m = norm.match(/\/packages\/[^/]+\/(.*)$/);
  return m ? m[1] : norm;
}

function firstLine(s) {
  return String(s ?? '').split('\n').find((l) => l.trim()) ?? '';
}

// -> { tests: Map<id, {outcome, message}>, error?: string }
function loadReport(dir, suite, server) {
  const file = path.join(dir, `${suite}-${server}.json`);
  const exitFile = path.join(dir, `${suite}-${server}.exit`);
  const exitCode = fs.existsSync(exitFile)
    ? Number(fs.readFileSync(exitFile, 'utf8').trim())
    : null;
  if (!fs.existsSync(file)) {
    return { tests: new Map(), error: `no report at ${file} (vitest exit ${exitCode ?? 'unknown'})` };
  }
  let report;
  try {
    report = JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch (e) {
    return { tests: new Map(), error: `unreadable report ${file}: ${e.message}` };
  }
  const tests = new Map();
  const add = (id, entry) => {
    let key = id;
    for (let n = 2; tests.has(key); n++) key = `${id} #${n}`;
    tests.set(key, entry);
  };
  for (const fileResult of report.testResults ?? []) {
    const rel = relFile(fileResult.name ?? '?');
    // a module-level error (import failure, failing beforeAll, ...) shows up
    // as the file's message; its tests may be reported as skipped
    add(`${rel} > (file)`, fileResult.message
      ? { outcome: 'failed', message: firstLine(fileResult.message) }
      : { outcome: 'passed', message: '' });
    for (const t of fileResult.assertionResults ?? []) {
      const id = [rel, ...(t.ancestorTitles ?? []), t.title].join(' > ');
      add(id, {
        outcome: outcome(t.status),
        message: firstLine((t.failureMessages ?? [])[0]),
      });
    }
  }
  const counted = [...tests.keys()].filter((k) => !k.endsWith(' > (file)'));
  if (counted.length === 0) {
    return { tests, error: `report ${file} contains no tests (vitest exit ${exitCode ?? 'unknown'})` };
  }
  if (exitCode !== null && exitCode !== 0 &&
      ![...tests.values()].some((t) => isBad(t.outcome))) {
    add('(run) vitest exit status', {
      outcome: 'failed',
      message: `vitest exited ${exitCode} with no failing test in its report (unhandled error?)`,
    });
  }
  return { tests };
}

function pad(s, n) {
  s = String(s);
  return s.length >= n ? s : s + ' '.repeat(n - s.length);
}

function main() {
  const opts = parseArgs(process.argv.slice(2));
  const allowlist = loadAllowlist(opts.allowed);
  const usedAllow = new Set();
  const problems = []; // {suite, id, kind, detail}
  const summary = [];
  const md = ['## Official SDK suites: legacy vs rust', ''];

  for (const suite of opts.suites) {
    const reports = Object.fromEntries(
      SERVERS.map((s) => [s, loadReport(opts.dir, suite, s)]),
    );
    console.log(`\n=== suite: ${suite} ===`);
    for (const s of SERVERS) {
      if (reports[s].error) {
        console.log(`  !! ${s}: ${reports[s].error}`);
        problems.push({ suite, id: `(report: ${s})`, kind: 'missing report', detail: reports[s].error });
      }
    }

    const ids = [...new Set(SERVERS.flatMap((s) => [...reports[s].tests.keys()]))].sort();
    const counts = Object.fromEntries(
      SERVERS.map((s) => [s, { passed: 0, failed: 0, skipped: 0, incomplete: 0, absent: 0 }]),
    );
    let same = 0;
    md.push(`### ${suite}`, '', '| test | legacy | rust | |', '|---|---|---|---|');
    console.log(`  ${pad('legacy', 11)}${pad('rust', 11)}test`);
    for (const id of ids) {
      const o = Object.fromEntries(
        SERVERS.map((s) => [s, reports[s].tests.get(id)?.outcome ?? 'absent']),
      );
      for (const s of SERVERS) counts[s][o[s]]++;
      let kind = null;
      if (o.legacy !== o.rust) kind = 'outcome differs';
      else if (isBad(o.legacy)) kind = 'fails on both';
      else same++;

      let mark = '';
      if (kind) {
        const allow = allowlist.find((e) => e.suite === suite && e.test === id &&
          (e.server === undefined ||
            (isBad(o[e.server]) && SERVERS.every((s) => s === e.server || o[s] === 'passed'))));
        if (allow) {
          usedAllow.add(allow);
          mark = `allowed: ${allow.reason}`;
        } else {
          const detail = SERVERS
            .map((s) => reports[s].tests.get(id))
            .map((t, i) => (t?.message ? `${SERVERS[i]}: ${t.message}` : null))
            .filter(Boolean)
            .join(' | ');
          problems.push({ suite, id, kind, detail });
          mark = `<< ${kind.toUpperCase()}`;
        }
      }
      console.log(`  ${pad(o.legacy, 11)}${pad(o.rust, 11)}${id}${mark ? `   ${mark}` : ''}`);
      md.push(`| ${id.replace(/\|/g, '\\|')} | ${o.legacy} | ${o.rust} | ${mark.replace(/\|/g, '\\|')} |`);
    }
    md.push('');
    summary.push({ suite, total: ids.length, same, counts });
  }

  for (const e of allowlist) {
    if (!usedAllow.has(e)) {
      console.log(`\nnote: allowlist entry matched nothing (stale?): ${e.suite} :: ${e.test}`);
    }
  }

  console.log('\n=== summary ===');
  const head = `${pad('suite', 8)}${pad('rows', 6)}${pad('agree', 7)}` +
    SERVERS.map((s) => pad(`${s} pass/fail/skip/incomplete/absent`, 42)).join('');
  console.log(head);
  md.push('### summary', '', '| suite | rows | agree | legacy p/f/s/i/a | rust p/f/s/i/a |', '|---|---|---|---|---|');
  for (const r of summary) {
    const cells = SERVERS.map((s) => {
      const c = r.counts[s];
      return `${c.passed}/${c.failed}/${c.skipped}/${c.incomplete}/${c.absent}`;
    });
    console.log(`${pad(r.suite, 8)}${pad(r.total, 6)}${pad(r.same, 7)}${cells.map((c) => pad(c, 42)).join('')}`);
    md.push(`| ${r.suite} | ${r.total} | ${r.same} | ${cells.join(' | ')} |`);
  }

  if (problems.length) {
    console.log(`\n=== ${problems.length} problem(s) ===`);
    md.push('', `### ${problems.length} problem(s)`, '');
    for (const p of problems) {
      console.log(`[${p.suite}] ${p.kind}: ${p.id}${p.detail ? `\n    ${p.detail}` : ''}`);
      md.push(`- **${p.suite}** ${p.kind}: \`${p.id}\`${p.detail ? ` — ${p.detail}` : ''}`);
    }
  } else {
    console.log('\nSDK SUITES: legacy and rust agree, no failures');
    md.push('', 'legacy and rust agree, no failures');
  }

  if (process.env.GITHUB_STEP_SUMMARY) {
    try {
      fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY, md.join('\n') + '\n');
    } catch {}
  }
  process.exit(problems.length ? 1 : 0);
}

main();
