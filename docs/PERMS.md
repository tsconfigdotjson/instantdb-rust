# InstantDB Permission System (CEL Rules)

Reference for the Rust reimplementation, derived from the legacy Clojure server.
All file paths are relative to `LEGACY/server/src/instant/` unless noted; the
client file is `LEGACY/client/packages/core/src/rulesTypes.ts`.

Key source files:

- `model/rule.clj` — rule storage, program lookup ($default chains), bind expansion, validation, system-table fallbacks.
- `db/cel.clj` — CEL compilers/runtime, custom functions, `data.ref` batching, the "rule-wheres" CEL→where-clause translator.
- `db/cel_builder.clj` — helpers to construct CEL ASTs programmatically (used elsewhere; not needed for baseline).
- `db/permissioned_transaction.clj` — transact-time checks.
- `db/instaql.clj` (lines ~1816–2307) — query-time filtering (`permissioned-query`).
- `util/exception.clj` — denial/error shapes.


## 1. Rules JSON format

Rules are stored one row per app: `rules (app_id uuid, code jsonb, version)`
(`model/rule.clj:30-53`, upsert bumps `version`). `code` is the JSON the user
writes in the dashboard / pushes with the CLI. Shape (see `rulesTypes.ts:3-122`):

```jsonc
{
  "<namespace>": {                     // etype name, e.g. "posts"; also "$users", "$files", "attrs"
    "bind": ["name1", "expr1", "name2", "expr2"]   // OR object form {"name1": "expr1", ...}
    ,
    "allow": {
      "$default": "<cel>",             // fallback for any action not listed
      "view":   "<cel>",               // may also be JSON boolean true/false (patch-code, rule.clj:176-179)
      "create": "<cel>",
      "update": "<cel>",
      "delete": "<cel>",
      "link":   { "<linkLabel>": "<cel>", "$default": "<cel>" },   // per-link-label
      "unlink": { "<linkLabel>": "<cel>", "$default": "<cel>" }
    },
    "fields": { "<attrName>": "<cel>" } // per-field view rules; "id" is forbidden (rule.clj:296-306, 397-414)
  },
  "$default": {                        // per-app default namespace
    "bind": [...],
    "allow": { "view": ..., "create": ..., "update": ..., "delete": ..., "$default": ... }
  },
  "attrs": {                           // controls schema (attr) creation via client transact
    "allow": { "create": "<cel>", ... }
  },
  "$rateLimits": {                     // named token buckets, used via rateLimit.<name>.limit(key)
    "<name>": { "limits": [ { "capacity": n, "refill": { "amount": n, "period": "1 hour", "type": "greedy"|"interval" } } ] }
  }
}
```

Notes:

- Rule expressions are strings of CEL; a bare JSON boolean is stringified
  (`patch-code`, `rule.clj:176-179`).
- `bind` gives named sub-expressions usable inside the namespace's rules.
  Array form is `[name, expr, name, expr, ...]`; object form is a map.
  `normalize-bind` (`rule.clj:104-111`) accepts both. Must have an even count
  and no repeated names (`bind-validation-errors`, `rule.clj:333-354`).
- **Resolution order for an (etype, action)** — `get-program!`
  (`rule.clj:278-290`) tries paths in order, first hit wins:
  1. `[etype "allow" action]`
  2. `[etype "allow" "$default"]`
  3. `["$default" "allow" action]`
  4. `["$default" "allow" "$default"]`
  5. built-in system fallback (`fallback-program`, only for system etypes; see §6)
  If nothing matches, `get-program!` returns `nil` and the *caller* decides
  (transact: allow; query: allow — see §6).
- **Link/unlink** rules are looked up per link label:
  `[etype "allow" "link" <label>]` then `[etype "allow" "link" "$default"]`
  (`permissioned_transaction.clj:300-324`). There is *no* `$default`-namespace
  fallback for link/unlink. If neither side of a link defines link/unlink
  rules, the system falls back to `update` on the forward entity + `view` on
  the reverse entity (§3).
- **Fields** rules are looked up only at `[etype "fields" <field>]` — no
  `$default` chain (`get-field-program!`, `rule.clj:292-306`). Field rules use
  the "view" environment.
- **Validation on save** (`validation-errors`, `rule.clj:436-440`):
  bind errors; per-(etype, view/create/update/delete) CEL compile of
  `[etype "allow" action]` with binds expanded; field rule compilation;
  `$rateLimits` config parsing. Restrictions:
  - `$users`: `view`/`create`/`update` rules may be set; `delete` must be
    absent or `"false"` (`$users-validation-errors`, `rule.clj:308-322`).
  - Namespaces starting with `$` other than
    `$users,$files,$default,$streams,$rateLimits` are rejected
    (`rule.clj:324-331`).
  - `fields.id` is rejected (`rule.clj:397-414`).
  - Custom AST validators: `auth.ref(...)` argument must be a constant string
    starting with `$user.` (`cel.clj:1826-1848`); `rateLimit.<name>` must be a
    key of `$rateLimits` (`cel.clj:1850-1872`).
  - Cyclic `bind` dependencies throw (`sort-binds`, `rule.clj:113-135`;
    `cel.clj:1882-1888`).

### Bind expansion semantics

`with-binds` (`rule.clj:137-174`) is a *textual* wrap using the CEL bindings
extension: for each bind variable actually referenced (transitively) by the
rule expression, the final code becomes

```
cel.bind(varN, <exprN>, ... cel.bind(var1, <expr1>, <rule expr>) ...)
```

Details:

- Binds from `$default.bind` and `<etype>.bind` are concatenated
  ($default first, so an etype bind with the same name wins in the
  `hash-map`) (`rule.clj:138-140`).
- Which binds are "used" is decided by parsing the expression and collecting
  IDENT node names (`ident-usages`, `cel.clj:509-523`) — AST-based, not
  string matching. Bind expressions may reference other binds; references are
  discovered transitively and the wraps are ordered by topological sort
  (`sort-binds`, `rule.clj:113-135`): the reduce at `rule.clj:171-174` wraps
  so that a bind's dependencies end up in an *enclosing* (outer) `cel.bind`,
  i.e. a dependency is already bound when the dependent bind's expression is
  evaluated. Cycles throw a validation error.
- So the Rust impl needs `cel.bind(var, expr, body)` (CEL bindings
  extension) OR can substitute an equivalent let-binding mechanism, plus a
  parser pass to collect identifiers.


## 2. CEL evaluation environment

### Compilers per action (`cel.clj:441-495`)

All compilers share: vars `data`, `auth`, `ruleParams` (all
`map<string,dyn>`), `request` (a proto struct), `rateLimit`
(`map<string,RateLimitBucket>`); the custom function declarations; standard
macros (`CelStandardMacro/STANDARD_MACROS`); extension libraries **bindings,
strings, math**; options `populateMacroCalls=true`,
`enableUnknownTracking=true` (`cel-options`, `cel.clj:431-434`).

Per action (`action->compiler`, `cel.clj:490-495`):

| action        | extra vars                          |
|---------------|-------------------------------------|
| view, delete  | (none)                              |
| create, update (and any other/default) | `newData` |
| link          | `newData`, `linkedData`, `actions`  |
| unlink        | `newData`, `linkedData`             |

### Variable contents (bound in `eval-program!` / `advance-program!`, `cel.clj:556-576`, `599-630`)

- `auth` — the current `$users` record as a map (`{:current-user ctx}`), e.g.
  `{"id": "<uuid str>", "email": ...}`. `nil` user → empty map, so
  `auth.id == null`. Wrapped in `AuthCelMap` which supports
  `auth.ref("$user.path...")` (§ custom functions).
- `data` — the entity snapshot for the rule (see §3/§4 for which snapshot).
  Wrapped in `DataCelMap` (supports `data.ref`). Always includes `"id"`
  (string uuid after stringify).
- `newData` — plain map of the entity *after* applying the tx steps
  (no `.ref` support — plain `CelMap`).
- `linkedData` — the entity on the other side of a link/unlink
  (`DataCelMap` with the linked etype, so `linkedData.ref(...)` works).
- `actions` — link only: `{"data": "create"|"update", "linkedData": "create"|"update"}`
  describing whether each side is being created in this tx
  (`permissioned_transaction.clj:359-360, 372-373, 543-544, 558-559`).
- `ruleParams` — map of client-supplied params (§5). Missing keys → null.
- `request` — a protobuf message (`db/proto.clj:7-77`) with fields:
  - `modifiedFields`: repeated string — labels of fields written by this tx
    step's entity, excluding `id` (`get-modified-fields-for-eid`,
    `permissioned_transaction.clj:262-279`). Only populated for
    create/update checks; empty for query-time.
  - `time`: `google.protobuf.Timestamp` — tx/query evaluation time
    (`(:timestamp ctx)` or now).
  - `ip`, `origin`: strings from the HTTP request (`*request-info*`).
  Because it is a proto struct, unset-field access follows proto semantics
  (defaults, `has()` macro works).
- `rateLimit` — map name → RateLimitBucket built from `$rateLimits`
  (`create-rate-limit-obj`, `cel.clj:548-554`).

### Null-safety / value coercion (critical to match)

`CelMap` (`cel.clj:199-215`): **`containsKey` always returns true and `get`
of a missing key returns CEL `null`** instead of throwing "no such key".
This applies to `data`, `newData`, `auth`, `ruleParams`, `actions`, and to
any *nested* map, because `stringify` (`cel.clj:219-235`) rewraps values:

- `nil` → `NullValue.NULL_VALUE`
- ints → longs (so `type(x)` works)
- keywords/symbols/uuids → strings
- sequential → `CelList` (elements stringified lazily; `in` works)
- maps → `CelMap` (recursive null-safe access)
- `java.util.Date` → ISO string `yyyy-MM-dd'T'HH:mm:ss'Z'` (UTC)

Consequences the Rust impl must reproduce:
- `data.someMissingField == null` is `true`, never an error.
- `has(data.x)` is always `true` (containsKey lies) — don't rely on `has`.
- `'k' in data` is always `true` for the top-level maps.
- Rule result: CEL `null` result is converted to `nil` and treated as
  **deny** (falsy) (`eval-program-with-bindings`, `cel.clj:534-546`;
  `check-pass? (boolean result)`, `permissioned_transaction.clj:624-632`).
  Non-boolean truthy results (e.g. a string) technically pass; safest to
  treat "result is exactly `true`" vs "anything else"; legacy uses Clojure
  truthiness (only `nil`/`false` deny).

### Custom functions (`cel.clj:372-429`)

- `data.ref("linkA.linkB.field")` (member `ref` on `data`/`linkedData`) —
  returns a **list** of values found by walking link path from the entity and
  reading `field` on the terminal entities (`ref-impl`, `cel.clj:249-262`;
  query built in `build-query`, `cel.clj:88-117`). Path segments may be
  forward or reverse link labels (`validate-refpath`, `cel.clj:1398-1411`);
  last segment is an attribute name (commonly `id`). If `data.id` is null
  (e.g. delete of a nonexistent entity), returns `[]`. Results are dedup-less
  lists of stringified values. Typical use: `auth.id in data.ref('owner.id')`.
  - Evaluation is batched: refs are registered as CEL *unknowns*
    (`enableUnknownTracking`), collected across all checks, prefetched in one
    datalog query, then evaluation re-runs with a `preloaded-refs` cache
    (`advance-program!` `cel.clj:590-672`, `eval-programs!` `cel.clj:679-727`,
    `prefetch-data-refs` `cel.clj:1752-1817`). A per-entity baseline (just
    fetch synchronously) is semantically equivalent.
- `auth.ref("$user.path.field")` — same, but rooted at the `$users` entity
  of the current user; the `$user.` prefix is stripped and etype forced to
  `"$users"` (`AuthCelMap.ref`, `cel.clj:279-293`). Validation requires the
  literal prefix `$user.` (`auth-ref-validator`, `cel.clj:1826-1848`).
- `timestamp(string)` / `timestamp(int)` — extra global overloads producing a
  proto Timestamp from a date string (any format `triple-model/parse-date-value`
  accepts) or epoch millis (`cel.clj:398-414`).
- `t.getTime()` — member on Timestamp, returns **epoch millis** as int
  (implementation calls `Timestamps/toMillis`, `proto.clj:56-57`, despite the
  fn being named `timestamp->epoch-seconds`).
- `rateLimit.<name>.limit(key)` / `.limit(key, tokens)` — consumes tokens
  from the named bucket keyed by `key`, returns bool (allowed?)
  (`cel.clj:372-385`, `295-314`).
- Standard `type(x)` works (ints coerced to long specifically so
  `type(data.x)` doesn't NPE, `cel.clj:222-226`).

### CEL features used (for sizing a Rust CEL dependency)

- Standard macros: `has`, `all`, `exists`, `exists_one`, `map`, `filter`
  (STANDARD_MACROS → comprehensions in the AST; `expr->ref-uses` handles
  COMPREHENSION nodes, `cel.clj:1703-1740`).
- Extensions: **bindings** (`cel.bind` — required for `bind`), **strings**
  (e.g. `split`, `lowerAscii`, ... cel-java strings ext), **math**
  (`math.greatest`, etc.).
- String member fns from the standard env: `startsWith`, `endsWith`,
  `contains`, `matches`, `size`, `in`, ternary `?:`, `&&/||/!`, indexing
  `a["b"]`, list literals, map literals.
- Timestamps (proto `google.protobuf.Timestamp`) + custom `timestamp()`/
  `getTime()` overloads; one proto message type (`request`). In Rust you can
  model `request` as a plain map (field access + `has`) rather than a proto.
- Custom member function `ref` on dynamic maps; member `limit` on an opaque
  type; overloading by runtime type.
- Unknown-tracking/partial evaluation is an *optimization only* — baseline:
  resolve `data.ref` eagerly during evaluation.
- `cel-interpreter` crate: supports macros/comprehensions and custom
  functions; you must add (a) null-safe map access (their default errors on
  missing keys — wrap maps or patch member resolution), (b) `cel.bind`
  (can be rewritten to nested substitution at rule-load time), (c) strings +
  math ext functions you actually need, (d) timestamp overloads.


## 3. Transact-time checks (`db/permissioned_transaction.clj`)

Flow (`transact!`, lines 634-744):

1. Preprocess tx-steps into maps; **admin bypass**: if `ctx :admin?` is true
   (platform/admin API with app admin token), skip *all* rule checks and just
   transact (line 679-682). System-table protections still run
   (`prevent-system-column-updates`, line 678, see below).
2. Load current entities for every referenced (eid, etype) — including the
   reverse side of links — into `entities-map` (`load-entities-map`, 142-171).
3. Resolve lookup-refs (`[attrId, value]` eids) against loaded entities.
4. `updated-entities-map` = entities-map with tx-steps applied in memory
   (`update-entities-map`, 224-260; `deep-merge-and-delete` for merge ops,
   203-222 — nested `null` deletes keys).
5. Collect `rule-params-map` from `:rule-params` tx-steps keyed by
   {eid, etype} (line 694-701).
6. **Pre-checks** (before writing, `pre-checks` 281-481) — run for entities
   that already exist:
   - `:update-attr`/`:delete-attr`/`:restore-attr` → allowed iff `admin?`
     (result hard-coded, 326-342).
   - `:add-triple` on a **ref attr** (link):
     - If a `link` rule exists on either side (paths
       `[etype allow link <fwdLabel>]`/`[... $default]`, and
       `[revEtype allow link <revLabel>]`/`[... $default]`):
       run action `link` on the forward entity (if it exists) with
       `data` = old entity, `newData` = merged entity, `linkedData` = old
       reverse entity, `actions` map; and action `link` on the reverse entity
       (if it exists) with sides swapped (345-374).
     - Else fallback: `update` check on the forward entity
       (`data`=old, `newData`=merged, `modifiedFields`) and `view` check on
       the reverse entity (`data`=old rev entity, `newData`=merged) (377-400).
   - `:retract-triple` on a ref attr: same but with `unlink` rules
     (paths `[etype allow unlink <label>]`/`$default`), fallback again
     `update` (fwd) + `view` (rev) (403-454).
   - `:add-triple`/`:deep-merge-triple` on a non-ref attr where the entity
     **exists** → `update` check: `data` = **old** entity, `newData` =
     merged entity, `request.modifiedFields` set (456-467).
   - `:delete-entity` → `delete` check: `data` = **old** entity, no
     `newData` (469-477).
7. Write the tx to the DB.
8. **Post-checks** (after writing, `post-create-checks` 483-611) — for
   entities that did **not** exist before (creates), so `data.ref` can see
   the new rows:
   - `:add-attr` → `attrs.allow.create` check (default allow), `data` = attr
     value (520-526).
   - Link involving a *created* entity, when link rules exist → `link`
     checks with `data` = `newData` = the created entity (both the merged
     map), `linkedData` = merged other side, `actions` (529-560).
   - Link fallback with created entity → `create` check on the created side
     (`data`=`newData`=created entity) and `view` check on a created reverse
     side (563-587).
   - Plain create (`:add-triple`/`:deep-merge-triple`, entity didn't exist)
     → `create` check, `data` = `newData` = created entity (with `id`
     resolved through lookups), `modifiedFields` set (589-607). **Direct
     `$users` creation is rejected here** with a validation error (592-595).
9. `run-checks!` (613-632): dedupe checks, evaluate all programs (batched
   ref prefetch), and for each result — unless `admin-check?` mode — call
   `ex/assert-permitted! :perms-pass? [etype scope] result`, which throws on
   falsy. In `admin-check?` (dry-run) mode results are collected and the tx
   is rolled back if `admin-dry-run?` or any check failed (733-743).
   Unresolved lookup-ref eids at check time → validation error
   "Could not find the entity for this lookup" (618-621).
10. When a program is missing (no rule configured), the check runs with
    `{:result true}` — **allow** (e.g. lines 385-386, 462-463, 474-475,
    600-601).

Snapshot summary:

| action | `data` | `newData` |
|--------|--------|-----------|
| create | merged/new entity | same as data |
| update | old entity | merged entity |
| delete | old entity | — |
| link/unlink (existing side) | old entity | merged entity (+`linkedData` old/merged other side) |
| link (created side) | created entity | created entity |
| view (link fallback, rev side) | old (pre) or created (post) entity | merged entity |

### System-table protections (independent of rules; run even for admins)

`prevent-system-column-updates` (81-110):

- `:delete-entity` on a `$`-etype: only allowed for `$files`, or if admin
  (44-47).
- Triples on system-catalog attrs: allowed only if the attr is "editable"
  (`$files.path`, `id`, per `system-catalog/editable-triple-ident-name?`);
  `$files`/`$streams` attrs are never editable (even admins); other system
  namespaces editable by admins only (49-79). `$files.path` values starting
  with `$stream/` are rejected.

### Error shapes (`util/exception.clj`)

Denial (`assert-permitted!`, 291-297):

```clojure
{::type ::permission-denied
 ::message "Permission denied: not perms-pass?"
 ::hint {:input [etype scope]      ;; e.g. ["posts" :object] / ["attrs" :attr]
         :expected :perms-pass?}}
```

(HTTP layer serializes this as `type: "permission-denied"`, with `message`
and `hint` keys — clients show `hint.input`/`hint.expected`.)

CEL runtime error during evaluation
(`throw-permission-evaluation-failed!`, 299-321):

```clojure
{::type ::permission-evaluation-failed
 ::message "Could not evaluate permission rule for `<etype>.<action>`. <cause>. Debug this in the sandbox and then update your permission rules."
 ::hint {:rule [etype action]
         :error {:type <cel error code kw>, :message <same>, :hint <cause>}}}
```

`<cause>` is the CEL exception message only when `ctx :show-cel-errors?`,
else `"You may have a typo"`. A rate-limit exception inside CEL is unwrapped
and rethrown as rate-limited.


## 4. Query-time checks (`db/instaql.clj:2236-2268`, `permissioned-query`)

If `admin?` → raw `query`, no filtering (2244-2245). Otherwise:

1. `rule-params` = `:$$ruleParams` key of the query object, then removed
   (2246-2248; `->forms!` also strips it, 558-559).
2. **rule-wheres optimization** (`get-rule-wheres`, 2116-2152) — optional
   (feature-flagged via `use-rule-wheres?`, 2087-2093; baseline can skip
   entirely and per-entity eval below still yields the same *visible set*):
   - For every etype referenced by the query, compile the `view` rule with a
     *separate* CEL environment (`where-cel-compiler`, `cel.clj:1488-1520`)
     where `data` is a symbolic `CheckedDataMap` and the operators
     `==, !=, in, &&, ||, !, <, <=, >, >=, startsWith, endsWith, contains,
     size, ref, timestamp` are overloaded to *build a where-clause value*
     instead of evaluating (`cel.clj:729-1470`). `auth`, `ruleParams`,
     `request`, `auth.ref` evaluate concretely.
   - Evaluating the program yields either a boolean or a `WhereClause`.
     `format-evaluation-result` (`cel.clj:1533-1542`):
     - result `false`/`null` → `:short-circuit? true` (rule can never pass →
       the query gets an impossible where `{:id <random-uuid>}`, 2208-2210,
       2160-2161).
     - result truthy non-WhereClause (e.g. `true`) → no where clause, but the
       etype is still marked as fully checked.
     - `WhereClause` → instaql where map, e.g.
       `{"or": [{"name": "Daniel"}, {"handle": "dww"}]}`,
       `{"ownerId": "<auth.id>"}`, `{"path" {:$like "prefix%"}}`,
       `{"field" {:$isNull true}}`, `{"owner.id" "<x>"}` (via
       `x in data.ref('owner.id')`).
   - Any rule the translator can't handle throws inside the overloads and
     that etype falls back to per-entity checks (2141-2148). Known
     untranslatable shapes are listed at `cel.clj:736-750` (size on refs
     except `== 0` idioms, JSON sub-field digs, indexing into ref lists,
     field-vs-field comparison, exists-over-literal-list, ternary).
   - Where clauses are ANDed into the query for the etype and for every
     linked etype reached through `where`/child-forms
     (`add-rule-wheres-to-query`, 2200-2234; `extend-where-with-rule-refs`,
     2154-2198). Entities returned under a rule-where are marked
     `checked-eids` and skip per-entity eval (1833-1846, 2021-2028).
   - Rate limits used inside translated rules are charged per returned
     entity (`check-rate-limits-for-rule-wheres`, 1874-1888).
3. Run the (possibly augmented) query.
4. **Per-entity eval** (the baseline semantics): walk the whole result tree
   collecting, per (etype, eid): the `view` program
   (`rule-model/get-program! rules etype "view"`) and per attribute label the
   field program (`join-rows->etype-maps`, 1823-1859). Fetch full entity maps
   (even if the query used `fields`) in batch (`preload-entity-maps`,
   1966-2007). Evaluate each program with
   `{:data <entity map>, :rule-params <params>}` (2009-2085; batched
   `cel/eval-programs!`).
   - No program for an etype → `{:result true}` (allow) (2043-2046).
   - Deny (falsy/null result) is **not an error**: the entity's triples are
     silently removed.
5. **Filtering** (`permissioned-node`, 1916-1954; `viewable-triple?`,
   1905-1911): a triple `[e a v]` is kept iff the view check for
   `(etype-of a, e)` passed AND the field check for `(etype, e, label)`
   passed (field default allow). A join row is kept iff *every* triple in it
   is viewable — so a child entity that fails `view` removes the whole join
   row (the parent link to it disappears, but the parent survives via its
   other rows). Page-info start/end cursors are recomputed from filtered
   rows; `has-next/previous-page?` may be stale (1923-1940, noted in code).
   Child nodes of a fully-filtered node are dropped (1945-1953).

The same machinery backs subscriptions (`reactive/query.clj:135` calls
`iq/permissioned-query`) and the admin HTTP API
(`admin/routes.clj:156`, and `permissioned-query-check` at
`instaql.clj:2270-2307` for the dashboard sandbox, which also returns
per-entity `check-results` and `rule-wheres`).


## 5. ruleParams

- **Queries**: client APIs take `ruleParams` in query opts and inject them as
  a top-level `$$ruleParams` key of the query object
  (`client/packages/core/src/infiniteQuery.ts:567-570`,
  `queryValidation.ts:512`). Server: `permissioned-query` pulls
  `(:$$ruleParams o)` (`instaql.clj:2247`) and passes the map as the
  `ruleParams` CEL var to *every* view/field program and to the rule-wheres
  translator. Keys are strings; missing keys → null.
- **Transactions**: `tx.ns[id].ruleParams({...})` produces a tx-step
  `["rule-params", <lookup/eid>, <etype>, {params}]`
  (`client/packages/core/src/instaml.ts:347-349`; server spec
  `transaction.clj:47-48`). Server groups them into
  `rule-params-map[{eid, etype}]` (merging multiple steps,
  `permissioned_transaction.clj:694-701`). Each check gets the params for
  its own entity; **link/unlink and ref-fallback checks get the merge of both
  sides' params** (fwd checks: `(merge rev-rule-params rule-params)`, rev
  checks: `(merge rule-params rev-rule-params)` — own side wins,
  e.g. lines 361, 374, 389, 400). Cascading deletes propagate a parent's
  rule-params to cascaded children (`transaction.clj:508-530`).
- `ruleParams` is scoped per entity in transactions but global per query.


## 6. Defaults when no rules are set

With `code = {}` (or no rules row):

- **Regular namespaces**: `get-program!` finds nothing and returns `nil` →
  every caller substitutes allow (`{:result true}` in transact; "no program →
  result true" in query). **Everything is allowed: view, create, update,
  delete, link, unlink.** Same for the `attrs` namespace (attr creation
  allowed; attr update/delete/restore are admin-only regardless of rules).
- **System namespaces** (`system-catalog/all-etypes` =
  `$users, $magicCodes, $userRefreshTokens, $oauthProviders, $oauthUserLinks,
  $oauthClients, $oauthCodes, $oauthRedirects, $files, $streams`,
  `system_catalog.clj:33-45`) get compiled fallbacks
  (`fallback-program`, `rule.clj:181-236`):
  - `$users.create` → `"true"` (signup flows; direct creation via transact is
    separately blocked, `permissioned_transaction.clj:592-595`).
  - `$users.view` and `$users.update` →
    `"auth.id == data.id || (data.linkedPrimaryUser != null && auth.id == data.linkedPrimaryUser)"`
    (a user sees/updates only themself, or a linked primary user's record).
  - `$users.delete` → `"false"` (via the else branch; display code
    `disallow_delete_on_system_tables`). Users cannot override (§1).
  - `$files.{view,create,update,delete}` → `"false"` (apps must opt in with
    explicit rules).
  - Every other system etype, any action → `"false"`
    (display `disallow_<action>_on_system_tables`).
  - The fallback is the **last** entry in the `$default` chain, so an app's
    `$users.allow.view` or even `$default.allow.view` overrides these
    (`get-program!` paths, `rule.clj:286-290`).
- `$default` namespace: purely user-supplied; there is no built-in `$default`.

Caching (informational): rules row cached per app (`rule-cache`,
`rule.clj:17-21`), compiled programs cached by `[rules paths]`
(`program-cache`, `rule.clj:238-244`).


## Appendix: minimal check matrix for tests

| Op | Rule path(s) tried | data / newData | default |
|----|--------------------|----------------|---------|
| query entity | `[etype allow view]` → `[etype allow $default]` → `[$default allow view]` → `[$default allow $default]` → system fallback | data=entity | allow |
| query field  | `[etype fields <field>]` | data=entity | allow |
| create | same chain with `create` (post-tx) | data=newData=new entity | allow |
| update | chain with `update` (pre-tx) | data=old, newData=merged | allow |
| delete | chain with `delete` (pre-tx) | data=old | allow |
| link   | `[etype allow link <label>]` → `[etype allow link $default]` on each side; if neither side has any → update(fwd)+view(rev) | see §3 | allow |
| unlink | same with `unlink` | see §3 | allow |
| add-attr | `[attrs allow create]` chain | data=attr | allow |
| update/delete/restore-attr | — | — | admin only |
