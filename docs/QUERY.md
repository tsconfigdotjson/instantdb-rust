# QUERY.md — InstaQL Query Engine

This documents the legacy Clojure implementation of InstaQL (the query side) in
enough detail to reimplement it in Rust over the same Postgres `triples` schema.

Primary sources (all paths relative to `LEGACY/`):

- `server/src/instant/db/instaql.clj` — InstaQL JSON → forms → datalog patterns, result-tree builder, permissions plumbing.
- `server/src/instant/db/datalog.clj` — datalog patterns → SQL (CTE chain) over `triples`, SQL result → `{join-rows, symbol-values, topics, page-info}`.
- `server/src/instant/db/model/attr_pat.clj` — attr-pattern helpers, index selection, typed-value coercion.
- `server/src/instant/db/instaql_topic.clj` — CEL-compiled "refined topic" programs (optional optimization for invalidation).
- `server/src/instant/reactive/topics.clj` + `server/src/instant/reactive/store.clj` — WAL changes → topics, and topic matching for invalidation.
- `client/packages/core/src/instaql.ts` — how the client consumes join-rows (isomorphic local query).
- `server/test/instant/db/instaql_test.clj` — behavioral spec.

Throughout, a **triple** is `[e a v t]`: entity-id (uuid), attr-id (uuid), value
(json), `t` = `triples.created_at`, a **bigint unix-epoch-milliseconds** column
(`migrations/08_add_createdAt_for_triples.up.sql`: `created_at bigint DEFAULT current_unix_timestamp_ms()`).

---

## 1. InstaQL query JSON shape

A query is a map of namespace → form. Each form has an optional `$` option map
and any other keys are child (link) forms:

```js
{
  users: {
    $: {
      where:  { handle: "stopa", "bookshelves.books.title": "Foo",
                or: [...], and: [...] },
      order:  { serverCreatedAt: "desc" },        // exactly one key
      limit:  10, // or first: 10, or last: 10 (mutually exclusive)
      offset: 0,
      before: [e, a, v, t],  after: [e, a, v, t], // cursors (join rows)
      beforeInclusive: true, afterInclusive: true,
      aggregate: "count",                          // admin only
      fields: ["handle", "email"],                 // projection
    },
    bookshelves: { $: {where: ...}, books: {} },   // nested child forms
  },
  $$ruleParams: {...},                             // top-level only, stripped before query
}
```

Spec: `instaql.clj:39-137`. Coercion into internal "forms" happens in
`coerce-forms!` / `->forms!` (`instaql.clj:534-568`). Each form becomes
`{:k <string key> :etype <string> :option-map {...} :child-forms [...]}`; at the
top level `:etype = :k` and `:level 0` are assigned in
`instaql-query->patterns` (`instaql.clj:1224-1237`). For child forms the etype
is resolved through the link name via `link-etype` (`instaql.clj:181-189`):
forward attr `[etype label]` → its reverse etype, else reverse attr → forward etype.

`$$ruleParams` is removed in `->forms!` (`instaql.clj:560`) and in
`permissioned-query` (`instaql.clj:2245-2249`), where it is passed as CEL
`ruleParams` bindings; it never reaches the datalog layer.

### 1.1 `where`

`where` is a map. Each entry is one of:

- **`or` / `and`**: key literally `"or"`/`"and"` with an array of where-maps
  (`or-where-cond?`/`and-where-cond?`, `instaql.clj:139-145`).
- **path → value**: key is a dot-path (`"a.b.c"` split on `.`,
  `instaql.clj:311`), value is either:
  - a **direct value** (`string | uuid | number | boolean`,
    `where-value-valid?` `instaql.clj:39-40`) — equality;
  - an **args-map** with these operators (`::where-args-map`,
    `instaql.clj:46-81`):

| key | value | meaning |
|---|---|---|
| `$in` (alias `in`) | array of values (nils allowed) | membership; conformed into a set |
| `$not` | value | not-equal, *including* entities missing the attr (see below) |
| `$ne` | value | alias of `$not` (`normalize-ne-to-not`, `instaql.clj:241-248`) |
| `$isNull` | boolean | attr is null/missing (true) or present & non-null (false) |
| `$gt $gte $lt $lte` | string\|number\|boolean (dates: number or ISO string) | typed comparison; requires indexed + checked-type attr |
| `$like` / `$ilike` | string (`%`,`_` wildcards) | string pattern match; requires indexed `string`-typed attr |
| `$entityIdStartsWith` | string uuid-prefix | dashboard hack: entity-id range scan |

Multiple operator keys can coexist in one args-map, but in practice
`->where-cond-attr-pats` only reads the *first* entry of an args-map
(`instaql.clj:672-675`) except for `$not`/`$isNull` which get special-cased
during coercion first. Unknown keys are a validation error
(`where-value-valid-keys?`, `instaql.clj:69-75`; the spec also tolerates the
legacy `:$entityId` key, never implemented).

**Coercion transforms** (`coerce-where-cond`, `instaql.clj:250-311`):

- `{path {$not v}}` (or `$ne`) rewrites to
  `{or [[path {$not v}], [p1 {$isNull true}], [p1.p2 {$isNull true}], ...]}`
  — one `$isNull true` cond for every prefix of the path (`grow-paths`,
  `instaql.clj:232-239`), because a plain `!=` scan can't find entities that
  lack the attr. If the *full* path's final attr is indexed
  (`indexed-attr?`, `instaql.clj:191-202`), the last isNull cond is dropped
  (indexed attrs store explicit `null` triples). (`instaql.clj:286-298`)
- `{path {$isNull true}}` with a multi-segment path becomes
  `{or [[p1 isNull], [p1.p2 isNull], ... [full-path isNull]]}` so a null at
  any intermediate link matches (`instaql.clj:300-309`); a single-segment path
  stays a single cond.
- Nested `or`/`and` are flattened/collapsed (`collapse-coerced-conds`,
  `instaql.clj:147-177`); empty `or:[]` / `and:[]` is a validation error
  (`instaql.clj:264-284`).
- **or→in optimization** (`combine-or-where-conds`, `instaql.clj:204-230`):
  inside an `or`, single-key branches `{k v}` with plain scalar values (key not
  starting with `$`) — plus `{k {$isNull true}}` when `k` resolves to an
  indexed attr (nulls are then represented as literal `nil` in the in-set) —
  are merged into `{k {:in [v1 v2 ...]}}`. Tested by
  `indexed-ors-collapse-isNull-true` (`instaql_test.clj:4981`).

Conformed where-conds are tagged: `[:cond {:path [...] :v [:value x |
:args-map {...}]}]`, `[:or {:or [conds]}]`, `[:and {:and [conds]}]`.

**id shortcut**: `where {id: <uuid>}` is nothing special — `id` is an ordinary
unique attr, so it compiles to an `:av` pattern
(`lookup-unique-uses-the-av-index` test, `instaql_test.clj:3243`). Matching a
*link name* directly against a uuid (`where {bookshelves: <uuid>}`) matches the
linked entity id, in either direction (`eid-relations` test,
`instaql_test.clj:3797`; reverse handled in `->value-attr-pat`,
`attr_pat.clj:431-434`, producing `[v attr-id ?var]`). Link values must coerce
to uuids or a validation error is thrown (`attr_pat.clj:396-419`).

`$entityIdStartsWith` is used as a *path label*: `where
{"$entityIdStartsWith": "abc"}` produces `[?e id-attr {:$entityIdStartsWith
"abc"}]` (`instaql.clj:705-707`), compiled to
`entity_id BETWEEN uuid(prefix + "000...") AND uuid(prefix + "fff...")`
(`datalog.clj:698-708, 751-755`).

### 1.2 Option-map validation (`coerce-option-map!`, `instaql.clj:437-532`)

- `limit`/`first`/`last`: positive int; only one of the three
  (`instaql.clj:509-518`).
- `offset`: non-negative int.
- `before`/`after`: a "join row" — sequential of 3 or 4 elements
  `[e a v t?]`; `a` must coerce to uuid; `t` (if present) must be int; `e` is
  uuid-coerced when possible (`assert-cursor!`, `instaql.clj:354-380`).
- `beforeInclusive`/`afterInclusive`: booleans.
- `order`: map with exactly one key; direction `"asc"|"desc"` (string or kw)
  (`coerce-order`, `instaql.clj:313-336`).
- `aggregate`: only `"count"`; admin-only; cannot be combined with child forms
  (`instaql.clj:426-435, 1186-1200`).
- `fields`: array of field-name strings (`instaql.clj:482-490`).
- Any other `$` key → error *"We only support `where`, `order`, `limit`,
  `offset`, `before`, and `after` clauses."* (`instaql.clj:492-498`).
- On non-top-level forms (`level > 0`), `offset`/`before`/`after` are rejected
  (*"We currently only support `offset`, `before`, and `after` clauses on the
  top-level field. Limit fields will be ignored"* — note: nested
  `limit`/`first`/`last`/`order` are silently accepted and ignored by the
  server; the client applies them locally) (`instaql.clj:500-507`,
  `instaql.ts:835-848`).

Validation errors carry `{:expected sym, :in [path...], :message str}`; see
`validations` test (`instaql_test.clj:212-325`) for exact shapes.

---

## 2. Where semantics: dot-paths, nulls, typed comparisons

### 2.1 Dot-paths = joins through refs

`->where-cond-attr-pats` (`instaql.clj:654-719`) splits path into
`refs-path + value-label`. `->ref-attr-pats` (`attr_pat.clj:144-164`) walks
refs-path producing one attr-pat per hop; each hop resolves the label as a
forward or reverse link attr (`->ref-attr-pat`, `attr_pat.clj:97-129`):

```
["users" "bookshelves" "books" "title"] "Foo"
=>
[?users-0 bookshelves-attr ?bookshelves-1]   ; fwd link: [fwd-var attr rev-var]
[?bookshelves-1 books-attr ?books-2]
[?books-2 title-attr "Foo"]                  ; value pattern
```

Variables are `(symbol "?<etype>-<level>")` (`default-level-sym`,
`attr_pat.clj:91-95`); reverse links put the *new* variable in the `e` slot.
Non-link labels used as path segments throw (`attr_pat.clj:110-114`). Within
`or`/`and` branches, all variables except the join variable are suffixed with
the branch index (`level-sym-gen`, `instaql.clj:645-652`) so branches don't
unify with each other; each branch is wrapped `{:and [...]}` and the whole
group becomes `{:or {:patterns [...] :join-sym ?etype-level}}`
(`where-cond->patterns`, `instaql.clj:798-842`).

Light optimization: within the *first* where-cond only, if the last attr-pat
has a constant and the first doesn't, the pat list is reversed so the constant
scan runs first (`optimize-attr-pats`, `instaql.clj:750-777`;
`where-query`/`where-conds->patterns` `instaql.clj:784-888`). This only affects
performance and per-pattern topics ordering, not the join-rows set.

### 2.2 Index selection

Each attr-pat `[e a v]` becomes a 4/5-element datalog pattern `[idx e a v t]`
via `attr-pats->patterns` (`attr_pat.clj:436-477`) using `best-index`
(`attr_pat.clj:17-52`):

- ref attr: `:vae` if v is actualized (constant or already-bound var), else `:eav`
  (a later rewrite may switch `:vae`→`:eav` when joining e→e/e→v:
  `transform-named-p-for-ref-joins`, `datalog.clj:1933-1957`);
- blob attr, v actualized:
  - indexed **and** checked-type: `{:idx-key :ave :data-type <type>}` (map form);
  - unique: `:av`;
  - indexed: `:ave`;
  - otherwise `:ea` (unindexed scan);
- v not actualized: `:ea` (blob) / `:vae` (ref).

Attrs mid-`indexing?`/`checking-data-type?`/`setting-unique?` are treated as
not indexed/typed/unique (tests `indexing?` `instaql_test.clj:3843`,
`uniqueing?` `:3913`).

For a form with no where and no join, the fallback pattern is
`[?etype-0 id-attr _]` (`->all-ids-attr-pat`, `instaql.clj:724-737`) — every
entity has an `id` attr.

### 2.3 Value comparison in SQL

The v slot of a pattern is either a constant set, a variable, `'_`, or a
function map: `{:$not v}`, `{:$isNull {...}}`, `{:$comparator {:op :data-type
:value}}`, `{:$entityIdStartsWith s}` (`datalog.clj:82-87`).

**Typed extraction functions** (defined in
`migrations/36_checked_data_type.up.sql`, boolean fixed in `39`,
`json_null_to_null` in `53`, `json_uuid_to_uuid` in `73`):

- `triples_extract_string_value(jsonb) -> text` (`value->>0` if json string else null)
- `triples_extract_number_value(jsonb) -> double precision`
- `triples_extract_boolean_value(jsonb) -> boolean`
- `triples_extract_date_value(jsonb) -> timestamptz` (number → `to_timestamp(x/1000)`, string → cast)
- `json_null_to_null(jsonb) -> jsonb` (json `null` → SQL NULL) — used for the `av` partial index
- `json_uuid_to_uuid(jsonb) -> uuid` (`(v->>0)::uuid`) — used for `eav`/`vae` indexes

`extract-value-fn` (`datalog.clj:594-601`): date/number/boolean always use
their extract fn; **string uses the extract fn only for like/ilike** — string
equality compares raw jsonb. pg-type mapping `datalog.clj:603-606`:
`{:date :timestamptz :number :float8 :string :text :boolean :boolean}`.

`data-type-comparison` (`datalog.clj:608-627`) — the core of every typed check:

```clojure
[:and
  [op [extract-fn :value] val]      ; or IN/=ANY for sets; nil handled with [:= nil [f col]]
  [:= :checked_data_type [:cast (name data-type) :checked_data_type]]]
```

i.e. typed comparisons only match rows whose `triples.checked_data_type`
column equals the attr's type. For `=` over a set with a `nil` member the nil
is pulled out into `OR extract(value) IS NULL` (`datalog.clj:610-620`);
`$in [nil ...]` requires the attr be indexed (`attr_pat.clj:420-430`).

Untyped (no `data-type` on idx): equality is jsonb equality against
`cast(->json v as jsonb)`; for `:av` the column is wrapped
`json_null_to_null(value)` and for `:eav`/`:vae` `json_uuid_to_uuid(value)`
with a uuid array when all values are uuids (`in-or-eq-value`,
`datalog.clj:643-665`).

**`$not`** (`not-eq-value`, `datalog.clj:629-641`): untyped →
`value != cast(json v)` (with `json_null_to_null` on `:av`); typed →
`data-type-comparison :is-distinct-from` (so SQL NULL / json null rows *match*
`$not`). Remember the query layer additionally OR's in `$isNull true` prefix
conds (§1.1); observable behavior: `$not "x"` returns entities where the attr
is missing, json-null, or ≠ x (`where-$not-with-nils` test,
`instaql_test.clj:1981`).

**`$isNull`** — the query layer builds the pattern
`[?e id-attr {:$isNull {:attr-id <target-attr> :indexed? bool
:indexed-checked-type type? :nil? bool :ref? bool :reverse? bool}}]`
(`instaql.clj:682-703`; note the *pattern attr* is the entity's `id` attr and
the checked attr goes inside the function map). SQL
(`value-function-clauses`, `datalog.clj:710-739`):

```sql
entity_id NOT IN (          -- $isNull true;  IN for $isNull false
  SELECT sub.entity_id      -- or json_uuid_to_uuid(sub.value) for reverse refs
  FROM triples sub
  WHERE sub.app_id = ?
    AND sub.entity_id = t.entity_id   -- correlated (reverse ref: sub.value -> t.entity_id, AND sub.eav)
    AND sub.attr_id = <target-attr>
    AND sub.value != 'null'::jsonb    -- or typed: extract(value) IS DISTINCT FROM NULL + ave
)
```

So "null" means: no triple, or triple whose value is json `null`.

**Comparators** (`$gt/$gte/$lt/$lte/$like/$ilike`): coerced at query-build
time by `coerce-value-for-typed-comparison!` (`attr_pat.clj:345-372`):

- requires attr `index?` + `checked-data-type` and neither in progress —
  else validation errors (`assert-checked-attr-data-type!`,
  `attr_pat.clj:189-226`);
- value type must match attr type; for `date`, numbers are epoch-ms and
  strings are parsed (`triple-model/parse-date-value`); words like
  `"today"` are rejected (`attr_pat.clj:279-304`);
- `$like/$ilike` value must be a string (`attr_pat.clj:307-317`);
- result stored in the pattern as `{:$comparator {:op :$gt, :data-type :date,
  :value <coerced>}}`, and SQL maps ops to `> >= < <= like ilike`
  (`datalog.clj:740-750`).

Plain **equality on typed attrs**: values are type-checked/coerced the same
way *only when the attr is indexed* (`attr_pat.clj:363-372`) — so date
equality against a differently-formatted string only matches on indexed date
attrs (tests `equality-on-dates-without-index` `instaql_test.clj:431` vs
`equality-on-dates-with-index` `:474`).

---

## 3. Pagination

Only top-level forms paginate: `page-info-of-form` (`instaql.clj:924-1021`)
returns nil unless `level == 0` and one of
`limit/first/last/offset/before/after/order` is set. Default order when
paginating without `order`: `{:k "serverCreatedAt" :direction :asc}`
(`instaql.clj:909`).

### 3.1 Ordering

- `serverCreatedAt`: order-pattern `[:ea ?etype-0 id-attr _ ?t-0]` — orders by
  the `created_at` (bigint ms) of the entity's **id triple**; order-col-type
  `:created-at-timestamp` (`instaql.clj:1010-1011`).
- Any other key `k`: order attr = forward attr `[etype k]`; must be indexed,
  checked-typed, cardinality `:one`, not mid-indexing/checking — else
  validation errors (`instaql.clj:951-977`). Pattern:
  `[{:idx-key :ave :data-type t} ?etype-0 attr-id ?k-0]` (`instaql.clj:1012-1016`).

page-info map produced (`instaql.clj:999-1021`): `{:limit (or limit first
last), :last? bool, :offset, :direction, :order-sym, :eid-sym,
:order-col-type, :order-col-required?, :pattern, :attr-id, :before,
:before-inclusive, :after, :after-inclusive}`.

### 3.2 Cursors

Cursor = join row `[e a v t]` (`t` = created_at ms; `v` may be null). The
cursor's attr-id (`a`, position 1) must equal the order attr's id, else
*"Invalid before/after cursor. The query orders by X, but the query that
returned the cursor orders by Y"* (`instaql.clj:911-997`; `serverCreatedAt`
cursors carry the **id attr's id**).

SQL comparison (`add-cursor-comparisons`, `datalog.clj:2360-2432`). Comparison
op: `before+asc → <`, `before+desc → >`, `after+asc → >`, `after+desc → <`;
inclusive cursors relax only the entity-id tiebreak (`inclusive-comparison`,
`datalog.clj:2344-2349`). The compared value is `cursor[triple-idx-of-order-sym]`
(v for attr ordering, t for serverCreatedAt). With required order column:

```
(order_col >=cmp v) AND (order_col cmp v OR (order_col = v AND entity_id cmp' cursor_e))
```

Nullable order column adds NULL handling — **nulls sort first under asc /
last under desc** (`datalog.clj:2411-2432` and the `order-by`
`asc-nulls-first`/`desc-nulls-last` at `datalog.clj:2513-2519`; tie-break is
always `entity_id` in the page direction).

### 3.3 SQL shape (`add-page-info`, `datalog.clj:2445-2681`)

An extra CTE is appended after the where CTEs for the order pattern, then:

- `SELECT DISTINCT ON (order_val, order_eid)` with
  `order_val = extract_fn(value)` (or raw column for serverCreatedAt/string),
  `order_eid = entity_id` (`datalog.clj:2495-2511`); typed ordering adds
  `checked_data_type = '<type>'` to the where (`datalog.clj:2496-2502`).
- `ORDER BY order_val <dir>, order_eid <dir>`; **`last` reverses the SQL
  direction** and rows are re-reversed when collecting results
  (`datalog.clj:2487-2489, 2992-2999`).
- `LIMIT limit+1` into a `-with-next` CTE (overfetch), then the real table
  `LIMIT limit`; `OFFSET` applied in `-with-next` (`datalog.clj:2524-2527,
  2650-2658`).
- `-first`/`-last` row CTEs capture the first and last returned rows; used as
  synthetic cursors by two `EXISTS` CTEs `-has-next` / `-has-prev`
  (`datalog.clj:2547-2647`): has-next = overfetched row exists (simple case)
  or a row exists past the last row; has-prev symmetric.

### 3.4 Result shape

`accumulate-results` builds `page-info-rows` — one `[e a v t]` per returned
row, substituting `[order_eid, page-info attr-id, nil, created-at-col]` when
the row's join triple is null (entity missing the order value)
(`datalog.clj:2946-2959`). Then (`datalog.clj:2992-3003`, `3159-3176`):

```clojure
:page-info {:start-cursor [e a v t]   ; first row (after un-reversing for last?)
            :end-cursor   [e a v t]   ; last row
            :has-next-page?     bool
            :has-previous-page? bool} ; forced true when page empty but offset>0 or after-cursor present
```

`page-info` lives on the top-level node's `:datalog-result`; the client
receives it keyed by namespace as
`{startCursor, endCursor, hasNextPage, hasPreviousPage}`
(`instaql.ts:890-911`). Permission filtering re-derives start/end cursors from
the filtered `page-info-rows` (`permissioned-node`, `instaql.clj:1922-1940`),
then drops `page-info-rows`. Expected behavior matrix: `pagination` test
(`instaql_test.clj:518-871`), `pagination-with-same-values` (entity-id
tiebreak), `pagination-with-null-values` (nulls-first asc).

---

## 4. Compilation and datalog→SQL

### 4.1 From forms to nested pattern groups

`query-one` (`instaql.clj:1162-1222`) turns each form into a **pattern
group**; `instaql-query->patterns` (`instaql.clj:1224-1237`) wraps them:

```clojure
{:children
 {:pattern-groups
  [{:patterns  [[:av ?users-0 handle-attr #{"stopa"}] ...] ; the where patterns
    :children  {:join-sym ?users-0
                :pattern-groups
                [{:patterns [[:ea ?users-0 #{attr-id ...}]]   ; fetch entity attrs
                  :children {:join-sym ?users-0               ; only if child forms
                             :pattern-groups [<child groups>]}}]}
    ;; optional:
    :missing-attr? true
    :page-info {...}
    :aggregate :count  ; with :children nil
    }]}}
```

- The `[:ea sym attr-ids]` group fetches the entity's attributes:
  `attr-ids = ea-ids-for-etype` = ids of all **cardinality-one forward attrs**
  of the etype (`attr.clj:954-958, 1076-1077`), or, with `fields`, only the
  named cardinality-one attrs plus `id` (plus `location-id` when `$files.url`
  requested) (`etype-attr-ids`, `instaql.clj:1142-1160`).
- Child forms get `:join-attr-pat`, the link pattern with the parent's
  variable replaced by a random-uuid placeholder (so index selection sees a
  constant), later swapped back to the symbol
  (`form->child-forms` `instaql.clj:1026-1043`,
  `replace-sym-placeholders` `instaql.clj:1111-1129`).
- `guarded-where-query` (`instaql.clj:890-902`): any attr-resolution failure →
  `missing-attr? = true` with dummy patterns `[[:ea ?sym] [:eav]]` (validation
  errors still propagate).

Patterns are coerced to 5 slots `[idx e a v t]`, constants wrapped in sets
(`coerce-pattern`, `datalog.clj:156-175`), then spec-conformed to "named
patterns" `{:idx [:keyword :av | :map {:idx-key :ave :data-type :string}],
:e/:a/:v/:created-at [:constant #{..}|:variable sym|:any _|:function {..}]}`
(`->named-patterns` `datalog.clj:207-211`, `nested->named-patterns`
`:213-242` which also stashes the original patterns as `:datalog-query`).
Un-joinable pattern sets (a pattern with no constants and no variable shared
with a constant-anchored pattern) throw `Pattern is not joinable`
(`throw-invalid-patterns`, `datalog.clj:453-498`).

### 4.2 CTE chain (the essence of datalog→SQL)

`match-query` (`datalog.clj:2298-2329`) emits one CTE per pattern
(`joining-with`, `datalog.clj:2000-2100`). CTE `match-N` (nested prefix `m-`):

```sql
WITH m-0 AS (
  SELECT t0.entity_id AS m-0-entity-id, t0.attr_id AS m-0-attr-id,
         t0.value AS m-0-value, t0.eav AS m-0-is-ref-val,
         t0.created_at AS m-0-created-at
  FROM triples t0
  WHERE t0.app_id = ? AND t0.av = true            -- idx-key flag column
    AND attr_id = ? AND json_null_to_null(value) = '"stopa"'::jsonb
), m-1 AS (
  SELECT m-0.*, t1.entity_id AS m-1-entity-id, ...
  FROM triples t1, m-0
  WHERE t1.app_id = ? AND t1.vae AND attr_id = ?
    AND json_uuid_to_uuid(t1.value) = m-0-entity-id   -- join on shared symbol
) ...
```

Every CTE carries **all previous CTE columns plus 5 new columns**
(`match-table-select`/`match-table-cols`, `datalog.clj:556-572`), so the final
CTE's rows are the full join rows. Join conditions come from a symbol-map
(sym → binding paths `{:pattern-idx :triple-idx :ctype :ref?}`); `:ref?` (v of
a `:vae` pattern) wraps the column in `json_uuid_to_uuid`
(`join-cols`/`join-conds`, `datalog.clj:899-988`). Entity-slot constants may
be **lookup refs** `[attr-id value]`, compiled to a
`entity_id = (SELECT entity_id FROM triples WHERE av AND attr_id=? AND
json_null_to_null(value)=?)` subquery (`constant->where-part`,
`datalog.clj:667-696`).

**OR**: each branch's patterns become their own CTE chain (branch-local
symbol namespaces), then a *gather CTE* left/full-joins the last CTE of each
branch and exposes `COALESCE(branch-entity-ids) AS <gather>-entity-id`,
joined back to the preceding chain on the `join-sym`
(`or-gather-cte`, `datalog.clj:2148-2194`; `accumulate-ctes` `:2196-2296`).
Un-matched branches contribute NULL columns — later filtered from join-rows.

**Nesting**: after a parent group's chain, a join CTE
`SELECT DISTINCT entity_id FROM triples, <last-parent-cte> WHERE app_id=? AND
<join conds>` (or `SELECT DISTINCT <col>` straight from the parent CTE when
the join is a single `entity_id =` condition) materializes the parent ids;
child groups then join to it via `additional-joins` (`datalog.clj:2740-2778`).
The entire query tree runs as **one SQL statement**; each result table is
aggregated as
`json_build_object('m-3', coalesce(json_agg(row_to_json(m-3)), '[]'), ...)`
inside a `json_build_array` chunked in groups of 50 tables
(`nested-match-query`, `datalog.clj:2785-2838`). Pagination adds
`-has-next`/`-has-prev` tables to the result set (`datalog.clj:2726-2730`).

pg_hint_plan hints, cost-model index choice (`best-index`
`datalog.clj:1503-1701`), sketches, and materialization keywords are
performance-only — a Rust port can ignore them as long as row output matches.

### 4.3 Observable output: join-rows, symbol-values, topics

`sql-result->result` (`datalog.clj:3021-3034`) + `accumulate-results`
(`:2905-3019`) transform the rows of one result table using per-pattern
metadata (`cte-cols`, `symbol-fields`):

- **join-rows**: a **set** of join rows; each join row is a vector of triples,
  **one triple per pattern, in pattern order**, each triple
  `[entity-id attr-id value created-at]` taken from that pattern's 5 columns
  (`sql-row->triple`, `datalog.clj:2847-2863`). If the row's
  `is-ref-val` column (the triple's `eav` flag) is true, `value` is parsed as
  a **uuid**; otherwise it is the JSON value as-is. For batched/nested results
  (JSON transport) `e` and `a` are parsed from strings to uuids
  (`coerce-uuids? = true`; single unbatched queries get uuids from the
  driver). Triples whose `e` is NULL (unmatched OR branch / page rows) are
  skipped, so join rows may be shorter than the pattern count
  (`datalog.clj:3005-3013`).
- **symbol-values**: map `sym → set of values` collected from the variable
  slots of every join row (`symbol-fields-of-pattern`,
  `datalog.clj:2102-2123`; collection `:2964-2972`). Ref-valued `v`
  (`:vae`) contributes the parsed uuid. Every variable that appears gets at
  least an empty set (`ensure-default-symbol-values`, `datalog.clj:2865-2871`).
- **topics**: see §6.
- Optional `:page-info` (§3) and `:aggregate {:count n}` (count aggregate
  replaces the json_agg with `SELECT count(*)`; `datalog.clj:2824-2826`,
  `3139-3145` — aggregate results have empty join-rows and coarse topics).

Result of `d/query` for a flat pattern list: `{:topics [...] :symbol-values
{...} :join-rows #{...}}` (`datalog.clj:3423-3457` docstring). For a nested
query: `{:data [...] :topics <merged>}` where `:data` is
(`nested-sql-result->result`, `datalog.clj:3123-3200`):

```clojure
[{:result {:join-rows #{...} :symbol-values {...} :topics [...] (:page-info ...) (:aggregate ...)}
  :datalog-query <original patterns, join-sym replaced by the actual parent eid>
  :children [ ;; one entry per parent-entity value of join-sym
    [{:result {...} :datalog-query [[:ea #uuid-parent [attr-ids]]] :children [...]}]
    ...]}]
```

Rows are pre-grouped by the string of their join-sym value
(`group-rows-by-join-sym`, `datalog.clj:3097-3121`), so each child slice
contains only that parent's rows. `replace-join-sym-in-datalog-query`
(`datalog.clj:3062-3084`) substitutes the parent eid into the child's stored
pattern list — this makes each child's `datalog-query` identical to what a
standalone query for that parent would have used (used as a permission-check
cache key, `instaql.clj:1861-1872`).

### 4.4 The InstaQL node tree

`collect-query-results` (`instaql.clj:1083-1109`) zips `:data` with the forms:

```clojure
;; one per top-level namespace:
{:data {:k "users" :etype "users" :option-map {...}
        :datalog-query <where patterns>
        :datalog-result {:join-rows #{...} :symbol-values {...} :topics [...]
                         :page-info {...}? :aggregate {...}?}}
 :child-nodes
 [;; one per matched parent entity:
  {:data {:etype "users"
          :datalog-query [[:ea #uuid <eid> #{attr-ids}]]
          :datalog-result {:join-rows #{[[e a v t] ...]} ...}}
   :child-nodes [<recursively: one node per child form, same shape as top>]}
  ...]}
```

Note child-nodes are **positional, keyed by entity via the `[:ea eid ...]`
datalog-query and the join-rows' eids** — there is no explicit eid key. The
client/object-tree builder derives entities from join-rows
(`entity-model/datalog-result->map`) and attaches each child form's results
under the form's `:k` (`util/instaql.clj:86-102`).

`$files` special-casing (`transform-$files-result`, `instaql.clj:1050-1081`):
on entity nodes for `$files`, a synthetic triple
`[eid $files/url-attr-id <signed-download-url> t]` is appended when `url` is
requested, and `location-id` triples are removed unless explicitly requested.

For the websocket client, results are flattened to
`[{:data {:datalog-result {:join-rows [<all-triples-deduped>]}, :page-info
{ns → page-info}, :aggregate {ns → agg}} :child-nodes []}]`
(`reactive/query.clj:76-96`) — the client re-runs the query locally over the
triple store (`instaql.ts`), so only the union of triples plus page-info
matters to it. The client trusts `start-cursor`/`end-cursor` to bound
paginated results (`instaql.ts:745-767`) and applies nested
limit/order/fields itself.

---

## 5. Nested children: execution & batching

- Child queries are **not** executed per-parent; the whole form tree compiles
  into one SQL statement (§4.2). "Per-parent" nesting is reconstructed from
  `symbol-values[join-sym]` — for each parent eid, a `:children` slice is
  emitted (`datalog.clj:3183-3196`; if the child's own symbol-values lack the
  join-sym — e.g. missing-attr child — the parent's join-val is used as the
  single key).
- Child join semantics: the child's first pattern is the link pattern
  `[idx <parent-sym> link-attr ?child-sym]` (direction per fwd/rev,
  `->guarded-ref-attr-pat`, `attr_pat.clj:131-142`), followed by the child's
  own where patterns, then its own `[:ea ?child-sym attr-ids]` fetch group,
  recursively.
- The **dataloader** (`make-loader`/`add-pending!`/`take-batch!`/
  `send-query-batch`, `datalog.clj:3205-3378, 3458-3509`) batches *flat*
  datalog queries (used by permission checks / `entity-map` fetches / rules)
  into a single `json_build_object` SQL statement of ≤50 sub-queries — it is
  not part of the main InstaQL nested query path.
- Empty parents: a parent with no matching entities produces no child slices;
  `permissioned-node` clears child-nodes entirely when the parent's join-rows
  are all filtered (`instaql.clj:1945-1954`).

---

## 6. Topics & invalidation

### 6.1 Topic format

A topic is a 4-tuple `[idx-set e-part a-part v-part]` where:

- `idx-set`: set of index keywords, e.g. `#{:ea}`, `#{:av}`, `#{:eav}`,
  `#{:ave}`, `#{:vae}` (query topics use singletons; WAL topics use the set of
  flags on the changed triple).
- `e-part` / `a-part` / `v-part`: either the wildcard symbol `'_` or a **set**
  of concrete values (uuids for e/a; json values or uuids for v). The v-part
  of a query topic may also be a function map `{:$not v}` or
  `{:$comparator {:op :data-type :value}}` carried through verbatim.

There is no app-id inside a topic; topics are bucketed by app-id in the
reactive store.

### 6.2 Topics produced by a query

Per pattern, using symbol-values accumulated from *previous* patterns
(`named-pattern->topics`, `datalog.clj:382-401`):

```clojure
;; variables bound by earlier patterns are replaced by their value sets;
;; unbound variables and '_ become '_ ; constants stay as their sets.
[[#{(idx-key idx)} e-part a-part v-part]]

;; $isNull patterns expand to two ea topics:
[[#{:ea} e-part a-part '_]
 [#{:ea} '_ #{isnull-target-attr-id} '_]]
```

Example (verbatim from the docstring, `datalog.clj:352-368`):

```
patterns [[:eav foo-id friend-id ?f]
          [:ea ?f ?a ?v]]
topics   [[:eav foo-id friend-id _]
          [:ea #{friend-ids-from-first-join} _ _]]
```

Bound-but-empty result sets produce `#{}` (matches nothing) rather than `'_`
(see `pagination` test topic `[#{:ea} #{} #{:users/id} _]`,
`instaql_test.clj:684`). Ref-valued (`:vae` v) symbols are excluded from
topic substitution (`symbol-values-for-topics`, `datalog.clj:2973-2990`).
The `[:ea eid attr-ids]` entity-fetch pattern yields
`[#{:ea} #{eid} #{attr-ids...} _]`. Missing-attr groups yield the catch-alls
`[[#{:ea} _ _ _] [#{:eav} _ _ _]]` (`datalog.clj:2899-2903`). Aggregates use
coarse topics of their patterns (`datalog.clj:3141`).

**Coarse topics** (flag `use-coarse-topics?`, or hashed queries): topics are
derived from the raw patterns without any result substitution —
constants→sets, symbols→`'_`, padded to 5 slots (`pats->coarse-topics`,
`datalog.clj:305-336`; expected output in `coarse-topics` test,
`instaql_test.clj:4197-4229`). Coarse topics are 5-element (trailing `_` for
created-at); the matcher only inspects the first 4.

Topics from all nodes are unioned; near-duplicate topics differing only in the
e-set (when e is a set) or v-set are merged by unioning that component
(`add-topics!`/`collect-all-topics`, `datalog.clj:3279-3308`).

### 6.3 Topics produced by a transaction (WAL)

`reactive/topics.clj` converts logical-replication changes on `triples`:

- insert (`:82-90`): `[[ks #{e} #{a} #{v}]]` where
  `ks = (set (filter m #{:ea :eav :av :ave :vae}))` — the boolean index-flag
  columns that are true on the row — and `v` is the json-decoded value
  (uuid-parsed when `eav`, Instant-parsed when `checked_data_type = date`,
  `parse-v` `:72-80`).
- update (`:92-122`): same e/a → `[[ks #{e} #{a} #{v old-v}]]`; unchanged
  (or TOASTed-unchanged) value → no topic; e/a changed → two topics.
- delete (`:124-132`): `[[ks #{e} #{a} #{v}]]` from the old row.
- attr/ident create/update/delete (`:146-195`):
  `[#{:ea :eav :av :ave :vae} '_ #{attr-id} '_]`; restoring a soft-deleted
  object attr additionally emits the catch-all `[#{:ea} '_ '_ '_]`.

### 6.4 Matching (decide refresh)

`store.clj:1111-1118`:

```clojure
(defn match-topic? [[iv-idx iv-e iv-a iv-v] [dq-idx dq-e dq-a dq-v]]
  (and (match-topic-part? iv-idx dq-idx)   ; per part:
       (match-topic-part? iv-e dq-e)
       (match-topic-part? iv-a dq-a)
       (match-topic-part? iv-v dq-v)))
```

`match-topic-part?` (`store.clj:1066-1109`), where `iv-part` is from the
transaction (always a set or `'_`… idx is a set) and `dq-part` from the query:

1. `'_` on either side → match.
2. both sets → match iff they intersect.
3. `dq-part = {:$comparator {:op :data-type :value}}` → match iff **any** iv
   value satisfies the comparison; per type: numbers use numeric `< > <= >=`;
   booleans/strings use natural ordering; dates compare `java.time.Instant`s;
   `$like`/`$ilike` compile the pattern to an anchored regex (`%`→`.*`,
   `_`→`.`, DOTALL, case-insensitive for ilike; `like-parts`/`like-pattern`
   `store.clj:968-1009`). A nil iv value matches `$lt/$lte` only
   (`match-nil`, `store.clj:1055-1064`).
4. `dq-part = {:$not v}` → match iff any iv value ≠ v.

A query is stale iff **any** transaction topic matches **any** of its topics
(`matching-topic-intersection?`, `store.clj:1120-1127`); `mark-stale-topics!`
(`store.clj:1444`) then schedules the refresh of the sessions holding those
queries.

### 6.5 Refined topics (`instaql_topic.clj`) — optional

For queries the compiler can handle, a CEL program is compiled from the forms
and evaluated against the *changed entity* (`{"etype": ..., "attrs":
{attr-id-str: value}}`) to skip refreshes that a coarse topic match would have
triggered. Structure (`:166-192`): top form → `entity.etype == <etype> AND
<each where-cond as attrs[attr-id] == v / != null>`; child forms → OR of
`entity.etype == <child etype>`; whole query → OR over forms. Only supported
shapes compile: single-segment paths, forward cardinality-one attrs, plain
values or `$isNull`; anything else (`or`/`and`, multi-part paths, reverse
attrs, `$in`, comparators…) returns `{:not-supported reason}` and the system
falls back to plain topic matching (`:23-24, 45-99, 271-277`). Dates compare
via a custom `instant_date_eq` epoch-millis function (`:199-237`). Runtime
errors default to `true` (refresh) (`:264-269`). This is purely an
optimization; a port can omit it.

---

## 7. Missing attrs, `$users`, guarded behaviors

- **Unknown namespace / unknown attr anywhere in a form's where or link
  path**: not an error. `guarded-where-query` (`instaql.clj:890-902`) and
  `->guarded-ref-attr-pat` (`attr_pat.clj:131-142`) catch the
  record-not-found exception (validation errors are re-thrown) and mark the
  group `missing-attr? true`. The datalog layer returns
  `{:join-rows #{} :symbol-values {sym #{} ...} :topics [[#{:ea} _ _ _]
  [#{:eav} _ _ _]]}` (`missing-attr-result`, `datalog.clj:2899-2903`) — so
  the query is subscribed to *any* attr creation and refreshes once the attr
  exists. Applies to top-level unknown namespaces, unknown where attrs, and
  unknown child link names (each parent still gets a missing-attr child node);
  see `missing-attrs` test (`instaql_test.clj:3672-3752`). The client mirrors
  this by swallowing `AttrNotFoundError` → empty result
  (`instaql.ts:862-871`).
- **`$users` (and other `$` system namespaces)**: ordinary etypes backed by
  system-catalog attrs; queries work identically (`users-table-queries`,
  `instaql_test.clj:4317-4367`; references to/from `$users`:
  `:4400`). Default permissions deny non-admin reads of other users
  (`users-table-read-permissions`, `:4369`). `$files` gets the url/location-id
  join-row rewrite (§4.4). Hidden system attrs are filtered from user-visible
  attr listings (`attr.clj:1079-1091`) but `ea-ids-for-etype` still includes
  e.g. `$files/location-id`.
- **Order-by / comparator guards**: attrs that are still `indexing?`,
  `checking-data-type?`, or `setting-unique?` are treated as
  unindexed/untyped/non-unique for index selection, and produce validation
  errors when used with comparators or `order` (§2.3, §3.1).
- **App read gate**: `iq/query` first asserts the app is read-enabled
  (`app-model/assert-read-allowed!`, `instaql.clj:1816-1818`).
- **Query modifiers**: an ops flag can merge extra `$` params (e.g. a `limit`)
  into specific query shapes by hash (`add-query-modifiers`,
  `instaql.clj:1239-1246`).
- **Aggregate guards**: non-admin → validation error; aggregate + children →
  validation error (`instaql.clj:1186-1200`).
- **Permissions** (context, not query-engine): `permissioned-query`
  (`instaql.clj:2236-2268`) may rewrite the query with rule-derived `where`
  clauses (`add-rule-wheres-to-query`), runs the query, then post-filters
  join-rows per-triple via CEL checks (`permissioned-node`,
  `instaql.clj:1916-1954`), fixing up page-info cursors.

---

## 8. Test cases worth porting (`server/test/instant/db/instaql_test.clj`)

| test (line) | behavior |
|---|---|
| `validations` (212) | exact validation-error shapes for bad forms, wheres, pagination opts, aggregate, fields |
| `validations-on-checked-data` (327) | comparator/type errors: wrong value type, unindexed/unchecked attrs, invalid date strings |
| `equality-on-dates-without-index` (431) | unindexed date attr: equality is raw-value only (no date normalization) |
| `equality-on-dates-with-index` (474) | indexed+checked date: differently-formatted equal dates match |
| `pagination` (518) | limit/offset/first/last/before/after/inclusive cursors, distinct entities, has-next/prev matrix incl. empty-page offset/after → has-prev true |
| `pagination-with-checked-fields` (873) | same pagination matrix ordering by an indexed typed attr instead of serverCreatedAt |
| `pagination-with-same-values` (1149) | order tie-break by entity-id (pg uuid ordering) |
| `pagination-with-null-values` (1263) | nulls-first (asc) ordering; cursors over null order values |
| `obj-tree-order` (1344) | object-tree output ordering: default serverCreatedAt asc, desc, typed fields |
| `flat-where` (1463) | plain scan `{users:{}}`, where by attr, where by id; exact topics+triples |
| `deep-where` (1552) | dot-path where through links (attr values and ref ids) |
| `multiple-where` (1628) | AND of several conds; no matches; child-form where |
| `where-in` (1709) | `$in`/`in` membership (topics use value sets) |
| `where-$like` (1749) / `where-$ilike` (1839) | `%`/`_` patterns, startsWith/endsWith/contains, deep paths, case-insensitivity |
| `where-$not` (1871) / `where-$ne` (1943) | not-equal includes entities missing the attr; `$ne` alias |
| `where-$not-with-nils` (1981) | `$not`/`$ne` match json-null values |
| `where-$isNull` (2091) | isNull true/false on attrs and links, incl. dot-paths and intermediate-null capture |
| `where-$not-$isNull-with-links-{1-to-1,1-to-many,many-to-1,many-to-many}` (2334-2564) | isNull/$not across every link cardinality, forward and reverse |
| `$isNull-and-$not-with-every-type` (2642) | isNull/$not on indexed string/number/date/boolean attrs |
| `where-or` (2745) | or with no/mixed matches, nested relations inside or |
| `where-and` (2851) | and combinations incl. nested ors inside and |
| `or-stress-test` (2942) | deeply nested or/and correctness |
| `comparators` (2997) | $gt/$gte/$lt/$lte/$like on all four types + verifies the type-specific indexes are used |
| `in-with-types` (3124) | `$in` with typed coercion per data type |
| `$not-with-refs` (3197) | `$not` where the attr is a link (uuid value) |
| `lookup-unique-uses-the-av-index` (3243) | unique-attr equality and lookup-ref queries use `av` (topics show `:av`) |
| `arbitrary-order-by-all-types` (3314) | order by indexed string/number/date/boolean, asc+desc, with cursors |
| `nested-order-by` (3426) | order in child forms is accepted & applied to object tree (server ignores for fetch) |
| `order-by-with-ors-and-ands` (3457) | ordering combined with or/and wheres |
| `child-forms` (3492) | nested children: plain, reverse links, inner wheres, inner or/and; child join-rows shape |
| `missing-attrs` (3672) | unknown namespace / where attr / child link → empty results + `[[#{:ea} _ _ _] [#{:eav} _ _ _]]` topics |
| `same-ids` (3754) | same eid in two namespaces stays separated by attr-ids (ea fetch restricted per etype) |
| `eid-relations` (3797) | `where {link: uuid}` forward + reverse (`:vae` topics) |
| `indexing?` (3843) / `uniqueing?` (3913) | attrs mid-indexing/mid-uniqueing are treated as unindexed/non-unique |
| `default-perms` (3996) / `read-perms` (4051) / `read-rule-params` (4035) | permission filtering of nodes, page-info filtering, `$$ruleParams` |
| `coarse-topics` (4197) | exact `pats->coarse-topics` output for a nested query |
| `aggregates` (4231) | `{$: {aggregate: :count}}` → `{:count n}`, empty triples, coarse topic |
| `namespaces-that-share-eids` (4256) | perms apply per-etype even when eids collide |
| `users-table-queries` (4317) | `$users` plain + where-by-email queries (system attrs incl. `:ave` email topic) |
| `users-table-references` (4400) / `-perms-with-references` (4469) / `auth-ref-perms` (4523) | links to/from `$users`, `auth.ref` rules |
| `ordering-with-where-and-limit` (4566) | order+where+limit interplay (regression suite for paged CTE) |
| `fields` (4714) / `fields-with-rules` (4758) | `fields` projection restricts ea attr-ids but keeps `id`; perms still see full entity |
| `files` (4797) | `$files`: url synthesis, location-id hiding, fields interplay |
| `rule-wheres` (4926) / `field-rules-can-filter-columns` (5108) / `query-rate-limit-e2e` (5188) | rule-where query rewriting, field-level rules, rate limits |
| `pg-hint-plan-is-working` (4955) | hints emit (perf-only; skip in a port) |
| `indexed-ors-collapse-isNull-true` (4981) | or of eq/isNull on indexed attrs collapses into `$in` (nil allowed in `$in` for all indexed types) |
| `flat-where-byop` (1415) | BYOP (bring-your-own-postgres) variant — separate subsystem (`instaql.clj:1279-1814`), likely out of scope |
