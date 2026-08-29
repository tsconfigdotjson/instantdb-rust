# InstantDB Core Data Model

Reference for the Rust reimplementation on Postgres, reusing the legacy schema. All paths are
relative to `LEGACY/server/` unless noted. Primary sources:

- `src/instant/db/model/attr.clj` — attr shape, idents, CRUD SQL
- `src/instant/db/model/triple.clj`, `triple_cols.clj` — triples, index flags, insert/delete SQL
- `src/instant/db/transaction.clj`, `db/model/transaction.clj` — tx-steps, tx-id
- `src/instant/system_catalog.clj`, `system_catalog_ops.clj` — system attrs
- `resources/migrations/*.up.sql` — table DDL (248 migrations; final shapes reconstructed below)

---

## 1. Postgres tables

### 1.1 `apps`

Created in `resources/migrations/01_bootstrap.up.sql:37-42`, then evolved.

| column | type | notes |
|---|---|---|
| `id` | `uuid PRIMARY KEY` | |
| `creator_id` | `uuid REFERENCES instant_users(id) ON DELETE CASCADE` | NOT NULL dropped by `79_add_orgs.up.sql:37` |
| `title` | `text NOT NULL` | |
| `created_at` | `timestamp DEFAULT NOW()` | |
| `connection_string` | `bytea` | BYOP, `25_byop_connection_string.up.sql` |
| `magic_code_expiry_minutes` | `integer` | `100_shorten-magic-code-lifetime.up.sql` |
| `org_id` | `uuid REFERENCES orgs(id)` | `79_add_orgs.up.sql:34` |
| `subscription_id` | `uuid REFERENCES instant_subscriptions(id) ON DELETE SET NULL` | `81_denormalize_subscription.up.sql` |
| `status` | `text NOT NULL DEFAULT 'active' CHECK (status IN ('active','read-only','disabled'))` | `115_add_app_status.up.sql` |

Constraints/indexes: `apps_creator_id` index (`01_bootstrap.up.sql:44`); `requires_creator_or_org`
CHECK — exactly one of `creator_id` / `org_id` is non-null (`89_creator_or_org_on_app.up.sql`);
`REPLICA IDENTITY FULL` (`116_apps_replica_identity_full.up.sql`). Trigger
`prevent_delete_system_catalog_app_trigger` (BEFORE DELETE) blocks deleting the system-catalog app
(`34_hardcoded_objects.up.sql:36-50`).

Writes are gated on `status` (`transaction.clj:574` → `app-model/assert-write-allowed!`;
errors `::app-read-only` / `::app-disabled` in `util/exception.clj:332,337`).

### 1.2 `attrs`

Created in `01_bootstrap.up.sql:72-83`; final shape (mirrored in `attr-table-cols`,
`db/model/attr.clj:205-212`):

| column | type | added by |
|---|---|---|
| `id` | `uuid PRIMARY KEY` (`attrs_pkey`) | bootstrap |
| `app_id` | `uuid NOT NULL REFERENCES apps(id) ON DELETE CASCADE` | bootstrap |
| `value_type` | `text NOT NULL` — `'blob'` \| `'ref'` | bootstrap |
| `cardinality` | `text NOT NULL` — `'one'` \| `'many'` | bootstrap |
| `is_unique` | `boolean NOT NULL` | bootstrap |
| `is_indexed` | `boolean NOT NULL` | bootstrap |
| `forward_ident` | `uuid NOT NULL` (points at `idents.id`) | bootstrap |
| `reverse_ident` | `uuid` (nullable; only refs have one) | bootstrap |
| `inferred_types` | `bit(32)` (nullable) | `27_inferred_type.up.sql` |
| `on_delete` | `attr_on_delete` enum, only value `'cascade'` | `35_cascade_delete.up.sql` |
| `checked_data_type` | `checked_data_type` enum `('string','number','boolean','date')` | `36_checked_data_type.up.sql:1-3` |
| `checking_data_type` | `boolean` (job in flight) | `36_checked_data_type.up.sql:4` |
| `indexing` | `boolean` (job in flight) | `36_checked_data_type.up.sql:5` |
| `setting_unique` | `boolean` (job in flight) | `38_uniqueing.up.sql` |
| `on_delete_reverse` | `attr_on_delete` | `54_on_delete_reverse.up.sql` |
| `is_required` | `boolean` (nullable, default false) | `59_is_required.up.sql`, NOT NULL dropped by `72_drop_not_null_from_required.up.sql` |
| `etype`, `label` | `text NOT NULL` (denormalized fwd ident name) | `68_attrs_etype_label_schema.up.sql:1-2`, NOT NULL via `69_attrs_etype_label_data.up.sql:7-8` |
| `reverse_etype`, `reverse_label` | `text` (denormalized rev ident name) | `68_attrs_etype_label_schema.up.sql:3-4` |
| `deletion_marked_at` | `timestamptz` (soft delete) | `75_attr_soft_deletes.up.sql` |
| `metadata` | `jsonb` (holds `soft_delete_snapshot`) | `76_attr_metadata.up.sql` |

Constraints/indexes: indexes `attrs_app_id`, `attrs_forward_ident`, `attrs_reverse_ident`
(`01_bootstrap.up.sql:85-87`); UNIQUE `attrs_etype_label_unique (app_id, etype, label)` and
`attrs_reverse_etype_label_unique (app_id, reverse_etype, reverse_label)`
(`68_attrs_etype_label_schema.up.sql:6-10`); partial index `idx_attrs_deletion_marked_at`
(`75_attr_soft_deletes.up.sql`); `REPLICA IDENTITY FULL` (`31_attrs_replica_full.up.sql`).
Trigger `trg_attrs_unique_names` (BEFORE INSERT/UPDATE) raises errcode `23505` if a new
`(etype,label)` collides with any existing `(reverse_etype,reverse_label)` or vice versa
(`68_attrs_etype_label_schema.up.sql:12-46`). A trigger also prevents deleting system-catalog
attrs unless `instant.allow_system_catalog_app_attr_delete` is set
(`34_hardcoded_objects.up.sql:52+`).

### 1.3 `idents`

`01_bootstrap.up.sql:89-100`; cols mirrored in `ident-table-cols` (`attr.clj:241-243`):

| column | type |
|---|---|
| `id` | `uuid PRIMARY KEY` (`idents_pkey`) |
| `app_id` | `uuid NOT NULL REFERENCES apps(id) ON DELETE CASCADE` |
| `attr_id` | `uuid NOT NULL REFERENCES attrs(id) ON DELETE CASCADE` |
| `etype` | `text NOT NULL` |
| `label` | `text NOT NULL` |

Constraint `app_ident_uq UNIQUE (app_id, etype, label)` (`01_bootstrap.up.sql:96`; also the
replica identity, `119_replica_identity.up.sql`). Indexes `idents_app_id`, `idents_attr_id`.
An attr is one logical object persisted across `attrs` + one or two `idents` rows (forward, and
reverse for refs) — see `insert-multi!` CTE, `attr.clj:352-528`.

### 1.4 `triples`

`01_bootstrap.up.sql:102-122` plus later columns. Column list as used by inserts
(`triple-cols`, `db/model/triple_cols.clj:4-5`): `app_id, entity_id, attr_id, value, value_md5,
ea, eav, av, ave, vae, checked_data_type` (+ `created_at`, `pg_size` maintained by default/trigger).

| column | type | notes |
|---|---|---|
| `app_id` | `uuid NOT NULL REFERENCES apps(id) ON DELETE CASCADE` | |
| `entity_id` | `uuid NOT NULL` | no FK; entities exist only as triples |
| `attr_id` | `uuid REFERENCES attrs(id) ON DELETE CASCADE` | |
| `value` | `jsonb NOT NULL` | JSON-encoded value; `'null'::jsonb` for null |
| `value_md5` | `text NOT NULL` | `md5(value::text)` of the JSON text |
| `ea` | `boolean NOT NULL` | cardinality-one flag |
| `eav` | `boolean NOT NULL` | ref (forward) flag |
| `av` | `boolean NOT NULL` | unique flag |
| `ave` | `boolean NOT NULL` | indexed flag |
| `vae` | `boolean NOT NULL` | ref (reverse) flag |
| `created_at` | `bigint DEFAULT current_unix_timestamp_ms()` | epoch ms, `08_add_createdAt_for_triples.up.sql` |
| `checked_data_type` | `checked_data_type` enum, nullable | `36_checked_data_type.up.sql:7` |
| `pg_size` | `integer` | `83_triples_pg_size.up.sql`, maintained by statement triggers |

**Primary key**: `(app_id, entity_id, attr_id, value_md5)` (`01_bootstrap.up.sql:121`,
`41_primary_keys.up.sql`).

**Indexes** (final state):

- `ea_index` — `UNIQUE (app_id, entity_id, attr_id) WHERE ea` (`01_bootstrap.up.sql:130-132`).
  Enforces one row per entity+attr for cardinality-one; the upsert conflict target.
- `av_index` — `UNIQUE (app_id, attr_id, json_null_to_null(value)) INCLUDE (entity_id) WHERE av`
  (`53_change_indexing_of_triple_nulls.up.sql`; `json_null_to_null` maps `'null'::jsonb`→SQL NULL
  so JSON nulls don't collide). Enforces uniqueness; serves lookup refs.
- `ave_index` — `(app_id, attr_id, value) WHERE ave` (`45_remove_entity_id_from_indexes.up.sql:38-42`)
  and `ave_with_e_index` — `(app_id, attr_id, value, entity_id) WHERE ave` (`90_add_e_to_ave_index.up.sql`).
- `eav_uuid_index` — `UNIQUE (app_id, entity_id, attr_id, json_uuid_to_uuid(value)) WHERE eav`
  (`73_vae_uuids.up.sql`; old `eav_index` dropped in `74_drop_old_vae_eav_indexes.up.sql`).
- `vae_uuid_index` — `(app_id, json_uuid_to_uuid(value), attr_id, entity_id) WHERE vae`
  (`73_vae_uuids.up.sql`).
- Typed partial indexes for checked types, all `WHERE ave AND checked_data_type = '<t>'`:
  `triples_string_trgm_gist_idx` (gist, `triples_extract_string_value(value) gist_trgm_ops`),
  `triples_number_type_idx` / `triples_boolean_type_idx` / `triples_date_type_idx` on
  `(app_id, attr_id, triples_extract_<t>_value(value) NULLS FIRST)`
  (`40_checked_indexes.up.sql`, `45_…`, `56_nulls_first_indexes.up.sql`).
- `triples_created_at_idx` on `(created_at)` (`51_add_triples_created_at_index.up.sql`).
- `triples_app_id`, `triples_attr_id` (`01_bootstrap.up.sql:124-126`).

**Check constraints**:

- `valid_ref_value` — when `eav OR vae`, `triples_extract_uuid_value(value)` must be non-null,
  i.e. value is a JSON string containing a valid uuid (`64_better_uuid_constraint.up.sql`;
  replaces `ref_values_are_uuid`, dropped in `65_drop_old_constraint.up.sql`).
- `indexed_values_are_constrained` — when `eav OR av OR ave OR vae`,
  `pg_column_size(value) <= 1024` (`01_bootstrap.up.sql:157-164`).
- `valid_value_data_type` — `triples_valid_value(checked_data_type, value)`: string/number/boolean
  must match `jsonb_typeof` (or be JSON null); date accepts epoch-ms numbers or parseable
  timestamp strings (`36_checked_data_type.up.sql:119-155`).

**Triggers** (statement-level, with transition tables; `85_triples_size_trigger.up.sql:96-113`,
functions last updated in `110_triples_triggers_include_system_catalog.up.sql`):
`triples_batched_after_insert` / `_update` / `_delete` — maintain `pg_size`, feed
`app_files_to_sweep` when a `$files.location-id` triple (attr id
`96653230-13ff-ffff-2a34-b40fffffffff`) disappears, and optionally emit entity-change WAL
messages when `instant.wal_msg_app_id` is set (set per-tx by `transaction-model/create!`,
`db/model/transaction.clj:38-53`). `REPLICA IDENTITY FULL` (`77_add_sketch.up.sql:45`).

**Helper SQL functions** the model code relies on: `json_null_to_null(jsonb)`
(`53_…up.sql`), `json_uuid_to_uuid(jsonb)` = `(v->>0)::uuid` (`73_vae_uuids.up.sql`),
`triples_extract_uuid_value/string/number/boolean/date_value` (`36_…`, `64_…`),
`current_unix_timestamp_ms()` (`08_…`), `raise_exception_message(text)` (used to throw from CTEs).

### 1.5 `transactions`

`03_add_transactions.up.sql`:

```sql
CREATE TABLE transactions (
  id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,   -- GLOBAL, monotonic across all apps
  app_id uuid NOT NULL REFERENCES apps(id) ON DELETE CASCADE,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
```

Indexes: `transactions_created_at_idx (created_at)`
(`46_add_created_at_index_to_transactions.up.sql`); `transactions_app_id_id_idx (app_id, id DESC)`
(`93_sync_table.up.sql:2`; old `transactions_app_id_idx` dropped by
`94_drop_old_transactions_index.up.sql`).

`tx-id` is the identity `id` — a **single global sequence**, not per-app. Clients see the latest
`tx-id` per app as `processed-tx-id` for invalidation ordering.

---

## 2. Attr semantics

Clojure spec: `db/model/attr.clj:59-112`. An attr is:

- `:id` — uuid (client-generated or system-derived).
- `:forward-identity` — triple `[ident-id etype label]` (`attr.clj:67-69`), e.g.
  `[#uuid… "posts" "author"]`. The "ident name" is `[etype label]` (`attr.clj:123-140`).
- `:reverse-identity` — `[ident-id rev-etype rev-label]`; **required for `:ref`, absent for
  `:blob`** (`attr.clj:99-107`).
- `:value-type` — `:blob` (scalar/JSON data) or `:ref` (link between entities) (`attr.clj:61`).
- `:cardinality` — `:one` or `:many` (`attr.clj:59`).
- `:unique?`, `:index?` — booleans, required keys (`attr.clj:85-91`).
- `:required?` — optional boolean (`attr.clj:77,93`). Adding/updating to required is validated
  against existing data (`validate-add-required!` `attr.clj:334-350`,
  `validate-update-required!` `attr.clj:533-581`).
- `:checked-data-type` — optional, one of `"number" "string" "boolean" "date"` (`attr.clj:54,79`).
- `:on-delete` / `:on-delete-reverse` — optional, only `:cascade` (`attr_on_delete` enum,
  `35_cascade_delete.up.sql`, `54_on_delete_reverse.up.sql`). `:on-delete :cascade` on a ref
  means: deleting the *reverse-side* entity cascades to delete the forward-side owner
  (children pointing at it). See §4 delete-entity.
- `:indexing?`, `:checking-data-type?`, `:setting-unique?` — optional booleans, true while a
  background indexing job is running (`attr.clj:81-83`); query planning ignores an index while
  its job is in flight (`attr_pat.clj:17-51` `best-index`).
- `:inferred-types` — set of `#{:number :string :boolean :json}`, decoded from the `bit(32)`
  column (bit i = type i in `types`, `attr.clj:22-52`). Updated opportunistically on every
  triple insert via `insert-attr-inferred-types-cte` (`triple.clj:120-153`; skipped for
  system-catalog attrs).
- `:catalog` — **derived, not stored**: `:system` if `app_id = system-catalog-app-id`, else
  `:user` (`row->attr`, `attr.clj:877-879`).

`row->attr` (`attr.clj:844-889`) maps SQL → clj: nil-able keys (`reverse-identity`, `on-delete`,
`on-delete-reverse`, `checked-data-type`, `indexing?`, `checking-data-type?`, `setting-unique?`,
`deletion-marked-at`, `metadata`) are omitted when null; `required?` coalesces null→false.

Attr loading: `get-by-app-id` returns the app's attrs **plus all system-catalog attrs**, excluding
soft-deleted (`attr.clj:981-1002`), cached per app (`attr.clj:174-187`).

### Wire JSON of an attr

Attrs are sent verbatim over the websocket (`init-ok` / `add-attr-ok` etc.,
`reactive/session.clj:186-191`) via cheshire, which renders keyword keys as their names —
including `?` and `-`. Authoritative client type: `client/packages/core/src/attrTypes.ts:13-29`:

```jsonc
{
  "id": "8b805e57-…",
  "value-type": "blob",              // "blob" | "ref"
  "cardinality": "one",              // "one" | "many"
  "forward-identity": ["<ident-uuid>", "posts", "author"],
  "reverse-identity": ["<ident-uuid>", "users", "posts"],  // refs only
  "unique?": true,
  "index?": true,
  "required?": false,
  "inferred-types": ["string"],      // or null
  "catalog": "user",                 // "system" | "user"
  "on-delete": "cascade",            // optional
  "on-delete-reverse": "cascade",    // optional
  "checked-data-type": "string",     // optional
  "indexing?": true,                 // optional, while indexing job runs
  "setting-unique?": true            // optional
}
```

Keyword values (`:blob`, `:one`, `:cascade`, `:string`) serialize as bare strings; ident uuids as
strings. `remove-hidden` (`attr.clj:1079-1091`) filters what schema APIs show users: system attrs
outside `$users`/`$files`/`$streams`, plus `$files` internals (`content-type`,
`content-disposition`, `size`, `location-id`, `key-version`) and `$streams` internals
(`machineId`, `hashedReconnectToken`).

### Attr CRUD SQL

- **Insert** (`insert-multi!`, `attr.clj:352-528`): one CTE statement inserting `idents` then
  `attrs`; conflicting re-inserts with identical column values are idempotent no-ops, otherwise
  `raise_exception_message` aborts ("Another attribute for etype.label exists…" /
  "The attribute with id X conflicts…"). Also inserts `'null'::jsonb` triples for every existing
  entity of the etype when a new **indexed blob** attr is created (`indexed-null-triples`,
  `attr.clj:459-499`) so `ave` scans can find nulls. Validates: reserved `$` names
  (`attr.clj:313-332`), system-catalog conflicts (`attr.clj:268-311`), required-on-nonempty-etype
  (`attr.clj:334-350`).
- **Update** (`update-multi!`, `attr.clj:583-655`): patches attr row (null args keep old values via
  `not-null-or`), rewrites `idents.etype/label`, and **rewrites the flag columns of all existing
  triples** whose derived `ea/eav/av/ave` would change (`triple-updates` CTE, `attr.clj:613-634`).
- **Delete** (`:delete-attr` tx-step) is a **soft delete** (`soft-delete-multi!`,
  `attr.clj:728-824`): sets `deletion_marked_at = now()`, forces `is_indexed=false,
  is_required=false`, brands etype/label as `{attr-id}_deleted$…` to free the unique name, and
  snapshots prior state into `metadata.soft_delete_snapshot`. `restore-multi!` (`attr.clj:657-726`)
  reverses it. Hard deletion happens later by sweeper (`get-for-hard-delete`, `attr.clj:1041-1049`;
  `hard-delete-multi!` `attr.clj:826-839` relies on FK CASCADE to remove idents + triples).

---

## 3. Triple semantics

A triple is `[e a v]`: `e` = entity uuid (or lookup ref), `a` = attr uuid, `v` = value
(spec `triple.clj:29-51`). Physical row carries five derived flags, computed **from the attr at
insert time** (`enhanced-triples` CTE, `triple.clj:902-924` and equivalents):

| flag | true when | meaning / index |
|---|---|---|
| `ea` | `cardinality = 'one'` | at most one row per `(entity, attr)`; unique `ea_index`; upsert target |
| `eav` | `value_type = 'ref'` | forward link traversal (`entity → value`); unique `eav_uuid_index` (dedupes many-cardinality links) |
| `av` | `is_unique` | uniqueness of `(attr, value)` per app; unique `av_index`; serves lookup refs |
| `ave` | `is_indexed` | value-indexed queries (`ave_index`, typed indexes) |
| `vae` | `value_type = 'ref'` | reverse link traversal (`value → entity`); `vae_uuid_index` |

So: blob+one ⇒ `ea` only (+`av` if unique, +`ave` if indexed); ref ⇒ `eav`+`vae` always
(+`ea` if cardinality one, +`av` if unique link, +`ave` if indexed). Changing attr properties
rewrites flags on existing rows (`attr.clj:613-634`).

**Value encoding.** `value` is jsonb of the raw value; the model serializes with `->json` before
casting (`triple.clj:864,900`). Ref values are the target entity's **uuid as a JSON string**
(`"d0f3…"`) — enforced by `valid_ref_value`. Clojure/Java `UUID` values serialize as strings.
`nil` is stored as JSON `null` (`'null'::jsonb`), not SQL NULL. On read, `eav` triples' values are
parsed back into UUIDs (`row->enhanced-triple`, `triple.clj:1327-1342`).

**`value_md5`** = `md5(<json text of value>)` (e.g. `[[:md5 :it.value]]`, `triple.clj:910`); part
of the PK so many-cardinality attrs can hold multiple values per `(entity, attr)`. md5 of `null`
is the constant `json-null-md5` = `"37a6259cc0c1dae299a7866489dff0bd"` (`util/crypt.clj:61`).

**`created_at`** = `current_unix_timestamp_ms()` at first insert (epoch millis, bigint). On a
cardinality-one conflict update it is preserved unless `overwrite-t` is set, in which case it is
reset to now (`ea-conflict-fields`, `triple.clj:403-407`; used by `$users` import paths).

**Cardinality-one upsert.** `insert-multi!` (`triple.clj:1158-1186`; doc-comment 1159-1180)
splits enhanced triples into `ea` and non-`ea` sets:

- `ea` rows: `INSERT … ON CONFLICT (app_id, entity_id, attr_id) WHERE ea = true DO UPDATE SET
  value = excluded.value, value_md5 = excluded.value_md5` (`triple.clj:937-951`) — the new value
  replaces the old row in place. Duplicate `(e,a)` pairs within one tx are deduped keeping the
  **last** step (`ea-triples-distinct`, DISTINCT ON with `idx DESC`, `triple.clj:926-930`).
  The new path (flag `skip-noop-id-triple-updates`) skips no-op writes
  (`WHERE value_md5 IS DISTINCT FROM excluded.value_md5`) and re-touches the `id` triple only when
  the entity actually changed, to emit a WAL "update" marker (`triple.clj:962-1000`).
- non-`ea` rows (many-cardinality refs): `ON CONFLICT (app_id, entity_id, attr_id, value_md5)
  DO NOTHING` (`triple.clj:953-960`) — adding a distinct value adds a row; re-adding the same
  value no-ops.
- Every new entity also gets `'null'::jsonb` rows for each **indexed blob** attr of its etype that
  wasn't written in this tx (`indexed-null-triples`, `triple.clj:1006-1088`).

**Unique enforcement.** Uniqueness is *only* the partial unique `av_index`. A violation surfaces
as PG error 23505 which `translate-and-throw-psql-exception!` converts to
`::record-not-unique` (`util/exception.clj:628-631`, `278-286`): body
`{"type": "record-not-unique", "message": "`<label>` is a unique attribute on `<etype>` and an
entity already exists with `<etype>.<label>` = <value>", "hint": {"record-type": "triples",
"attr-id": …, "etype": …, "label": …, "value": …}}` (message built in
`extract-unique-triple-data`, `util/exception.clj:244-272`, by parsing the PG DETAIL for
`json_null_to_null(value)`). Ident-name collisions on `app_ident_uq` produce the analogous
"`label` already exists on `etype`" (`util/exception.clj:229-242`). Oversized indexed values hit
`indexed_values_are_constrained` → `::validation-failed` "Value is too large for a unique/indexed
attribute" (`util/exception.clj:686-689`); type mismatches hit `valid_value_data_type` →
`::validation-failed` "Invalid value type for etype.label. Value must be a <type>…"
(`util/exception.clj:668-704`).

**Lookup refs.** A lookup ref `[attr-id value]` may appear as the *eid* (any sequential,
`eid-lookup-ref?` `triple.clj:92-95`) or as the *value* of a ref triple (2-vector whose first is a
uuid, `value-lookup-ref?` `triple.clj:97-102`). Semantics in `insert-multi!`:

- The lookup attr **must be unique** (`av` computed as `raise_exception_message "attribute is not
  unique"` otherwise — `triple.clj:797`); soft-deleted attrs excluded.
- Eid lookups **upsert**: the CTE inserts the `(attr, value)` triple if absent, generating
  `entity_id` = `gen_random_uuid()` (or the value itself, parsed, when the lookup attr's label is
  `"id"`), `ON CONFLICT (app_id, attr_id, json_null_to_null(value)) WHERE av DO UPDATE` dummy-write
  to make the row visible in-tx (`enhanced-lookup-refs`/`lookup-ref-inserts`,
  `triple.clj:784-829`); subsequent triples in the tx resolve `entity_id` from
  `lookup-ref-lookups` (`triple.clj:836-862`).
- Value lookups **must resolve** to an existing entity (or one created by an eid lookup in the same
  tx); otherwise error `missing-lookup-value` → validation error `{"type":"validation-failed", hint
  {data-type: "lookup", input: {attribute-id, namespace, label, value}}, message "The entity for
  the lookup does not exist."}` (`triple.clj:863-899`, `1141-1153`). Value lookups are only allowed
  on `ref` attrs (or `id` label) — `value-lookupable-sql`, `triple.clj:104-118`. The reverse etype
  of the ref attr must match the lookup attr's etype (`validate-value-lookup-etypes`,
  `transaction.clj:532-556`).
- "Update via lookup can't change the lookup attribute's own value unless the entity exists" —
  PG "ON CONFLICT DO UPDATE command cannot affect row a second time" is translated to that
  validation error (`triple.clj:1128-1139`).
- `delete-multi!`/`deep-merge-multi!` resolve lookups similarly but never create entities for the
  delete case (`triple.clj:1262-1322`, `249-399`).
- Batch resolution helpers: `fetch-lookups->eid` (`triple.clj:53-85`), `resolve-lookups`
  (`transaction.clj:229-251`) — both filter on `av` rows.

---

## 4. tx-steps

Wire format: a JSON array of step arrays; first element is the op (string → keyword via `coerce!`,
`transaction.clj:147-166`, which also parses uuid-looking strings anywhere in the step). Spec:
`transaction.clj:32-69`.

| step | shape | semantics |
|---|---|---|
| `add-triple` | `["add-triple", e, attr-id, v, opts?]` | upsert per §3. `opts` = `{"mode": "create"\|"update"\|"upsert"}` (`transaction.clj:25-30`); `create` fails if the entity already exists, `update` fails if it doesn't (`validate-mode`, `transaction.clj:283-358`, errors "Creating entities that exist: …" / "Updating entities that don't exist: …"). |
| `deep-merge-triple` | `["deep-merge-triple", e, attr-id, v, opts?]` | jsonb deep-merge of `v` into the existing cardinality-one blob value via SQL `jsonb_deep_merge_many(old, patches[])` (`deep-merge-multi!`, `triple.clj:249-399`). Multiple patches to the same `(e,a)` in one tx are applied in order (grouped, `triple.clj:252-267`). Object keys merge recursively; JSON `null` deletes; non-object values replace. **Not supported for refs** — raises "merge operation is not supported for links" (`triple.clj:355-357`). Upserts on the `ea` conflict target like add-triple. |
| `retract-triple` | `["retract-triple", e, attr-id, v]` | deletes the exact triple matched by `(app_id, e, a, md5(v))` (`delete-multi!`, `triple.clj:1262-1322`). In practice used for links; retracting indexed blobs would need null back-fill (comment `triple.clj:1259-1261`). |
| `delete-entity` | `["delete-entity", e, etype?]` | deletes (1) all triples `[e _ _]` of that etype and (2) all reverse-ref triples `[_ ref-attr e]` whose attr's `reverse_etype` = etype (`delete-entity-multi!`, `triple.clj:1188-1257`; rows locked in PK order `FOR UPDATE`). Missing etype is resolved from existing triples — one delete per etype found (`resolve-etypes-for-delete-entity`, `transaction.clj:380-391`). Lookup-ref eids are resolved first; unresolvable ones are dropped silently (`transaction.clj:360-378`). **Cascade**: `expand-delete-entity-cascade` (`transaction.clj:393-530`) runs a recursive CTE following ref attrs with `on_delete = 'cascade'` (child→parent: deleting the reverse-side entity deletes forward-side entities pointing at it via `vae`) and `on_delete_reverse = 'cascade'` (deleting the forward-side entity deletes the reverse-side entities it points to via `eav`), emitting additional `delete-entity` steps (and inherited `rule-params`). Returns deleted `(entity_id, attr_id, value, created_at)` rows. |
| `add-attr` | `["add-attr", {attr-map}]` | attr map uses the wire JSON keys of §2; `value-type`/`cardinality` string→keyword coercion (`transaction.clj:136-141`). Runs `attr-model/insert-multi!` (§2 CRUD). |
| `update-attr` | `["update-attr", {"id": …, partial attr keys}]` | spec `attr.clj:114-118` (id required; forward/reverse-identity, unique?, index?, cardinality optional; on-delete/on-delete-reverse settable). Runs `update-multi!` incl. triple flag rewrite. |
| `delete-attr` | `["delete-attr", attr-id]` | soft delete (§2). |
| `restore-attr` | `["restore-attr", attr-id]` | undo soft delete. |
| `rule-params` | `["rule-params", e, etype?, {params}]` | **no DB write** (skipped in the execution reduce, `transaction.clj:586-587`); carries per-entity params into permission checks. Lookup-ref eids resolved (`transaction.clj:369-374`); cascaded deletes inherit the parent's rule-params (`transaction.clj:524-530`). |

**Ordering/grouping**: steps are grouped by op but groups execute in the order each op **first
appears** in the submitted list (`tx-steps-order`/`reorder-tx-steps`, `transaction.clj:636-650`;
execution reduce over groups at `transaction.clj:580-616`). Attr steps take effect optimistically
for resolving later steps in the same tx (`optimistic-attrs`, `transaction.clj:652-666`).
A bare map `{"id": …, "etype": …, k: v, …}` in the steps expands to `add-triple`s
(`expand-maps`, `transaction.clj:668-693`).

**Atomicity & tx-id**: `transact!` wraps everything in one Postgres transaction
(`next-jdbc/with-transaction`, `transaction.clj:711-716`). The **first write** must be
`transaction-model/create!` — `INSERT INTO transactions (app_id) … RETURNING *` plus
`pg_current_wal_insert_lsn()` (`db/model/transaction.clj:31-66`) — so the WAL-based invalidator
can order the tx; the returned `id` (global bigint identity) is the tx-id sent to clients.
After the step writes, `validate-required!` re-checks that every touched-and-alive entity still
has all `required` attrs non-null (`triple.clj:198-233`; error `::validation-failed` with message
``Missing required attribute `etype/label`: <eids>`` and `hint {:records […]}`), and
`validate-update-required!` re-checks attrs newly marked required (`attr.clj:533-581`).
System-catalog app writes are rejected unless `:allow-system-catalog-updates?`
(`transaction.clj:221-227`).

**Validation error shape** (all `throw-validation-err!`, `util/exception.clj:366-385`):
`{type: "validation-failed", message: "Validation failed for <input-type>: <msgs>", hint:
{"data-type": <input-type>, "input": …, "errors": [{message, in?, expected?}…]}}`. Input-types
used here: `tx-steps` (bad shapes, non-uuid eids — `transaction.clj:183-213`), `attributes`
(attr CRUD), `lookup`, `tx-step`, `app`. Uniqueness → `record-not-unique`; missing records →
`record-not-found` (`util/exception.clj:104,130-135`); permission failures → `permission-denied`
(`util/exception.clj:290-296`).

---

## 5. System catalog

`src/instant/system_catalog.clj`. All system attrs live under one hardcoded app:

- **system-catalog app id**: `a1111111-1111-1111-1111-111111111ca7` (`system_catalog.clj:13`)
- **system-catalog user id**: `e1111111-1111-1111-1111-111111111ca7` (`system_catalog.clj:17`)
- Rows created by `34_hardcoded_objects.up.sql`, protected by BEFORE DELETE triggers.

Every app's attr load unions these in (`attr.clj:981-1002`), with `:catalog "system"` on the wire.

**Deterministic UUIDs** (`system_catalog.clj:94-123`): attr id = `encode("system"+"at")` in the
high 64 bits and `encode("<etype-shortcode>/<label-shortcode>")` in the low 64 bits; ident id uses
`"id"` instead of `"at"` (so ident id = attr id with the second uuid group `13ff` → `03ff`).
`encode` packs each char of the (≤12-char) string as 5 bits over alphabet
`"abcdefghijklmnopqrstuvwxzy/"` (note the swapped `x z y` order, `system_catalog.clj:22`), padding
with 1-bits. Shortcode tables: `etype-shortcodes` (`system_catalog.clj:33-43`), `label-shortcodes`
(`system_catalog.clj:50-90`). Defaults per attr (`make-attr`, `system_catalog.clj:155-163`):
blob, cardinality one, not unique/indexed/required.

All attr ids below verified against the hardcoded `$files.location-id` id used by the triggers
(`85_triples_size_trigger.up.sql:17`). Ident uuid = attr uuid with `13ff` replaced by `03ff`.

### $users (`system_catalog.clj:172-184`)
| label | attr id | properties |
|---|---|---|
| `id` | `96653230-13ff-ffff-a4b4-81ffffffffff` | unique, indexed |
| `email` | `96653230-13ff-ffff-a4b4-46010bffffff` | unique, indexed, checked string |
| `type` | `96653230-13ff-ffff-a4b5-3cbc9fffffff` | checked string |
| `imageURL` | `96653230-13ff-ffff-a4b4-8600c4a457ff` | checked string |
| `linkedPrimaryUser` | `96653230-13ff-ffff-a4b4-b7d3ffffffff` | ref one, on-delete cascade; reverse `$users.linkedGuestUsers` (ident `96653231-03ff-ffff-a4b4-b353ffffffff`) |

### $magicCodes (`system_catalog.clj:186-197`)
| label | attr id | properties |
|---|---|---|
| `id` | `96653230-13ff-ffff-60b4-81ffffffffff` | unique, indexed |
| `codeHash` | `96653230-13ff-ffff-60b4-270c87048fff` | indexed, checked string |
| `email` | `96653230-13ff-ffff-60b4-46010bffffff` | indexed, checked string |

### $userRefreshTokens (`system_catalog.clj:199-211`)
| label | attr id | properties |
|---|---|---|
| `id` | `96653230-13ff-ffff-a474-81ffffffffff` | unique, indexed |
| `hashedToken` | `96653230-13ff-ffff-a474-7048e41cdcaf` | unique, indexed, checked string |
| `$user` | `96653230-13ff-ffff-a475-49123fffffff` | ref one, indexed, on-delete cascade; reverse `$users.$userRefreshTokens` (ident `96653231-03ff-ffff-a4b5-48ffffffffff`) |

### $oauthProviders (`system_catalog.clj:213-220`)
| label | attr id | properties |
|---|---|---|
| `id` | `96653230-13ff-ffff-73f4-81ffffffffff` | unique, indexed |
| `name` | `96653230-13ff-ffff-73f4-d0309fffffff` | unique, indexed, checked string |

### $oauthUserLinks (`system_catalog.clj:222-245`)
| label | attr id | properties |
|---|---|---|
| `id` | `96653230-13ff-ffff-72f4-81ffffffffff` | unique, indexed |
| `sub` | `96653230-13ff-ffff-72f5-2a07ffffffff` | indexed, checked string |
| `$user` | `96653230-13ff-ffff-72f5-49123fffffff` | ref one, indexed, on-delete cascade; reverse `$users.$oauthUserLinks` (ident `96653231-03ff-ffff-a4b4-e5ffffffffff`) |
| `$oauthProvider` | `96653230-13ff-ffff-72f4-e7c5d540c91f` | indexed, on-delete cascade; reverse `$oauthProviders.$oauthUserLinks` (ident `96653231-03ff-ffff-73f4-e5ffffffffff`). **N.B. declared without `:value-type :ref`** — it stays `:blob` per `make-attr` defaults (`system_catalog.clj:235-238`). |
| `sub+$oauthProvider` | `96653230-13ff-ffff-72f5-2a05f175503f` | unique, indexed, checked string (manual composite-unique trick, comment `system_catalog.clj:239-241`) |

### $oauthClients (`system_catalog.clj:247-269`)
| label | attr id | properties |
|---|---|---|
| `id` | `96653230-13ff-ffff-70b4-81ffffffffff` | unique, indexed |
| `$oauthProvider` | `96653230-13ff-ffff-70b4-e7c5d540c91f` | ref one, on-delete cascade; reverse `$oauthProviders.$oauthClients` (ident `96653231-03ff-ffff-73f4-e17fffffffff`) |
| `name` | `96653230-13ff-ffff-70b4-d0309fffffff` | unique, indexed, checked string |
| `clientId` | `96653230-13ff-ffff-70b4-25a08d9a07ff` | indexed |
| `encryptedClientSecret` | `96653230-13ff-ffff-70b4-46884b44882f` | checked string |
| `discoveryEndpoint` | `96653230-13ff-ffff-70b4-34484ea91a3f` | checked string |
| `meta` | `96653230-13ff-ffff-70b4-c24c1fffffff` | plain blob |
| `redirectTo` | `96653230-13ff-ffff-70b5-120d1120a6ef` | checked string |
| `useSharedCredentials` | `96653230-13ff-ffff-70b5-49124704451f` | checked boolean |

### $oauthCodes (`system_catalog.clj:271-287`)
| label | attr id | properties |
|---|---|---|
| `id` | `96653230-13ff-ffff-13b4-81ffffffffff` | unique, indexed |
| `codeHash` | `96653230-13ff-ffff-13b4-270c87048fff` | unique, indexed, checked string |
| `codeChallengeMethod` | `96653230-13ff-ffff-13b4-211c0b61267f` | checked string |
| `codeChallenge` | `96653230-13ff-ffff-13b4-270c823816bf` | checked string |
| `userInfo` | `96653230-13ff-ffff-13b5-49122d2bbfff` | plain blob |
| `$oauthClient` | `96653230-13ff-ffff-13b4-e0504b411b3f` | ref one, on-delete cascade; reverse `$oauthClients.$oauthCodes` (ident `96653231-03ff-ffff-70b4-277fffffffff`) |

### $oauthRedirects (`system_catalog.clj:289-316`)
| label | attr id | properties |
|---|---|---|
| `id` | `96653230-13ff-ffff-7474-81ffffffffff` | unique, indexed |
| `stateHash` | `96653230-13ff-ffff-7475-29826438247f` | unique, indexed, checked string |
| `cookieHash` | `96653230-13ff-ffff-7474-27394838247f` | checked string |
| `redirectUrl` | `96653230-13ff-ffff-7475-120d112522bf` | checked string |
| `redirectTo` | `96653230-13ff-ffff-7475-120d1120a6ef` | checked string |
| `$oauthClient` | `96653230-13ff-ffff-7474-e0504b411b3f` | ref one, on-delete cascade; reverse `$oauthClients.$oauthRedirects` (ident `96653231-03ff-ffff-70b4-e8ffffffffff`) |
| `codeChallengeMethod` | `96653230-13ff-ffff-7474-211c0b61267f` | checked string |
| `codeChallenge` | `96653230-13ff-ffff-7474-270c823816bf` | checked string |

### $files (`system_catalog.clj:318-352`)
| label | attr id | properties |
|---|---|---|
| `id` | `96653230-13ff-ffff-2a34-81ffffffffff` | unique, indexed |
| `path` | `96653230-13ff-ffff-2a34-f04cffffffff` | unique, indexed, checked string, required |
| `size` | `96653230-13ff-ffff-2a35-24609fffffff` | indexed, checked number, required |
| `content-type` | `96653230-13ff-ffff-2a34-29e5e4ffffff` | indexed, checked string |
| `content-disposition` | `96653230-13ff-ffff-2a34-21a24fffffff` | indexed, checked string |
| `location-id` | `96653230-13ff-ffff-2a34-b40fffffffff` | unique, indexed, checked string, required (hardcoded in triples triggers) |
| `key-version` | `96653230-13ff-ffff-2a34-aaffffffffff` | checked number |
| `url` | `96653230-13ff-ffff-2a35-48afffffffff` | checked string (computed/signed at query time) |

### $streams (`system_catalog.clj:354-380`)
| label | attr id | properties |
|---|---|---|
| `id` | `96653230-13ff-ffff-94f4-81ffffffffff` | unique, indexed |
| `clientId` | `96653230-13ff-ffff-94f4-25a08d9a07ff` | unique, indexed, checked string, required |
| `machineId` | `96653230-13ff-ffff-94f4-c008e869103f` | checked string |
| `$files` | `96653230-13ff-ffff-94f4-547fffffffff` | ref **many**, unique, on-delete-reverse cascade; reverse `$files.$stream` (ident `96653231-03ff-ffff-2a35-29c48067ffff`) |
| `done` | `96653230-13ff-ffff-94f4-37349fffffff` | checked boolean |
| `size` | `96653230-13ff-ffff-94f5-24609fffffff` | checked number |
| `hashedReconnectToken` | `96653230-13ff-ffff-94f4-7048f124dcaf` | checked string |
| `abortReason` | `96653230-13ff-ffff-94f4-00c6712024df` | checked string |

**Client visibility** (`remove-hidden`, `attr.clj:1079-1091`): clients see system attrs only for
etypes `$users`, `$files`, `$streams`; within those, hidden labels are `$files`
{`content-type`, `content-disposition`, `size`, `location-id`, `key-version`} and `$streams`
{`machineId`, `hashedReconnectToken`}. All `$magicCodes`/`$userRefreshTokens`/`$oauth*` attrs are
server-only. Users may create *new* attrs only on `editable-etypes` `#{"$users" "$files"
"$streams"}` (`system_catalog.clj:422-424`); user-editable *triples* on system attrs are limited
to `$users.id`, `$files.id`, `$files.path`, `$streams.id` (`system_catalog.clj:426-443`).
Reserved ident names (all system idents + a feature-flag list) can't be claimed by users
(`system_catalog.clj:397-420`, enforced at `attr.clj:313-332`).

`system_catalog_ops.clj` shows how server code reads/writes these entities through the ordinary
triple machinery: `triples->db-format` folds an entity's triples into a row-like map, renaming
labels (`$user`→`user_id` etc., `system_catalog_ops.clj:32-82`) and taking `created_at` from the
`id` triple (`system_catalog_ops.clj:79`); `update-op`/`query-op` wrap transact/query with
attr resolution (`system_catalog_ops.clj:213-280`).

---

## 6. Server timestamps / `$serverCreatedAt`

- Each triple row carries `created_at` (bigint epoch **ms**, default `current_unix_timestamp_ms()`;
  `08_add_createdAt_for_triples.up.sql`). It survives cardinality-one value updates unless
  `overwrite-t` (`triple.clj:403-407,419-429`).
- An entity's creation time is defined as the `created_at` of its **`id` triple** (the triple is
  written on every update too, but `ON CONFLICT` preserves the original row's `created_at`).
- Query results expose it as `$serverCreatedAt`: `entity-model/triples->map` attaches
  `"$serverCreatedAt" = Date(created_at-of-id-triple)` when `include-server-created-at?`
  (`db/model/entity.clj:22-34`); datalog join-rows carry `t` as the 4th tuple element.
- Default ordering of query results is by `$serverCreatedAt` (`util/instaql.clj:66-78`; the wire
  order key `serverCreatedAt` maps to it). The `triples_created_at_idx` supports time-ordered
  scans (`51_add_triples_created_at_index.up.sql`).
- `transactions.created_at` is a server `timestamp` used for invalidator e2e tracking
  (`transaction.clj:628-633`); the client-facing ordering token is the tx `id` itself.
