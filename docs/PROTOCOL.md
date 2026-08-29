# InstantDB Client ↔ Server Wire Protocol

Single source of truth for a wire-compatible server reimplementation.
Derived from the official client at `LEGACY/client/packages/core/src/`
(primarily `Reactor.js`, `Connection.ts`, `instaml.ts`, `SyncTable.ts`,
`Stream.ts`, `store.ts`, `instaql.ts`, `model/instaqlResult.js`).

All file:line references below are relative to
`LEGACY/client/packages/core/src/`.

**Casing matters.** Wire messages use kebab-case keys almost everywhere
(`client-event-id`, `tx-steps`, `room-id`, ...). A few keys are camelCase or
snake_case; every such exception is called out explicitly.

---

## 1. Transport & Connection

### 1.1 WebSocket

- Default URI: `wss://api.instantdb.com/runtime/session` (`Reactor.js:63-66`).
- The client connects to `${wsURI}?app_id=${appId}` (`Reactor.js:85-106`):

  ```js
  return new WSConnection(`${wsURI}?app_id=${appId}`);
  ```

  So the **only** query param is `app_id` (snake_case), a UUID (validated
  client-side, `Reactor.js:332`). No custom headers, no subprotocol
  (`new WebSocket(url)`, `Connection.ts:76`).

- Every frame in both directions is a single JSON text message
  (`JSON.stringify` on send, `Connection.ts:114-116`; `JSON.parse` on receive,
  `Connection.ts:82-89`).
- **The server MAY send a JSON array of messages in one frame.** The client
  handles both a single object and an array (`Reactor.js:1798-1804`):

  ```js
  if (Array.isArray(e.message)) {
    for (const msg of e.message) { this._handleReceive(targetTransport.id, msg); }
  } else { this._handleReceive(targetTransport.id, e.message); }
  ```

- Client-to-server frames are always a single JSON object.
- Every message (both directions) has an `op` field (string) that dispatches
  handling. Unknown server ops are logged and ignored (`Reactor.js:932-934`),
  so adding new server ops is backwards-compatible.

### 1.2 Reconnect behavior (informative)

On close, the client reconnects with linear backoff (+1s per attempt, capped
at 10s, `Reactor.js:1820-1842`). On a fresh socket the client re-sends `init`,
then after `init-ok` re-sends every active `add-query`, every unconfirmed
`transact`, `join-room` for every joined room, and sync-table resyncs
(`_flushPendingMessages`, `Reactor.js:1635-1671`; rooms rejoin at
`Reactor.js:653-658`).

### 1.3 SSE fallback (secondary transport)

If WebSockets fail before receiving any message (`_wsOk` false), the client
falls back to SSE at `${apiURI}/runtime/sse?app_id=${appId}`
(`Reactor.js:98-102`, `Reactor.js:1820-1825`). SSE specifics
(`Connection.ts:125-277`):

- The server's first SSE event must be
  `{"op": "sse-init", "machine-id": ..., "session-id": ..., "sse-token": ...}`
  (`Connection.ts:172-184`). Only then is the connection considered open.
- Client→server messages are HTTP POSTs to the same URL with JSON body
  (note **snake_case** here, `Connection.ts:216-225`):

  ```json
  {
    "machine_id": "...", "session_id": "...", "sse_token": "...",
    "messages": [ { "client-event-id": "...", "op": "...", ... }, ... ]
  }
  ```

- SSE data events may be a single message or an array (`Connection.ts:156-165`).

A WS-only server implementation can ignore SSE, but clients on networks that
block WS will then fail.

### 1.4 `client-event-id`

Every client→server message carries a `client-event-id` (a UUIDv4 generated
per message). It is injected by the send path, not by the callers
(`Reactor.js:1744`):

```js
this._transport.send({ 'client-event-id': eventId, ...msg });
```

The server must echo `client-event-id` back on direct responses
(`transact-ok`, `start-stream-ok`, `stream-append`, `error`, ...). For
queries, correlation is done by the query object `q` itself (hash), not by
event id — but `error` responses to `add-query` are matched via
`original-event.q` (see §6).

---

## 2. Client → Server operations

All messages implicitly include `"client-event-id": "<uuid>"` (see §1.4).

### 2.1 `init` (`Reactor.js:1760-1777`)

Sent immediately after the socket opens (client status `opened`). Fields:

```json
{
  "client-event-id": "8b7f...",
  "op": "init",
  "app-id": "0e2a...-uuid",
  "refresh-token": "user-refresh-token-or-undefined",
  "versions": { "@instantdb/react": "v0.21.x", "@instantdb/core": "v0.21.x" },
  "__admin-token": "optional-admin-token-or-undefined"
}
```

- `app-id` — the app UUID (kebab-case here, unlike the query param `app_id`).
- `refresh-token` — `user.refresh_token` of the locally stored authed user;
  key is absent/undefined when logged out. This is how auth enters the
  protocol; there is no separate auth message.
- `versions` — object mapping package name → version string. Core always adds
  `'@instantdb/core': version` (`Reactor.js:316`); wrappers add e.g.
  `'@instantdb/react'` (`react/src/init.ts:49`). Version strings look like
  `"v0.21.25"`. Informational only.
- `__admin-token` — undefined unless the app was initialized with the
  (unofficial) admin-token config; if present the server should skip
  permission checks.

The client clears its in-flight mutation set when sending `init`
(`Reactor.js:1739-1742`).

Expected reply: `init-ok` (§3.1) or `error` with `original-event.op === "init"`
(§3.14/§6).

### 2.2 `add-query` (`Reactor.js:1096-1116`, `1643`, `1649`)

```json
{ "client-event-id": "...", "op": "add-query", "q": { ...instaql query... } }
```

- `q` is the InstaQL query object verbatim, e.g.
  `{"todos": {"$": {"where": {"done": false}}, "owner": {}}}`.
- If the caller supplied `ruleParams`, they are merged into the query object
  as a top-level `$$ruleParams` key (`Reactor.js:1161-1163`):
  `{ "$$ruleParams": {...}, "todos": {} }`. The server must strip/interpret
  `$$ruleParams` — it is not an entity name (instaql.ts:929 skips it too).
- Sent only after `authenticated` (i.e. after `init-ok`); queued queries are
  re-sent on `init-ok` via `_flushPendingMessages`.
- The client may send `add-query` for the same `q` multiple times (once per
  new subscriber). The server may respond `add-query-exists` (§3.2) if it's
  already registered for this session.

### 2.3 `remove-query` (`Reactor.js:1254`)

```json
{ "client-event-id": "...", "op": "remove-query", "q": { ...same q object... } }
```

Sent when the last local subscriber to a query unsubscribes. Identified by
`q`, not by any subscription id. No reply is expected or handled (a
`remove-query-ok` may be sent; it lands in the ignored-unknown-op path).

### 2.4 `transact` (`Reactor.js:1555-1577`, `1592-1629`)

```json
{
  "client-event-id": "...",
  "op": "transact",
  "tx-steps": [ ...see §4... ],
  "created": 1735689600123,
  "error": null,
  "order": 3
}
```

Note: the persisted mutation object is sent as-is, so the extra bookkeeping
fields `created` (epoch ms), `order` (integer, client-side send ordering) and
`error` (always undefined when actually sent — errored mutations are never
transmitted, `Reactor.js:1592-1597`) appear on the wire. Servers must ignore
unknown fields. The meaningful field is `tx-steps`.

Replies: `transact-ok` (§3.5) on success, `error` with matching
`client-event-id` on failure. The client times out a sent mutation after
≥6 seconds × (in-flight count) with no reply (`Reactor.js:1603-1627`) —
respond promptly.

### 2.5 `join-room` (`Reactor.js:2603-2611`)

```json
{
  "client-event-id": "...",
  "op": "join-room",
  "room-type": "chat",
  "room-id": "room-123",
  "data": { ...initial presence, may be undefined... }
}
```

`data` is the user's initial presence blob (arbitrary JSON) or absent.
Replies: `join-room-ok` or `join-room-error` (§3.9/§3.10), then presence
snapshots.

### 2.6 `leave-room` (`Reactor.js:2613-2615`)

```json
{ "client-event-id": "...", "op": "leave-room", "room-id": "room-123" }
```

Reply: `leave-room-ok` (§3.11).

### 2.7 `set-presence` (`Reactor.js:2595-2601`)

```json
{
  "client-event-id": "...",
  "op": "set-presence",
  "room-id": "room-123",
  "data": { "cursor": { "x": 1, "y": 2 }, ... }
}
```

`data` is the full (merged) presence object for this session — the client
merges partial updates locally before sending (`Reactor.js:2571-2593`), so the
server should replace, not merge. The server may reply `set-presence-ok`; the
client has **no handler** for it (it's only in the log-suppression list,
`Reactor.js:117-122`) so it is optional and ignored.

### 2.8 `client-broadcast` (`Reactor.js:2732-2740`)

```json
{
  "client-event-id": "...",
  "op": "client-broadcast",
  "room-id": "room-123",
  "roomType": "chat",
  "topic": "emoji",
  "data": { ...arbitrary... }
}
```

⚠️ Note `roomType` is **camelCase** here (only place on the wire), while
`room-id` is kebab. The server fans this out to room peers as
`server-broadcast` (§3.8).

### 2.9 Sync-table ops (`SyncTable.ts`)

- **`start-sync`** (`SyncTable.ts:523-528`):

  ```json
  { "client-event-id": "...", "op": "start-sync", "q": { "todos": { "$": { "order": { "serverCreatedAt": "asc" } } } } }
  ```

  `q` is the query object (the TS type at `SyncTable.ts:57-60` says `string`,
  but the actual value passed is the query object — `sendStart(query)` at
  `SyncTable.ts:590`). Single top-level table; optional `$.order` with one
  field.

- **`resync-table`** (`SyncTable.ts:530-539`) — resume a persisted
  subscription after reconnect. Note the `client-event-id` for this message is
  the **subscription id** (`this.trySend(state.subscriptionId, ...)`):

  ```json
  {
    "client-event-id": "<subscription-id>",
    "op": "resync-table",
    "subscription-id": "sub-uuid",
    "tx-id": 4211,
    "token": "server-issued-token"
  }
  ```

- **`remove-sync`** (`SyncTable.ts:541-547`):

  ```json
  { "client-event-id": "...", "op": "remove-sync", "subscription-id": "sub-uuid", "keep-subscription": false }
  ```

### 2.10 Stream ops (`Stream.ts:760-795`)

- **`start-stream`** (writer, `Stream.ts:922-932`):

  ```json
  { "client-event-id": "...", "op": "start-stream", "client-id": "app-chosen-id", "reconnect-token": "uuid", "rule-params": { ... } }
  ```

  `rule-params` only present if supplied. Reply: `start-stream-ok` (echoing
  `client-event-id`) or `error`.

- **`append-stream`** (`Stream.ts:955-967`):

  ```json
  {
    "client-event-id": "...",
    "op": "append-stream",
    "stream-id": "stream-uuid",
    "chunks": ["text chunk 1", "text chunk 2"],
    "offset": 1024,
    "done": false,
    "abort-reason": "optional string"
  }
  ```

  `offset` is the byte offset (UTF-8 encoded length) of the first chunk.
  `done: true` with empty `chunks` closes the stream; `abort-reason` marks an
  abort.

- **`subscribe-stream`** (reader, `Stream.ts:1013-1042`): one of
  `stream-id`/`client-id` is required; `offset` and `rule-params` optional.

  ```json
  { "client-event-id": "...", "op": "subscribe-stream", "stream-id": "...", "client-id": "...", "offset": 512, "rule-params": { ... } }
  ```

  The `client-event-id` of this message becomes the correlation key for all
  subsequent `stream-append` messages.

- **`unsubscribe-stream`** (`Stream.ts:1047-1051`):

  ```json
  { "client-event-id": "...", "op": "unsubscribe-stream", "subscribe-event-id": "<client-event-id of the subscribe-stream>" }
  ```

---

## 3. Server → Client operations

Dispatch switch: `Reactor.js:643-935`. Field-by-field, what the client reads:

### 3.1 `init-ok` (`Reactor.js:644-660`)

```json
{
  "op": "init-ok",
  "session-id": "sess-uuid",
  "attrs": [ { ...attr... }, ... ],
  "app-status": { "status": "active" }
}
```

Client reads:

- `msg.attrs` → **mandatory**. Array of attr objects (shape in §3.1.1); loaded
  into the attrs store keyed by `attr.id` (`_setAttrs`, `Reactor.js:1076-1086`).
  This must be the complete set of the app's attrs (schema); every triple the
  server ever sends must reference an attr id present here or added later via
  `refresh-ok.attrs` / client-created `add-attr`.
- `msg['session-id']` → **mandatory**. Stored as `this._sessionId`; used to
  identify "self" in presence data and broadcasts (`Reactor.js:651`,
  `2693`, `2702`, `2769`).
- `msg['app-status']?.status` → optional; `'active' | 'read-only' | 'disabled'`,
  defaults to `'active'` when absent (`Reactor.js:647`).

There is no `user` field — the client already knows the user from the
refresh token it sent; `init-ok` only confirms it. (If the refresh token is
invalid, reply with an `error` of `type: "record-not-found"` and
`hint: {"record-type": "app-user"}` — see §6 — which makes the client log the
user out.)

Effect: status → `authenticated`; pending queries/mutations flushed; rooms
rejoined.

#### 3.1.1 Attr object shape (`attrTypes.ts:13-29`)

```json
{
  "id": "attr-uuid",
  "value-type": "blob",                    // "blob" | "ref"
  "cardinality": "one",                    // "one" | "many"
  "forward-identity": ["ident-uuid", "todos", "title"],   // [id, etype, label]
  "reverse-identity": ["ident-uuid", "owners", "todos"],  // refs only
  "unique?": false,
  "index?": false,
  "required?": false,
  "inferred-types": ["string"],            // or null
  "catalog": "user",                       // "system" | "user"
  "on-delete": "cascade",                  // optional
  "on-delete-reverse": "cascade",          // optional
  "checked-data-type": "date",             // "number"|"string"|"boolean"|"date", optional
  "indexing?": false,                      // optional
  "setting-unique?": false                 // optional
}
```

Client code paths that read attr fields: `forward-identity`/
`reverse-identity` tuples (`store.ts:createAttrIndexes`), `value-type`
(`isBlob`/`isRef`), `cardinality`, `unique?`, `checked-data-type` (date
coercion, `store.ts:236-239`), `primary?` (optional boolean, read at
`store.ts:265` for primary-key index).

### 3.2 `add-query-exists` (`Reactor.js:661-664`)

```json
{ "op": "add-query-exists", "q": { ...the query... } }
```

Client reads only `msg.q`, hashes it, and resolves any pending `queryOnce`
with the already-cached local result. Send this when the session already has
that query registered (dedup); otherwise send `add-query-ok`.

### 3.3 `add-query-ok` (`Reactor.js:665-700`)

```json
{
  "op": "add-query-ok",
  "q": { ...the exact query object from add-query... },
  "result": [ ...instaql-result nodes, see §5... ],
  "processed-tx-id": 4207
}
```

Client reads:

- `msg.q` → **mandatory**; hashed (`weakHash`) to find the local
  subscription. Must be structurally identical to what the client sent
  (the client hashes what it sent; it only uses `q` from the message to
  compute the hash, so it must hash-equal — safest is to echo verbatim).
- `msg.result` → **mandatory**; instaql-result tree (§5). Client extracts:
  - `result?.[0]?.data?.['page-info']` → pagination info (§5.3),
  - `result?.[0]?.data?.['aggregate']` → aggregate results,
  - all triples via `extractTriples(result)` (§5.1).
- `msg['processed-tx-id']` → transaction id watermark (integer). Stored as
  `processedTxId`; used to drop optimistic pending mutations with
  `tx-id <= min(processedTxId over all queries)` (`Reactor.js:1676-1691`) and
  to decide which optimistic mutations still need re-applying
  (`Reactor.js:1454-1463`). Omitting it means the client can never garbage
  collect confirmed mutations against this query — include it.

### 3.4 `refresh-ok` (`Reactor.js:733-814`)

Pushed by the server whenever data changes affect registered queries.

```json
{
  "op": "refresh-ok",
  "processed-tx-id": 4212,
  "attrs": [ ...optional full attrs array... ],
  "computations": [
    {
      "instaql-query": { ...q... },
      "instaql-result": [ ...instaql-result nodes, §5... ]
    }
  ]
}
```

Client reads:

- `msg.attrs` → optional; if present, **replaces** the whole attrs store
  (`Reactor.js:736-738`). Send it whenever attrs changed (e.g. after a
  transaction created new attrs).
- `msg['processed-tx-id']` → applied to every computation's stored result and
  used for pending-mutation rewriting/cleanup.
- `msg.computations` → **mandatory** array; for each element the client reads
  `x['instaql-query']` (hashed to find the sub) and `x['instaql-result']`
  (same format as `add-query-ok.result`: `page-info`, `aggregate`,
  triples via extractTriples — `Reactor.js:759-788`).

### 3.5 `transact-ok` (`Reactor.js:815-855`)

```json
{ "op": "transact-ok", "client-event-id": "<echo of transact>", "tx-id": 4212 }
```

Client reads:

- `msg['client-event-id']` → **mandatory**; matches the pending mutation.
- `msg['tx-id']` → **mandatory**; server-assigned monotonically increasing
  transaction id. Stored on the pending mutation; compared against
  `processed-tx-id` from query refreshes to decide when the optimistic
  mutation can be dropped (`Reactor.js:1676-1691`) and whether to re-apply it
  on top of fresh results (`_applyOptimisticUpdates`, `Reactor.js:1454-1463`:
  re-applied while `mut['tx-id'] > processedTxId`).

Consistency requirement: after a `transact-ok` with `tx-id = N`, refreshed
query results for queries affected by that tx must carry
`processed-tx-id >= N`, otherwise the client keeps re-applying the optimistic
update (harmless) but never GCs the mutation.

### 3.6 `patch-presence` (`Reactor.js:856-862`)

```json
{
  "op": "patch-presence",
  "room-id": "room-123",
  "edits": [ [["<session-id>", "data", "cursor"], "r", {"x": 5}], ... ]
}
```

Client reads `msg['room-id']` and `msg['edits']`. `edits` is an array of
`[path, op, value]` triples applied to the room's sessions map
(`_patchPresencePeers`, `Reactor.js:2672-2697`):

- `path`: array of keys into `{ <session-id>: { "data": {...presence...} } }` —
  first element is a peer session-id, then object keys (e.g.
  `["sess-1"]` to add/remove a whole peer, `["sess-1", "data", "cursor"]`
  to touch a field). Path elements can also be array indices for `+`.
- `op`: `"+"` insert (array insert semantics), `"r"` replace/assoc, `"-"`
  dissoc/remove.
- `value`: for `+`/`r` the value; omitted/ignored for `-`.

The client deletes its own `session-id` entry from the resulting map (peers
never include self). The value stored per session is `{"data": <presence>}` —
edits address presence under the `"data"` key of each session.

### 3.7 `refresh-presence` (`Reactor.js:863-869`)

Full snapshot; sent e.g. after `join-room-ok`.

```json
{
  "op": "refresh-presence",
  "room-id": "room-123",
  "data": {
    "<session-id-1>": { "peer-id": "<session-id-1>", "user": null, "data": { ...presence... } },
    "<session-id-2>": { ... }
  }
}
```

Client reads `msg['room-id']` and `msg['data']`. From each session entry the
client uses **only** `v.data` (`_setPresencePeers`, `Reactor.js:2699-2710`)
and removes its own session-id. Any other keys in the per-session object are
ignored.

### 3.8 `server-broadcast` (`Reactor.js:870-876`, `2764-2775`)

```json
{
  "op": "server-broadcast",
  "room-id": "room-123",
  "topic": "emoji",
  "data": {
    "peer-id": "<sender-session-id>",
    "data": { ...payload from client-broadcast... },
    "user": null
  }
}
```

Client reads `msg['room-id']`, `msg.topic`, `msg.data.data` (the payload) and
`msg.data['peer-id']` (to resolve the sending peer's presence; if it equals
own session-id, uses own presence). So the envelope's `data` **must** be an
object containing `peer-id` and nested `data`.

### 3.9 `join-room-ok` (`Reactor.js:877-893`)

```json
{ "op": "join-room-ok", "room-id": "room-123" }
```

Client reads `msg['room-id']` only. Marks room connected and flushes queued
presence/broadcasts. (If the client had already abandoned the room it replies
with `leave-room`.)

### 3.10 `join-room-error` (`Reactor.js:921-927`)

```json
{ "op": "join-room-error", "room-id": "room-123", "error": { "message": "...", ... } }
```

Client reads `msg['room-id']` and `msg['error']` (opaque; stored and surfaced
to presence subscribers as `error`).

### 3.11 `leave-room-ok` (`Reactor.js:894-898`)

```json
{ "op": "leave-room-ok", "room-id": "room-123" }
```

Client reads `msg['room-id']`; marks room disconnected.

### 3.12 `app-status-changed` (`Reactor.js:899-920`)

```json
{ "op": "app-status-changed", "status": "read-only" }
```

Client reads `msg.status` (`'active' | 'read-only' | 'disabled'`). `disabled`
makes every registered query surface an error and blocks local writes;
leaving `disabled` triggers a full socket restart.

### 3.13 Sync-table server ops

- **`start-sync-ok`** (`Reactor.js:701-703`, `SyncTable.ts:605-631`):

  ```json
  {
    "op": "start-sync-ok",
    "client-event-id": "<echo>",
    "subscription-id": "sub-uuid",
    "q": { ...echo of start-sync q... },
    "token": "resync-token"
  }
  ```

  Client reads `subscription-id`, `q` (hashed to find the sub — must
  hash-equal the sent query), `token` (opaque; stored for `resync-table`).

- **`sync-load-batch`** (`SyncTable.ts:639-692`):

  ```json
  {
    "op": "sync-load-batch",
    "subscription-id": "sub-uuid",
    "join-rows": [ [ [e,a,v,t], [e,a,v,t] ], [ ... ] ]
  }
  ```

  `join-rows` is an **array of entities**, each entity being an array of
  `[e, a, v, t]` triples (all triples of one entity). Client builds one store
  per entity from its triples.

- **`sync-init-finish`** (`SyncTable.ts:694-724`):

  ```json
  { "op": "sync-init-finish", "subscription-id": "sub-uuid", "tx-id": 4210 }
  ```

  Marks initial load complete; `tx-id` is the watermark stored for future
  `resync-table`.

- **`sync-update-triples`** (`SyncTable.ts:726-853`):

  ```json
  {
    "op": "sync-update-triples",
    "subscription-id": "sub-uuid",
    "txes": [
      {
        "tx-id": 4211,
        "changes": [
          { "action": "added",   "triple": ["eid", "attr-id", "value", 1735689600123] },
          { "action": "removed", "triple": ["eid", "attr-id", "old-value", 1735689500000] }
        ]
      }
    ]
  }
  ```

  Client reads `subscription-id`, and per tx: `tx-id` (skips txes with
  `tx-id <= state.txId`), `changes[].action` (`"added" | "removed"`) and
  `changes[].triple` (`[e, a, v, t]`).

- **Resync after reconnect**: on `resync-table` the server should replay
  `sync-update-triples` for txes after the given `tx-id`. On any error reply
  to `resync-table` (see §6) the client wipes the sub and starts over with
  `start-sync` (`SyncTable.ts:904-918`).

### 3.14 Stream server ops (`Stream.ts:797-829`, handlers `970-1090`)

- **`start-stream-ok`**:

  ```json
  { "op": "start-stream-ok", "client-event-id": "<echo>", "stream-id": "stream-uuid", "offset": 0 }
  ```

  `offset` = bytes already durably flushed for this stream (0 for new; on
  reconnect via `reconnect-token`, the persisted offset). Client resumes
  appending from there.

- **`stream-flushed`**:

  ```json
  { "op": "stream-flushed", "stream-id": "stream-uuid", "offset": 2048, "done": false }
  ```

  Acknowledges durable bytes up to `offset`; `done: true` completes the
  writer.

- **`append-failed`**:

  ```json
  { "op": "append-failed", "stream-id": "stream-uuid" }
  ```

  Client re-runs `start-stream` (with its `reconnect-token`) and resends the
  unflushed buffer.

- **`stream-append`** (to readers; correlated by the `client-event-id` of the
  `subscribe-stream` message):

  ```json
  {
    "op": "stream-append",
    "client-event-id": "<subscribe-stream event id>",
    "stream-id": "stream-uuid",
    "offset": 0,
    "content": "inline text chunk",
    "files": [ { "url": "https://...", "size": 12345 } ],
    "done": false,
    "error": "optional error string",
    "retry": false
  }
  ```

  Client reads: `error` (+`retry`: if `retry` true → resubscribe, else fatal),
  `offset` (byte offset of this payload; must be `<=` what the reader has
  seen — the client discards overlap and errors on gaps), `files` (fetched by
  URL in order, contents streamed to the reader), `content` (inline data,
  applied after files), `stream-id`, `done` (closes the reader).

### 3.15 `error`

See §6.

### 3.16 Ops the server may send that the client ignores

`set-presence-ok` (suppressed from logs at `Reactor.js:117-122`, no handler);
any unknown op (logged at `Reactor.js:932-934`).

---

## 4. `transact` payload: tx-steps (instaml.ts)

`tx-steps` is a flat array of steps. Each step is an array whose first
element is the action. All steps produced by `instaml.transform`
(`instaml.ts:707-714`): first all `add-attr` steps for missing attrs, then
data steps.

### 4.1 Entity id form

Wherever `eid` appears below, it is either

- a UUID string, or
- a **lookup ref**: a 2-tuple `[<attr-id>, <value>]` (`instaml.ts:120-133`)
  where `<attr-id>` is the id of a `unique?` attr. The server must resolve it
  to the entity whose unique attr equals the value (creating the entity on
  add if it doesn't exist).

When a lookup ref is used, the client also emits a companion step
`["add-triple", <lookup>, <id-attr-id>, <lookup>]` so the entity's `id`
triple exists (`withIdAttrForLookup`, `instaml.ts:135-152`; and the `id`
tuple in each update/merge expansion).

### 4.2 Step types

- **`["add-attr", <attr-object>]`** (`instaml.ts:573-577`)
  Creates an attr. This is how **schemaless mode** works: for any label the
  client doesn't have an attr for, it generates one client-side with fresh
  UUIDs and sends `add-attr` inline, in the same transaction, **before** the
  triples that use it. Shapes:

  - Object (blob) attr (`createObjectAttr`, `instaml.ts:415-433`):

    ```json
    ["add-attr", {
      "id": "new-uuid",
      "forward-identity": ["new-uuid", "todos", "title"],
      "value-type": "blob",
      "cardinality": "one",
      "unique?": false,
      "index?": false,
      "isUnsynced": true
    }]
    ```

    With a schema, `index?`, `unique?`, `checked-data-type` come from the
    schema (`instaml.ts:399-413`). The `id` attr gets `"unique?": true`.
    Lookup-created attrs get `"unique?": true, "index?": true`
    (`instaml.ts:508`).

  - Ref attr (`createRefAttr`, `instaml.ts:465-492`):

    ```json
    ["add-attr", {
      "id": "new-uuid",
      "forward-identity": ["new-uuid", "todos", "owner"],
      "reverse-identity": ["new-uuid", "owner", "todos"],
      "value-type": "ref",
      "cardinality": "many",
      "unique?": false,
      "index?": false,
      "isUnsynced": true
    }]
    ```

    With schema: identities/cardinality/`unique?`/`on-delete`/
    `on-delete-reverse` from the schema link def (`instaml.ts:449-463`).

  `isUnsynced` is client bookkeeping that leaks onto the wire; ignore it
  server-side. The server must reconcile client-generated attr ids with
  existing attrs (same forward-identity etype+label) — the client does the
  same reconciliation locally when it later learns the real ids
  (`_rewriteMutations`, `Reactor.js:1274-1352`).

- **`["add-triple", eid, attr-id, value]`** — set a value
  (`instaml.ts:281,303`). Optional 5th element `{"mode": "create"}` (from
  `db.tx.create`, `instaml.ts:265-284`) or `{"mode": "update"}` (from
  `update`/`merge` when `upsert: false` or the entity is known to exist,
  `convertOpts`, `instaml.ts:255-263`). No 5th element = upsert semantics.
  For links, `value` is the target eid or lookup ref
  (`expandLink`, `instaml.ts:154-181`); for reverse links the triple is
  emitted swapped: `["add-triple", <target-eid>, <rev-attr-id>, <this-eid>]`.

- **`["retract-triple", eid, attr-id, value]`** — remove a link/value
  (`expandUnlink`, `instaml.ts:183-210`).

- **`["deep-merge-triple", eid, attr-id, value]`** (+ optional
  `{"mode": "update"}`) — deep-merge `value` (JSON) into the existing blob
  value (`expandDeepMerge`, `instaml.ts:318-345`). `null` values inside the
  merge payload delete keys (client-side merge semantics:
  `utils/object.js` `immutableDeepMerge`). Always preceded by the entity's
  id `add-triple`.

- **`["delete-entity", eid, etype]`** — delete an entity and its triples
  (`expandDelete`, `instaml.ts:313-316`). `eid` may be a lookup ref.

- **`["rule-params", eid, etype, {..params..}]`** — attach permission-rule
  params for this entity (`expandRuleParams`, `instaml.ts:347-350`).

- **`["update-attr", {"id": ..., ...partial attr...}]`** and
  **`["delete-attr", <attr-id>]`** — sent by admin/schema tooling paths; the
  client understands them in local optimistic application
  (`Reactor.js:1364-1401`, `store.ts:transact`) so the server must accept
  them too.

Example full payload for `db.tx.todos[id].update({title: "hi"})` in
schemaless mode with no prior attrs:

```json
"tx-steps": [
  ["add-attr", {"id":"a1","forward-identity":["f1","todos","id"],"value-type":"blob","cardinality":"one","unique?":true,"index?":false,"isUnsynced":true}],
  ["add-attr", {"id":"a2","forward-identity":["f2","todos","title"],"value-type":"blob","cardinality":"one","unique?":false,"index?":false,"isUnsynced":true}],
  ["add-triple", "e-uuid", "a1", "e-uuid"],
  ["add-triple", "e-uuid", "a2", "hi"]
]
```

(Every update/create/merge emits the `["add-triple", eid, <id-attr>, eid]`
self-id triple first — `instaml.ts:271,293,335`.)

Date values: when `useDateObjects` is on, JS `Date`s are serialized by
`JSON.stringify` to ISO-8601 strings; servers should accept ISO strings /
epoch millis for `checked-data-type: "date"` attrs.

### 4.3 `transact-ok` follow-up on new attrs

After `transact-ok`, the client adds any `add-attr` attrs from that mutation
into its local attrs store (`Reactor.js:838-848`). The server does **not**
need to echo new attrs immediately, but the next `refresh-ok` should carry an
updated `attrs` array with server-canonical attr definitions so clients
reconcile ids (`Reactor.js:736-738`, `1274-1352`).

---

## 5. Query result encoding (instaql-result)

Used in `add-query-ok.result` and `refresh-ok.computations[].instaql-result`.

### 5.1 Node tree

The result is an **array of nodes**; each node is:

```json
{
  "data": {
    "datalog-result": {
      "join-rows": [ [ [e, a, v, t], [e, a, v, t], ... ], ... ]
    },
    "page-info":  { ... },     // top-level nodes only, optional
    "aggregate":  { ... }      // top-level nodes only, optional
  },
  "child-nodes": [ ...same node shape, for nested link levels... ]
}
```

The client's *only* consumption of this tree (`model/instaqlResult.js:1-25`):

```js
const { 'datalog-result': datalogResult } = data;
const { 'join-rows': joinRows } = datalogResult;
for (const rows of joinRows) { for (const triple of rows) acc.push(triple); }
_extractTriplesHelper(idNode['child-nodes'], acc);
```

So: every node **must** have `data['datalog-result']['join-rows']` (array of
arrays of triples; may be empty) and `child-nodes` (array; may be empty).
All triples across all nodes are flattened, deduped by the triple store, and
loaded into an in-memory triple store; the client then re-runs InstaQL locally
against that store (`Reactor.js:673-680`, `instaql.ts:query`). Grouping into
rows/nodes is therefore irrelevant for correctness — a valid minimal encoding
is one top-level node per top-level query key with all triples in one
join-row — **except** that `page-info`/`aggregate` are read only from
`result[0].data` (`Reactor.js:671-672`).

### 5.2 Triple format

`[e, a, v, t]` (`store.ts:7`, `SyncTable.ts:84`):

- `e` — entity id (UUID string),
- `a` — attr id (UUID string),
- `v` — the value (JSON scalar/object; for ref attrs, the target entity id),
- `t` — server timestamp in **epoch milliseconds** (integer). Used for
  ordering/`serverCreatedAt` (`SyncTable.ts:168-183` reads `t = triple[3]` of
  the id-triple as `serverCreatedAt`) and conflict resolution in the local
  store.

**Required triples:** for every entity returned, include its id triple
`[eid, <id-attr-id>, eid, t]` — the local InstaQL resolves entities by their
`id` attr, and `serverCreatedAt` ordering depends on the id triple's `t`.
For every link include the ref triple `[from-eid, <ref-attr-id>, to-eid, t]`
plus the linked entity's own triples.

### 5.3 `page-info`

Read from `result[0].data['page-info']`, stored raw, and consumed by instaql
(`instaql.ts:707-717`, `890-911`). Shape — a map keyed by the **top-level
query key** (etype/label used in `q`):

```json
"page-info": {
  "todos": {
    "start-cursor": ["<eid>", "<attr-id>", <value>, <t>],
    "end-cursor":   ["<eid>", "<attr-id>", <value>, <t>],
    "has-next-page?": true,
    "has-previous-page?": false
  }
}
```

Cursor = `[e, a, v, t]` 4-tuple (`queryTypes.ts:121`). The client uses
`start-cursor`/`end-cursor` to order/slice locally (`instaql.ts:715-716`,
`817`) and reformats to camelCase for the app. Clients send cursors back in
queries under `$.after` / `$.before` verbatim.

### 5.4 `aggregate`

Read from `result[0].data['aggregate']`; opaque map keyed by top-level query
key, passed straight through to the app (e.g. `{"todos": {"count": 5}}`).
When a key has an aggregate, instaql skips join-row processing for it
(`instaql.ts:928-932`).

---

## 6. Error messages

Single op `error`, handled at `_handleReceiveError` (`Reactor.js:984-1067`).
Full shape (all fields the client reads):

```json
{
  "op": "error",
  "client-event-id": "<echo of the failing client message>",
  "status": 400,
  "type": "param-malformed",
  "message": "Human-readable message.",
  "hint": { ...arbitrary debugging JSON... },
  "trace-id": "abc123",
  "original-event": { "client-event-id": "...", "op": "add-query", "q": { ... }, ... }
}
```

Client behavior by field:

- `client-event-id` → matched against pending mutations first
  (`Reactor.js:986-989`). If it matches a `transact`, the mutation is rolled
  back and its promise rejected with `{type, message, hint, status}`
  (`_finishTransaction`, `Reactor.js:513-548`: if `type` is present it becomes
  an `InstantAPIError` with HTTP-ish `status`; otherwise a generic error with
  `message`/`hint`).
- `original-event` → the client relies on this to route non-mutation errors.
  **The server must echo the full original message** (at minimum its `op`,
  plus `q` for `add-query`, `subscription-id` for `resync-table`,
  `q` for `start-sync`, `stream-id` for `append-stream`):
  - `original-event.op === 'add-query'` + `original-event.q` → query error;
    all subscribers of `weakHash(q)` get `{error: {message, hint?, traceId?}}`
    (`Reactor.js:1009-1018`).
  - `original-event.op === 'init'` → if
    `msg.type === 'record-not-found' && msg.hint?.['record-type'] === 'app-user'`,
    the client logs the user out (`Reactor.js:1020-1029`); otherwise status →
    `errored` with `message` surfaced on all queries.
  - `original-event.op === 'resync-table' | 'start-sync'` → routed to
    SyncTable (`Reactor.js:1038-1046`; `SyncTable.ts:879-918` reads `status`,
    `type`, `message`, `hint`, and `original-event.q` /
    `original-event['subscription-id']`).
  - `original-event.op` in `start-stream | append-stream | subscribe-stream |
    unsubscribe-stream` → routed to InstantStream (`Stream.ts:1121-1159`;
    reads `message`, `hint`, and `original-event['stream-id']` for
    `append-stream`).
- `message` → fallback default is used if absent, but always send it.
- `hint` → optional structured details (object). Notable conventions:
  `{"record-type": "app-user"}` with `type: "record-not-found"` (logged-out),
  `{"status": "disabled"}` (app disabled).
- `trace-id` (kebab) → optional; concatenated with
  `original-event['trace-id']` for debugging (`Reactor.js:998-1002`).
- `status` → numeric HTTP-like code; used in rejected mutation errors and
  sync-table errors.
- `type` → machine-readable error type string; presence of `type` is what
  marks an error as "server-originated" for promise rejection
  (`Reactor.js:529-537`).

Errors with no matching mutation, no recognized `original-event.op`, are
`console.error`ed and otherwise dropped (`Reactor.js:1055-1066`).

---

## 7. Presence / rooms summary (payload cheat-sheet)

| Direction | op | Keys |
|---|---|---|
| C→S | `join-room` | `room-type`, `room-id`, `data` (initial presence, optional) |
| C→S | `leave-room` | `room-id` |
| C→S | `set-presence` | `room-id`, `data` (full presence object) |
| C→S | `client-broadcast` | `room-id`, `roomType` (camel!), `topic`, `data` |
| S→C | `join-room-ok` | `room-id` |
| S→C | `join-room-error` | `room-id`, `error` |
| S→C | `leave-room-ok` | `room-id` |
| S→C | `refresh-presence` | `room-id`, `data`: `{sessionId: {"data": presence, ...}}` |
| S→C | `patch-presence` | `room-id`, `edits`: `[[path, "+"|"r"|"-", value?], ...]` |
| S→C | `server-broadcast` | `room-id`, `topic`, `data`: `{"peer-id": sid, "data": payload}` |
| S→C | `set-presence-ok` | ignored by client (optional) |

Recommended server flow: on `join-room` → `join-room-ok`, then
`refresh-presence` with the current room snapshot (the client marks the room
connected on any of `join-room-ok`/`refresh-presence`/`patch-presence`/
`server-broadcast`, `Reactor.js:858,865,873,890`). Afterwards, incremental
`patch-presence` on peer changes. Presence for the joining session itself may
be included in snapshots — the client strips its own `session-id`.

---

## 8. Auth summary

- REST endpoints (outside this protocol) mint refresh tokens
  (`/runtime/auth/*` via `authAPI.ts`).
- The WS protocol carries auth solely via `init['refresh-token']`
  (`Reactor.js:1765`). The token maps to a user; absence = anonymous.
- `init-ok` needs no user payload; it must include `attrs` and `session-id`
  (§3.1).
- Auth changes client-side always tear down the socket and re-`init`
  (`updateUser`, `Reactor.js:2337-2338`).
- Server-initiated logout: send `error` for the `init` event with
  `type: "record-not-found"`, `hint: {"record-type": "app-user"}`.
- `__admin-token` in `init` (if present and valid) bypasses permissions.

---

## 9. Client status model (informative)

`connecting → opened (socket open, init sent) → authenticated (init-ok)`;
`closed` on socket close/offline; `errored` on fatal init error
(`Reactor.js:47-53`). Only in `authenticated` does the client send
`add-query`/`transact`/room/sync/stream traffic (`_trySendAuthed`,
`Reactor.js:1717-1722`); `init` is the only message sent in `opened`.
