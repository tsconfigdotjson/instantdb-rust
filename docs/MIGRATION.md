# Migrating an app from hosted Instant

This server reuses the legacy database schema and the exact deterministic
system-catalog attribute UUIDs, so migration is a data copy plus a URL switch.

## 1. Export your data

From the legacy Postgres (or a hosted export), you need these rows for your
`app_id`:

| table | what |
|---|---|
| `apps` | the app row (`id`, `title`) |
| `attrs` | your schema (all rows with your `app_id`) |
| `idents` | ident rows for those attrs |
| `triples` | all data rows for your `app_id` |
| `rules` | your permission rules |
| `app_admin_tokens` | admin tokens (or mint new ones) |
| `app_authorized_redirect_origins` | oauth redirect origins, if used |

The legacy repo ships `server/scripts/export/copy_*.sql` helpers producing
CSVs with exactly these shapes.

## 2. Prepare the target

```bash
./scripts/apply-migrations.sh        # legacy schema onto stock Postgres
cargo run --release -p instant-server &   # boots + creates the system catalog
```

The server bootstraps the system-catalog app (`a1111111-…1ca7`) and its 57
attrs with the same UUIDs the hosted service uses, so your `$users`,
`$userRefreshTokens`, `$files`, and oauth-entity triples resolve unchanged —
**users stay signed in** (refresh tokens are hashed the same way).

## 3. Import

Order matters for foreign keys:

```sql
-- 1. a creator (apps.creator_id must exist)
INSERT INTO instant_users (id, email) VALUES ('<any-uuid>', 'you@example.com')
ON CONFLICT DO NOTHING;

-- 2. apps, then attrs, then idents, then triples, then rules/app_admin_tokens
\copy apps FROM 'apps.csv' CSV
\copy attrs FROM 'attrs.csv' CSV
\copy idents FROM 'idents.csv' CSV
\copy triples FROM 'triples.csv' CSV
\copy rules FROM 'rules.csv' CSV
\copy app_admin_tokens FROM 'app_admin_tokens.csv' CSV
```

(If your export contains system-catalog attr rows for app
`a1111111-1111-1111-1111-111111111ca7`, skip them — the server already created
identical rows.)

## 4. Switch your clients

```js
const db = init({
  appId: "<same app id>",
  apiURI: "https://your-server.example.com",
  websocketURI: "wss://your-server.example.com/runtime/session",
});
```

`@instantdb/admin` likewise takes `apiURI`. Nothing else changes: schemas,
permissions, auth sessions, and data are all preserved.

## 5. Storage blobs

`$files` rows migrate with the triples; the blobs themselves live in S3 under
`<app-id>/<hash-bin>/<location-id>` on the hosted service. The `s3` storage
backend uses that exact layout (including the Java `hashCode`-derived bin),
so the zero-copy path is to point it at a bucket holding the same objects:

```bash
STORAGE_BACKEND=s3 S3_BUCKET=<bucket> AWS_REGION=<region> \
AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=… ./instant-server
```

(`S3_ENDPOINT` for R2/MinIO; see the env table in the README.) Existing
`$files.url` values are re-presigned by this server the same way the hosted
service did, so clients keep working unchanged. Alternatively copy each
object to `$STORAGE_DIR/<app-id>/<location-id>` for the `disk` backend (the
bin subdirectory is dropped in the local-disk layout), or leave
`STORAGE_BACKEND=postgres` and re-upload.

## 6. OAuth

Re-enter your OAuth client credentials (client id/secret/discovery endpoint):

```bash
./scripts/create-oauth-client.sh <app-id> google google \
  <client-id> <client-secret> \
  https://accounts.google.com/.well-known/openid-configuration
```

Secrets are stored as-provided in this build (the hosted service encrypts them
with a KMS key that doesn't travel with exports anyway), and the provider
redirect URI to register with Google becomes
`https://your-server.example.com/runtime/oauth/callback`.
