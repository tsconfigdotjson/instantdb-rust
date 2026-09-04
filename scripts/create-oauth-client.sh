#!/usr/bin/env bash
# Provision an OAuth provider + client for an app (writes system-namespace triples).
# Usage: ./scripts/create-oauth-client.sh <app-id> <client-name> <provider-name> \
#           <client-id> <client-secret> <discovery-endpoint> [<authorized-origin-host>]
# The optional 7th argument adds a `generic` authorized redirect origin
# (host[:port], e.g. localhost:5173). Like legacy without shared credentials,
# no redirect target is allowed until one is listed here.
set -euo pipefail
DATABASE_URL="${DATABASE_URL:-postgres://instant:instant@localhost:5432/instant}"
APP_ID=$1; CLIENT_NAME=$2; PROVIDER_NAME=$3; CLIENT_ID=$4; SECRET=$5; DISCOVERY=$6
ORIGIN_HOST="${7:-}"
PROVIDER_EID=$(python3 -c "import uuid; print(uuid.uuid4())")
CLIENT_EID=$(python3 -c "import uuid; print(uuid.uuid4())")

psql "$DATABASE_URL" -q -v ON_ERROR_STOP=1 <<SQL
CREATE OR REPLACE FUNCTION pg_temp.ins_triple(p_app uuid, p_eid uuid, p_etype text, p_label text, p_value jsonb)
RETURNS void AS \$\$
DECLARE a record;
BEGIN
  SELECT * INTO a FROM attrs
   WHERE app_id = 'a1111111-1111-1111-1111-111111111ca7' AND etype = p_etype AND label = p_label;
  IF a IS NULL THEN RAISE EXCEPTION 'missing system attr %.%', p_etype, p_label; END IF;
  INSERT INTO triples (app_id, entity_id, attr_id, value, value_md5, ea, eav, av, ave, vae, checked_data_type)
  VALUES (p_app, p_eid, a.id, p_value, md5(p_value::text),
          a.cardinality = 'one', a.value_type = 'ref', a.is_unique, a.is_indexed, a.value_type = 'ref',
          a.checked_data_type)
  ON CONFLICT (app_id, entity_id, attr_id) WHERE ea
  DO UPDATE SET value = excluded.value, value_md5 = excluded.value_md5;
END \$\$ LANGUAGE plpgsql;

-- provider (reuse by name if exists)
DO \$\$
DECLARE existing uuid;
BEGIN
  SELECT t.entity_id INTO existing FROM triples t
  JOIN attrs a ON a.id = t.attr_id
  WHERE t.app_id = '$APP_ID' AND a.etype = '\$oauthProviders' AND a.label = 'name'
    AND t.value = to_jsonb('$PROVIDER_NAME'::text);
  IF existing IS NULL THEN
    PERFORM pg_temp.ins_triple('$APP_ID', '$PROVIDER_EID', '\$oauthProviders', 'id', to_jsonb('$PROVIDER_EID'::text));
    PERFORM pg_temp.ins_triple('$APP_ID', '$PROVIDER_EID', '\$oauthProviders', 'name', to_jsonb('$PROVIDER_NAME'::text));
  ELSE
    UPDATE triples SET value = to_jsonb(existing::text) WHERE false; -- no-op
    PERFORM set_config('vars.provider_eid', existing::text, false);
  END IF;
END \$\$;

SELECT pg_temp.ins_triple('$APP_ID', '$CLIENT_EID', '\$oauthClients', 'id', to_jsonb('$CLIENT_EID'::text));
SELECT pg_temp.ins_triple('$APP_ID', '$CLIENT_EID', '\$oauthClients', 'name', to_jsonb('$CLIENT_NAME'::text));
SELECT pg_temp.ins_triple('$APP_ID', '$CLIENT_EID', '\$oauthClients', 'clientId', to_jsonb('$CLIENT_ID'::text));
SELECT pg_temp.ins_triple('$APP_ID', '$CLIENT_EID', '\$oauthClients', 'encryptedClientSecret', to_jsonb('$SECRET'::text));
SELECT pg_temp.ins_triple('$APP_ID', '$CLIENT_EID', '\$oauthClients', 'discoveryEndpoint', to_jsonb('$DISCOVERY'::text));
SELECT pg_temp.ins_triple('$APP_ID', '$CLIENT_EID', '\$oauthClients', '\$oauthProvider',
  to_jsonb(COALESCE(current_setting('vars.provider_eid', true), '$PROVIDER_EID')));
SQL
if [ -n "$ORIGIN_HOST" ]; then
  ORIGIN_ID=$(python3 -c "import uuid; print(uuid.uuid4())")
  psql "$DATABASE_URL" -q -v ON_ERROR_STOP=1 <<SQL
INSERT INTO app_authorized_redirect_origins (id, app_id, service, params)
SELECT '$ORIGIN_ID', '$APP_ID', 'generic', ARRAY['$ORIGIN_HOST']::text[]
WHERE NOT EXISTS (
  SELECT 1 FROM app_authorized_redirect_origins
   WHERE app_id = '$APP_ID' AND service = 'generic' AND params = ARRAY['$ORIGIN_HOST']::text[]);
SQL
  echo "authorized_origin=$ORIGIN_HOST"
fi
echo "provider_eid=$PROVIDER_EID"
echo "client_eid=$CLIENT_EID"
