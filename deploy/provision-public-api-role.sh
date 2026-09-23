#!/bin/sh
set -eu

admin_file=${POSTGRES_ADMIN_PASSWORD_FILE:-/run/secrets/postgres-admin-password}
api_file=${PUBLIC_API_PASSWORD_FILE:-/run/secrets/public-api-db-password}
[ -r "$admin_file" ] && [ -r "$api_file" ] || { echo 'database password files unavailable' >&2; exit 78; }
[ -n "${POSTGRES_ADMIN_USER:-}" ] && [ -n "${POSTGRES_DB:-}" ] || { echo 'database identity unavailable' >&2; exit 78; }
API_ROLE_PASSWORD=$(cat "$api_file")
[ "${#API_ROLE_PASSWORD}" -ge 32 ] || { echo 'public API password too short' >&2; exit 78; }
export API_ROLE_PASSWORD
PGPASSWORD=$(cat "$admin_file")
export PGPASSWORD

psql -X -q -v ON_ERROR_STOP=1 -v VERBOSITY=terse -v db_name="$POSTGRES_DB" -U "$POSTGRES_ADMIN_USER" -d "$POSTGRES_DB" >/dev/null <<'SQL'
\getenv api_password API_ROLE_PASSWORD
SELECT format('CREATE ROLE collector_public_api LOGIN NOINHERIT PASSWORD %L', :'api_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='collector_public_api') \gexec
DO $guard$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_roles r WHERE r.rolname='collector_public_api'
    AND (r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls)
  ) OR EXISTS (
    SELECT 1 FROM pg_auth_members m JOIN pg_roles r ON r.oid=m.member
    WHERE r.rolname='collector_public_api'
  ) THEN
    RAISE EXCEPTION 'public API database role is not isolated';
  END IF;
END
$guard$;
SELECT format('ALTER ROLE collector_public_api LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L', :'api_password') \gexec
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM collector_public_api;
REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM collector_public_api;
GRANT CONNECT ON DATABASE :"db_name" TO collector_public_api;
GRANT USAGE ON SCHEMA public TO collector_public_api;
GRANT SELECT ON collector_channels, collector_owner_grants, collection_opt_outs,
  broadcast_sessions, archive_sessions, archive_segments, archive_chat_hot,
  archive_quality_gaps
  TO collector_public_api;
SQL
echo 'public API read-only database role provisioned'
