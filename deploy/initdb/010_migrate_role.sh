#!/bin/sh
set -eu
password=$(cat /run/secrets/postgres-migrate-password)
psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -v ON_ERROR_STOP=1 \
  --set=migrate_user="$POSTGRES_MIGRATE_USER" --set=migrate_password="$password" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'migrate_user', :'migrate_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'migrate_user') \gexec
SELECT format('ALTER ROLE %I PASSWORD %L', :'migrate_user', :'migrate_password') \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), :'migrate_user') \gexec
GRANT CREATE, USAGE ON SCHEMA public TO :"migrate_user";
SQL
