#!/bin/sh
set -eu
export PGPASSFILE=/run/secrets/pgpass
found=0
work=${TMPDIR:-/tmp}/rogi-migration.$$
migrations_dir=${RELEASE_MIGRATIONS_DIR:-/release/migrations}
trap 'rm -f "$work"' EXIT HUP INT TERM
for file in "$migrations_dir"/*.sql; do
  [ -f "$file" ] || continue
  found=1
  version=$(basename "$file" .sql)
  checksum=$(sha256sum "$file" | awk '{print $1}')
  cat > "$work" <<'SQL'
SELECT pg_advisory_xact_lock(hashtextextended('rogi-collector-schema-migrations', 0));
CREATE TABLE IF NOT EXISTS schema_migrations(
  version text PRIMARY KEY,
  sha256 char(64) NOT NULL,
  applied_at timestamptz NOT NULL DEFAULT now()
);
SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = :'version' AND sha256 <> :'checksum') AS checksum_conflict,
       NOT EXISTS(SELECT 1 FROM schema_migrations WHERE version = :'version') AS apply_migration
\gset
\if :checksum_conflict
  DO $$ BEGIN RAISE EXCEPTION 'migration checksum conflict'; END $$;
\endif
\if :apply_migration
SQL
  cat "$file" >> "$work"
  printf '\n' >> "$work"
  cat >> "$work" <<'SQL'
INSERT INTO schema_migrations(version, sha256) VALUES (:'version', :'checksum');
\endif
SQL
  psql -v ON_ERROR_STOP=1 --single-transaction --set=version="$version" --set=checksum="$checksum" -f "$work"
done
[ "$found" = 1 ] || { echo 'no migration files found' >&2; exit 66; }
