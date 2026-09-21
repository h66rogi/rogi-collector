#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd -P)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
mkdir -p "$tmp/bin" "$tmp/migrations"
cat > "$tmp/migrations/001_test.sql" <<'SQL'
CREATE TABLE migration_atomic_probe(id integer PRIMARY KEY);
SQL
cat > "$tmp/bin/psql" <<'SH'
#!/bin/sh
count_file=$PSQL_CAPTURE/count
count=$(cat "$count_file" 2>/dev/null || echo 0); count=$((count + 1)); echo "$count" > "$count_file"
printf '%s\n' "$*" > "$PSQL_CAPTURE/args.$count"
file=
previous=
for arg in "$@"; do [ "$previous" = -f ] && file=$arg; previous=$arg; done
if [ -n "$file" ]; then cat "$file" > "$PSQL_CAPTURE/stdin.$count"; else cat > "$PSQL_CAPTURE/stdin.$count"; fi
[ "${PSQL_FORCE_FAILURE:-0}" = 1 ] && exit 42
exit 0
SH
chmod +x "$tmp/bin/psql"
export PATH="$tmp/bin:$PATH" PSQL_CAPTURE="$tmp" RELEASE_MIGRATIONS_DIR="$tmp/migrations" TMPDIR="$tmp"
"$root/deploy/run-migrations.sh"
[ "$(cat "$tmp/count")" -eq 1 ]
grep -q -- '--single-transaction' "$tmp/args.1"
grep -q -- '--set=version=001_test' "$tmp/args.1"
! grep -q -- ' -c ' "$tmp/args.1"
python3 - "$tmp/stdin.1" <<'PY'
from pathlib import Path
import re, sys
s=Path(sys.argv[1]).read_text()
parts=["pg_advisory_xact_lock", "\\if :apply_migration", "CREATE TABLE migration_atomic_probe", "INSERT INTO schema_migrations"]
pos=[s.index(x) for x in parts] + [s.rindex("\\endif")]
assert pos == sorted(pos), pos
assert s.count("INSERT INTO schema_migrations") == 1
assert "\\quit" not in s
assert "RAISE EXCEPTION 'migration checksum conflict'" in s
assert re.search(r"PRIMARY KEY\);\s+INSERT INTO schema_migrations", s)
PY
rm -f "$tmp/count"
export PSQL_FORCE_FAILURE=1
if "$root/deploy/run-migrations.sh" >/dev/null 2>&1; then echo 'psql failure was not propagated' >&2; exit 1; fi
echo 'migration runner atomic wrapper and failure propagation passed'
