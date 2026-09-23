#!/bin/sh
set -eu
export EXPECTED_SECRET_UID=$(id -u)
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd -P);tmp=$(mktemp -d);trap 'rm -rf "$tmp"' EXIT HUP INT TERM
names='postgres-admin-password postgres-migrate-password migrate.pgpass redis-password discover.env coordinator.env worker.env query.env data-api.env cookie-auth.env app-migrate.env tls-ca.pem tls-ca.key'
make_valid(){ rm -rf "$tmp/run";mkdir -m 700 "$tmp/run" "$tmp/run/source-secrets.123";for n in $names;do printf x > "$tmp/run/source-secrets.123/$n";chmod 400 "$tmp/run/source-secrets.123/$n";ln -s "source-secrets/$n" "$tmp/run/$n";done;ln -s source-secrets.123 "$tmp/run/source-secrets"; }
make_valid;"$root/tools/ops/validate-runtime-secrets.sh" "$tmp/run"
mkdir "$tmp/outside";for n in $names;do printf x > "$tmp/outside/$n";chmod 400 "$tmp/outside/$n";done
rm "$tmp/run/source-secrets";ln -s "$tmp/outside" "$tmp/run/source-secrets";if "$root/tools/ops/validate-runtime-secrets.sh" "$tmp/run" 2>/dev/null;then echo 'outside generation accepted' >&2;exit 1;fi
make_valid;printf x > "$tmp/escape";chmod 400 "$tmp/escape";rm "$tmp/run/query.env";ln -s "$tmp/escape" "$tmp/run/query.env";if "$root/tools/ops/validate-runtime-secrets.sh" "$tmp/run" 2>/dev/null;then echo 'individual escape accepted' >&2;exit 1;fi
make_valid;mv "$tmp/run/source-secrets.123" "$tmp/run/source-secrets.1234";rm "$tmp/run/source-secrets";ln -s source-secrets.1234 "$tmp/run/source-secrets";mkdir "$tmp/run/source-secrets.1234-sibling";rm "$tmp/run/query.env";printf x > "$tmp/run/source-secrets.1234-sibling/query.env";chmod 400 "$tmp/run/source-secrets.1234-sibling/query.env";ln -s "$tmp/run/source-secrets.1234-sibling/query.env" "$tmp/run/query.env";if "$root/tools/ops/validate-runtime-secrets.sh" "$tmp/run" 2>/dev/null;then echo 'sibling-prefix escape accepted' >&2;exit 1;fi
echo 'runtime secret symlink boundary tests passed'
