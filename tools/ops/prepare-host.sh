#!/bin/sh
set -eu
product=rogi-collector
data_root=/srv/$product
config_root=/etc/$product
runtime_root=/run/$product
library_root=/usr/local/lib/$product
[ "$(findmnt -n -o TARGET "$data_root" 2>/dev/null || true)" = "$data_root" ] || { echo "$data_root is not a mount point" >&2; exit 78; }
expected_uuid=$(cat "$config_root/data-volume.uuid")
actual_uuid=$(findmnt -n -o UUID "$data_root")
[ -n "$expected_uuid" ] && [ "$expected_uuid" = "$actual_uuid" ] || { echo 'data volume UUID mismatch' >&2; exit 78; }
[ "$(findmnt -n -o FSTYPE /run)" = tmpfs ] || { echo '/run must be tmpfs' >&2; exit 78; }
install -d -m 0700 "$runtime_root"
install -d -m 0700 -o 70 -g 70 "$data_root/postgres"
install -d -m 0700 -o 999 -g 1000 "$data_root/redis"
install -d -m 0700 -o 65532 -g 65532 "$data_root/spool"
install -d -m 0700 -o 0 -g 0 "$data_root/backups"
install -d -m 0700 -o 65532 -g 65532 "$data_root/cookies"
install -d -m 0700 -o 65532 -g 65532 "$data_root/tls"
if [ -x "$config_root/load-secrets" ]; then
  "$config_root/load-secrets" "$runtime_root"
else
  "$library_root/load-secrets-aws.py" "$runtime_root"
fi
"$library_root/validate-runtime-secrets.sh" "$runtime_root"
chown 70:70 "$runtime_root/postgres-admin-password" "$runtime_root/postgres-migrate-password" "$runtime_root/migrate.pgpass"
chown 999:1000 "$runtime_root/redis-password"
chown 65532:65532 "$runtime_root/discover.env" "$runtime_root/coordinator.env" "$runtime_root/worker.env" "$runtime_root/query.env" "$runtime_root/cookie-auth.env" "$runtime_root/app-migrate.env"
[ ! -e "$runtime_root/data-api.env" ] || chown 65532:65532 "$runtime_root/data-api.env"
[ ! -e "$runtime_root/archive-exporter.env" ] || chown 65532:65532 "$runtime_root/archive-exporter.env"
[ ! -e "$runtime_root/tunnel-token" ] || chown 65532:65532 "$runtime_root/tunnel-token"
"$library_root/rotate-server-tls.py"
