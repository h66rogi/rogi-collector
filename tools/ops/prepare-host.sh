#!/bin/sh
set -eu
product=rogi-collector
data_root=/srv/$product
config_root=/etc/$product
runtime_root=/run/$product
[ "$(findmnt -n -o TARGET "$data_root" 2>/dev/null || true)" = "$data_root" ] || { echo "$data_root is not a mount point" >&2; exit 78; }
expected_uuid=$(cat "$config_root/data-volume.uuid")
actual_uuid=$(findmnt -n -o UUID "$data_root")
[ -n "$expected_uuid" ] && [ "$expected_uuid" = "$actual_uuid" ] || { echo 'data volume UUID mismatch' >&2; exit 78; }
[ "$(findmnt -n -o FSTYPE /run)" = tmpfs ] || { echo '/run must be tmpfs' >&2; exit 78; }
install -d -m 0700 "$runtime_root"
install -d -m 0700 -o 70 -g 70 "$data_root/postgres"
install -d -m 0700 -o 999 -g 1000 "$data_root/redis"
install -d -m 0700 -o 65532 -g 65532 "$data_root/spool"
[ -x "$config_root/load-secrets" ] || { echo 'private secret loader is not installed' >&2; exit 78; }
"$config_root/load-secrets" "$runtime_root"
for name in postgres-admin-password postgres-migrate-password migrate.pgpass redis-password discover.env coordinator.env worker.env query.env; do
  path=$runtime_root/$name
  [ -s "$path" ] || { echo "missing secret file: $name" >&2; exit 78; }
  mode=$(stat -c '%a' "$path")
  case "$mode" in 600|400) :;; *) echo "unsafe secret mode $mode: $name" >&2; exit 78;; esac
done
chown 70:70 "$runtime_root/postgres-admin-password" "$runtime_root/postgres-migrate-password" "$runtime_root/migrate.pgpass"
chown 999:1000 "$runtime_root/redis-password"
chown 65532:65532 "$runtime_root/discover.env" "$runtime_root/coordinator.env" "$runtime_root/worker.env" "$runtime_root/query.env"
