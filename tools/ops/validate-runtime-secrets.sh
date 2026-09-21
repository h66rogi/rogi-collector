#!/bin/sh
set -eu
runtime_root=${1:-/run/rogi-collector}
expected_uid=${EXPECTED_SECRET_UID:-0}
runtime_real=$(realpath -e -- "$runtime_root")
allowed=$(realpath -e -- "$runtime_root/source-secrets")
[ -d "$allowed" ] || { echo 'published secret generation is not a directory' >&2; exit 78; }
case "$allowed" in "$runtime_real"/source-secrets.*) :;; *) echo 'published secret generation escapes runtime root' >&2; exit 78;; esac
for name in postgres-admin-password postgres-migrate-password migrate.pgpass redis-password discover.env coordinator.env worker.env query.env cookie-auth.env app-migrate.env tls-ca.pem tls-ca.key; do
  link=$runtime_root/$name
  target=$(realpath -e -- "$link") || { echo "dangling secret link: $name" >&2; exit 78; }
  case "$target" in "$allowed"/*) :;; *) echo "secret target escapes published generation: $name" >&2; exit 78;; esac
  [ "$(stat -L -c '%F' -- "$link")" = 'regular file' ] || { echo "secret target is not regular: $name" >&2; exit 78; }
  [ "$(stat -L -c '%a' -- "$link")" = 400 ] || { echo "unsafe secret target mode: $name" >&2; exit 78; }
  [ "$(stat -L -c '%u' -- "$link")" = "$expected_uid" ] || { echo "secret target is not root-owned: $name" >&2; exit 78; }
done
