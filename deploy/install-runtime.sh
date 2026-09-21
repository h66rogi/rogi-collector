#!/bin/sh
set -eu
host_prefix=${ROGI_HOST_ROOT_PREFIX:-}
[ "$(id -u)" = 0 ] || [ -n "$host_prefix" ] || { echo 'install-runtime.sh must run as root' >&2; exit 77; }
if [ -z "$host_prefix" ] && ! python3 -c 'import boto3' >/dev/null 2>&1;then apt-get update;DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends python3-boto3;fi
update_only=false
[ "${1:-}" = --update-only ] && update_only=true
source_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
library_root=$host_prefix/usr/local/lib/rogi-collector
unit_root=$host_prefix/etc/systemd/system
systemctl_bin=${SYSTEMCTL_BIN:-systemctl}
install -d -m 0755 "$library_root" "$unit_root"
for file in deploy.sh status.sh prepare-host.sh validate-manifest.mjs render-runtime-env.mjs fetch-release.py production-status.py backup-postgres.sh load-secrets-aws.py upload-backup-s3.py validate-runtime-secrets.sh load-registry-auth.py; do
  install -m 0755 "$source_root/tools/ops/$file" "$library_root/.$file.new"
  mv -f "$library_root/.$file.new" "$library_root/$file"
  cmp -s "$source_root/tools/ops/$file" "$library_root/$file" || { echo "installed helper verification failed: $file" >&2;exit 74; }
done
for unit in rogi-collector-host-ready.service rogi-collector-migrate.service rogi-collector-role@.service rogi-collector-update.service rogi-collector-update.timer rogi-collector-backup.service rogi-collector-backup.timer rogi-collector.target; do
  install -m 0644 "$source_root/deploy/systemd/$unit" "$unit_root/.$unit.new"
  mv -f "$unit_root/.$unit.new" "$unit_root/$unit"
  cmp -s "$source_root/deploy/systemd/$unit" "$unit_root/$unit" || { echo "installed unit verification failed: $unit" >&2;exit 74; }
done
$systemctl_bin daemon-reload
if [ "$update_only" = false ];then $systemctl_bin enable rogi-collector.target rogi-collector-update.timer rogi-collector-backup.timer;fi
echo 'collector runtime installed and enabled; it was not started'
