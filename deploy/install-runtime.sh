#!/bin/sh
set -eu
[ "$(id -u)" = 0 ] || { echo 'install-runtime.sh must run as root' >&2; exit 77; }
source_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
library_root=/usr/local/lib/rogi-collector
unit_root=/etc/systemd/system
install -d -m 0755 "$library_root" "$unit_root"
for file in deploy.sh status.sh prepare-host.sh validate-manifest.mjs render-runtime-env.mjs fetch-release.py production-status.py backup-postgres.sh load-secrets-aws.py upload-backup-s3.py; do
  install -m 0755 "$source_root/tools/ops/$file" "$library_root/$file"
done
for unit in rogi-collector-host-ready.service rogi-collector-migrate.service rogi-collector-role@.service rogi-collector-update.service rogi-collector-update.timer rogi-collector-backup.service rogi-collector-backup.timer rogi-collector.target; do
  install -m 0644 "$source_root/deploy/systemd/$unit" "$unit_root/$unit"
done
systemctl daemon-reload
systemctl enable rogi-collector.target rogi-collector-update.timer rogi-collector-backup.timer
echo 'collector runtime installed and enabled; it was not started'
