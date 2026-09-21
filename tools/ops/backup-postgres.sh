#!/bin/sh
set -eu
umask 077
env_file=/etc/rogi-collector/runtime.env
compose_file=/opt/rogi-collector/app/current/deploy/compose.production.yaml
backup_root=/srv/rogi-collector/backups
test -r "$env_file"; mkdir -p "$backup_root"; stamp=$(date -u +%Y%m%dT%H%M%SZ)
raw=$backup_root/.postgres-$stamp.dump; temporary=$backup_root/.postgres-$stamp.dump.gz; target=$backup_root/postgres-$stamp.dump.gz
trap 'rm -f "$raw" "$temporary"' EXIT HUP INT TERM
docker compose --env-file "$env_file" -f "$compose_file" exec -T postgres sh -eu -c 'exec pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" --format=custom' > "$raw"
test -s "$raw"; gzip -9 < "$raw" > "$temporary"; test -s "$temporary"; mv "$temporary" "$target"; rm -f "$raw"
/usr/local/lib/rogi-collector/upload-backup-s3.py "$target"
find "$backup_root" -maxdepth 1 -type f -name 'postgres-*.dump.gz' -mtime +7 -delete
trap - EXIT HUP INT TERM
