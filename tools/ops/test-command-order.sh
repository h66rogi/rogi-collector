#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd -P)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
prefix=$tmp/host
release=$prefix/opt/rogi-collector/app/releases/test-release
mkdir -p "$release/deploy/migrations" "$release/deploy/initdb" "$release/deploy/systemd" "$release/tools/ops" "$prefix/etc/rogi-collector" "$prefix/srv/rogi-collector" "$prefix/usr/local/lib/rogi-collector" "$tmp/bin"
cp "$root/deploy/compose.production.yaml" "$release/deploy/compose.production.yaml"
cp "$root/deploy/migrations/001_foundation.sql" "$release/deploy/migrations/001_foundation.sql"
cp "$root/deploy/run-migrations.sh" "$release/deploy/run-migrations.sh"
cp "$root/deploy/initdb/010_migrate_role.sh" "$release/deploy/initdb/010_migrate_role.sh"
cp "$root/deploy/install-runtime.sh" "$release/deploy/install-runtime.sh"
cp "$root/deploy/systemd/rogi-collector-update.service" "$release/deploy/systemd/rogi-collector-update.service"
cp "$root/deploy/systemd/rogi-collector-update.timer" "$release/deploy/systemd/rogi-collector-update.timer"
cp "$root/deploy/systemd/rogi-collector-backup.service" "$release/deploy/systemd/rogi-collector-backup.service"
cp "$root/deploy/systemd/rogi-collector-backup.timer" "$release/deploy/systemd/rogi-collector-backup.timer"
cp "$root/tools/ops/fetch-release.py" "$release/tools/ops/fetch-release.py"
cp "$root/tools/ops/production-status.py" "$release/tools/ops/production-status.py"
cp "$root/tools/ops/backup-postgres.sh" "$release/tools/ops/backup-postgres.sh"
cp "$root/tools/ops/load-secrets-aws.py" "$release/tools/ops/load-secrets-aws.py"
cp "$root/tools/ops/upload-backup-s3.py" "$release/tools/ops/upload-backup-s3.py"
cp "$root/tools/ops/validate-manifest.mjs" "$root/tools/ops/render-runtime-env.mjs" "$prefix/usr/local/lib/rogi-collector/"
printf 'test-volume\n' > "$prefix/etc/rogi-collector/data-volume.uuid"
compose_sha=$(sha256sum "$release/deploy/compose.production.yaml" | awk '{print $1}')
migration_sha=$(sha256sum "$release/deploy/migrations/001_foundation.sql" | awk '{print $1}')
runner_sha=$(sha256sum "$release/deploy/run-migrations.sh" | awk '{print $1}')
initdb_sha=$(sha256sum "$release/deploy/initdb/010_migrate_role.sh" | awk '{print $1}')
runtime_json=$(node -e "const fs=require('fs'),crypto=require('crypto'),root=process.argv[1],paths=process.argv.slice(2);console.log(JSON.stringify(paths.map(path=>({path,sha256:crypto.createHash('sha256').update(fs.readFileSync(root+'/'+path)).digest('hex')}))))" "$release" 'deploy/run-migrations.sh' 'deploy/initdb/010_migrate_role.sh' 'deploy/install-runtime.sh' 'deploy/systemd/rogi-collector-update.service' 'deploy/systemd/rogi-collector-update.timer' 'deploy/systemd/rogi-collector-backup.service' 'deploy/systemd/rogi-collector-backup.timer' 'tools/ops/fetch-release.py' 'tools/ops/production-status.py' 'tools/ops/backup-postgres.sh' 'tools/ops/load-secrets-aws.py' 'tools/ops/upload-backup-s3.py')
digest=$(printf 'a%.0s' $(seq 1 64))
cat > "$release/manifest.json" <<EOF
{"schemaVersion":1,"product":"rogi-collector","profile":"feedback","sourceSha":"1111111111111111111111111111111111111111","releaseId":"test-release","contractVersion":"v1","composeSha256":"$compose_sha","runtimeFiles":$runtime_json,"images":{"postgres":"registry.example/test/postgres@sha256:$digest","redis":"registry.example/test/redis@sha256:$digest","discover":"registry.example/test/discover@sha256:$digest","coordinator":"registry.example/test/coordinator@sha256:$digest","worker":"registry.example/test/worker@sha256:$digest","query":"registry.example/test/query@sha256:$digest"},"migrations":[{"path":"deploy/migrations/001_foundation.sql","sha256":"$migration_sha"}],"runtimeNonSecret":{"composeProjectName":"rogi-collector","dataRoot":"/srv/rogi-collector","secretsRoot":"/run/rogi-collector","releaseRoot":"/opt/rogi-collector/app/current","postgresDatabase":"collector","postgresAdminUser":"collector_admin","postgresMigrateUser":"collector_migrate","capabilities":{"grpc7443":"unavailable-health-only"}}}
EOF
cat > "$tmp/bin/systemctl" <<'EOF'
#!/bin/sh
echo "systemctl $*" >> "$COMMAND_LOG"
exit 0
EOF
cat > "$tmp/bin/docker" <<'EOF'
#!/bin/sh
echo "docker $*" >> "$COMMAND_LOG"
exit 0
EOF
cat > "$tmp/bin/findmnt" <<'EOF'
#!/bin/sh
case "$*" in *UUID*) echo test-volume;; *) echo "$ROGI_HOST_ROOT_PREFIX/srv/rogi-collector";; esac
EOF
chmod +x "$tmp/bin/systemctl" "$tmp/bin/docker" "$tmp/bin/findmnt"
export COMMAND_LOG=$tmp/commands.log ROGI_HOST_ROOT_PREFIX=$prefix PATH=$tmp/bin:$PATH
export SYSTEMCTL_BIN=$tmp/bin/systemctl DOCKER_BIN=$tmp/bin/docker
"$root/tools/ops/deploy.sh" --manifest "$release/manifest.json" >/dev/null
test "$(readlink "$prefix/opt/rogi-collector/app/current")" = "$release"
host_line=$(grep -n 'systemctl restart rogi-collector-host-ready' "$COMMAND_LOG" | cut -d: -f1)
pull_line=$(grep -n 'docker compose .* pull' "$COMMAND_LOG" | cut -d: -f1)
migrate_line=$(grep -n 'docker compose .* run --rm migrate' "$COMMAND_LOG" | cut -d: -f1)
target_line=$(grep -n 'systemctl restart rogi-collector.target' "$COMMAND_LOG" | cut -d: -f1)
[ "$host_line" -lt "$pull_line" ] && [ "$pull_line" -lt "$migrate_line" ] && [ "$migrate_line" -lt "$target_line" ]
echo 'collector deploy fake-command order passed'
