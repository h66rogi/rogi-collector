#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd -P)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
mkdir -p "$tmp/deploy/migrations" "$tmp/deploy/initdb" "$tmp/deploy/systemd" "$tmp/tools/ops"
cp "$root/deploy/compose.production.yaml" "$tmp/deploy/compose.production.yaml"
cp "$root/deploy/migrations/001_foundation.sql" "$tmp/deploy/migrations/001_foundation.sql"
cp "$root/deploy/run-migrations.sh" "$tmp/deploy/run-migrations.sh"
cp "$root/deploy/initdb/010_migrate_role.sh" "$tmp/deploy/initdb/010_migrate_role.sh"
runtime_paths='deploy/install-runtime.sh deploy/systemd/rogi-collector-host-ready.service deploy/systemd/rogi-collector-migrate.service deploy/systemd/rogi-collector-role@.service deploy/systemd/rogi-collector-update.service deploy/systemd/rogi-collector-update.timer deploy/systemd/rogi-collector-backup.service deploy/systemd/rogi-collector-backup.timer deploy/systemd/rogi-collector.target tools/ops/deploy.sh tools/ops/status.sh tools/ops/prepare-host.sh tools/ops/validate-manifest.mjs tools/ops/render-runtime-env.mjs tools/ops/fetch-release.py tools/ops/production-status.py tools/ops/backup-postgres.sh tools/ops/load-secrets-aws.py tools/ops/upload-backup-s3.py tools/ops/validate-runtime-secrets.sh tools/ops/load-registry-auth.py'
for path in $runtime_paths;do mkdir -p "$tmp/$(dirname "$path")";cp "$root/$path" "$tmp/$path";done
compose_sha=$(sha256sum "$tmp/deploy/compose.production.yaml" | awk '{print $1}')
migration_sha=$(sha256sum "$tmp/deploy/migrations/001_foundation.sql" | awk '{print $1}')
runner_sha=$(sha256sum "$tmp/deploy/run-migrations.sh" | awk '{print $1}')
initdb_sha=$(sha256sum "$tmp/deploy/initdb/010_migrate_role.sh" | awk '{print $1}')
runtime_json=$(node -e "const fs=require('fs'),crypto=require('crypto'),root=process.argv[1],paths=process.argv.slice(2);console.log(JSON.stringify(paths.map(path=>({path,sha256:crypto.createHash('sha256').update(fs.readFileSync(root+'/'+path)).digest('hex')}))))" "$tmp" deploy/run-migrations.sh deploy/initdb/010_migrate_role.sh $runtime_paths)
digest=$(printf 'a%.0s' $(seq 1 64))
cat > "$tmp/manifest.json" <<EOF
{"schemaVersion":1,"product":"rogi-collector","profile":"feedback","sourceSha":"1111111111111111111111111111111111111111","releaseId":"test-release","contractVersion":"v1","composeSha256":"$compose_sha","runtimeFiles":$runtime_json,"images":{"postgres":"registry.example/test/postgres@sha256:$digest","redis":"registry.example/test/redis@sha256:$digest","discover":"registry.example/test/discover@sha256:$digest","coordinator":"registry.example/test/coordinator@sha256:$digest","worker":"registry.example/test/worker@sha256:$digest","query":"registry.example/test/query@sha256:$digest"},"migrations":[{"path":"deploy/migrations/001_foundation.sql","sha256":"$migration_sha"}],"runtimeNonSecret":{"composeProjectName":"rogi-collector","dataRoot":"/srv/rogi-collector","secretsRoot":"/run/rogi-collector","releaseRoot":"/opt/rogi-collector/app/current","postgresDatabase":"collector","postgresAdminUser":"collector_admin","postgresMigrateUser":"collector_migrate","capabilities":{"grpc7443":"unavailable-health-only"}}}
EOF
node "$root/tools/ops/validate-manifest.mjs" "$tmp/manifest.json" >/dev/null
cp "$tmp/deploy/run-migrations.sh" "$tmp/deploy/run-migrations.sh.good"
printf '\n# tampered\n' >> "$tmp/deploy/run-migrations.sh"
if node "$root/tools/ops/validate-manifest.mjs" "$tmp/manifest.json" >/dev/null 2>&1; then echo 'changed migration runner was accepted' >&2; exit 1; fi
mv "$tmp/deploy/run-migrations.sh.good" "$tmp/deploy/run-migrations.sh"
mv "$tmp/deploy/initdb/010_migrate_role.sh" "$tmp/deploy/initdb/010_migrate_role.sh.missing"
if node "$root/tools/ops/validate-manifest.mjs" "$tmp/manifest.json" >/dev/null 2>&1; then echo 'missing initdb file was accepted' >&2; exit 1; fi
mv "$tmp/deploy/initdb/010_migrate_role.sh.missing" "$tmp/deploy/initdb/010_migrate_role.sh"
printf 'SELECT 1;\n' > "$tmp/deploy/migrations/999_unlisted.sql"
if node "$root/tools/ops/validate-manifest.mjs" "$tmp/manifest.json" >/dev/null 2>&1; then echo 'unlisted migration was accepted' >&2; exit 1; fi
rm "$tmp/deploy/migrations/999_unlisted.sql"
python3 - "$tmp/manifest.json" <<'PYFIX'
import json,sys
p=sys.argv[1]; v=json.load(open(p)); v['runtimeNonSecret']['unexpected']='bad'; open(p+'.bad','w').write(json.dumps(v))
PYFIX
if node "$root/tools/ops/validate-manifest.mjs" "$tmp/manifest.json.bad" >/dev/null 2>&1; then echo 'unexpected runtime field was accepted' >&2; exit 1; fi
node "$root/tools/ops/render-runtime-env.mjs" "$tmp/manifest.json" | grep -q "QUERY_IMAGE='registry.example/test/query@sha256:"
sh -n "$root/tools/ops/deploy.sh" "$root/tools/ops/status.sh" "$root/tools/ops/test.sh" "$root/tools/ops/prepare-host.sh" "$root/deploy/install-runtime.sh" "$root/deploy/initdb/010_migrate_role.sh" "$root/deploy/run-migrations.sh"
grep -q 'restart: "no"' "$root/deploy/compose.production.yaml"
! grep -Eq 'ports:|7443:|build:' "$root/deploy/compose.production.yaml"
! grep -q 'tmpfs: \[/' "$root/deploy/compose.production.yaml"
! grep -Eq 'tmpfs:.*(^|, )(noexec|nosuid|nodev|size=|mode=|uid=|gid=)' "$root/deploy/compose.production.yaml"
grep -q 'unavailable-health-only' "$root/tools/ops/validate-manifest.mjs"
grep -q 'AssertPathIsMountPoint=/srv/rogi-collector' "$root/deploy/systemd/rogi-collector-host-ready.service"
grep -q 'manifest must be inside the selected release bundle root' "$root/tools/ops/deploy.sh"
grep -q 'create_host_path: false' "$root/deploy/compose.production.yaml"
! grep -q 'find /run/rogi-collector .* -delete' "$root/deploy/systemd/rogi-collector-host-ready.service"
grep -q 'ExecStop=/usr/local/lib/rogi-collector/load-secrets-aws.py --cleanup /run/rogi-collector' "$root/deploy/systemd/rogi-collector-host-ready.service"
! grep -q -- '--attach %i' "$root/deploy/systemd/rogi-collector-role@.service"
"$root/tools/ops/test-command-order.sh" >/dev/null
"$root/tools/ops/test-migration-runner.sh" >/dev/null
python3 "$root/tools/ops/test_fetch_release.py" >/dev/null
python3 "$root/tools/ops/test_load_secrets_aws.py" >/dev/null
python3 "$root/tools/ops/test_upload_backup_s3.py" >/dev/null
python3 "$root/tools/ops/test_registry_auth.py" >/dev/null
python3 "$root/tools/ops/test_production_status.py" >/dev/null
"$root/tools/ops/test_validate_runtime_secrets.sh" >/dev/null
echo 'collector production runtime static tests passed (no Docker or EC2 execution)'
