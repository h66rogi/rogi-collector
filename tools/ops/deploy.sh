#!/bin/sh
set -eu
umask 077

manifest=
while [ "$#" -gt 0 ]; do case "$1" in --manifest) manifest=$2; shift 2;; *) echo "unknown argument: $1" >&2; exit 64;; esac; done
[ -n "$manifest" ] || { echo 'usage: deploy.sh --manifest PATH' >&2; exit 64; }

product=rogi-collector
host_prefix=${ROGI_HOST_ROOT_PREFIX:-}
release_root=$host_prefix/opt/$product/app
config_root=$host_prefix/etc/$product
runtime_root=$host_prefix/run/$product
library_root=$host_prefix/usr/local/lib/$product
lock_file=$runtime_root/deploy.lock
node_bin=${NODE_BIN:-node}
docker_bin=${DOCKER_BIN:-docker}
systemctl_bin=${SYSTEMCTL_BIN:-systemctl}
install_bin=${INSTALL_BIN:-install}

$install_bin -d -m 0750 "$release_root" "$config_root" "$runtime_root" "$library_root"
exec 9>"$lock_file"
flock -n 9 || { echo 'another collector deployment is active' >&2; exit 75; }
$systemctl_bin restart rogi-collector-host-ready.service
$systemctl_bin is-active --quiet rogi-collector-host-ready.service

release_id=$($node_bin -e "const m=require(process.argv[1]); process.stdout.write(m.releaseId)" "$manifest")
case "$release_id" in ''|*[!A-Za-z0-9._-]*|.*|*..*) echo 'invalid releaseId' >&2; exit 66;; esac
target=$release_root/releases/$release_id
[ -d "$target" ] || { echo "release bundle missing: $target" >&2; exit 66; }
manifest_dir=$(CDPATH= cd -- "$(dirname -- "$manifest")" && pwd -P)
target_real=$(CDPATH= cd -- "$target" && pwd -P)
[ "$manifest_dir" = "$target_real" ] || { echo 'manifest must be inside the selected release bundle root' >&2; exit 66; }
$node_bin "$library_root/validate-manifest.mjs" "$manifest"
data_root=$host_prefix/srv/$product
[ "$(findmnt -n -o TARGET "$data_root" 2>/dev/null || true)" = "$data_root" ] || { echo "data EBS is not mounted at $data_root" >&2; exit 78; }
[ -f "$config_root/data-volume.uuid" ] || { echo 'missing expected data volume UUID' >&2; exit 78; }
expected_uuid=$(cat "$config_root/data-volume.uuid")
actual_uuid=$(findmnt -n -o UUID "$data_root")
[ "$expected_uuid" = "$actual_uuid" ] || { echo 'data volume UUID mismatch' >&2; exit 78; }

$node_bin "$library_root/render-runtime-env.mjs" "$manifest" > "$runtime_root/runtime.env.new"
sed "s|^RELEASE_ROOT=.*|RELEASE_ROOT='$target_real'|" "$runtime_root/runtime.env.new" > "$runtime_root/candidate.env"

cd "$target_real"
$docker_bin compose --env-file "$runtime_root/candidate.env" -f deploy/compose.production.yaml config --quiet
registry_auth=$runtime_root/docker-auth
rm -rf "$registry_auth"
trap 'rm -rf "$registry_auth"' EXIT HUP INT TERM
"$target_real/tools/ops/load-registry-auth.py" --metadata "$config_root/registry.json" --output "$registry_auth"
$docker_bin --config "$registry_auth" compose --env-file "$runtime_root/candidate.env" -f deploy/compose.production.yaml pull
rm -rf "$registry_auth";trap - EXIT HUP INT TERM
$docker_bin compose --env-file "$runtime_root/candidate.env" -f deploy/compose.production.yaml up --no-deps --wait postgres redis
$docker_bin compose --env-file "$runtime_root/candidate.env" -f deploy/compose.production.yaml run --rm migrate

"$target_real/deploy/install-runtime.sh" --update-only

ln -sfn "$target_real" "$release_root/current.next"
mv -Tf "$release_root/current.next" "$release_root/current"
$install_bin -m 0600 "$runtime_root/runtime.env.new" "$config_root/runtime.env"
rm "$runtime_root/runtime.env.new" "$runtime_root/candidate.env"
$systemctl_bin daemon-reload
$systemctl_bin restart rogi-collector.target
for role in postgres redis discover coordinator worker query; do $systemctl_bin is-active --quiet "rogi-collector-role@$role.service"; done
for role in discover coordinator worker query; do
  healthy=false
  for attempt in $(seq 1 60);do
    if $docker_bin compose --env-file "$config_root/runtime.env" -f deploy/compose.production.yaml exec -T "$role" /service -healthcheck http://127.0.0.1:8080/health >/dev/null 2>&1;then healthy=true;break;fi
    sleep 1
  done
  [ "$healthy" = true ] || { echo "$role health smoke failed" >&2; exit 70; }
done
$install_bin -m 0600 "$manifest" "$runtime_root/deployed-release.json"
echo "collector release $release_id activated; profile feedback is health-only and gRPC 7443 remains unavailable"
