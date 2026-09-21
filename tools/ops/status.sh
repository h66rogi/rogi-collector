#!/bin/sh
set -eu
env_file=${RUNTIME_ENV:-/etc/rogi-collector/runtime.env}
release=${RELEASE_ROOT:-/opt/rogi-collector/app/current}
cd "$release"
docker compose --env-file "$env_file" -f deploy/compose.production.yaml ps
printf '\nConfigured capability profile: soop-single-channel\n'
printf 'gRPC 7443: collector v1 with mTLS; verify channel state using GetCollectionStatus\n'
printf 'Compose health does not prove broadcast reception or game integration\n'
