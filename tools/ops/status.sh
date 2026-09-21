#!/bin/sh
set -eu
env_file=${RUNTIME_ENV:-/etc/rogi-collector/runtime.env}
release=${RELEASE_ROOT:-/opt/rogi-collector/app/current}
cd "$release"
docker compose --env-file "$env_file" -f deploy/compose.production.yaml ps
printf '\nCapability profile: feedback (health-only)\n'
printf 'gRPC 7443: unavailable; no listener or published port in this release\n'
