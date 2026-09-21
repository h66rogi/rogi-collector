#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd); tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/tools/contracts" "$tmp/proto/collector/v1" "$tmp/proto/gen" "$tmp/contracts/typescript/src" "$tmp/contracts"
cp "$root/tools/contracts/write-manifest.mjs" "$tmp/tools/contracts/"; printf 'syntax = "proto3";\n' > "$tmp/proto/collector/v1/collector.proto"; printf go > "$tmp/proto/gen/file.go"; printf ts > "$tmp/contracts/typescript/src/file.ts"; printf input > "$tmp/input.txt"
git -C "$tmp" init -q; git -C "$tmp" config user.name contract-test; git -C "$tmp" config user.email contract-test@example.invalid
CONTRACT_ROOT="$tmp" node "$tmp/tools/contracts/write-manifest.mjs"; git -C "$tmp" add .; git -C "$tmp" commit -qm initial; head=$(git -C "$tmp" rev-parse HEAD)
CONTRACT_ROOT="$tmp" node "$tmp/tools/contracts/write-manifest.mjs"; test "$(node -e "console.log(require('$tmp/contracts/manifest.json').sourceSha)")" = "$head"; first=$(sha256sum "$tmp/contracts/manifest.json"|cut -d' ' -f1); CONTRACT_ROOT="$tmp" node "$tmp/tools/contracts/write-manifest.mjs"; test "$(sha256sum "$tmp/contracts/manifest.json"|cut -d' ' -f1)" = "$first"
printf staged >> "$tmp/input.txt"; git -C "$tmp" add input.txt; CONTRACT_ROOT="$tmp" node "$tmp/tools/contracts/write-manifest.mjs"; test "$(node -e "console.log(require('$tmp/contracts/manifest.json').sourceSha)")" = uncommitted
git -C "$tmp" commit -qm staged; printf unstaged >> "$tmp/input.txt"; CONTRACT_ROOT="$tmp" node "$tmp/tools/contracts/write-manifest.mjs"; test "$(node -e "console.log(require('$tmp/contracts/manifest.json').sourceSha)")" = uncommitted
