#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
"$root/tools/contracts/bootstrap-tools.sh"
protoc="$root/.tools/protoc/bin/protoc"
test -x "$protoc"
rm -rf "$root/proto/gen" "$root/contracts/typescript/src/collector"
mkdir -p "$root/proto/gen" "$root/contracts/typescript/src"
PATH="$root/.tools/bin:$root/node_modules/.bin:$PATH" "$protoc" -I "$root/proto" -I "$root/.tools/protoc/include" \
  --go_out="$root/proto/gen" --go_opt=paths=source_relative \
  --es_out="$root/contracts/typescript/src" --es_opt=target=ts \
  "$root/proto/collector/v1/collector.proto"
CONTRACT_ROOT="$root" node "$root/tools/contracts/write-manifest.mjs"
