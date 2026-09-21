#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
"$root/tools/contracts/bootstrap-tools.sh"
protoc="$root/.tools/protoc/bin/protoc"
test -x "$protoc"
rm -rf "$root/proto/gen/collector" "$root/contracts/typescript/src/collector"
mkdir -p "$root/proto/gen" "$root/contracts/typescript/src"
PATH="$root/.tools/bin:$root/node_modules/.bin:$PATH" "$protoc" -I "$root/proto" -I "$root/.tools/protoc/include" \
  --go_out="$root/proto/gen" --go_opt=paths=source_relative \
  --plugin=protoc-gen-go-grpc="$root/.tools/upstream-bin/protoc-gen-go-grpc" --go-grpc_out="$root/proto/gen" --go-grpc_opt=paths=source_relative \
  --es_out="$root/contracts/typescript/src" --es_opt=target=ts \
  "$root/proto/collector/v1/collector.proto"
# Regenerate descriptors after changing go_package; textual replacement inside
# a generated descriptor can invalidate its protobuf length prefixes.
PATH="$root/.tools/upstream-bin:$PATH" "$protoc" -I "$root/proto" -I "$root/.tools/protoc/include" \
  --go_out="$root/proto/gen/go" --go_opt=paths=source_relative \
  --go-grpc_out="$root/proto/gen/go" --go-grpc_opt=paths=source_relative \
  "$root/proto/meloming/chat/v1/chat_admin.proto" \
  "$root/proto/meloming/chat/v1/chat_query.proto" \
  "$root/proto/meloming/common/v1/pagination.proto"
CONTRACT_ROOT="$root" node "$root/tools/contracts/write-manifest.mjs"
