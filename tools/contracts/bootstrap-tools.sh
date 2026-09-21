#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
mkdir -p "$root/.tools/bin"
archive="$root/.tools/protoc.zip"
expected=d99c011b799e9e412064244f0be417e5d76c9b6ace13a2ac735330fa7d57ad8f
if [ ! -x "$root/.tools/protoc/bin/protoc" ]; then
  curl -fsSL -o "$archive" https://github.com/protocolbuffers/protobuf/releases/download/v33.0/protoc-33.0-linux-x86_64.zip
  printf '%s  %s\n' "$expected" "$archive" | sha256sum -c -
  unzip -q -o "$archive" -d "$root/.tools/protoc"
fi
test "$("$root/.tools/protoc/bin/protoc" --version)" = "libprotoc 33.0"
if [ ! -x "$root/.tools/bin/protoc-gen-go" ]; then GOBIN="$root/.tools/bin" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.10; fi
test "$("$root/.tools/bin/protoc-gen-go" --version)" = "protoc-gen-go v1.36.10"
if [ ! -x "$root/node_modules/.bin/protoc-gen-es" ]; then (cd "$root" && npm ci); fi
npm --prefix "$root" ls --depth=0 @bufbuild/protoc-gen-es@2.10.1 @bufbuild/protobuf@2.10.1 typescript@5.9.3 >/dev/null
