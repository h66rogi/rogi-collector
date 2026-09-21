#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"
mkdir -p contracts/fixtures
"$root/tools/contracts/test-manifest-provenance.sh"
npm run contracts:build
go run ./proto/cmd/wirefixture
node --test contracts/typescript/test/*.test.mjs
go test -count=1 ./proto/contracttest
