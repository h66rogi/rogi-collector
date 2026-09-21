.PHONY: build contracts test race compose-config
build:
	go build ./cmd/discover ./cmd/coordinator ./cmd/worker ./cmd/query
contracts:
	npm run contracts:test
test: contracts
	go test ./cmd/discover/... ./cmd/coordinator/... ./cmd/worker/... ./cmd/query/... ./pkg/shared/... ./proto/...
race: contracts
	go test -race ./cmd/discover/... ./cmd/coordinator/... ./cmd/worker/... ./cmd/query/... ./pkg/shared/... ./proto/...
compose-config:
	docker compose --env-file deploy/.env -f deploy/compose.yaml config --quiet
