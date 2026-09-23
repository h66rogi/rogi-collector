.PHONY: build contracts test race source-check cookie-test
modules = ./discover/... ./coordinator/... ./worker/... ./query/... ./shared/... ./cleanup/... ./chat-exporter/... ./proto/...
build:
	go build ./discover/cmd ./coordinator/cmd ./worker/cmd ./query/cmd ./query/cmd/data-api ./query/cmd/archive-exporter ./cleanup/cmd ./chat-exporter/cmd ./shared/cmd/migrate ./shared/cmd/rotate-generation ./shared/cmd/healthcheck ./shared/cmd/collector-check
contracts:
	npm run contracts:test
test: contracts
	go test $(modules)
race: contracts
	go test -race $(modules)
source-check:
	python3 tools/source/verify-import.py

# Use the Python environment with cookie-auth/requirements.txt installed.
PYTHON ?= python3
cookie-test:
	$(PYTHON) -m unittest discover -s cookie-auth -v

# Explicit test services only; ordinary make test remains connection-free.
integration-test:
	test -n "$$COLLECTOR_TEST_DATABASE_URL"
	test -n "$$COLLECTOR_TEST_REDIS_ADDR"
	go test -race ./shared/store/... ./worker/internal/pipeline/... ./query/internal/... -run "TestCollector|TestSpoolDatabase|TestProductMTLS|TestChatWatch|TestProductChat" -count=1
