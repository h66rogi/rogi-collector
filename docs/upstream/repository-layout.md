# Repository guide

This guide explains where the main responsibilities live and how to follow a
runtime path through the source.

## Top-level map

```text
.
├── .github/               CI and dependency-update automation
├── chat-exporter/         Optional object-storage export command
├── cleanup/               Retention and stale-state cleanup command
├── coordinator/           Worker health, assignment, and handoff control plane
├── discover/              Live-channel discovery and viewer-history emission
├── docs/                  Architecture and design documentation
│   └── design/            Coordination, storage, and reliability rationale
├── postgresql/            Generic PostgreSQL image and migration runner
├── proto/                 Protobuf sources and generated Go bindings
├── query/                 gRPC and HTTP query/admin APIs
├── shared/                Models, stores, migrations, and shared middleware
├── worker/                Platform connections and chat ingestion pipeline
├── go.work                Local workspace joining the independent Go modules
├── README.md              Project entry point and local-development overview
├── LICENSE                AGPL-3.0-only license text
├── NOTICE                 Project and third-party attribution notice
└── DISCLAIMER.md          Project status and warranty limitations
```

Each executable component is its own Go module with a `go.mod`, `go.sum`, and
Dockerfile. `go.work` joins those modules for local development; it does not
turn them into one deployable process.

## Runtime components

| Path | Important files | Responsibility |
| --- | --- | --- |
| `discover/cmd/main.go` | process entry point | Loads configuration and starts discovery, probes, leadership, and optional history emission. |
| `discover/internal/discovery/` | platform-specific discovery clients | Finds live channels and maps platform responses to the shared model. |
| `discover/internal/history/` | viewer-history emitter and writer | Buffers optional analytical viewer history and reports queue/write health. |
| `coordinator/cmd/main.go` | process entry point | Starts the assignment control plane and its operational endpoints. |
| `coordinator/internal/` | leader, assigner, health, and lifecycle logic | Chooses healthy workers, persists ownership, publishes commands, and reconciles handoffs. |
| `coordinator/internal/grpcserver/` | coordinator RPC server | Exposes coordinator operations to other components. |
| `worker/cmd/main.go` | process entry point | Wires stores, connectors, metrics, probes, and worker lifecycle. |
| `worker/internal/connector/` | platform chat connectors | Establishes platform connections, parses events, and manages retries. |
| `worker/internal/pipeline/` | Redis publisher | Publishes normalized chat events and deduplicates overlap during graceful handoff. |
| `worker/internal/manager.go` | connection ownership | Applies coordinator commands and reconciles active connections with durable assignments. |
| `query/cmd/main.go` | process entry point | Starts the gRPC and HTTP query/admin surfaces. |
| `query/internal/server.go` | gRPC query/admin implementation | Reads recent and historical data and handles administrative operations. |
| `query/internal/admin_http.go` | HTTP administration | Handles collection administration over HTTP. |
| `cleanup/cmd/main.go` | maintenance command | Removes expired or stale state according to operator-supplied retention policy. |
| `chat-exporter/cmd/main.go` | export command | Copies selected analytical data to operator-configured S3-compatible storage. |

The usual reading path is `cmd/main.go` for dependency wiring, followed by the
component's `internal/` package for behavior. Files ending in `_test.go` sit next
to the behavior they verify. Integration tests require the corresponding data
store and are kept separate from production configuration.

## Shared contracts and persistence

| Path | Contents |
| --- | --- |
| `shared/model/` | Common channel, message, platform, worker, and administration types. |
| `shared/store/` | PostgreSQL, Redis, and ClickHouse access plus bounded batch writers. |
| `shared/grpcauth/` | Shared gRPC server middleware. |
| `shared/migrations/*.sql` | Ordered PostgreSQL schema migrations for control-plane state. |
| `shared/migrations/clickhouse/` | Optional analytical schemas and materialized views. |
| `proto/meloming/` | Source Protobuf definitions; edit these rather than generated bindings. |
| `proto/gen/go/` | Generated Go code committed for reproducible consumers and builds. |
| `postgresql/run-migrations.sh` | Generic migration entry point used by the PostgreSQL image. |

Migrations and protocol sources are included because they define the data and
API contracts required to understand the code.

## Automation and repository policy

- `.github/workflows/ci.yml` runs project validation and container builds.
- `.github/dependabot.yml` proposes reviewed GitHub Actions updates.
- `.gitignore` and `.dockerignore` exclude local and build-context files that do
  not belong in the repository.
