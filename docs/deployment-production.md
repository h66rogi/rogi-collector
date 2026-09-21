# Collector production deployment preparation

This is a production delivery contract, not evidence of an EC2 deployment. The current Go images expose process health only. They do not implement collection, persistence, authenticated gRPC, or mTLS. The feedback profile therefore does not listen on or publish TCP 7443. Infrastructure may reserve an inbound rule from the marble application security group, but it must remain unused until the query implementation and certificate checks pass.

## Host and product boundary

Collector runs on its own EC2 instance, Compose project, PostgreSQL, Redis, encrypted data EBS, secrets, release lock, and backup lifecycle. Marble uses a different EC2 instance and does not mount collector paths or connect to collector PostgreSQL/Redis.

| Purpose | Path |
| --- | --- |
| immutable release bundles and `current` link | `/opt/rogi-collector/app` |
| non-secret host configuration and expected EBS UUID | `/etc/rogi-collector` |
| role-specific runtime secrets and deployment lock | `/run/rogi-collector` |
| PostgreSQL, Redis, and worker spool EBS mount | `/srv/rogi-collector` |
| installed public deployment helpers | `/usr/local/lib/rogi-collector` |

The EC2 root volume must never substitute for a missing data volume. `deploy.sh` requires `/srv/rogi-collector` to be a mount point and compares its UUID with `/etc/rogi-collector/data-volume.uuid` before pulling or starting anything. EC2 replacement and EBS deletion are separate operations; deletion protection and attachment are owned by the infrastructure root.

## Release manifest v1

The real manifest is a private release input and is not committed. It lives at the root of the staged release bundle so migration and Compose checksums resolve within that bundle. Required fields are shared with marble:

- `schemaVersion: 1`, `product: rogi-collector`, and `profile: feedback`;
- a 40-character lowercase `sourceSha`, bounded `releaseId`, and `contractVersion`;
- `composeSha256` for `deploy/compose.production.yaml`;
- an exact `runtimeFiles` allow-list with SHA-256 values for `deploy/run-migrations.sh` and every file mounted from `deploy/initdb/`;
- `images` entries for postgres, redis, discover, coordinator, worker, and query, each as `repository@sha256:<64 lowercase hex>`;
- a sorted `migrations` array that exactly lists every on-disk `deploy/migrations/*.sql` file once with its SHA-256;
- `runtimeNonSecret` with the fixed host roots, non-secret database role names, Compose project name, and `capabilities.grpc7443=unavailable-health-only`.

The validator rejects tags without digests, unknown product/profile paths, migration traversal, missing files, and checksum drift. Missing, duplicate, or changed allow-listed runtime files also reject the bundle before image pull or database access. It does not accept secret values. Image registry authentication, database passwords, TLS material, AWS state, host addresses, and the actual manifest remain outside Git.

## Secrets and database roles

`/run/rogi-collector` is tmpfs populated by the private executable `/etc/rogi-collector/load-secrets` before Docker or application units start. `rogi-collector-host-ready.service` asserts the EBS mount point and UUID, invokes that hook, and rejects missing, empty, or group/world-readable secret files. Files are mounted individually:

| File | Consumer |
| --- | --- |
| `postgres-admin-password` | PostgreSQL bootstrap only |
| `migrate.pgpass` | one-shot migration only; mode 0600 |
| `redis-password` | Redis only |
| `discover.env` | discover only |
| `coordinator.env` | coordinator only |
| `worker.env` | worker only |
| `query.env` | query only |

The official PostgreSQL and Redis images run as fixed container UIDs 70 and 999; host preparation creates and owns their bind directories before Compose, and every bind mount uses `create_host_path: false`. The PostgreSQL first-volume initialization script creates the named migration role from its dedicated secret. The migration runner serializes apply operations with a PostgreSQL transaction advisory lock and commits each SQL file together with its checksum ledger row in one transaction. It skips an identical applied file and rejects checksum drift. The migration database role owns schema changes but is not mounted into application roles. Runtime roles receive only their own future credentials. The current health-only images do not consume these files, which is an explicit capability limitation rather than proof of credential isolation. Before live use, container tests must show that each role cannot read another role's secret, migration credentials, or IMDS.

## Deployment sequence and supervision

From a verified release source, `sudo deploy/install-runtime.sh` installs the public helpers under `/usr/local/lib/rogi-collector`, installs and enables the systemd units without starting them, and leaves EBS mounting and private secret-loader installation to the approved host process. Stage the bundle as `/opt/rogi-collector/app/releases/<releaseId>`, then invoke:

```sh
sudo /usr/local/lib/rogi-collector/deploy.sh --manifest /opt/rogi-collector/app/releases/<releaseId>/manifest.json
```

The helper acquires `/run/rogi-collector/deploy.lock`, requires the manifest to be inside the exact selected release directory, validates manifest and checksums, checks the data mount UUID, renders a candidate non-secret environment, validates Compose interpolation, pulls immutable digests, and runs database readiness and the one-shot migration against that candidate. Only after migration succeeds does it atomically promote `current`, install the durable non-secret runtime environment, restart attached systemd role units, and execute health smoke checks. A failed migration leaves the previous `current` link in place. It never runs a down migration, `compose down -v`, or a host image build.

Compose sets `restart: "no"`. Each systemd template invocation runs `docker compose up --no-deps <one-role>` in the foreground; the role name is a positional service argument, not an `--attach` option value. Container exit is therefore visible and restarted under host supervision. A detached `compose up -d` result is not operational evidence. Migration failure prevents role units from starting. Rollback selects a previously staged compatible manifest and images; it does not reverse schema migrations automatically.

## Network and capability gate

PostgreSQL and Redis have internal networks only and no host ports. Role health endpoints are container-internal. Query has no published 7443 port in the feedback profile. Live enablement requires all of the following in a later release: implemented query RPCs, SAN/CA-verified mTLS, consumer/channel authorization, private 7443 listener, SG source limited to the marble app SG, certificate rotation evidence, and integration replay tests.

## CI and release boundary

Public pull-request CI may run Go/contract tests, shell syntax, manifest fixture validation, secret scanning, and static infrastructure checks. It receives no AWS, SSH, registry-push, production manifest, or secret access. A future image publishing workflow must be limited to manual dispatch or a protected trusted branch, build the selected 40-character source SHA, publish immutable image digests, and emit a manifest input artifact. It must not apply Terraform or deploy to a host. No release workflow is added here because action commit pins and registry trust policy have not yet been approved; inventing pins would weaken the boundary.

## Verification status

`tools/ops/test.sh` is deliberately Docker-free. It validates a synthetic manifest, migration and Compose checksums, rendered non-secret environment, YAML parsing, shell syntax, absence of host builds/ports, bind auto-creation, `restart: "no"`, and the health-only capability declaration. Its fake-command deployment run records and checks host-ready → pull → migration → target restart ordering and verifies release promotion without invoking Docker or systemd. Passing it does not prove Compose startup, database permissions, EBS persistence, systemd recovery, mTLS, collection, or EC2 deployment. Those require the selected AWS account path and an actual host.

## Public release updater, monitoring, and backups

`rogi-collector-update.timer` polls the configured public repository every ten minutes without a GitHub token. The fetcher accepts only non-draft `production-<sourceSha>` assets whose archive checksum, embedded manifest SHA, and successful `.github/workflows/release.yml` push on `main` agree. It safely extracts into `/opt/rogi-collector/app/releases`, strictly merges root-owned `/etc/rogi-collector/runtime-overlay.json`, and invokes the same locked deploy helper. `deploy/release-source.example.json` and `deploy/runtime-overlay.example.json` document the non-secret host input shape.

`rogi-collector-backup.timer` creates a daily PostgreSQL custom-format logical dump under the data EBS and retains seven days. `production-status.py` reports target/container state, data-disk usage, latest backup age, deployed receipt, and the explicit health-only capability profile. These checks do not claim that collection or gRPC 7443 is available.

### Instance-role secret materialization

When no private `/etc/rogi-collector/load-secrets` override is installed, host readiness reads root-owned mode 0400/0600 `/etc/rogi-collector/secrets-manager.json`, uses the EC2 instance role through `python3-boto3` to fetch exactly one Secrets Manager JSON `SecretString`, rejects missing, empty, or extra keys, and atomically writes the eight role-specific files into `/run/rogi-collector`. The ARN and region are metadata only; secret values never enter the release bundle, manifest, environment overlay, or persistent application directory.

The updater skips a candidate only when the deployed receipt has the same source SHA and image map and every supervised role unit is active. It queries GitHub's compare API and accepts automatic movement only when the candidate is the same commit or a descendant of the deployed SHA. Rollback therefore remains an explicit operator action rather than an automatic poll result.

Backups are written to a raw temporary file before compression, so a failing `pg_dump` cannot be hidden by a successful `gzip`. After local validation, `upload-backup-s3.py` reads root-owned mode 0400/0600 `/etc/rogi-collector/backup-s3.json`, uploads through the instance role to the configured private bucket with S3 server-side encryption, and fails the backup unit if upload fails. Bucket credentials are never stored in the repository or host metadata.
