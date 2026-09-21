# CI and release publishing

Public pull requests run `make test`, all role builds, contract checks, and the Docker-free production runtime suite with read-only repository permission and no secrets. A successful trusted `main` push, or manual execution whose ref is explicitly `main`, repeats that gate before publishing four `linux/amd64` role images to `ghcr.io/<owner>/rogi-collector-{discover,coordinator,worker,query}`.

Repository variables `POSTGRES_IMAGE` and `REDIS_IMAGE` must be reviewed immutable digest references. The publisher writes those and the four built image digests into `manifest.build.json`. It publishes public GitHub Release assets `release-bundle.tar.gz` and `release-bundle.tar.gz.sha256`, and retains the same files as the `rogi-collector-production-release` Actions artifact. The bundle contains only the allow-listed release source and build-owned manifest fields. Runtime host configuration, secrets, tokens, account identifiers, and state are excluded.

Hosts poll the public Releases API no more often than every five minutes, accept only a release tied to a successful `release.yml` main push, verify the release/source SHA and checksums, merge the root-owned runtime overlay, and pull public GHCR packages anonymously by digest. The workflow never applies infrastructure or connects to EC2.

A rerun never overwrites a published SHA tag, image tag, or Release asset. If `production-<sourceSha>` already exists, the workflow verifies its asset checksum and embedded source SHA and skips all publishing; malformed or conflicting existing assets fail the run. Published images carry OCI `org.opencontainers.image.source` and `org.opencontainers.image.revision` labels.

The trusted release job emits a GitHub artifact provenance attestation for `release-bundle.tar.gz` using OIDC. Host enforcement remains checksum, source/tag, and successful workflow verification until an approved, checksum-pinned GitHub CLI verifier and its authentication/public-access contract are installed; the attestation is not yet claimed as a host admission gate.
