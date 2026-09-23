# data-api.rogi.chat production rollout

Status: deployed to production on 2026-09-23. This was a direct production
rollout; there is no staging hostname or environment. The API, Tunnel, private
R2 bucket, archive exporter, and read-only database role are active. External
checks confirmed readiness, live status, recent chat, WebSocket chat, and an
archived-chat page. End-to-end archive completeness remains unproven, so
responses report `complete=false`.

The donation spool and chat archive spool must be separate sibling directories
on the data volume. Nesting the chat directory inside the donation spool makes
the donation spool reject the extra directory and reports `storage_delayed`.
Production uses separate `spool/donations` and `spool/chat` paths. The worker
now rejects overlapping paths at startup.

## Public surface

- Cloudflare Tunnel routes only `data-api.rogi.chat` to the Compose service
  `http://data-api:8080`. The origin has no public host port or new inbound
  firewall rule. TLS terminates at Cloudflare.
- Anonymous `GET /v1/broadcasts/current`, `GET /v1/chats/recent`, and
  `GET /v1/chat/stream` use the existing diagnostics and short Redis stream.
  The global origin budget is 20 HTTP requests per second with a burst of 60;
  up to 32 WebSocket connections are accepted. Access logs rotate locally.
- Historical endpoints use a private R2 bucket with no object expiration rule.
  The exporter uploads immutable gzip NDJSON, reads it back and checks its hash
  before indexing the segment and pruning PostgreSQL hot rows. The API reads
  through a separate bucket-scoped object-read credential. `complete` remains
  `false` until the gap and recovery mechanisms can prove completeness.
- Archive coverage starts when `HISTORY_ENABLED=true` and
  `CHAT_ARCHIVE_ENABLED=true` have both been activated. Older expired Redis
  messages cannot be reconstructed. Session listing currently includes shows
  with at least one accepted archived chat; empty shows are absent.

## Infrastructure and secret preparation

1. Use the independent collector Cloudflare Terraform root. Confirm the public
   DNS name, R2 bucket, and Tunnel name are not already owned. Use the private
   S3 backend and operator inputs. Review a full saved plan: exactly one
   private bucket, one remotely managed Tunnel, one ingress configuration, and
   one proxied CNAME; no changes or deletes. Apply the approved saved plan and
   verify the resources through the provider API. No public R2 domain is used.
2. Create bucket-scoped R2 S3 credentials: object read/write for
   `archive-exporter`, object read for `data-api`. Retrieve the Tunnel connector
   token. Keep values out of Git, CI logs, release bundles, Terraform output,
   and public documentation.
3. The production runtime secret must preserve its existing keys and add
   `public-api-db-password`, `data-api.env`, `archive-exporter.env`, and
   `tunnel-token`. The new loader
   admits the legacy set and each consecutive expanded set. Install the new
   loader, secret validator, and host preparation code before adding keys; then
   reload and validate the secret files. The Tunnel token file is mode 0400.
4. The deployment creates a dedicated `collector_public_api` DB role with
   SELECT on only the seven tables used by the public surface. Its password
   is supplied in `public-api-db-password`. `data-api.env` uses this role,
   Redis address and password, the discover diagnostic token, a new cursor HMAC key, and the
   object-read R2 credentials. `archive-exporter.env` uses the existing worker
   DB role and the object-read/write R2 credentials. Both set the single
   allowed channel. Never give the public API the migrate role, worker role,
   SOOP login, or object-write credential.

## Application activation

1. Validate the public source audit, all CI jobs, and the exact staged diff.
   Merge the reviewed collector PR to `main`. The release workflow publishes
   immutable image digests and the private deployment workflow applies the
   bundle. The new archive grant migration and public DB role provisioning run
   before service startup. The API rejects a DB login with write privileges.
2. Set `HISTORY_ENABLED=true` and `VIEWER_HISTORY_ENABLED=false` in discover;
   set `CHAT_ARCHIVE_ENABLED=true`, an absolute spool path on the data volume
   that does not contain or sit inside the donation spool,
   and a 32-byte-or-longer HMAC key in worker; set
   `PUBLIC_ARCHIVE_HISTORY_ENABLED=true` in data-api. Keep the existing Redis
   retention independent from the R2 archive.
3. Verify all supervised roles and container health, including exporter R2
   probe. A running cloudflared container has no in-container health check;
   verify its Cloudflare connector status and external route separately.
4. From outside the server, verify DNS, TLS, `GET /readyz`, current state,
   recent chat, WebSocket upgrade/hello/status/chat and reconnect, and the
   historical list/page contract. Exercise a real new show before asserting
   that archive data can be read. Test R2 readback and PostgreSQL-to-R2
   recovery with a controlled synthetic segment outside public records.

Rollback uses the previous immutable release while preserving the new R2
bucket, archive objects, additive DB schema, and expanded secret keys. Do not
delete the R2 bucket or move public history cursors backwards.
