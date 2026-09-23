# Production data API Cloudflare resources

This independent Terraform root owns only the private chat archive bucket, the
dedicated `data-api.rogi.chat` Tunnel, its ingress configuration, and its DNS
record. It does not manage `rogi.chat`, `api.rogi.chat`, or `docs.rogi.chat`.
The bucket has no object expiry rule and no public bucket domain. Do not add a
public R2 domain, lifecycle deletion rule, or a credential to this repository.

Use a private S3 backend configuration with a separate state key. Supply the
Cloudflare account ID, zone ID, and API token from the operator's private
environment. Review a saved full Terraform plan before applying it. The plan
must contain exactly four creates and no changes or deletes for a new setup.

After the Tunnel exists, retrieve its connector token using Cloudflare's token
API and place it in the production runtime secret as `tunnel-token`. Create two
bucket-scoped R2 S3 API tokens outside Git: object read/write for the exporter,
and object read for the public API. Store their access key IDs and secret access
keys only in the respective production role secret files. Do not print token
responses or Terraform state to terminal logs.

Deployment order: R2/Tunnel/DNS plan and apply, private credentials, host secret
loader rollout, application release, then external HTTP and WebSocket checks.
