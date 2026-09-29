# Collector production monitoring baseline

The collector Terraform root owns the EC2 alarms, custom health alarms, SNS topic, and dashboard. Dated deployment evidence is recorded in [implementation status](implementation-status.md).

The production host uses `m8i-flex.large` with a 40 GiB root EBS volume and a separate retained 40 GiB data EBS volume. Host alarms cover `StatusCheckFailed_System`, `StatusCheckFailed_Instance`, and sustained `CPUUtilization`. The former burst-credit alarm is removed because M8i-flex does not use EC2 CPU credits.

Every release-timer run (every 5-6 minutes) first performs image cleanup and publishes custom metrics to `Rogi/rogi-collector`, even if the subsequent GitHub release poll fails. Image cleanup retains current and previous release images and every image referenced by a container. The `MetricHeartbeat` alarm detects a missing emitter. The `ImagePruneHealthy` alarm reports cleanup failures. The host IAM role can publish only to the collector metric namespace.

Custom alarms cover root free space below 10 GiB and 6 GiB, data free space below 8 GiB, unhealthy units/containers/receipt, public status failure, live-broadcast collection failure, and the latest backup missing from S3 or older than 36 hours. The collection metric is healthy when SOOP is explicitly offline; when SOOP is live, it requires the public status endpoint to report an active, connected collector. The emitter checks the public status timestamp and does not infer collection from process liveness. These checks do not yet measure donation journal lag or spool saturation.

The update unit emits `DeploymentHealthy` after each release fetch or host deployment attempt. A failed attempt alarms on one 60-second datapoint; a successful subsequent attempt clears it. The release and private delivery workflows also publish failure notifications to the same topic. Their GitHub secret `AWS_ALERT_TOPIC_ARN` must reference the Terraform-owned topic; the dedicated OIDC role can publish only there.

Alarms publish ALARM and OK transitions to the Terraform-owned `rogi-collector-prod-alerts` SNS topic. An email subscription is created only when the private `alert_email` Terraform input is supplied and confirmed by its owner. An unconfirmed or absent subscription does not deliver notifications. Verify the topic policy, subscription confirmation, and an end-to-end test notification before relying on delivery.

The timer's public metadata poll and healthy-receipt no-op result do not prove private registry credentials are available. A new release is pulled only while the trusted `private-deploy` workflow's packages-read job token is temporarily present in the dedicated Secrets Manager entry. Monitor the fixed SSM invocation result and compare the receipt SHA returned by `production-status.py` with the triggering release SHA without logging the full status JSON. Retry a failed rollout by rerunning that failed workflow to obtain a fresh token. Recovery must not install a human PAT, persistent Docker login, or anonymous GHCR fallback.

S3 object presence verifies upload but does not prove that a backup can be restored. Restore testing remains a separate acceptance check.

## Operator checks

- Run `tools/ops/status.sh` from an installed release for the Compose service overview. Its capability text describes the configured product; it does not perform an authenticated RPC.
- Run `/usr/local/lib/rogi-collector/production-status.py` on the host to compare the current manifest and receipt, systemd units, and all seven containers. It exits nonzero when the deployment assessment fails. Its local backup age is informational and does not verify S3 or restore success.
- The same status command exits nonzero when either volume has less than 4 GiB or 10% free, whichever reserve is larger. CloudWatch alarms warn earlier. Track `archive_event_ids` growth and expand the data volume before this threshold is reached.
- Use `GetCollectionStatus` from the consumer host with its mTLS identity to inspect the configured channel, connection/storage state, observation times, and journal cursors. `waiting` is expected when the broadcast is offline.
- Cookie `/healthz` checks process liveness; authenticated `/v1/status` checks login/refresh readiness. Neither proves entry into the broadcast. Keep the API internal and never print cookies or credentials.
- Check update, backup, and TLS timer/service results separately. Server certificate rotation is automatic; consumer certificate issuance and renewal remain operator procedures.

The `shared/cmd/collector-check` CLI can perform an authenticated acceptance probe using credentials supplied outside Git. Its isolated file inbox is a bounded, single-process verification tool, not the rogimarble transactional game consumer. See [code handoff](collector-code-handoff.md) and the [isolated probe procedure](../deploy/live-check/README.md).

## Terraform ownership

The collector root owns the alarm rules, SNS topic and subscription, namespace-scoped metric publishing permission, and dashboard. Keep private recipient values outside Git. CloudWatch custom metrics, alarms, and SNS delivery incur AWS charges.
