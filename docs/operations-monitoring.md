# Collector production monitoring baseline

This document separates implemented checks from remaining monitoring work. The collector Terraform root owns alarms and a dashboard for its EC2 instance. Dated deployment evidence is recorded in [implementation status](implementation-status.md).

Baseline host alarms are `StatusCheckFailed_System`, `StatusCheckFailed_Instance`, sustained `CPUUtilization`, and low `CPUCreditBalance` for the `t8i.medium` burstable instance. Status checks use maximum values over two five-minute periods and treat missing data as missing. CPU uses a sustained three-period threshold; the credit reserve is finalized from observed load.

No notification action is configured until an approved, owned destination already exists. This work does not invent email, SNS, Slack, or paging recipients. Host health cannot be presented as collection success. Role probes and collector v1 status are implemented, but a complete application metrics/alerting pipeline for observation age, journal lag, spool usage, and collection failures is not.

The timer's public metadata poll and healthy-receipt no-op result do not prove private registry credentials are available. A new release is pulled only while the trusted `private-deploy` workflow's packages-read job token is temporarily present in the dedicated Secrets Manager entry. Monitor the fixed SSM invocation result and compare the receipt SHA returned by `production-status.py` with the triggering release SHA without logging the full status JSON. Retry a failed rollout by rerunning that failed workflow to obtain a fresh token. Recovery must not install a human PAT, persistent Docker login, or anonymous GHCR fallback.

A future backup metric emitter should publish `LastSuccessfulBackupAgeSeconds` under `Rogi/rogi-collector`, with IAM permission limited by that namespace condition. The emitter, permission, and freshness alarm are not implemented. Add the alarm after the job emits the metric and restore testing establishes the schedule and threshold. Missing data becomes breaching only after that enablement. S3 object presence alone does not prove backup freshness or restorability.

## Operator checks

- Run `tools/ops/status.sh` from an installed release for the Compose service overview. Its capability text describes the configured product; it does not perform an authenticated RPC.
- Run `/usr/local/lib/rogi-collector/production-status.py` on the host to compare the current manifest and receipt, systemd units, and all seven containers. It exits nonzero when the deployment assessment fails. Its local backup age is informational and does not verify S3 or restore success.
- The same status command exits nonzero when the data volume has less than 4 GiB or 10% free, whichever reserve is larger. Track `archive_event_ids` growth and expand the volume before this threshold is reached; this check is not a remote notification.
- Use `GetCollectionStatus` from the consumer host with its mTLS identity to inspect the configured channel, connection/storage state, observation times, and journal cursors. `waiting` is expected when the broadcast is offline.
- Cookie `/healthz` checks process liveness; authenticated `/v1/status` checks login/refresh readiness. Neither proves entry into the broadcast. Keep the API internal and never print cookies or credentials.
- Check update, backup, and TLS timer/service results separately. Server certificate rotation is automatic; consumer certificate issuance and renewal remain operator procedures.

The `shared/cmd/collector-check` CLI can perform an authenticated acceptance probe using credentials supplied outside Git. Its isolated file inbox is a bounded, single-process verification tool, not the rogimarble transactional game consumer. See [code handoff](collector-code-handoff.md) and the [isolated probe procedure](../deploy/live-check/README.md).

## Implemented IaC baseline

The collector root creates the four host alarms and its CloudWatch dashboard and outputs `monitoring_dashboard_url` and `monitoring_alarm_names`. Alarm and OK action lists are empty. CloudWatch alarm/dashboard charges belong in deployment cost review. No backup freshness alarm or custom-metric IAM permission exists until a real backup emitter is implemented.
