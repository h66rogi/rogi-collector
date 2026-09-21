# Collector production monitoring baseline

This is a monitoring contract, not deployment evidence. The collector product root should own alarms for its one EC2 instance and expose them on the shared two-host CloudWatch dashboard.

Baseline host alarms are `StatusCheckFailed_System`, `StatusCheckFailed_Instance`, sustained `CPUUtilization`, and low `CPUCreditBalance` for the `t8i.medium` burstable instance. Status checks use maximum values over two five-minute periods and treat missing data as missing. CPU uses a sustained three-period threshold; the credit reserve is finalized from observed load.

No notification action is configured until an approved, owned destination already exists. This work does not invent email, SNS, Slack, or paging recipients. Host health cannot be presented as collection success: role health, registered-channel count, last observation, journal lag, spool usage, query readiness, and gRPC capability remain application metrics and are currently incomplete.

The timer's public metadata poll and healthy-receipt no-op result do not prove private registry credentials are available. A new release is pulled only while the trusted `private-deploy` workflow's packages-read job token is temporarily present in the dedicated Secrets Manager entry. Monitor the fixed SSM invocation result and compare the receipt SHA returned by `production-status.py` with the triggering release SHA without logging the full status JSON. Retry a failed rollout by rerunning that failed workflow to obtain a fresh token. Recovery must not install a human PAT, persistent Docker login, or anonymous GHCR fallback.

A verified backup job should publish `LastSuccessfulBackupAgeSeconds` under `Rogi/rogi-collector`. IAM permission is limited with a namespace condition. The freshness alarm remains disabled until the job emits the metric and restore testing establishes the schedule and threshold. Missing data becomes breaching only after that enablement. S3 object presence alone does not prove backup freshness or restorability.

## Implemented IaC baseline

The collector root creates the four host alarms and its CloudWatch dashboard and outputs `monitoring_dashboard_url` and `monitoring_alarm_names`. Alarm and OK action lists are empty. CloudWatch alarm/dashboard charges belong in deployment cost review. No backup freshness alarm or custom-metric IAM permission exists until a real backup emitter is implemented.
