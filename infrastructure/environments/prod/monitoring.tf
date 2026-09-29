locals {
  monitoring_dashboard_name = "rogi-collector-prod-host"
  monitoring_alarm_actions  = [aws_sns_topic.alerts.arn]
  monitoring_dimensions     = { Environment = "prod" }
  custom_alarms = {
    root_disk_low        = { metric = "RootFreeBytes", threshold = 10 * 1024 * 1024 * 1024, operator = "LessThanThreshold", statistic = "Minimum", description = "Collector root filesystem has less than 10 GiB free." }
    root_disk_critical   = { metric = "RootFreeBytes", threshold = 6 * 1024 * 1024 * 1024, operator = "LessThanThreshold", statistic = "Minimum", description = "Collector root filesystem has less than 6 GiB free." }
    data_disk_low        = { metric = "DataFreeBytes", threshold = 8 * 1024 * 1024 * 1024, operator = "LessThanThreshold", statistic = "Minimum", description = "Collector retained data volume has less than 8 GiB free." }
    host_unhealthy       = { metric = "HostHealthy", threshold = 0.5, operator = "LessThanThreshold", statistic = "Minimum", description = "Collector units, containers, receipt, or disk reserve are unhealthy." }
    prune_failed         = { metric = "ImagePruneHealthy", threshold = 0.5, operator = "LessThanThreshold", statistic = "Minimum", description = "Scheduled collector image cleanup failed." }
    public_status_failed = { metric = "PublicStatusHealthy", threshold = 0.5, operator = "LessThanThreshold", statistic = "Minimum", description = "Public collector broadcast status cannot be checked." }
    collection_failed    = { metric = "CollectionHealthy", threshold = 0.5, operator = "LessThanThreshold", statistic = "Minimum", description = "Live broadcast is not being collected." }
    backup_missing       = { metric = "BackupStored", threshold = 0.5, operator = "LessThanThreshold", statistic = "Minimum", description = "Latest local backup is absent from the S3 backup bucket." }
    backup_stale         = { metric = "BackupAgeSeconds", threshold = 36 * 3600, operator = "GreaterThanThreshold", statistic = "Maximum", description = "Latest verified S3 backup is older than 36 hours." }
    metrics_missing      = { metric = "MetricHeartbeat", threshold = 0.5, operator = "LessThanThreshold", statistic = "Minimum", description = "Collector maintenance metrics stopped arriving." }
    deployment_failed    = { metric = "DeploymentHealthy", threshold = 0.5, operator = "LessThanThreshold", statistic = "Minimum", description = "Collector release fetch or deployment failed on the host." }
  }
}

resource "aws_sns_topic" "alerts" {
  name = "rogi-collector-prod-alerts"
  tags = local.tags
}

resource "aws_sns_topic_policy" "alerts" {
  arn = aws_sns_topic.alerts.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid = "OwnerAdministration", Effect = "Allow", Principal = { AWS = "arn:aws:iam::${var.aws_account_id}:root" }, Action = "SNS:*", Resource = aws_sns_topic.alerts.arn
      },
      {
        Sid       = "CollectorCloudWatchAlarms", Effect = "Allow", Principal = { Service = "cloudwatch.amazonaws.com" }, Action = "SNS:Publish", Resource = aws_sns_topic.alerts.arn,
        Condition = { StringEquals = { "aws:SourceAccount" = var.aws_account_id }, ArnLike = { "aws:SourceArn" = "arn:aws:cloudwatch:${var.region}:${var.aws_account_id}:alarm:rogi-collector-prod-*" } }
      }
    ]
  })
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_iam_role_policy" "monitoring" {
  name_prefix = "metrics-"
  role        = aws_iam_role.host.id
  policy = jsonencode({ Version = "2012-10-17", Statement = [{
    Effect    = "Allow", Action = ["cloudwatch:PutMetricData"], Resource = "*",
    Condition = { StringEquals = { "cloudwatch:namespace" = "Rogi/rogi-collector" } }
  }] })
}

resource "aws_cloudwatch_metric_alarm" "status_system" {
  alarm_name          = "rogi-collector-prod-status-system"
  alarm_description   = "EC2 system status check failed; inspect AWS infrastructure status."
  namespace           = "AWS/EC2"
  metric_name         = "StatusCheckFailed_System"
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 2
  datapoints_to_alarm = 2
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "missing"
  alarm_actions       = local.monitoring_alarm_actions
  ok_actions          = local.monitoring_alarm_actions
  dimensions          = { InstanceId = aws_instance.host.id }
  tags                = local.tags
}

resource "aws_cloudwatch_metric_alarm" "status_instance" {
  alarm_name          = "rogi-collector-prod-status-instance"
  alarm_description   = "EC2 instance status check failed; inspect the guest host."
  namespace           = "AWS/EC2"
  metric_name         = "StatusCheckFailed_Instance"
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 2
  datapoints_to_alarm = 2
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "missing"
  alarm_actions       = local.monitoring_alarm_actions
  ok_actions          = local.monitoring_alarm_actions
  dimensions          = { InstanceId = aws_instance.host.id }
  tags                = local.tags
}

resource "aws_cloudwatch_metric_alarm" "cpu_high" {
  alarm_name          = "rogi-collector-prod-cpu-high"
  alarm_description   = "Sustained EC2 CPU pressure; validate application and host capacity."
  namespace           = "AWS/EC2"
  metric_name         = "CPUUtilization"
  statistic           = "Average"
  period              = 300
  evaluation_periods  = 3
  datapoints_to_alarm = 3
  threshold           = var.cpu_high_threshold_percent
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.monitoring_alarm_actions
  ok_actions          = local.monitoring_alarm_actions
  dimensions          = { InstanceId = aws_instance.host.id }
  tags                = local.tags
}

resource "aws_cloudwatch_metric_alarm" "custom" {
  for_each            = local.custom_alarms
  alarm_name          = "rogi-collector-prod-${replace(each.key, "_", "-")}"
  alarm_description   = each.value.description
  namespace           = "Rogi/rogi-collector"
  metric_name         = each.value.metric
  statistic           = each.value.statistic
  period              = each.key == "deployment_failed" ? 60 : each.key == "metrics_missing" ? 900 : 600
  evaluation_periods  = each.key == "deployment_failed" ? 1 : 2
  datapoints_to_alarm = each.key == "deployment_failed" ? 1 : 2
  threshold           = each.value.threshold
  comparison_operator = each.value.operator
  treat_missing_data  = each.key == "metrics_missing" ? "breaching" : each.key == "deployment_failed" ? "ignore" : "notBreaching"
  alarm_actions       = local.monitoring_alarm_actions
  ok_actions          = local.monitoring_alarm_actions
  dimensions          = local.monitoring_dimensions
  tags                = local.tags
}

resource "aws_cloudwatch_dashboard" "host" {
  dashboard_name = local.monitoring_dashboard_name
  dashboard_body = jsonencode({
    widgets = [
      { type = "metric", x = 0, y = 0, width = 12, height = 6, properties = { title = "rogi-collector EC2 CPU", region = var.region, period = 300, stat = "Average", metrics = [["AWS/EC2", "CPUUtilization", "InstanceId", aws_instance.host.id]] } },
      { type = "metric", x = 12, y = 0, width = 12, height = 6, properties = { title = "rogi-collector EC2 status checks", region = var.region, period = 300, stat = "Maximum", metrics = [["AWS/EC2", "StatusCheckFailed_System", "InstanceId", aws_instance.host.id], [".", "StatusCheckFailed_Instance", ".", "."]] } },
      { type = "metric", x = 0, y = 6, width = 12, height = 6, properties = { title = "Collector free disk bytes", region = var.region, period = 600, stat = "Minimum", metrics = [["Rogi/rogi-collector", "RootFreeBytes", "Environment", "prod"], [".", "DataFreeBytes", ".", "."]] } },
      { type = "metric", x = 12, y = 6, width = 12, height = 6, properties = { title = "Collector health and backup age", region = var.region, period = 600, stat = "Minimum", metrics = [["Rogi/rogi-collector", "HostHealthy", "Environment", "prod"], [".", "CollectionHealthy", ".", "."], [".", "BackupAgeSeconds", ".", ".", { yAxis = "right", stat = "Maximum" }]] } },
      { type = "alarm", x = 0, y = 12, width = 24, height = 5, properties = { title = "rogi-collector production alarms", alarms = concat([aws_cloudwatch_metric_alarm.status_system.arn, aws_cloudwatch_metric_alarm.status_instance.arn, aws_cloudwatch_metric_alarm.cpu_high.arn], [for alarm in values(aws_cloudwatch_metric_alarm.custom) : alarm.arn]) } }
    ]
  })
}

output "monitoring_dashboard_url" {
  value = "https://${var.region}.console.aws.amazon.com/cloudwatch/home?region=${var.region}#dashboards/dashboard/${aws_cloudwatch_dashboard.host.dashboard_name}"
}

output "monitoring_alarm_names" {
  value = concat([aws_cloudwatch_metric_alarm.status_system.alarm_name, aws_cloudwatch_metric_alarm.status_instance.alarm_name, aws_cloudwatch_metric_alarm.cpu_high.alarm_name], [for alarm in values(aws_cloudwatch_metric_alarm.custom) : alarm.alarm_name])
}

output "monitoring_topic_arn" {
  value = aws_sns_topic.alerts.arn
}
