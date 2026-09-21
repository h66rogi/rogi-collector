locals {
  monitoring_dashboard_name = "rogi-collector-prod-host"
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
  alarm_actions       = []
  ok_actions          = []
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
  alarm_actions       = []
  ok_actions          = []
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
  alarm_actions       = []
  ok_actions          = []
  dimensions          = { InstanceId = aws_instance.host.id }
  tags                = local.tags
}

resource "aws_cloudwatch_metric_alarm" "cpu_credit_low" {
  alarm_name          = "rogi-collector-prod-cpu-credit-low"
  alarm_description   = "Burst CPU credit reserve is low; review sustained load before exhaustion."
  namespace           = "AWS/EC2"
  metric_name         = "CPUCreditBalance"
  statistic           = "Minimum"
  period              = 300
  evaluation_periods  = 3
  datapoints_to_alarm = 3
  threshold           = var.cpu_credit_low_threshold
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "missing"
  alarm_actions       = []
  ok_actions          = []
  dimensions          = { InstanceId = aws_instance.host.id }
  tags                = local.tags
}

resource "aws_cloudwatch_dashboard" "host" {
  dashboard_name = local.monitoring_dashboard_name
  dashboard_body = jsonencode({
    widgets = [
      { type = "metric", x = 0, y = 0, width = 12, height = 6, properties = { title = "rogi-collector EC2 CPU and credits", region = var.region, period = 300, stat = "Average", metrics = [["AWS/EC2", "CPUUtilization", "InstanceId", aws_instance.host.id], [".", "CPUCreditBalance", ".", ".", { stat = "Minimum", yAxis = "right" }]] } },
      { type = "metric", x = 12, y = 0, width = 12, height = 6, properties = { title = "rogi-collector EC2 status checks", region = var.region, period = 300, stat = "Maximum", metrics = [["AWS/EC2", "StatusCheckFailed_System", "InstanceId", aws_instance.host.id], [".", "StatusCheckFailed_Instance", ".", "."]] } },
      { type = "alarm", x = 0, y = 6, width = 24, height = 5, properties = { title = "rogi-collector host alarms (no notification actions)", alarms = [aws_cloudwatch_metric_alarm.status_system.arn, aws_cloudwatch_metric_alarm.status_instance.arn, aws_cloudwatch_metric_alarm.cpu_high.arn, aws_cloudwatch_metric_alarm.cpu_credit_low.arn] } }
    ]
  })
}

output "monitoring_dashboard_url" {
  value = "https://${var.region}.console.aws.amazon.com/cloudwatch/home?region=${var.region}#dashboards/dashboard/${aws_cloudwatch_dashboard.host.dashboard_name}"
}

output "monitoring_alarm_names" {
  value = [aws_cloudwatch_metric_alarm.status_system.alarm_name, aws_cloudwatch_metric_alarm.status_instance.alarm_name, aws_cloudwatch_metric_alarm.cpu_high.alarm_name, aws_cloudwatch_metric_alarm.cpu_credit_low.alarm_name]
}
