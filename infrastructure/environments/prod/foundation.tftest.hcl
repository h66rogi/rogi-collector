mock_provider "aws" {
  mock_data "aws_ami" {
    defaults = { id = "ami-0123456789abcdef0", architecture = "x86_64", root_device_type = "ebs", virtualization_type = "hvm", owner_id = "099720109477" }
  }
}
run "collector_host_contract" {
  command = apply
  variables {
    aws_account_id           = "123456789012"
    region                   = "ap-northeast-2"
    ami_id                   = "ami-0123456789abcdef0"
    vpc_id                   = "vpc-00000000000000000"
    public_subnet_id         = "subnet-00000000000000000"
    availability_zone        = "ap-northeast-2a"
    marble_security_group_id = "sg-00000000000000000"
  }
  assert {
    condition     = aws_instance.host.metadata_options[0].http_tokens == "required" && aws_instance.host.metadata_options[0].http_put_response_hop_limit == 1
    error_message = "IMDSv2 hop limit contract changed."
  }
  assert {
    condition     = aws_instance.host.root_block_device[0].encrypted && aws_ebs_volume.data.encrypted
    error_message = "Host volumes must be encrypted."
  }
  assert {
    condition     = length(aws_security_group.host.ingress) == 1 && one(aws_security_group.host.ingress).from_port == 7443 && one(aws_security_group.host.ingress).security_groups == toset([var.marble_security_group_id])
    error_message = "Collector ingress must be private 7443 from marble SG only."
  }
  assert {
    condition     = length(aws_cloudwatch_metric_alarm.status_system.alarm_actions) == 0 && length(aws_cloudwatch_metric_alarm.status_instance.alarm_actions) == 0 && length(aws_cloudwatch_metric_alarm.cpu_high.alarm_actions) == 0 && length(aws_cloudwatch_metric_alarm.cpu_credit_low.alarm_actions) == 0
    error_message = "Baseline alarms must not invent notification destinations."
  }
  assert {
    condition     = aws_cloudwatch_metric_alarm.status_system.dimensions.InstanceId == aws_instance.host.id && aws_cloudwatch_metric_alarm.cpu_high.dimensions.InstanceId == aws_instance.host.id
    error_message = "Alarms must target only this product host."
  }
  assert {
    condition     = alltrue([for ingress in aws_security_group.host.ingress : ingress.from_port != 22])
    error_message = "SSH ingress is forbidden."
  }
}

run "private_delivery_scope" {
  command = apply
  variables {
    aws_account_id           = "123456789012"
    region                   = "ap-northeast-2"
    ami_id                   = "ami-0123456789abcdef0"
    vpc_id                   = "vpc-00000000000000000"
    public_subnet_id         = "subnet-00000000000000000"
    availability_zone        = "ap-northeast-2a"
    github_oidc_subject      = "repo:h66rogi@123/rogi-collector@456:ref:refs/heads/main"
    github_oidc_provider_arn = "arn:aws:iam::123456789012:oidc-provider/token.actions.githubusercontent.com"
    marble_security_group_id = "sg-00000000000000000"
  }
  assert {
    condition     = jsondecode(aws_iam_role.github_delivery[0].assume_role_policy).Statement[0].Condition.StringEquals["token.actions.githubusercontent.com:sub"] == "repo:h66rogi@123/rogi-collector@456:ref:refs/heads/main"
    error_message = "Delivery must trust only this repository's main branch."
  }
  assert {
    condition     = jsondecode(aws_iam_role_policy.github_delivery[0].policy).Statement[0].Resource == [aws_secretsmanager_secret.registry_pull[0].arn]
    error_message = "The workflow may write only its ephemeral registry secret."
  }
  assert {
    condition     = length(jsondecode(aws_iam_role_policy.github_delivery[0].policy).Statement[1].Resource) == 2 && contains(jsondecode(aws_iam_role_policy.github_delivery[0].policy).Statement[1].Resource, aws_ssm_document.delivery[0].arn)
    error_message = "SSM must bind the fixed document and exactly one host."
  }
}
