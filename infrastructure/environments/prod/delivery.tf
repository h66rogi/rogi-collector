variable "github_oidc_subject" {
  type        = string
  default     = ""
  description = "Exact main-branch OIDC subject from the repository API, including immutable IDs when enabled."
  validation {
    condition     = var.github_oidc_subject == "" || can(regex("^repo:h66rogi(@[0-9]+)?/rogi-collector(@[0-9]+)?:ref:refs/heads/main$", var.github_oidc_subject))
    error_message = "Only this product repository's exact main-branch subject is allowed."
  }
}
# Only the successful trusted main release workflow can request this product's pull.
variable "github_oidc_provider_arn" {
  type        = string
  default     = ""
  description = "Existing account GitHub Actions OIDC provider; empty disables automated delivery."
  validation {
    condition     = var.github_oidc_provider_arn == "" || can(regex("^arn:aws:iam::[0-9]{12}:oidc-provider/token\\.actions\\.githubusercontent\\.com$", var.github_oidc_provider_arn))
    error_message = "Use the existing GitHub Actions OIDC provider ARN."
  }
}
locals { delivery_enabled = var.github_oidc_provider_arn != "" }
resource "aws_secretsmanager_secret" "registry_pull" {
  count                   = local.delivery_enabled ? 1 : 0
  name_prefix             = "rogi-collector/prod/registry-pull-"
  description             = "Ephemeral read-only GitHub Actions job credential; no long-lived PAT"
  recovery_window_in_days = 30
  tags                    = local.tags
}
resource "aws_iam_role_policy" "registry_pull" {
  count       = local.delivery_enabled ? 1 : 0
  name_prefix = "registry-pull-"
  role        = aws_iam_role.host.name
  policy      = jsonencode({ Version = "2012-10-17", Statement = [{ Effect = "Allow", Action = ["secretsmanager:GetSecretValue"], Resource = [aws_secretsmanager_secret.registry_pull[0].arn] }] })
}
resource "aws_ssm_document" "delivery" {
  count           = local.delivery_enabled ? 1 : 0
  name            = "${local.name}-pull-release"
  document_type   = "Command"
  document_format = "JSON"
  content = jsonencode({
    schemaVersion = "2.2"
    description   = "Activate the verified main release using installed product admission checks"
    mainSteps     = [{ action = "aws:runShellScript", name = "PullVerifiedRelease", inputs = { timeoutSeconds = "900", runCommand = ["set -eu", "systemctl reset-failed rogi-collector-update.service || true", "systemctl start rogi-collector-update.service", "/usr/local/lib/rogi-collector/production-status.py"] } }]
  })
  tags = local.tags
}
resource "aws_iam_role" "github_delivery" {
  lifecycle {
    precondition {
      condition     = var.github_oidc_subject != ""
      error_message = "Read the actual repository OIDC subject before enabling delivery."
    }
  }
  count       = local.delivery_enabled ? 1 : 0
  name_prefix = "${local.name}-github-delivery-"
  assume_role_policy = jsonencode({ Version = "2012-10-17", Statement = [{
    Effect    = "Allow", Action = "sts:AssumeRoleWithWebIdentity", Principal = { Federated = var.github_oidc_provider_arn },
    Condition = { StringEquals = { "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com", "token.actions.githubusercontent.com:sub" = var.github_oidc_subject } }
  }] })
  tags = local.tags
}
resource "aws_iam_role_policy" "github_delivery" {
  count       = local.delivery_enabled ? 1 : 0
  name_prefix = "deliver-"
  role        = aws_iam_role.github_delivery[0].name
  policy = jsonencode({ Version = "2012-10-17", Statement = [
    { Effect = "Allow", Action = ["secretsmanager:PutSecretValue"], Resource = [aws_secretsmanager_secret.registry_pull[0].arn] },
    { Effect = "Allow", Action = ["ssm:SendCommand"], Resource = [aws_ssm_document.delivery[0].arn, "arn:aws:ec2:${var.region}:${var.aws_account_id}:instance/${aws_instance.host.id}"] },
    { Effect = "Allow", Action = ["ssm:GetCommandInvocation"], Resource = ["*"] }
  ] })
}
output "github_delivery_role_arn" { value = try(aws_iam_role.github_delivery[0].arn, null) }
output "registry_pull_secret_arn" { value = try(aws_secretsmanager_secret.registry_pull[0].arn, null) }
output "delivery_document_name" { value = try(aws_ssm_document.delivery[0].name, null) }
