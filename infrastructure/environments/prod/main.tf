provider "aws" {

  region = var.region

  allowed_account_ids = [var.aws_account_id]
}
data "aws_ami" "selected" {
  owners = [var.ami_owner_id]
  filter {
    name   = "image-id"
    values = [var.ami_id]
  }
  filter {
    name   = "state"
    values = ["available"]
  }
  filter {
    name   = "name"
    values = [var.ami_name_pattern]
  }
}
locals {
  name = "rogi-collector-prod"
  tags = merge(var.tags, {
    Project = "rogi-collector", Environment = "prod", ManagedBy = "terraform"
  })
}
resource "aws_security_group" "host" {

  name_prefix = "${local.name}-"
  description = "Private collector RPC; no public ingress or SSH"
  vpc_id      = var.vpc_id

  ingress {
    description     = "Private mTLS gRPC from rogimarble"
    from_port       = 7443
    to_port         = 7443
    protocol        = "tcp"
    security_groups = [var.marble_security_group_id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.tags, {
    Name = local.name
  })
}
resource "aws_iam_role" "host" {
  name_prefix = "${local.name}-"
  assume_role_policy = jsonencode({ Version = "2012-10-17", Statement = [{ Effect = "Allow", Principal = {
    Service   = "ec2.amazonaws.com"
    }, Action = "sts:AssumeRole"
    }]
  })
  tags = local.tags
}
resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.host.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}
resource "aws_iam_instance_profile" "host" {
  name_prefix = "${local.name}-"
  role        = aws_iam_role.host.name
}
resource "aws_s3_bucket" "backup" {
  bucket_prefix = "rogi-collector-prod-backup-"
  force_destroy = false
  tags          = local.tags
}
resource "aws_s3_bucket_versioning" "backup" {
  bucket = aws_s3_bucket.backup.id
  versioning_configuration {
    status = "Enabled"
  }
}
resource "aws_s3_bucket_server_side_encryption_configuration" "backup" {
  bucket = aws_s3_bucket.backup.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
resource "aws_s3_bucket_public_access_block" "backup" {
  bucket                  = aws_s3_bucket.backup.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
resource "aws_secretsmanager_secret" "runtime" {
  name_prefix             = "rogi-collector/prod/runtime-"
  recovery_window_in_days = 30
  tags                    = local.tags
}
resource "aws_secretsmanager_secret" "backup" {
  name_prefix             = "rogi-collector/prod/backup-"
  recovery_window_in_days = 30
  tags                    = local.tags
}
resource "aws_iam_role_policy" "runtime" {

  name_prefix = "runtime-"
  role        = aws_iam_role.host.id

  policy = jsonencode({ Version = "2012-10-17", Statement = [
    {
      Effect = "Allow", Action = ["secretsmanager:GetSecretValue"], Resource = [aws_secretsmanager_secret.runtime.arn, aws_secretsmanager_secret.backup.arn]
    },
    {
      Effect = "Allow", Action = ["s3:ListBucket"], Resource = [aws_s3_bucket.backup.arn]
    },
    {
      Effect = "Allow", Action = ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload"], Resource = ["${aws_s3_bucket.backup.arn}/*"]
    }
    ]
  })
}
resource "aws_ebs_volume" "data" {

  availability_zone = var.availability_zone
  size              = var.data_volume_gib
  type              = "gp3"
  encrypted         = true

  tags = merge(local.tags, {
    Name = "${local.name}-data"
  })

  lifecycle {
    prevent_destroy = true
  }
}
resource "aws_instance" "host" {

  ami               = data.aws_ami.selected.id
  instance_type     = var.instance_type
  subnet_id         = var.public_subnet_id
  availability_zone = var.availability_zone

  associate_public_ip_address = true
  vpc_security_group_ids      = [aws_security_group.host.id]
  iam_instance_profile        = aws_iam_instance_profile.host.name

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  root_block_device {
    encrypted             = true
    volume_size           = var.root_volume_gib
    volume_type           = "gp3"
    delete_on_termination = true
  }
  user_data = templatefile("${path.module}/../../bootstrap/ubuntu-24.04.sh.tftpl", {
    product               = "rogi-collector"
    docker_ce_version     = var.docker_ce_version
    containerd_version    = var.containerd_version
    buildx_version        = var.buildx_version
    compose_version       = var.compose_version
    node_version          = var.node_version
    node_linux_x64_sha256 = var.node_linux_x64_sha256
  })

  lifecycle {
    precondition {
      condition     = data.aws_ami.selected.architecture == "x86_64" && data.aws_ami.selected.root_device_type == "ebs" && data.aws_ami.selected.virtualization_type == "hvm" && data.aws_ami.selected.owner_id == var.ami_owner_id
      error_message = "AMI must be an available Canonical Ubuntu 24.04 x86_64 HVM/EBS image owned by the configured owner."
    }
  }
  tags = merge(local.tags, {
    Name = local.name
  })
}
resource "aws_volume_attachment" "data" {
  device_name                    = "/dev/sdf"
  volume_id                      = aws_ebs_volume.data.id
  instance_id                    = aws_instance.host.id
  stop_instance_before_detaching = true
}
output "instance_id" {
  value = aws_instance.host.id
}
output "ephemeral_public_ip" {
  value = aws_instance.host.public_ip
}
output "private_ip" {
  value = aws_instance.host.private_ip
}
output "collector_security_group_id" {
  value = aws_security_group.host.id
}
output "data_volume_id" {
  value = aws_ebs_volume.data.id
}
output "runtime_secret_arn" {
  value = aws_secretsmanager_secret.runtime.arn
}
output "backup_bucket" {
  value = aws_s3_bucket.backup.id
}
