variable "aws_account_id" {
  type = string
  validation {

    condition     = can(regex("^[0-9]{12}$", var.aws_account_id))
    error_message = "AWS account ID must be 12 digits."
  }
}

variable "region" {
  type = string
}

variable "ami_id" {
  type = string
  validation {

    condition     = can(regex("^ami-[0-9a-f]+$", var.ami_id))
    error_message = "Supply a real regional AMI ID."
  }
}

variable "instance_type" {
  type    = string
  default = "t8i.medium"
}

variable "vpc_id" {
  type = string
}

variable "public_subnet_id" {
  type = string
}

variable "availability_zone" {
  type = string
}

variable "marble_security_group_id" {
  type = string
}

variable "root_volume_gib" {
  type    = number
  default = 20
}

variable "data_volume_gib" {
  type    = number
  default = 40
}

variable "tags" {
  type    = map(string)
  default = {}
}


variable "ami_owner_id" {
  type    = string
  default = "099720109477"
}
variable "docker_ce_version" {
  type    = string
  default = "5:29.8.1-1~ubuntu.24.04~noble"
}
variable "containerd_version" {
  type    = string
  default = "2.3.5-1~ubuntu.24.04~noble"
}
variable "buildx_version" {
  type    = string
  default = "0.37.1-1~ubuntu.24.04~noble"
}
variable "compose_version" {
  type    = string
  default = "5.5.1-1~ubuntu.24.04~noble"
}

variable "ami_name_pattern" {
  type    = string
  default = "ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"
}

variable "node_version" {
  type    = string
  default = "22.23.2"
}
variable "node_linux_x64_sha256" {
  type    = string
  default = "d60acfe00a2932254bb0ad20e01b0d74397a0875595de719654b214f4b03f307"
}

variable "cpu_high_threshold_percent" {
  type    = number
  default = 85
  validation {
    condition     = var.cpu_high_threshold_percent > 0 && var.cpu_high_threshold_percent <= 100
    error_message = "CPU threshold must be in (0, 100]."
  }
}
variable "cpu_credit_low_threshold" {
  type    = number
  default = 24
  validation {
    condition     = var.cpu_credit_low_threshold >= 0
    error_message = "CPU credit threshold cannot be negative."
  }
}
