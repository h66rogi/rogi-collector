variable "account_id" {
  type      = string
  sensitive = true
  validation {
    condition     = can(regex("^[0-9a-f]{32}$", var.account_id))
    error_message = "A Cloudflare account ID is required."
  }
}

variable "zone_id" {
  type      = string
  sensitive = true
  validation {
    condition     = can(regex("^[0-9a-f]{32}$", var.zone_id))
    error_message = "A Cloudflare zone ID is required."
  }
}

resource "cloudflare_r2_bucket" "chat_archive" {
  account_id    = var.account_id
  name          = "rogi-collector-chat-archive"
  location      = "apac"
  storage_class = "Standard"

  lifecycle {
    prevent_destroy = true
  }
}

resource "cloudflare_zero_trust_tunnel_cloudflared" "data_api" {
  account_id = var.account_id
  name       = "rogi-collector-data-api-prod"
  config_src = "cloudflare"

  lifecycle {
    prevent_destroy = true
  }
}

resource "cloudflare_zero_trust_tunnel_cloudflared_config" "data_api" {
  account_id = var.account_id
  tunnel_id  = cloudflare_zero_trust_tunnel_cloudflared.data_api.id
  source     = "cloudflare"
  config = {
    ingress = [
      { hostname = "data-api.rogi.chat", service = "http://data-api:8080" },
      { service = "http_status:404" },
    ]
  }
}

resource "cloudflare_dns_record" "data_api" {
  zone_id = var.zone_id
  name    = "data-api.rogi.chat"
  type    = "CNAME"
  content = "${cloudflare_zero_trust_tunnel_cloudflared.data_api.id}.cfargotunnel.com"
  proxied = true
  ttl     = 1
  comment = "Public read-only collector API through its dedicated Cloudflare Tunnel"

  lifecycle {
    prevent_destroy = true
  }
}
