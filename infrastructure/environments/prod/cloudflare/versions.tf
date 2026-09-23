terraform {
  required_version = "= 1.16.3"
  required_providers {
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "= 5.25.0"
    }
  }
  backend "s3" {}
}

# Supply CLOUDFLARE_API_TOKEN only in the private operator environment.
provider "cloudflare" {}
