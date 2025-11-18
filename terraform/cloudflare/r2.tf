locals {
  seconds_in_a_day = 86400
}

resource "cloudflare_r2_bucket" "playground_snippets" {
  account_id = cloudflare_account.account.id
  name       = "flow-php-playground-snippets"
  location   = "eeur"
}

resource "cloudflare_r2_bucket_cors" "playground_snippets_cors" {
  account_id  = cloudflare_account.account.id
  bucket_name = cloudflare_r2_bucket.playground_snippets.name

  rules = [{
    allowed = {
      methods = ["GET", "PUT", "POST", "DELETE", "HEAD"]
      origins = ["https://flow-php.com"]
      headers = ["*"]
    }
    id              = "playground-snippets-cors"
    expose_headers  = ["ETag"]
    max_age_seconds = 3600
  }]
}

resource "cloudflare_r2_custom_domain" "playground_snippets" {
  account_id  = cloudflare_account.account.id
  bucket_name = cloudflare_r2_bucket.playground_snippets.name
  domain      = "playground-snippets.flow-php.com"
  zone_id     = cloudflare_zone.flow_php.id
  enabled     = true
}

resource "cloudflare_r2_bucket_lifecycle" "playground_snippets_lifecycle" {
  account_id  = cloudflare_account.account.id
  bucket_name = cloudflare_r2_bucket.playground_snippets.name

  rules = [{
    id      = "expire-snippets-after-90-days"
    enabled = true
    conditions = {
      prefix = ""
    }
    delete_objects_transition = {
      condition = {
        max_age = local.seconds_in_a_day * 90
        type    = "Age"
      }
    }
  }]
}
