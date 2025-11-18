locals {
  # Rate limiting for snippet uploads
  # 1 submission = 1 code file + up to 3 dataset files = ~5 requests (rounded up)
  requests_per_submission = 5

  # Rate limits (in submissions)
  # Note: Cloudflare rate limiting only supports periods up to 60 seconds
  # Allowed periods: [10, 15, 20, 30, 40, 45, 60] seconds
  max_submissions_per_minute = 2

  # Rate limits (in requests)
  max_requests_per_minute = local.max_submissions_per_minute * local.requests_per_submission # 25 requests

  # Time period (in seconds)
  # Maximum allowed period is 60 seconds
  period_minute = 60
}

resource "cloudflare_ruleset" "playground_bot_protection" {
  zone_id     = cloudflare_zone.flow_php.id
  name        = "Playground Bot Protection"
  description = "Bot protection for playground route"
  kind        = "zone"
  phase       = "http_request_firewall_custom"

  rules = [
    {
      action      = "block"
      expression  = "(starts_with(http.request.uri.path, \"/playground\") and ip.geoip.country in {\"CN\" \"RU\" \"VN\" \"IN\" \"BR\" \"ID\"})"
      description = "Block playground access from CN, RU, VN, IN, BR, ID"
      enabled     = true
    },
    {
      action      = "managed_challenge"
      expression  = "(starts_with(http.request.uri.path, \"/playground\"))"
      description = "Managed challenge for playground access"
      enabled     = true
    },
    {
      action      = "challenge"
      expression  = "(starts_with(http.request.uri.path, \"/playground\") and cf.threat_score gt 14)"
      description = "Challenge IPs with bad reputation accessing playground"
      enabled     = true
    }
  ]
}


resource "cloudflare_ruleset" "snippet_upload_rate_limit" {
  zone_id     = cloudflare_zone.flow_php.id
  name        = "Rate limit snippet uploads"
  description = "Rate limiting for playground snippet uploads"
  kind        = "zone"
  phase       = "http_ratelimit"

  rules = [
    {
      action = "block"
      action_parameters = {
        response = {
          status_code  = 429
          content      = jsonencode({
            success = false
            error   = "Rate limit exceeded. Please try again later."
          })
          content_type = "application/json"
        }
      }
      expression  = "(http.request.uri.path eq \"/api/playground/snippets\" and http.request.method eq \"POST\")"
      description = "Rate limit snippet uploads (${local.max_submissions_per_minute} submissions per minute = ${local.max_requests_per_minute} requests)"
      enabled     = true
      ratelimit = {
        characteristics = [
          "cf.colo.id",
          "ip.src"
        ]
        period              = local.period_minute
        requests_per_period = local.max_requests_per_minute
        mitigation_timeout  = local.period_minute
      }
    }
  ]
}
