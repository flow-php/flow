resource "cloudflare_ruleset" "playground_bot_protection" {
  zone_id     = cloudflare_zone.flow_php.id
  name        = "Playground Bot Protection"
  description = "Bot protection for playground route"
  kind        = "zone"
  phase       = "http_request_firewall_custom"

  rules = [
    {
      action      = "managed_challenge"
      expression  = "(http.request.uri.path contains \"/playground\")"
      description = "Managed challenge for playground access"
      enabled     = true
    }
  ]
}
