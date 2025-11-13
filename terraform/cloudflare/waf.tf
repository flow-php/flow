resource "cloudflare_ruleset" "playground_bot_protection" {
  zone_id     = cloudflare_zone.flow_php.id
  name        = "Playground Bot Protection"
  description = "Bot protection for playground route"
  kind        = "zone"
  phase       = "http_request_firewall_custom"

  rules = [
    {
      action      = "block"
      expression  = "(http.request.uri.path contains \"/playground\" and ip.geoip.country in {\"CN\" \"RU\" \"VN\" \"IN\" \"BR\" \"ID\"})"
      description = "Block playground access from CN, RU, VN, IN, BR, ID"
      enabled     = true
    },
    {
      action      = "managed_challenge"
      expression  = "(http.request.uri.path contains \"/playground\")"
      description = "Managed challenge for playground access"
      enabled     = true
    },
    {
      action      = "challenge"
      expression  = "(http.request.uri.path contains \"/playground\" and cf.threat_score gt 14)"
      description = "Challenge IPs with bad reputation accessing playground"
      enabled     = true
    }
  ]
}
