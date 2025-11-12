resource "cloudflare_workers_script" "turnstile_verify" {
  account_id  = cloudflare_account.account.id
  script_name = "turnstile-verify"
  content     = file("${path.module}/workers/turnstile-verify.js")

  bindings = [{
    name = "TURNSTILE_SECRET_KEY"
    type = "secret_text"
    text = cloudflare_turnstile_widget.flow_php.secret
  }]
}

resource "cloudflare_workers_route" "turnstile_verify" {
  zone_id = cloudflare_zone.flow_php.id
  pattern = "flow-php.com/api/verify-turnstile"
  script  = cloudflare_workers_script.turnstile_verify.script_name
}
