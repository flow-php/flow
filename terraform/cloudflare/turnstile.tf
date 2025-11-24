resource "cloudflare_turnstile_widget" "flow_php" {
  account_id = cloudflare_account.account.id
  name       = "Flow PHP"
  domains    = ["flow-php.com"]
  mode       = "managed"
  region     = "world"
}
