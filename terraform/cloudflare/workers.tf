resource "cloudflare_workers_script" "snippet_upload" {
  account_id  = cloudflare_account.account.id
  script_name = "snippet-upload"
  content     = file("${path.module}/workers/snippet-upload.js")
  main_module = "snippet-upload.js"

  bindings = [
    {
      name = "SNIPPETS_BUCKET"
      type = "r2_bucket"
      bucket_name = cloudflare_r2_bucket.playground_snippets.name
    },
    {
      name = "TURNSTILE_SECRET_KEY"
      type = "secret_text"
      text = cloudflare_turnstile_widget.flow_php.secret
    },
    {
      name = "RATE_LIMITER"
      type = "durable_object_namespace"
      class_name = "SnippetRateLimiter"
    }
  ]

  migrations = {
    new_tag = "v1"
    new_sqlite_classes = ["SnippetRateLimiter"]
  }
}

resource "cloudflare_workers_route" "snippet_upload" {
  zone_id = cloudflare_zone.flow_php.id
  pattern = "flow-php.com/api/playground/snippets*"
  script  = cloudflare_workers_script.snippet_upload.script_name
}
