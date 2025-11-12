output "turnstile_site_key" {
  description = "Turnstile site key for Flow PHP Playground (public, use in frontend)"
  value       = cloudflare_turnstile_widget.flow_php.id
}

output "turnstile_secret_key" {
  description = "Turnstile secret key for Flow PHP Playground (private, use in backend)"
  value       = cloudflare_turnstile_widget.flow_php.secret
  sensitive   = true
}

output "r2_playground_snippets_bucket_name" {
  description = "Name of the R2 bucket for playground snippets"
  value       = cloudflare_r2_bucket.playground_snippets.name
}

output "r2_playground_snippets_bucket_id" {
  description = "ID of the R2 bucket for playground snippets"
  value       = cloudflare_r2_bucket.playground_snippets.id
}

output "r2_playground_snippets_custom_domain" {
  description = "Custom domain URL for R2 playground snippets bucket"
  value       = "https://playground-snippets.flow-php.com"
}
