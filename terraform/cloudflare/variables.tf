variable "account_id" {
  description = "Cloudflare Account ID"
  type        = string
}

variable "zone_id" {
  description = "Cloudflare Zone ID"
  type        = string
}


variable "notification_email" {
  description = "Email address for Cloudflare notifications"
  type        = string
  default     = "norbert@orzechowicz.pl"
}
