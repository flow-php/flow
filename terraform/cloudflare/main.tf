resource "cloudflare_account" "account" {
  name = "norbert@orzechowicz.pl"
  type = "standard"
}

import {
  id = var.account_id
  to = cloudflare_account.account
}

resource "cloudflare_zone" "flow_php" {
  account = cloudflare_account.account
  name    = "flow-php.com"

  paused = false
}

import {
  id = var.zone_id
  to = cloudflare_zone.flow_php
}