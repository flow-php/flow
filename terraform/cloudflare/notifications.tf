locals {
  k  = 1000
  m  = 1000 * local.k
  mb = 1024 * 1024
  gb = 1024 * 1024 * 1024

  r2_storage_free_tier            = 10 * local.gb # 10 GB
  r2_class_a_operations_free_tier = 1 * local.m   # 1 million requests
  r2_class_b_operations_free_tier = 10 * local.m  # 10 million requests

  thresholds = [10, 20, 30, 40, 50, 60, 70, 80, 90, 95]

  notification_configs = merge(
    {
      for threshold in local.thresholds :
      "r2_storage_${threshold}" => {
        name        = "R2 Storage - ${threshold}% of Free Tier"
        product     = "r2_storage"
        limit       = local.r2_storage_free_tier * (threshold / 100)
        description = "Alert when R2 storage usage reaches ${threshold}% of free tier (${threshold / 10} GB)"
      }
    },
    {
      for threshold in local.thresholds :
      "r2_class_a_${threshold}" => {
        name        = "R2 Class A Operations - ${threshold}% of Free Tier"
        product     = "r2_class_a_operations"
        limit       = local.r2_class_a_operations_free_tier * (threshold / 100)
        description = "Alert when R2 Class A operations reach ${threshold}% of free tier (${threshold * 10000} requests)"
      }
    },
    {
      for threshold in local.thresholds :
      "r2_class_b_${threshold}" => {
        name        = "R2 Class B Operations - ${threshold}% of Free Tier"
        product     = "r2_class_b_operations"
        limit       = local.r2_class_b_operations_free_tier * (threshold / 100)
        description = "Alert when R2 Class B operations reach ${threshold}% of free tier (${threshold * 100000} requests)"
      }
    }
  )
}

resource "cloudflare_notification_policy" "r2_notifications" {
  for_each = local.notification_configs

  account_id = cloudflare_account.account.id
  name       = each.value.name
  enabled    = true
  alert_type = "billing_usage_alert"

  filters = {
    limit   = [each.value.limit]
    product = [each.value.product]
  }

  mechanisms = {
    email = [{
      id = var.notification_email
    }]
  }
}
