/**
 * Copyright 2023 Google LLC.
 *
 * This software is provided as-is, without warranty or representation for any use or purpose.
 * Your use of it is subject to your agreement with Google.
 */
/************************* TERADATA TO GOOGLE BIGQUERY END TO END MIGRATION INFRASTRUCTURE*************************/
output "dvt_image" {
  value       = "${local.image_prefix}/${local.dvt_image_name}"
  description = "Container image for running Data Validation Tool"
}
output "event_listener_image" {
  value       = "${local.image_prefix}/${local.event_listener_image_name}"
  description = "Container image for running Cloud Run event listener code"
}
