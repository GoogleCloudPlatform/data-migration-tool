/**
 * Copyright 2023 Google LLC.
 *
 * This software is provided as-is, without warranty or representation for any use or purpose.
 * Your use of it is subject to your agreement with Google.
 */
/************************* TERADATA TO GOOGLE BIGQUERY END TO END MIGRATION INFRASTRUCTURE*************************/
output "dtsagent_controller_topic_name" {
  value       = "${var.dtsagent_controller_topic_name}-${var.customer_name}"
  description = "Name of pubsub topic for DTS agent controller"
}

output "dtsagent_controller_sub_name" {
  value       = "${var.dtsagent_controller_sub_name}-${var.customer_name}-sub"
  description = "Name of pubsub subscription for DTS agent controller topic"
}
