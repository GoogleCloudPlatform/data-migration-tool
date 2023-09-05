/**
 * Copyright 2021 Google LLC.
 *
 * This software is provided as-is, without warranty or representation for any use or purpose.
 * Your use of it is subject to your agreement with Google.
 */
/************************* TERADATA TO GOOGLE BIGQUERY END TO END MIGRATION INFRASTRUCTURE*************************/
/** Sample Terraform Variables File **/
variable "project_id" {
  description = "Project ID where Cloud Run Environment is deployed."
  type        = string
}
variable "customer_name" {
  type        = string
  description = "Name of the customer to append in all service names"
}
variable "cloudrun_name" {
  description = "Name of Cloud Run environment"
  type        = string
  default     = "dmt-translation-run"
}
variable "location" {
  description = "Region where Cloud Run is deployed."
  type        = string
  default     = "us-central1"
}
variable "topic_names" {
  type        = list(string)
  description = "Pub/Sub topics to set subcriptions for Cloud Run on"
  default = [
    "dmt-teradata-dts-notification-topic"
  ]
}
variable "service_account_pubsub" {
  type        = string
  description = "Service Account for Cloud Pub Sub"
  default     = "dmt-sa-pubsub"
}