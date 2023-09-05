/**
 * Copyright 2021 Google LLC.
 *
 * This software is provided as-is, without warranty or representation for any use or purpose.
 * Your use of it is subject to your agreement with Google.
 */
/************************* TERADATA TO GOOGLE BIGQUERY END TO END MIGRATION INFRASTRUCTURE*************************/
/** Sample Terraform Variables File **/
variable "project_id" {
  description = "Project ID where PubSub topics are to be created"
  type        = string
}
variable "customer_name" {
  type        = string
  description = "Name of the customer to append in all service names"
}

variable "dts_notification_topic_name" {
  description = "Name of pubsub topic for DTS notification"
  type        = string
  default     = "dmt-teradata-dts-notification-topic"
}

variable "dtsagent_controller_topic_name" {
  description = "Name of pubsub topic for DTS agent controller"
  type        = string
  default     = "dmt-dtsagent-controller-topic"
}

variable "dtsagent_controller_sub_name" {
  description = "Name of pubsub subscription for DTS agent controller"
  type        = string
  default     = "dmt-dtsagent-controller"
}

variable "topic_names" {
  type        = list(string)
  description = "List of Topics to be created"
  default = [
    "dmt-teradata-dts-notification-topic",
    "dmt-dtsagent-controller-topic"
  ]
}

/* List of buckets created for E2E migration - Cloud Functions Service Account will be provided Storage Access to these Buckets */
variable "bucket_names" {
  type        = list(string)
  description = "A set of GCS bucket names for which Cloud Pubsub Service account will be provided access to"
  default = [
    "dmt-teradata-data",
    "dmt-teradata-datamigration-logging"
  ]
}
variable "service_account_pubsub" {
  type        = string
  description = "Service Account for Cloud pubsub"
  default     = "dmt-sa-pubsub"
}

variable "labels" {
  type        = map(string)
  description = "The resource labels (a map of key/value pairs) to be applied to the Pubsub topic"
  default     = { "purpose" = "dmt-config-trigger-topic" }
}