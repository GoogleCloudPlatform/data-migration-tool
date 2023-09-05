/**
 * Copyright 2021 Google LLC.
 *
 * This software is provided as-is, without warranty or representation for any use or purpose.
 * Your use of it is subject to your agreement with Google.
 */
/************************* TERADATA TO GOOGLE BIGQUERY END TO END MIGRATION INFRASTRUCTURE*************************/

/** Sample Terraform Variables File **/

variable "project_id" {
  description = "Project ID where Cloud Composer Environment is created."
  type        = string
}

variable "customer_name" {
  type        = string
  description = "Name of the customer to append in all service names"
}

variable "config_file_topic_name" {
  description = "Name of pubsub topic for DDL input translation files"
  type        = string
  default     = "dmt-config-file-topic"
}

variable "config_bucket" {
  type        = string
  description = "Name of the bucket where DDL/SQL/DML files will be stored"
  default     = "dmt-config"
}

/* List of buckets created for E2E migration - Cloud Functions Service Account will be provided Storage Access to these Buckets */
variable "bucket_names" {
  type        = list(string)
  description = "Config File GCS bucket names for which Cloud Pubsub Service account will be provided access to"
  default = [
    "dmt-translation",
    "dmt-config"
  ]
}

variable "topic_names" {
  type        = list(string)
  description = "List of Topics to be created"
  default = [
    "dmt-config-file-topic"
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
