/**
 * Copyright 2021 Google LLC.
 *
 * This software is provided as-is, without warranty or representation for any use or purpose.
 * Your use of it is subject to your agreement with Google.
 */
/************************* TERADATA TO GOOGLE BIGQUERY END TO END MIGRATION INFRASTRUCTURE*************************/

variable "project_id" {
  description = "Project ID where Cloud Composer Environment is created."
  type        = string
}

variable "customer_name" {
  type        = string
  description = "Name of the customer to append in all service names"
}

variable "composer_env_name" {
  description = "Name of Cloud Composer Environment that will be created"
  type        = string
  default     = "dmt-composer"
}

variable "location" {
  description = "Region where the Cloud Composer Environment is created."
  type        = string
  default     = "us-central1"
}

variable "service_account_gcc" {
  type        = string
  description = "Service Account for Cloud Composer Environment"
  default     = "dmt-sa-gcc"
}

/* List of buckets created for E2E migration - Composer Service Account will be provided Storage Access to these Buckets */
variable "bucket_names" {
  type        = list(string)
  description = "A set of GCS bucket names for which Cloud Composer Service account will be provided access to"
  default = [
    "dmt-teradata-data",
    "dmt-teradata-datamigration-logging"
  ]
}
variable "datamigration_teradata_dag_source_path" {
  type        = string
  description = "Path to datamigration teradata dag source"
}

variable "datamigration_utils" {
  type        = string
  description = "Path to datamigration utilities"
}
