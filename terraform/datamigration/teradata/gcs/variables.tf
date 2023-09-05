/**
 * Copyright 2021 Google LLC.
 *
 * This software is provided as-is, without warranty or representation for any use or purpose.
 * Your use of it is subject to your agreement with Google.
 */
/************************* TERADATA TO GOOGLE BIGQUERY END TO END MIGRATION INFRASTRUCTURE*************************/

variable "project_id" {
  type        = string
  description = "Project to create GCS Bucket."
}

variable "customer_name" {
  type        = string
  description = "Name of the customer to append in all service names"
}

variable "bucket_names" {
  type        = list(string)
  description = "A set of GCS bucket names to be created"
  default = [
    "dmt-teradata-data",
    "dmt-teradata-datamigration-logging"
  ]
}

variable "location" {
  type        = string
  description = "Region to create the GCS Bucket."
  default     = "US"
}

variable "folders" {
  type = map(list(string))
  default = {
    "dmt-teradata-data" = ["files"]
  }
}

variable "storage_class" {
  type        = string
  description = "Storage Class for GCS Bucket"
  default     = "MULTI_REGIONAL"
}

variable "service_account_gcs" {
  type        = string
  description = "Service Account for GCS Buckets"
  default     = "dmt-sa-gcs"
}

variable "service_account_gcc" {
  type        = string
  description = "Service Account for Cloud Composer Environment"
  default     = "dmt-sa-gcc"
}

variable "datamigration_teradata_script" {
  type        = string
  description = "Teradata Data Migration Scripts"
}
/* Configuration Bucket holding the Teradata Agent VM script files */
variable "config_gcs_bucket" {
  type        = string
  description = "Configuration Bucket for dmt"
  default     = "dmt-config"
}
