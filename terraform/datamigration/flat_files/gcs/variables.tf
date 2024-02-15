/**
 * Copyright 2021 Google LLC.
 *
 * This software is provided as-is, without warranty or representation for any use or purpose.
 * Your use of it is subject to your agreement with Google.
 */
/************************* FROM GCS TO GOOGLE BIGQUERY END TO END MIGRATION INFRASTRUCTURE*************************/

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
    "dmt-flat_files-data",
    "dmt-temp",
  ]
}
variable "config_bucket" {
  type        = string
  description = "Config GCS Bucket "
  default     = "dmt-config"
}

variable "location" {
  type        = string
  description = "Region to create the GCS Bucket."
  default     = "US"
}

variable "folders" {
  type = map(list(string))
  default = {
    "dmt-flat_files-data" = ["files"]
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
