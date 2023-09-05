/**
 * Copyright 2021 Google LLC.
 *
 * This software is provided as-is, without warranty or representation for any use or purpose.
 * Your use of it is subject to your agreement with Google.
 */
/************************* TERADATA TO GOOGLE BIGQUERY END TO END INFRASTRUCTURE*************************/

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
    "dmt-translation",
    "dmt-config"
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
    "dmt-translation" = ["input/ddl", "input/sql", "input/dml", "output/ddl", "output/sql", "output/dml", ],
    "dmt-config"      = ["ddl", "sql", "dml", "data", "scripts/translation/hive", "scripts/translation/teradata", "scripts/translation/oracle", "scripts/translation/redshift", "scripts/datamigration/hive", "scripts/datamigration/teradata", "software/teradata", "software/hive", "software/oracle", "software/redshift", "validation/teradata", "validation/hive", "validation/oracle", "validation/redshift"]
  }
}

variable "config_bucket" {
  type        = string
  description = "Config GCS Bucket "
  default     = "dmt-config"
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

variable "translation_hive_script" {
  type        = string
  description = "Hive Translation Scripts"
}

variable "translation_teradata_script" {
  type        = string
  description = "Teradata Translation Scripts"
}
