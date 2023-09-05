/**
 * Copyright 2021 Google LLC.
 *
 * This software is provided as-is, without warranty or representation for any use or purpose.
 * Your use of it is subject to your agreement with Google.
 */

variable "project_id" {
  type        = string
  description = "ID of Google Cloud project ID where resources are deployed"
}

variable "customer_name" {
  type        = string
  description = "Name of the customer to append in all service names"
}

variable "logging_dataset" {
  type        = string
  description = "BigQuery Dataset ID for DVT Reporting"
  default     = "dmt_logs"
}

variable "hive_inc_load_pubsub_audit" {
  type        = string
  description = "BigQuery Table Name for tracking"
  default     = "hive_pubsub_audit"
}

variable "hive_inc_load_table_list" {
  type        = string
  description = "BigQuery Table Name for tracking"
  default     = "hive_inc_load_table_list"
}

variable "hive_inc_bq_load" {
  type        = string
  description = "BigQuery Audit Table Name for incremental load"
  default     = "hive_inc_bqload_audit"
}

variable "data_bucket" {
  type        = string
  description = "HIVE Bucket (To Be Provided by the user)"
  default     = "dmt-teradata-data-dmt-test-hive"
}
