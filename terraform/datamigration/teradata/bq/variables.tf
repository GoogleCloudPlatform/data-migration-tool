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

variable "transfer_run_summary_table" {
  type        = string
  description = "BigQuery Table Name for transfer run"
  default     = "dmt_teradata_transfer_run_summary"
}

variable "transfer_run_jobwise_details_table" {
  type        = string
  description = "BigQuery Table Name for transfer run jobwise"
  default     = "dmt_teradata_transfer_run_jobwise_details"
}

variable "transfer_id_tracking_table" {
  type        = string
  description = "BigQuery Table Name for tracking"
  default     = "dmt_teradata_transfer_tracking"
}

variable "bq_tables_deletion_protection" {
  type        = bool
  description = "Whether or not to allow Terraform to destroy all BQ tables."
  default     = true
}