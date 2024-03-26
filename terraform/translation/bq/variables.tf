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

variable "service_account_bq" {
  type        = string
  description = "Service account for BigQuery Tasks. Mainly used by Dashboards to be built on top of BQ Datasets"
  default     = "dmt-sa-bq"
}

variable "dmt_dataset" {
  type        = string
  description = "BigQuery Dataset ID for DVT Reporting"
  default     = "dmt_dataset"
}

variable "logging_dataset" {
  type        = string
  description = "BigQuery Dataset ID for logging"
  default     = "dmt_logs"
}

variable "location" {
  type        = string
  description = "BigQuery Dataset Location/Region."
  default     = "US"
}

variable "dvt_table" {
  type        = string
  description = "BigQuery Table Name for DVT Reporting"
  default     = "dmt_dvt_results"
}

variable "aggregate_dvt_table" {
  type        = string
  description = "BigQuery Table Name for DVT Reporting"
  default     = "dmt_dvt_aggregated_results"
}

variable "translation_table" {
  type        = string
  description = "BigQuery Table Name for Batch Translation reporting"
  default     = "dmt_translation_results"
}

variable "aggregate_translation_table" {
  type        = string
  description = "BigQuery Table Name for Batch Translation reporting"
  default     = "dmt_translation_aggregated_results"
}

variable "schema_table" {
  type        = string
  description = "BigQuery Table Name for Schema Creation Reporting"
  default     = "dmt_schema_results"
}

variable "aggregate_schema_table" {
  type        = string
  description = "BigQuery Table Name for Aggregated Schema Reporting"
  default     = "dmt_schema_aggregated_results"
}

variable "sql_file_tbl_mapping_table" {
  type        = string
  description = "BigQuery Table Name for sql_file_table_mapping Table"
  default     = "dmt_file_table_mapping"
}


variable "dml_validation_table" {
  type        = string
  description = "BigQuery Table Name for DML Validation Reporting"
  default     = "dmt_dml_validation_results"
}

variable "aggregate_dml_validation_table" {
  type        = string
  description = "BigQuery Table Name for Aggregated DML Validation Reporting"
  default     = "dmt_dml_validation_aggregated_results"
}

variable "extract_ddl_table" {
  type        = string
  description = "BigQuery Table Name for ddl extraction Reporting"
  default     = "dmt_extract_ddl_results"
}

variable "dmt_reporting_table" {
  type        = string
  description = "BigQuery Table Name for DMT DAG reporting"
  default     = "dmt_report_table"
}

variable "hive_ddl_metadata" {
  type        = string
  description = "BigQuery Table Name for HIVE ddl extraction"
  default     = "hive_ddl_metadata"
}


/* List of Big Query roles to be granted to BQ Service Account */
variable "bq_roles" {
  type        = list(string)
  description = "BigQuery Service Account Roles"
  default = [
    "roles/bigquery.connectionUser",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
  ]
}

# Unless this field is set to false, a terraform destroy or terraform apply
# that would delete the BQ tables will fail.
variable "bq_tables_deletion_protection" {
  type        = bool
  description = "Whether or not to allow Terraform to destroy all BQ tables."
  default     = true
}
