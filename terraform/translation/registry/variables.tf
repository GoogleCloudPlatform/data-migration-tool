/**
 * Copyright 2023 Google LLC.
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
variable "artifact_registry_region" {
  description = "Region where the docker image will be stored"
  type        = string
  default     = "us-central1"
}

variable "dvt_source_path" {
  type        = string
  description = "Path to data validation tool source."
}

variable "event_listener_source_path" {
  type        = string
  description = "Path to event listener Cloud Run source."
}

variable "oracle_instant_client_file_path" {
  type        = string
  description = "Oracle instance client rpm library gcs location path"
}

variable "oracle_odbc_version_number" {
  type        = string
  description = "Oracle instance client rpm library version number"
}
