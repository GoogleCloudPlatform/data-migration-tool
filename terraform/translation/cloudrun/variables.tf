/**
 * Copyright 2021 Google LLC.
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
variable "cloudrun_name" {
  description = "Name of Cloud Run environment"
  type        = string
  default     = "dmt-translation-run"
}
variable "location" {
  description = "Region where the Run Environment is created. Recommended to Run it in the same region as Cloud Composer"
  type        = string
  default     = "us-central1"
}
variable "topic_names" {
  type        = list(string)
  description = "Pub/Sub topics to set subcriptions for Cloud Run on"
  default = [
    "dmt-config-file-topic"
  ]
}

variable "run_cpu" {
  type        = string
  description = "Cloud Run CPU resources"
  default     = "2.0"
}

variable "run_mem" {
  type        = string
  description = "Cloud Run Memory resources"
  default     = "4096Mi"
}

variable "composer_env_name" {
  description = "Name of Cloud Composer Environment to set Airflow API as subscription in Cloud Run"
  type        = string
  default     = "dmt-composer"
}
variable "service_account_cloudrun" {
  type        = string
  description = "Service Account for Cloud run"
  default     = "dmt-sa-run"
}
variable "service_account_pubsub" {
  type        = string
  description = "Service Account for Cloud Pub Sub"
  default     = "dmt-sa-pubsub"
}
variable "labels" {
  type        = map(string)
  description = "The resource labels (a map of key/value pairs) to be applied to the Cloud Run environment"
  default     = { "purpose" = "dmt-translation-trigger-cloudrun" }
}
variable "ingress_settings" {
  description = "Ingress settings for Cloud Run"
  type        = string
  default     = "internal-and-cloud-load-balancing"
}
/* List of Composer roles to be granted to Cloud Run Service Account */
variable "composer_roles" {
  type        = list(string)
  description = "Cloud Run Service Account Roles"
  default = [
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/composer.user",
  ]
}
variable "dvt_image" {
  type        = string
  description = "Container image for running Data Validation Tool"
}
variable "event_listener_image" {
  type        = string
  description = "Container image for running Cloud Run event listener code."
}
