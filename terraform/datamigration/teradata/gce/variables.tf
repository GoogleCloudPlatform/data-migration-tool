/**
 * Copyright 2021 Google LLC.
 *
 * This software is provided as-is, without warranty or representation for any use or purpose.
 * Your use of it is subject to your agreement with Google.
 */
/************************* TERADATA TO GOOGLE BIGQUERY END TO END MIGRATION INFRASTRUCTURE*************************/
variable "project_id" {
  type        = string
  description = "Project to create Agent VM"
}
variable "customer_name" {
  type        = string
  description = "Name of the customer to append in all service names"
}
variable "agentvm_sa" {
  type        = string
  description = "Service Account tagged to the Compute Engine Running Teradata Agents"
  default     = "dmt-teradata-agent-vm"
}
/* List of IAM roles to be granted to Agent VM Service Account */
variable "agentsa_roles" {
  type        = list(string)
  description = "Agent VM Service Account Roles"
  default = [
    "roles/bigquery.admin",
    "roles/storage.admin",
    "roles/pubsub.admin",
    "roles/logging.viewer",
    "roles/secretmanager.secretAccessor"
  ]
}
variable "dtsagent_controller_sub_name" {
  description = "Name of pubsub subscription for DTS agent controller"
  type        = string
}
/* Configuration Bucket holding the Teradata Agent VM script files */
variable "config_gcs_bucket" {
  type        = string
  description = "Configuration Bucket for dmt"
  default     = "dmt-config"
}
variable "name" {
  type        = string
  description = "compute engine instance name"
  default     = "dm-vm-teradata-bq"
}
variable "zone" {
  type        = string
  description = "zone for compute engine instance"
  default     = "us-central1-a"
}
variable "machine_type" {
  type        = string
  description = "machine_type for compute engine instance"
  default     = "n2-standard-16"
}
variable "network" {
  type        = string
  description = "network for compute engine instance"
  default     = "default"
}
variable "subnetwork" {
  type        = string
  description = "subnetwork for compute engine instance"
  default     = "default"
}
variable "image" {
  type        = string
  description = "image of compute engine instance"
  default     = "family/ubuntu-minimal-2204-lts"
  # Can pin to a specific version, eg. "ubuntu-minimal-2204-jammy-v20240926"
}
variable "boot_size" {
  type        = number
  description = "boot disk size for compute engine instance"
  default     = 20
}
variable "disk_name" {
  type        = string
  description = "attached disk name for data migration"
  default     = "dm-disk-terdata-bq"
}
variable "disk_type" {
  type        = string
  description = "attached disk type for data migration"
  default     = "pd-ssd"
  # pd-standard, pd-balanced, pd-ssd, pd-extreme
}
variable "disk_size" {
  type        = number
  description = "attached disk size for data migration"
  default     = 1000
}
variable "datamigration_teradata_script" {
  type        = string
  description = "Path to datamigration teradata scripts"
}

variable "create_nat" {
  type        = bool
  default     = true
  description = "Set value to true or false depending on whether you want Terraform to create a Cloud NAT"
}

variable "location" {
  description = "Region where the Cloud NAT is created. Should be the same as Compute Engine VPC region "
  type        = string
  default     = "us-central1"
}

variable "cloud_router" {
  type        = string
  description = "Name of cloud router"
  default     = "dmt-cloud-router"
}

variable "cloud_nat" {
  type        = string
  description = "Name of cloud nat"
  default     = "dmt-cloud-nat"
}
