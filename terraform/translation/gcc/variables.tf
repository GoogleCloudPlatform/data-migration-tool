/**
 * Copyright 2021 Google LLC.
 *
 * This software is provided as-is, without warranty or representation for any use or purpose.
 * Your use of it is subject to your agreement with Google.
 */
/************************* DMT END-TO-END MIGRATION INFRASTRUCTURE*************************/
variable "project_id" {
  description = "Project ID where Cloud Composer Environment is created."
  type        = string
}
variable "customer_name" {
  type        = string
  description = "Name of the customer to append in all service names"
}

/****** The image version controls whether Composer 2 or 3 is deployed. ******/
/* Fill in the Composer 2 or 3 variables in the sections below as required. */
variable "image_version" {
  type        = string
  description = "The version of the Airflow running in the cloud composer environment."
  default     = "composer-3-airflow-2.9.3"
}
/*****************************************************************************/

/****** Composer 2 and 3 common configuration, applicable regardless of which version you intend to use. ******/
/*****************************************************************************/
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
/* List of Composer roles to be granted to Cloud Composer Service Account */
variable "composer_roles" {
  type        = list(string)
  description = "Composer Service Account Roles"
  default = [
    "roles/bigquery.admin",
    "roles/bigquerymigration.editor",
    "roles/composer.ServiceAgentV2Ext",
    "roles/composer.worker",
    "roles/dataproc.editor",
    "roles/iam.serviceAccountUser",
    "roles/pubsub.admin",
    "roles/run.developer",
    "roles/secretmanager.secretAccessor",
  ]
}
variable "labels" {
  type        = map(string)
  description = "The resource labels (a map of key/value pairs) to be applied to the Cloud Composer."
  default     = { "purpose" = "dmt-migration-composer" }
}
/* The variables network and subnetwork assume a non-shared VPC network topology*/
variable "network" {
  type        = string
  description = "The VPC network to host the composer cluster."
  default     = "default"
}
variable "subnetwork" {
  type        = string
  description = "The subnetwork to host the composer cluster."
  default     = "default"
}

/* The variables network_project_id and subnetwork_region assume a shared VPC network topology where network details of host project are required*/
variable "network_project_id" {
  type        = string
  description = "The project ID of the shared VPC's host (for shared vpc support)"
  default     = ""
}
variable "subnetwork_region" {
  type        = string
  description = "The subnetwork region of the shared VPC's host (for shared vpc support)"
  default     = ""
}

variable "use_private_environment" {
  description = "Enable private environment."
  type        = bool
  default     = true
}

variable "environment_size" {
  type        = string
  description = "The environment size controls the performance parameters of the managed Cloud Composer infrastructure that includes the Airflow database. Values for environment size are: ENVIRONMENT_SIZE_SMALL, ENVIRONMENT_SIZE_MEDIUM, and ENVIRONMENT_SIZE_LARGE."
  default     = "ENVIRONMENT_SIZE_MEDIUM"
  validation {
    condition     = can(regex("^(ENVIRONMENT_SIZE_SMALL|ENVIRONMENT_SIZE_MEDIUM|ENVIRONMENT_SIZE_LARGE)$", var.environment_size))
    error_message = "Composer environment_size must be one of ENVIRONMENT_SIZE_SMALL, ENVIRONMENT_SIZE_MEDIUM, ENVIRONMENT_SIZE_LARGE."
  }
}
variable "use_custom_resource_sizing" {
  type        = bool
  description = "If true, uses cpu/mem/storage values provided for the scheduler/triggerer/web_server/worker/dag_processor. If false, uses predefined sizing based on environment_size."
  default     = false
}
variable "scheduler" {
  type = object({
    cpu        = number
    memory_gb  = number
    storage_gb = number
    count      = number
  })
  default     = null
  description = "Configuration for resources used by Airflow schedulers."
}
variable "triggerer" {
  type = object({
    cpu       = number
    memory_gb = number
    count     = number
  })
  default     = null
  description = "Configuration for resources used by the Airflow triggerer."
}
variable "web_server" {
  type = object({
    cpu        = number
    memory_gb  = number
    storage_gb = number
  })
  default     = null
  description = "Configuration for resources used by Airflow web server."
}
variable "worker" {
  type = object({
    cpu        = number
    memory_gb  = number
    storage_gb = number
    min_count  = number
    max_count  = number
  })
  default     = null
  description = "Configuration for resources used by Airflow workers."
}
/*****************************************************************************/


/****** Composer 3 configuration ******/
/*****************************************************************************/
/* Only used if Composer version is composer-3. */

/* TODO: Add Composer3-specific flags. */
variable "dag_processor" {
  type = object({
    cpu        = number
    memory_gb  = number
    storage_gb = number
    count      = number
  })
  default     = null
  description = "Configuration for resources used by Airflow DAG processors (Composer 3 only)."
}
/*****************************************************************************/


/******* Composer 2 configuration *******/
/*****************************************************************************/
/* Only used if Composer version is composer-2. */
variable "pod_ip_allocation_range_name" {
  description = "The name of the cluster's secondary range used to allocate IP addresses to pods."
  type        = string
  default     = null
}
variable "service_ip_allocation_range_name" {
  type        = string
  description = "The name of the services' secondary range used to allocate IP addresses to the cluster."
  default     = null
}

/* Enabling private endpoint will be left false by default with the option to provide authorized master network to access Composer endpoints from only */
variable "enable_private_endpoint" {
  description = "Configure public access to the cluster endpoint through authorized network."
  type        = bool
  default     = true
}
/* List if CIDR blocks needed for enabling Private IP Composer environment. If a specific CIDR Block is not provided, default values for CIDR blocks are assumed */
variable "composer_private_service_connect_connectivity" {
  description = "Use composer private service connect connectivity for Private IP environment (if it is set to false means VPC Peerings would be required)"
  type        = bool
  default     = true
}
/* Cloud Composer Network and Subnetwork is needed when Composer Connectivity type is Private Service Connect (default) */
/* If specified as null default subnetwork used which is specified in create composer environment */
variable "cloud_composer_connection_subnetwork_name" {
  description = "VPC subnetwork ID for Composer connection to managed resources."
  type        = string
  default     = null
}
variable "master_ipv4_cidr" {
  description = "The CIDR block from which IP range in tenant project will be reserved for the master."
  type        = string
  default     = null
}
variable "cloud_sql_ipv4_cidr" {
  description = "The CIDR block from which IP range in tenant project will be reserved for Cloud SQL."
  type        = string
  default     = null
}
variable "web_server_ipv4_cidr" {
  description = "The CIDR block from which IP range in tenant project will be reserved for the web server"
  type        = string
  default     = null
}
/* Cloud Composer Network and Cloud SQL IPv4 CIDR block is needed whe Composer Connectivity type is VPC PEERING */
variable "cloud_composer_network_ipv4_cidr_block" {
  description = "The CIDR block from which IP range in tenant project will be reserved."
  type        = string
  default     = null
}


variable "master_authorized_networks" {
  type = list(object({
    cidr_block   = string
    display_name = string
  }))
  default     = []
  description = "List of master authorized networks. If none are provided, disallow external access (except the cluster node IPs, which GKE automatically allows)."
}

variable "web_server_allowed_ip_ranges" {
  type = list(object({
    value       = string,
    description = string
  }))
  default     = null
  description = "The network-level access control policy for the Airflow web server. If unspecified, no network-level access restrictions will be applied."
}
/*****************************************************************************/



/* List of buckets created for E2E migration - Composer Service Account will be provided Storage Access to these Buckets */
variable "bucket_names" {
  type        = list(string)
  description = "A set of GCS bucket names for which Cloud Composer Service account will be provided access to"
  default = [
    "dmt-translation",
  ]
}
variable "translation_dag_source_path" {
  type        = string
  description = "Path to dag source"
}
variable "common_utils" {
  type        = string
  description = "Path to common utilities"
}

variable "config_bucket" {
  type        = string
  description = "Config GCS Bucket "
  default     = "dmt-config"
}
variable "dvt_image" {
  type        = string
  description = "Container image for running Data Validation Tool"
}
variable "dtsagent_controller_topic_name" {
  description = "Name of pubsub topic for DTS agent controller"
  type        = string
  default     = "dmt-dtsagent-controller-topic"
}
variable "max_map_length" {
  type        = number
  description = "Maximum number of tasks that Dynamic Task mapping expands can create"
  default     = 10240
}

