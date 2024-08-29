#
#Copyright 2021 Google LLC.
#This software is provided as-is, without warranty or representation for any use or purpose.
#Your use of it is subject to your agreement with Google.

#/************************* TERADATA TO GOOGLE BIGQUERY END TO END MIGRATION INFRASTRUCTURE*************************

/*****************************************
 Cloud Composer V2 Deployment
*****************************************/

data "google_project" "project" {
  project_id = var.project_id
}

locals {

  /* Check if network topology is host-service (shared VPC) */
  network_project_id = var.network_project_id != "" ? var.network_project_id : var.project_id
  subnetwork_region  = var.subnetwork_region != "" ? var.subnetwork_region : var.location
  /* Check if master authorized network has been set */
  master_authorized_networks_config = length(var.master_authorized_networks) == 0 ? [] : [{
    cidr_blocks : var.master_authorized_networks
  }]

  /* Set sizing values for Scheduler, Worker and Web Server based on Composer Sizing Choice - Small, Medium, Large */

  /* Determine Infrastructure attributes of Scheduler based on Environment Size */
  scheduler_cpu        = var.environment_size == "ENVIRONMENT_SIZE_SMALL" ? 0.5 : var.environment_size == "ENVIRONMENT_SIZE_MEDIUM" ? 2 : var.environment_size == "ENVIRONMENT_SIZE_LARGE" ? 4 : var.scheduler.value["cpu"]
  scheduler_memory_gb  = var.environment_size == "ENVIRONMENT_SIZE_SMALL" ? 1.875 : var.environment_size == "ENVIRONMENT_SIZE_MEDIUM" ? 7.5 : var.environment_size == "ENVIRONMENT_SIZE_LARGE" ? 15 : var.scheduler.value["memory_gb"]
  scheduler_storage_gb = var.environment_size == "ENVIRONMENT_SIZE_SMALL" ? 1 : var.environment_size == "ENVIRONMENT_SIZE_MEDIUM" ? 5 : var.environment_size == "ENVIRONMENT_SIZE_LARGE" ? 10 : var.scheduler.value["storage_gb"]
  scheduler_count      = var.environment_size == "ENVIRONMENT_SIZE_SMALL" ? 1 : var.environment_size == "ENVIRONMENT_SIZE_MEDIUM" ? 2 : var.environment_size == "ENVIRONMENT_SIZE_LARGE" ? 3 : var.scheduler.value["count"]

  /* Determine Infrastructure attributes of Web Server based on Environment Size */
  web_server_cpu        = var.environment_size == "ENVIRONMENT_SIZE_SMALL" ? 0.5 : var.environment_size == "ENVIRONMENT_SIZE_MEDIUM" ? 2 : var.environment_size == "ENVIRONMENT_SIZE_LARGE" ? 2 : var.web_server.value["cpu"]
  web_server_memory_gb  = var.environment_size == "ENVIRONMENT_SIZE_SMALL" ? 1.875 : var.environment_size == "ENVIRONMENT_SIZE_MEDIUM" ? 7.5 : var.environment_size == "ENVIRONMENT_SIZE_LARGE" ? 7.5 : var.web_server.value["memory_gb"]
  web_server_storage_gb = var.environment_size == "ENVIRONMENT_SIZE_SMALL" ? 1 : var.environment_size == "ENVIRONMENT_SIZE_MEDIUM" ? 5 : var.environment_size == "ENVIRONMENT_SIZE_LARGE" ? 10 : var.web_server.value["storage_gb"]

  /* Determine Infrastructure attributes of Worker based on Environment Size */
  worker_cpu        = var.environment_size == "ENVIRONMENT_SIZE_SMALL" ? 0.5 : var.environment_size == "ENVIRONMENT_SIZE_MEDIUM" ? 2 : var.environment_size == "ENVIRONMENT_SIZE_LARGE" ? 4 : var.worker.value["cpu"]
  worker_memory_gb  = var.environment_size == "ENVIRONMENT_SIZE_SMALL" ? 1.875 : var.environment_size == "ENVIRONMENT_SIZE_MEDIUM" ? 7.5 : var.environment_size == "ENVIRONMENT_SIZE_LARGE" ? 15 : var.worker.value["memory_gb"]
  worker_storage_gb = var.environment_size == "ENVIRONMENT_SIZE_SMALL" ? 1 : var.environment_size == "ENVIRONMENT_SIZE_MEDIUM" ? 5 : var.environment_size == "ENVIRONMENT_SIZE_LARGE" ? 10 : var.worker.value["storage_gb"]
  worker_min_count  = var.environment_size == "ENVIRONMENT_SIZE_SMALL" ? 1 : var.environment_size == "ENVIRONMENT_SIZE_MEDIUM" ? 2 : var.environment_size == "ENVIRONMENT_SIZE_LARGE" ? 3 : var.worker.value["min_count"]
  worker_max_count  = var.environment_size == "ENVIRONMENT_SIZE_SMALL" ? 3 : var.environment_size == "ENVIRONMENT_SIZE_MEDIUM" ? 6 : var.environment_size == "ENVIRONMENT_SIZE_LARGE" ? 12 : var.worker.value["max_count"]
}

resource "google_project_service_identity" "composer_service_agent_identity" {
  provider = google-beta

  project = data.google_project.project.project_id
  service = "composer.googleapis.com"
}

resource "google_project_iam_member" "composer_service_agent_roles" {
  project = data.google_project.project.project_id
  for_each = toset([
    "roles/composer.serviceAgent",
    "roles/composer.ServiceAgentV2Ext",
  ])
  role   = each.key
  member = google_project_service_identity.composer_service_agent_identity.member
}

/* Cloud Composer Service Account creation that will be attached to the Composer */

resource "google_service_account" "composer_service_account" {
  project      = var.project_id
  account_id   = var.service_account_gcc
  display_name = "Service Account for Cloud Composer"
}

/* Provide Composer Worker IAM role and Composer Admin IAM role */

resource "google_project_iam_member" "composer_worker" {
  depends_on = [google_service_account.composer_service_account]
  project    = var.project_id
  for_each   = toset(var.composer_roles)
  role       = each.value
  member     = google_service_account.composer_service_account.member
}

/* Provide Object Admin authorization for Service Account to the created GCS buckets */

resource "google_storage_bucket_iam_member" "storage_object_admin" {
  depends_on = [google_service_account.composer_service_account]
  for_each   = toset(var.bucket_names)
  bucket     = "${each.value}-${var.customer_name}"
  role       = "roles/storage.objectAdmin"
  member     = google_service_account.composer_service_account.member
}

/* Properties of Composer Environment*/

resource "google_composer_environment" "composer_env" {
  depends_on = [google_storage_bucket_iam_member.storage_object_admin, google_project_iam_member.composer_worker]
  project    = var.project_id
  provider   = google-beta
  name       = "${var.composer_env_name}-${var.customer_name}"
  region     = var.location
  labels     = var.labels
  config {
    node_config {
      /* Picks up VPC network and subnet name based on Choice of Shared VPC project or not */
      network         = "projects/${local.network_project_id}/global/networks/${var.network}"
      subnetwork      = "projects/${local.network_project_id}/regions/${local.subnetwork_region}/subnetworks/${var.subnetwork}"
      service_account = google_service_account.composer_service_account.email
      dynamic "ip_allocation_policy" {
        for_each = (var.pod_ip_allocation_range_name != null || var.service_ip_allocation_range_name != null) ? [1] : []
        content {
          cluster_secondary_range_name  = var.pod_ip_allocation_range_name
          services_secondary_range_name = var.service_ip_allocation_range_name
        }
      }
    }
    software_config {
      airflow_config_overrides = {
        "api-auth_backends"                = "airflow.composer.api.backend.composer_auth",
        "core-dags_are_paused_at_creation" = "True",
        "webserver-dag_orientation"        = "TB",
        "secrets-backend"                  = "airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend",
        "secrets-backend_kwargs"           = jsonencode({ "connections_prefix" : "composer-connections", "variables_prefix" : "secret", "sep" : "-" })
        "core-max_map_length"              = var.max_map_length
      }
      image_version = var.image_version
      pypi_packages = {
        # Add any extra depedencies here
        oracledb           = ""
        redshift-connector = ""
        openpyxl           = ""

      }
      env_variables = {
        "GCP_PROJECT_ID"              = var.project_id
        "REGION"                      = var.location
        "COMPOSER_ENV"                = "${var.composer_env_name}-${var.customer_name}"
        "COMPOSER_SA"                 = var.service_account_gcc
        "TRANSLATION_REPORT_FILENAME" = "batch_translation_report.csv"
        "CUSTOMER_NAME"               = var.customer_name
        "CONFIG_BUCKET_NAME"          = "${var.config_bucket}-${var.customer_name}"
        "DVT_IMAGE"                   = var.dvt_image
        /* Adding this env variable for Data Transfer use later */
        "CONTROLLER_TOPIC" = "${var.dtsagent_controller_topic_name}-${var.customer_name}"
      }
    }

    /* Check if Private IP is enabled for Cloud Composer Networking Type */

    dynamic "private_environment_config" {
      for_each = var.use_private_environment ? [
        {
          enable_private_endpoint                = var.enable_private_endpoint
          cloud_composer_connection_subnetwork   = var.composer_private_service_connect_connectivity == true ? (var.cloud_composer_connection_subnetwork_name == null ? "projects/${local.network_project_id}/regions/${local.subnetwork_region}/subnetworks/${var.subnetwork}" : "projects/${local.network_project_id}/regions/${local.subnetwork_region}/subnetworks/${var.cloud_composer_connection_subnetwork_name}") : null
          master_ipv4_cidr_block                 = var.composer_private_service_connect_connectivity == false ? var.master_ipv4_cidr : null
          cloud_sql_ipv4_cidr_block              = var.composer_private_service_connect_connectivity == false ? var.cloud_sql_ipv4_cidr : null
          web_server_ipv4_cidr_block             = var.composer_private_service_connect_connectivity == false ? var.web_server_ipv4_cidr : null
          cloud_composer_network_ipv4_cidr_block = var.composer_private_service_connect_connectivity == false ? var.cloud_composer_network_ipv4_cidr_block : null
      }] : []
      content {
        enable_private_endpoint                = private_environment_config.value["enable_private_endpoint"]
        cloud_composer_connection_subnetwork   = private_environment_config.value["cloud_composer_connection_subnetwork"]
        master_ipv4_cidr_block                 = private_environment_config.value["master_ipv4_cidr_block"]
        cloud_sql_ipv4_cidr_block              = private_environment_config.value["cloud_sql_ipv4_cidr_block"]
        web_server_ipv4_cidr_block             = private_environment_config.value["web_server_ipv4_cidr_block"]
        cloud_composer_network_ipv4_cidr_block = private_environment_config.value["cloud_composer_network_ipv4_cidr_block"]
      }
    }

    /* Allow only authorized networks to connect to Cluster Endpoints */
    dynamic "master_authorized_networks_config" {
      for_each = local.master_authorized_networks_config
      content {
        enabled = length(var.master_authorized_networks) > 0
        dynamic "cidr_blocks" {
          for_each = master_authorized_networks_config.value["cidr_blocks"]
          content {
            cidr_block   = master_authorized_networks_config.value["cidr_block"]
            display_name = master_authorized_networks_config.value["display_name"]
          }
        }
      }
    }

    dynamic "web_server_network_access_control" {
      for_each = var.web_server_allowed_ip_ranges != null ? [1] : []

      content {
        dynamic "allowed_ip_range" {
          for_each = var.web_server_allowed_ip_ranges

          content {
            value       = allowed_ip_range.value.value
            description = allowed_ip_range.value.description
          }
        }
      }
    }
    /* Composer Environment Resource Configuration for Scheduler, Worker & Web Server */

    workloads_config {

      dynamic "scheduler" {
        for_each = var.scheduler != null ? [var.scheduler] : []
        content {
          cpu        = local.scheduler_cpu
          memory_gb  = local.scheduler_memory_gb
          storage_gb = local.scheduler_storage_gb
          count      = local.scheduler_count
        }
      }

      dynamic "web_server" {
        for_each = var.web_server != null ? [var.web_server] : []
        content {
          cpu        = local.web_server_cpu
          memory_gb  = local.web_server_memory_gb
          storage_gb = local.web_server_storage_gb
        }
      }

      dynamic "worker" {
        for_each = var.worker != null ? [var.worker] : []
        content {
          cpu        = local.worker_cpu
          memory_gb  = local.worker_memory_gb
          storage_gb = local.worker_storage_gb
          min_count  = local.worker_min_count
          max_count  = local.worker_max_count
        }
      }
    }

    environment_size = var.environment_size

  }
}

/* Copy translation DAGs to Composer bucket */

resource "null_resource" "upload_translation_dags" {
  depends_on = [google_composer_environment.composer_env]
  triggers = {
    translation_dag_hashes = jsonencode({
      for fn in fileset(var.translation_dag_source_path, "**") :
      fn => filesha256("${var.translation_dag_source_path}/${fn}")
    })
  }
  provisioner "local-exec" {
    command = "gsutil -o \"GSUtil:parallel_process_count=1\" -m cp -r ${var.translation_dag_source_path}/* ${google_composer_environment.composer_env.config.0.dag_gcs_prefix}/"
  }
}

/* Copy common_utils/ to Composer bucket */

resource "null_resource" "upload_common_utils" {
  depends_on = [google_composer_environment.composer_env]
  triggers = {
    common_util_hashes = jsonencode({
      for fn in fileset(var.common_utils, "**") :
      fn => filesha256("${var.common_utils}/${fn}")
    })
  }
  provisioner "local-exec" {
    command = "gsutil -o \"GSUtil:parallel_process_count=1\" -m cp -r ${var.common_utils} ${google_composer_environment.composer_env.config.0.dag_gcs_prefix}/"
  }
}
