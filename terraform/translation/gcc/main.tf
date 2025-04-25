# Copyright 2021 Google LLC.
# This software is provided as-is, without warranty or representation for any use or purpose.
# Your use of it is subject to your agreement with Google.

#/************************* DMT END-TO-END MIGRATION INFRASTRUCTURE*************************

/*****************************************
 Cloud Composer Deployment
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

  /* Set sizing for Airflow components based on user values, or other environment sizing presets - Small, Medium, Large. */
  scheduler_presets = {
    "ENVIRONMENT_SIZE_SMALL" = {
      "cpu"        = 0.5
      "memory_gb"  = 2,
      "storage_gb" = 1,
      "count"      = 1,
    },
    "ENVIRONMENT_SIZE_MEDIUM" = {
      "cpu"        = startswith(var.image_version, "composer-3") ? 1 : 2,
      "memory_gb"  = startswith(var.image_version, "composer-3") ? 4 : 7.5,
      "storage_gb" = 5,
      "count"      = 2,
    },
    "ENVIRONMENT_SIZE_LARGE" = {
      "cpu"        = startswith(var.image_version, "composer-3") ? 1 : 4,
      "memory_gb"  = startswith(var.image_version, "composer-3") ? 4 : 15,
      "storage_gb" = 10,
      "count"      = 2,
  } }
  scheduler_config = var.scheduler != null ? var.scheduler : local.scheduler_presets[var.environment_size]

  triggerer_presets = {
    "ENVIRONMENT_SIZE_SMALL" = {
      "cpu"       = 0.5,
      "memory_gb" = 1,
      "count"     = 1,
    },
    "ENVIRONMENT_SIZE_MEDIUM" = {
      "cpu"       = 0.5,
      "memory_gb" = 1,
      "count"     = 1,
    },
    "ENVIRONMENT_SIZE_LARGE" = {
      "cpu"       = 0.5,
      "memory_gb" = 1,
      "count"     = 1,
  } }
  triggerer_config = var.triggerer != null ? var.triggerer : local.triggerer_presets[var.environment_size]

  web_server_presets = {
    "ENVIRONMENT_SIZE_SMALL" = {
      "cpu"        = 1,
      "memory_gb"  = 2,
      "storage_gb" = 1,
    },
    "ENVIRONMENT_SIZE_MEDIUM" = {
      "cpu"        = 2,
      "memory_gb"  = 7.5,
      "storage_gb" = 5,
    },
    "ENVIRONMENT_SIZE_LARGE" = {
      "cpu"        = 2,
      "memory_gb"  = 7.5,
      "storage_gb" = 10,
  } }
  web_server_config = var.web_server != null ? var.web_server : local.web_server_presets[var.environment_size]

  worker_presets = {
    "ENVIRONMENT_SIZE_SMALL" = {
      "cpu"        = 0.5,
      "memory_gb"  = 2,
      "storage_gb" = startswith(var.image_version, "composer-3") ? 10 : 1,
      "min_count"  = 1,
      "max_count"  = 3,
    },
    "ENVIRONMENT_SIZE_MEDIUM" = {
      "cpu"        = 2,
      "memory_gb"  = 7.5,
      "storage_gb" = startswith(var.image_version, "composer-3") ? 20 : 5,
      "min_count"  = 2,
      "max_count"  = 6,
    },
    "ENVIRONMENT_SIZE_LARGE" = {
      "cpu"        = 4,
      "memory_gb"  = 15,
      "storage_gb" = startswith(var.image_version, "composer-3") ? 50 : 10,
      "min_count"  = 3,
      "max_count"  = 12,
  } }
  worker_config = var.worker != null ? var.worker : local.worker_presets[var.environment_size]

  dag_processor_presets = {
    "ENVIRONMENT_SIZE_SMALL" = {
      "cpu"        = 1,
      "memory_gb"  = 4,
      "storage_gb" = 1,
      "count"      = 1,
    },
    "ENVIRONMENT_SIZE_MEDIUM" = {
      "cpu"        = 2,
      "memory_gb"  = 7.5,
      "storage_gb" = 5,
      "count"      = 1,
    },
    "ENVIRONMENT_SIZE_LARGE" = {
      "cpu"        = 4,
      "memory_gb"  = 15,
      "storage_gb" = 10,
      "count"      = 2,
  } }
  dag_processor_config = var.dag_processor != null ? var.dag_processor : local.dag_processor_presets[var.environment_size]

}

resource "google_project_service_identity" "composer_service_agent_identity" {
  provider = google-beta

  project = data.google_project.project.project_id
  service = "composer.googleapis.com"
}

resource "google_project_iam_member" "composer_service_agent_roles" {
  depends_on = [google_project_service_identity.composer_service_agent_identity]
  project    = data.google_project.project.project_id
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
  depends_on = [google_project_iam_member.composer_service_agent_roles, google_storage_bucket_iam_member.storage_object_admin, google_project_iam_member.composer_worker]
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
        for_each = (startswith(var.image_version, "composer-2") && (var.pod_ip_allocation_range_name != null || var.service_ip_allocation_range_name != null)) ? [1] : []
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
    enable_private_environment = var.use_private_environment && startswith(var.image_version, "composer-3") ? true : null
    dynamic "private_environment_config" {
      for_each = var.use_private_environment && startswith(var.image_version, "composer-2") ? [
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
      for_each = startswith(var.image_version, "composer-2") ? local.master_authorized_networks_config : []
      content {
        enabled = length(var.master_authorized_networks) > 0
        dynamic "cidr_blocks" {
          for_each = master_authorized_networks_config.value["cidr_blocks"]
          content {
            cidr_block   = cidr_blocks.value.cidr_block
            display_name = cidr_blocks.value.display_name
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

    /* Composer Environment Resource Configuration */
    workloads_config {
      scheduler {
        cpu        = local.scheduler_config.cpu
        memory_gb  = local.scheduler_config.memory_gb
        storage_gb = local.scheduler_config.storage_gb
        count      = local.scheduler_config.count
      }
      triggerer {
        cpu       = local.triggerer_config.cpu
        memory_gb = local.triggerer_config.memory_gb
        count     = local.triggerer_config.count
      }
      web_server {
        cpu        = local.web_server_config.cpu
        memory_gb  = local.web_server_config.memory_gb
        storage_gb = local.web_server_config.storage_gb
      }
      worker {
        cpu        = local.worker_config.cpu
        memory_gb  = local.worker_config.memory_gb
        storage_gb = local.worker_config.storage_gb
        min_count  = local.worker_config.min_count
        max_count  = local.worker_config.max_count
      }
      dynamic "dag_processor" {
        for_each = startswith(var.image_version, "composer-3") ? [1] : []
        content {
          cpu        = local.dag_processor_config.cpu
          memory_gb  = local.dag_processor_config.memory_gb
          storage_gb = local.dag_processor_config.storage_gb
          count      = local.dag_processor_config.count
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


/* Composer 2: Set up firewall rule to allow Composer GKE cluster pods (KubernetesPodOperator) to reach the rest of the VPC network. */
data "google_container_cluster" "composer_gke_cluster" {
  count    = startswith(var.image_version, "composer-2") ? 1 : 0
  name     = split("/", google_composer_environment.composer_env.config.0.gke_cluster)[5]
  location = split("/", google_composer_environment.composer_env.config.0.gke_cluster)[3]
}

resource "google_compute_firewall" "dmt-pod-operator" {
  count       = startswith(var.image_version, "composer-2") ? 1 : 0
  name        = "dmt-allow-composer-gke-pods"
  network     = google_composer_environment.composer_env.config.0.node_config.0.network
  description = "Allows Composer cluster GKE pods to reach the default VPC range."

  priority      = 1001
  direction     = "INGRESS"
  source_ranges = [data.google_container_cluster.composer_gke_cluster[0].ip_allocation_policy.0.cluster_ipv4_cidr_block]
  allow {
    protocol = "all"
  }
}
