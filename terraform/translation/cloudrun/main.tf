/**
 * Copyright 2021 Google LLC.
 *
 * This software is provided as-is, without warranty or representation for any use or purpose.
 * Your use of it is subject to your agreement with Google.
 */

/*********************************************
 Eventarc setup with Cloud Run as destination
**********************************************/

/* Retrieve Composer API endpoint URL */

data "google_composer_environment" "composer_env" {
  name    = "${var.composer_env_name}-${var.customer_name}"
  project = var.project_id
  region  = var.location
}

locals {
  cloudrun_dag_id_mapping = {
    "projects/${var.project_id}/subscriptions/dmt-config-file-topic-${var.customer_name}-sub" = "controller_dag"
  }
}

/* Cloud Run Service Account creation */

resource "google_service_account" "service_account" {
  project      = var.project_id
  account_id   = var.service_account_cloudrun
  display_name = "Service Account for Cloud Run"
}

/* Cloud Run creation */

resource "google_cloud_run_service" "dmt_run" {
  depends_on                 = [google_service_account.service_account]
  name                       = "${var.cloudrun_name}-${var.customer_name}"
  project                    = var.project_id
  location                   = var.location
  autogenerate_revision_name = true
  metadata {
    annotations = {
      namespace                    = var.project_id
      "run.googleapis.com/ingress" = var.ingress_settings
    }
  }
  template {
    spec {
      container_concurrency = 50
      timeout_seconds       = 100
      service_account_name  = "${var.service_account_cloudrun}@${var.project_id}.iam.gserviceaccount.com"
      containers {
        image = var.event_listener_image

        env {
          name  = "COMPOSER_ENV_URL"
          value = data.google_composer_environment.composer_env.config.0.airflow_uri
        }
        env {
          name  = "DAG_ID_MAPPING"
          value = jsonencode(local.cloudrun_dag_id_mapping)
        }

        ports {
          container_port = 8080
        }
      }
    }
  }
  traffic {
    percent         = 100
    latest_revision = true
  }
}

/* DVT Cloud Run Creation */

resource "google_cloud_run_service" "dvt_run" {
  depends_on                 = [google_project_iam_member.secret-accesor]
  name                       = "edw-dvt-tool-${var.customer_name}"
  project                    = var.project_id
  location                   = var.location
  autogenerate_revision_name = true
  metadata {
    annotations = {
      namespace                    = var.project_id
      "run.googleapis.com/ingress" = var.ingress_settings
    }
  }
  template {
    spec {
      container_concurrency = 50
      timeout_seconds       = 100
      service_account_name  = "${var.service_account_cloudrun}@${var.project_id}.iam.gserviceaccount.com"
      containers {
        image = var.dvt_image

        resources {
          limits = {
            cpu    = var.run_cpu
            memory = var.run_mem
          }
        }
        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }

        env {
          name  = "COMPOSER_ENV"
          value = var.composer_env_name
        }

        env {
          name  = "COMPOSER_GCS_BUCKET"
          value = split("/", data.google_composer_environment.composer_env.config.0.dag_gcs_prefix)[2]
        }

        ports {
          container_port = 8080
        }
      }
    }
  }
  traffic {
    percent         = 100
    latest_revision = true
  }
}

/* Make Cloud Run service accessible by Pub Sub Service Account with Invoker role*/

resource "google_cloud_run_service_iam_member" "run_invoker" {
  depends_on = [google_cloud_run_service.dmt_run]
  project    = var.project_id
  service    = google_cloud_run_service.dmt_run.name
  location   = google_cloud_run_service.dmt_run.location
  role       = "roles/run.invoker"
  member     = "serviceAccount:${var.service_account_pubsub}@${var.project_id}.iam.gserviceaccount.com"
}

/* Provide Composer Worker IAM roles to Cloud Run Service Account */

resource "google_project_iam_member" "composer-worker" {
  depends_on = [google_service_account.service_account]
  project    = var.project_id
  for_each   = toset(var.composer_roles)
  role       = each.value
  member     = "serviceAccount:${var.service_account_cloudrun}@${var.project_id}.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "secret-accesor" {
  depends_on = [google_service_account.service_account]
  project    = var.project_id
  role       = "roles/secretmanager.secretAccessor"
  member     = "serviceAccount:${var.service_account_cloudrun}@${var.project_id}.iam.gserviceaccount.com"
}

/* Make Cloud Run endpoint URL as subscriber to translation Pubsub topic */

resource "google_pubsub_subscription" "push_subscribe" {
  for_each             = toset(var.topic_names)
  topic                = "${each.value}-${var.customer_name}"
  project              = var.project_id
  name                 = "${each.value}-${var.customer_name}-sub"
  ack_deadline_seconds = 20
  push_config {
    push_endpoint = google_cloud_run_service.dmt_run.status[0].url
    oidc_token {
      service_account_email = "${var.service_account_pubsub}@${var.project_id}.iam.gserviceaccount.com"
    }
    attributes = {
      x-goog-version = "v1"
    }
  }
  expiration_policy {
    ttl = ""
  }
  enable_message_ordering = true
}
