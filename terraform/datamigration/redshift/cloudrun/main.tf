/**
 * Copyright 2021 Google LLC.
 *
 * This software is provided as-is, without warranty or representation for any use or purpose.
 * Your use of it is subject to your agreement with Google.
 */

/*********************************************
Cloud Run updates for data migration topics
**********************************************/

/* Retrieve Cloud run API endpoint URL */

data "google_cloud_run_service" "cloudrun_service" {
  name     = "${var.cloudrun_name}-${var.customer_name}"
  location = var.location
}

locals {
  cloudrun_dag_id_mapping = {
    "projects/${var.project_id}/subscriptions/dmt-config-file-topic-${var.customer_name}-sub"               = "controller_dag"
    "projects/${var.project_id}/subscriptions/dmt-teradata-dts-notification-topic-${var.customer_name}-sub" = "controller_dag"
    "projects/${var.project_id}/subscriptions/dmt-redshift-dts-notification-topic-${var.customer_name}-sub" = "controller_dag"
  }
  source_path = "../../src/source"
}

/* Update Cloud Run Environment Variables */

resource "null_resource" "crun_env_vars_updation" {
  triggers = {
    always_run = "${timestamp()}"
  }
  provisioner "local-exec" {
    command = "gcloud run services update ${data.google_cloud_run_service.cloudrun_service.name} --update-env-vars '^@^DAG_ID_MAPPING=${jsonencode(local.cloudrun_dag_id_mapping)}' --region ${data.google_cloud_run_service.cloudrun_service.location} --project ${var.project_id}"
  }
}


/* Make Cloud Run endpoint URL as subscriber to DTS Notification Pubsub topic */

resource "google_pubsub_subscription" "push_subscribe" {
  for_each             = toset(var.topic_names)
  topic                = "${each.value}-${var.customer_name}"
  project              = var.project_id
  name                 = "${each.value}-${var.customer_name}-sub"
  ack_deadline_seconds = 20
  push_config {
    push_endpoint = data.google_cloud_run_service.cloudrun_service.status[0].url
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










