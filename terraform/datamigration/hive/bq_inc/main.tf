/**
 * Copyright 2021 Google LLC.
 *
 * This software is provided as-is, without warranty or representation for any use or purpose.
 * Your use of it is subject to your agreement with Google.
 */

/******************************************
 BigQuery Table Creation
*****************************************/

data "google_project" "project" {
  project_id = var.project_id
}

locals {
  pubsub_service_agent_sa = format("service-%s@gcp-sa-pubsub.iam.gserviceaccount.com", data.google_project.project.number)
}

/* Create BQ load audit table for Hive with JSON schema */
resource "google_bigquery_table" "hive_inc_load_pubsub_audit" {
  project    = var.project_id
  dataset_id = var.logging_dataset
  table_id   = var.hive_inc_load_pubsub_audit
  schema     = file("bq_schemas/hive_inc_load_pubsub_audit.json")
  clustering = ["publish_time"]

}

resource "google_bigquery_table" "hive_inc_bq_load" {
  project    = var.project_id
  dataset_id = var.logging_dataset
  table_id   = var.hive_inc_bq_load
  schema     = file("bq_schemas/hive_inc_bqload_audit.json")
  clustering = ["load_dtm"]
}

/* Create BQ load audit table for BQ inc load with JSON schema */
resource "google_bigquery_table" "hive_inc_load_table_list" {
  project    = var.project_id
  dataset_id = var.logging_dataset
  table_id   = var.hive_inc_load_table_list
  schema     = file("bq_schemas/hive_inc_load_table_list.json")
  clustering = ["job_run_time"]

}


resource "google_pubsub_topic" "hive_incremental_watcher" {
  name    = "hive_incremental_watcher"
  project = var.project_id
}

resource "google_pubsub_subscription" "hive_incremental_sub" {
  depends_on           = [google_project_iam_binding.bq_roles_pubsub_service_agent]
  name                 = "hive_incremental_sub"
  topic                = google_pubsub_topic.hive_incremental_watcher.name
  project              = var.project_id
  ack_deadline_seconds = 600

  bigquery_config {
    table          = "${var.project_id}:${var.logging_dataset}.${var.hive_inc_load_pubsub_audit}"
    write_metadata = true
  }
  expiration_policy {
    ttl = ""
  }

}

resource "google_project_iam_binding" "bq_roles_pubsub_service_agent" {
  project = var.project_id
  for_each = toset([
    "roles/bigquery.dataEditor",
    "roles/bigquery.metadataViewer",
  ])
  role = each.value
  members = [
    format("serviceAccount:%s", local.pubsub_service_agent_sa)
  ]
}


# resource "google_storage_notification" "hive_inc_notif" {
#   topic = google_pubsub_topic.hive_incremental_watcher.name
#   payload_format = "JSON_API_V1"
#   bucket = var.data_bucket
# }

/* Below command to be used to create bucket notification for incremental data *\
/*gcloud storage buckets notifications create gs://dmt-teradata-data-<project-id> --topic=hive_incremental_watcher*/
