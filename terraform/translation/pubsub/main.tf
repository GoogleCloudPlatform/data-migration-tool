/**
 * Copyright 2021 Google LLC.
 *
 * This software is provided as-is, without warranty or representation for any use or purpose.
 * Your use of it is subject to your agreement with Google.
 */

/******************************************
 Pub Sub GCS notification
*****************************************/

/* Cloud Pub Sub Service Account creation*/

resource "google_service_account" "service_account" {
  project      = var.project_id
  account_id   = var.service_account_pubsub
  display_name = "Service Account for Pub/Sub Topic"
}

/* Create PubSub Topic for Config File Drops*/

resource "google_pubsub_topic" "config_file_topic" {
  depends_on = [google_service_account.service_account]
  project    = var.project_id
  name       = "${var.config_file_topic_name}-${var.customer_name}"
  labels     = var.labels
}

/* IAM role assignment for unique service account which is used as the identity for various Google Cloud Storage operations */

data "google_storage_project_service_account" "gcs_agent" {
  project = var.project_id
}

resource "google_pubsub_topic_iam_member" "gcs_agent_binding" {
  depends_on = [google_pubsub_topic.config_file_topic, data.google_storage_project_service_account.gcs_agent]
  for_each   = toset(var.topic_names)
  topic      = "${each.value}-${var.customer_name}"
  project    = var.project_id
  role       = "roles/pubsub.publisher"
  member     = data.google_storage_project_service_account.gcs_agent.member
}

/* IAM role assignment for Pub Sub Service Account. Change this if you require more control here */

resource "google_pubsub_topic_iam_member" "invoker" {
  depends_on = [google_pubsub_topic_iam_member.gcs_agent_binding, google_service_account.service_account]
  for_each   = toset(var.topic_names)
  topic      = "${each.value}-${var.customer_name}"
  project    = var.project_id
  role       = "roles/pubsub.editor"
  member     = google_service_account.service_account.member
}


/* Provide Object Admin authorization for Service Account to the created GCS buckets */

resource "google_storage_bucket_iam_member" "storage_object_admin" {
  depends_on = [google_service_account.service_account]
  for_each   = toset(var.bucket_names)
  bucket     = "${each.value}-${var.customer_name}"
  role       = "roles/storage.objectAdmin"
  member     = google_service_account.service_account.member
}


/* Create a Storage Notification for config file */

resource "google_storage_notification" "config_notification" {
  depends_on     = [google_pubsub_topic_iam_member.invoker]
  bucket         = "${var.config_bucket}-${var.customer_name}"
  payload_format = "JSON_API_V1"
  topic          = google_pubsub_topic.config_file_topic.id
  event_types    = ["OBJECT_FINALIZE"]
  custom_attributes = {
    new-attribute = "new-attribute-value"
  }
}
