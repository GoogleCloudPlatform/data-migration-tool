/**
 * Copyright 2021 Google LLC.
 *
 * This software is provided as-is, without warranty or representation for any use or purpose.
 * Your use of it is subject to your agreement with Google.
 */

/******************************************
 Pub Sub topics notification
*****************************************/

data "google_storage_project_service_account" "gcs_account" {
  project = var.project_id
}

/* Create PubSub Topic for DTS notification */

resource "google_pubsub_topic" "dmt_dts_notification_topic" {
  project = var.project_id
  name    = "${var.dts_notification_topic_name}-${var.customer_name}"
  labels  = var.labels
}

/* Create PubSub Topic for DTS Agent Controller */

resource "google_pubsub_topic" "dmt_dts_controller_topic" {
  project = var.project_id
  name    = "${var.dtsagent_controller_topic_name}-${var.customer_name}"
  labels  = var.labels
}

/* Create PubSub Subscription for DTS Agent Controller */

resource "google_pubsub_subscription" "dmt_dts_controller_subscription" {
  depends_on = [google_pubsub_topic.dmt_dts_controller_topic]
  name       = "${var.dtsagent_controller_sub_name}-${var.customer_name}-sub"
  topic      = google_pubsub_topic.dmt_dts_controller_topic.name
  # 20 minutes
  message_retention_duration = "1200s"
  retain_acked_messages      = true
  ack_deadline_seconds       = 60
  expiration_policy {
    ttl = ""
  }
  retry_policy {
    minimum_backoff = "10s"
  }
  enable_message_ordering      = true
  enable_exactly_once_delivery = true
}

/* IAM role assignment for unique service account which is used as the identity for various Google Cloud Storage operations */

resource "google_pubsub_topic_iam_binding" "binding" {
  depends_on = [google_pubsub_topic.dmt_dts_notification_topic, google_pubsub_topic.dmt_dts_controller_topic]
  for_each   = toset(var.topic_names)
  topic      = "${each.value}-${var.customer_name}"
  project    = var.project_id
  role       = "roles/pubsub.publisher"
  members    = ["serviceAccount:${data.google_storage_project_service_account.gcs_account.email_address}"]
}

/* IAM role assignment for Pub Sub Service Account. Change this if you require more control here */

resource "google_pubsub_topic_iam_member" "invoker" {
  depends_on = [google_pubsub_topic_iam_binding.binding]
  for_each   = toset(var.topic_names)
  topic      = "${each.value}-${var.customer_name}"
  project    = var.project_id
  role       = "roles/pubsub.admin"
  member     = "serviceAccount:${var.service_account_pubsub}@${var.project_id}.iam.gserviceaccount.com"
}

/* Provide Object Admin authorization for Service Account to the created GCS buckets */

resource "google_storage_bucket_iam_member" "storage_object_admin" {
  depends_on = [google_pubsub_topic_iam_member.invoker]
  for_each   = toset(var.bucket_names)
  bucket     = "${each.value}-${var.customer_name}"
  role       = "roles/storage.objectAdmin"
  member     = "serviceAccount:${var.service_account_pubsub}@${var.project_id}.iam.gserviceaccount.com"
}
