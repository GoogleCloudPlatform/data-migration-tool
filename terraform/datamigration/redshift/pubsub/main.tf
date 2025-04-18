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

/* IAM role assignment for unique service account which is used as the identity for various Google Cloud Storage operations */

resource "google_pubsub_topic_iam_member" "publisher" {
  depends_on = [google_pubsub_topic.dmt_dts_notification_topic]
  for_each   = toset(var.topic_names)
  topic      = "${each.value}-${var.customer_name}"
  project    = var.project_id
  role       = "roles/pubsub.publisher"
  member     = data.google_storage_project_service_account.gcs_account.member
}


/* IAM role assignment for Pub Sub Service Account. Change this if you require more control here */

resource "google_pubsub_topic_iam_member" "invoker" {
  depends_on = [google_pubsub_topic_iam_member.publisher]
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
