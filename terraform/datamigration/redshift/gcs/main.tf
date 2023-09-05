/**
 * Copyright 2021 Google LLC.
 *
 * This software is provided as-is, without warranty or representation for any use or purpose.
 * Your use of it is subject to your agreement with Google.
 */
/************************* REDSHIFT TO GOOGLE BIGQUERY END TO END MIGRATION INFRASTRUCTURE*************************/

/******************************************
  GCS Bucket deployment
*****************************************/

/* Create necessary Google Cloud Storage Buckets */

resource "google_storage_bucket" "buckets" {
  project                     = var.project_id
  for_each                    = toset(var.bucket_names)
  name                        = "${each.value}-${var.customer_name}"
  location                    = var.location
  uniform_bucket_level_access = true
  storage_class               = var.storage_class
  force_destroy               = true
  versioning {
    enabled = true
  }
}

/* Provide Admin authorization for GCS Service Account to the created GCS bucket */

resource "google_storage_bucket_iam_member" "storage_admin" {
  depends_on = [google_storage_bucket.buckets]
  for_each   = toset(var.bucket_names)
  bucket     = "${each.value}-${var.customer_name}"
  role       = "roles/storage.admin"
  member     = "serviceAccount:${var.service_account_gcs}@${var.project_id}.iam.gserviceaccount.com"
}

/* Provide Object Admin authorization for GCC Service Account to the created GCS buckets */

resource "google_storage_bucket_iam_member" "storage_object_admin" {
  depends_on = [google_storage_bucket.buckets]
  for_each   = toset(var.bucket_names)
  bucket     = "${each.value}-${var.customer_name}"
  role       = "roles/storage.objectAdmin"
  member     = "serviceAccount:${var.service_account_gcc}@${var.project_id}.iam.gserviceaccount.com"
}
