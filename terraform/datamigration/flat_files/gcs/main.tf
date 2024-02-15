/**
 * Copyright 2021 Google LLC.
 *
 * This software is provided as-is, without warranty or representation for any use or purpose.
 * Your use of it is subject to your agreement with Google.
 */
/************************* FROM GCS TO GOOGLE BIGQUERY END TO END MIGRATION INFRASTRUCTURE*************************/

/******************************************
  GCS Bucket deployment
*****************************************/

locals {
  names_set    = toset(var.bucket_names)
  buckets_list = [for name in var.bucket_names : google_storage_bucket.buckets[name]]
  folder_list = flatten([
    for bucket, folders in var.folders : [
      for folder in folders : {
        bucket = "${bucket}-${var.customer_name}",
        folder = folder
      }
    ]
  ])
}

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

/* Create folder - files in the created buckets */

resource "google_storage_bucket_object" "folders" {
  depends_on = [google_storage_bucket.buckets]
  for_each   = { for obj in local.folder_list : "${obj.bucket}_${obj.folder}" => obj }
  bucket     = each.value.bucket

  /* Trailing '/' creates a directory */
  name = "${each.value.folder}/"

  /* Note that the content string isn't actually used, but is only there since the resource requires it */
  content = "Empty Directory Structure"
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
