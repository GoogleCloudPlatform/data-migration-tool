/**
 * Copyright 2021 Google LLC.
 *
 * This software is provided as-is, without warranty or representation for any use or purpose.
 * Your use of it is subject to your agreement with Google.
 */
/************************* TERADATA TO GOOGLE BIGQUERY END TO END MIGRATION INFRASTRUCTURE*************************/

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

/* Create necessary Google Cloud Storage Buckets for Translation and Configuration File Drops */

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

/* Create respective folders in buckets - ddl, sql, dml in the created buckets */

resource "google_storage_bucket_object" "folders" {
  depends_on = [google_storage_bucket.buckets]
  for_each   = { for obj in local.folder_list : "${obj.bucket}_${obj.folder}" => obj }
  bucket     = each.value.bucket

  /* Trailing '/' creates a directory */
  name = "${each.value.folder}/"

  /* Note that the content string isn't actually used, but is only there since the resource requires it */
  content = "Empty Directory Structure"
}

/* Create a service account for Google Cloud Storage Bucket */

resource "google_service_account" "gcs" {
  depends_on   = [google_storage_bucket.buckets, google_storage_bucket_object.folders]
  project      = var.project_id
  account_id   = var.service_account_gcs
  display_name = "Service Account for Cloud Storage"
}

/* Provide Admin authorization for Service Account to the created GCS bucket */

resource "google_storage_bucket_iam_member" "storage_admin" {
  depends_on = [google_service_account.gcs]
  for_each   = toset(var.bucket_names)
  bucket     = "${each.value}-${var.customer_name}"
  role       = "roles/storage.objectAdmin"
  member     = google_service_account.gcs.member
}

/* Copy files to GCS bucket dmt-config hive ddl extraction scripts - translation  */
resource "null_resource" "file_copy_gcs_py" {
  depends_on = [google_storage_bucket_object.folders]
  provisioner "local-exec" {
    command = "gsutil -o \"GSUtil:parallel_process_count=1\" -m cp -r ${var.translation_hive_script}/*.py gs://${var.config_bucket}-${var.customer_name}/scripts/translation/hive/"
  }
}

/* Copy files to GCS bucket dmt-config global config file - translation  */
resource "null_resource" "file_copy_gcs_yaml" {
  depends_on = [google_storage_bucket_object.folders]
  provisioner "local-exec" {
    command = "gsutil -o \"GSUtil:parallel_process_count=1\" -m cp -r ${var.translation_hive_script}/*.yaml gs://${var.config_bucket}-${var.customer_name}/scripts/translation/hive/"
  }
}


/* Copy files to GCS bucket dmt-config teradata ddl extraction scripts - translation  */
resource "null_resource" "teradata_file_copy_gcs" {
  depends_on = [google_storage_bucket_object.folders]
  provisioner "local-exec" {
    command = "gsutil -o \"GSUtil:parallel_process_count=1\" -m cp -r ${var.translation_teradata_script}/*.sh gs://${var.config_bucket}-${var.customer_name}/scripts/translation/teradata/"
  }
}
