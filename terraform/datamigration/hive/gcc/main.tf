#
#Copyright 2021 Google LLC.
#This software is provided as-is, without warranty or representation for any use or purpose.
#Your use of it is subject to your agreement with Google.

#/************************* TERADATA TO GOOGLE BIGQUERY END TO END MIGRATION INFRASTRUCTURE*************************

/******************************************
 Cloud Composer V2 Deployment
*****************************************/

data "google_composer_environment" "composer_env" {
  name    = "${var.composer_env_name}-${var.customer_name}"
  project = var.project_id
  region  = var.location
}

data "google_project" "project" {
  project_id = var.project_id
}


/* Provide Object Admin authorization for Service Account to the created GCS buckets */

resource "google_storage_bucket_iam_member" "storage_object_admin" {
  for_each = toset(var.bucket_names)
  bucket   = "${each.value}-${var.customer_name}"
  role     = "roles/storage.objectAdmin"
  member   = "serviceAccount:${var.service_account_gcc}@${var.project_id}.iam.gserviceaccount.com"
}


/* Copy Hive data migration DAGs to Composer bucket */

resource "null_resource" "upload_hive_datamigration_dags" {
  depends_on = [google_storage_bucket_iam_member.storage_object_admin]
  triggers = {
    datamigration_hive_dag_hashes = jsonencode({
      for fn in fileset(var.datamigration_hive_dag_source_path, "**") :
      fn => filesha256("${var.datamigration_hive_dag_source_path}/${fn}")
    })
  }
  provisioner "local-exec" {
    command = "gcloud storage cp --recursive ${var.datamigration_hive_dag_source_path}/* ${data.google_composer_environment.composer_env.config.0.dag_gcs_prefix}/"
  }
}

/* Copy datamigration_utils/ to Composer bucket */

resource "null_resource" "upload_datamigration_utils" {
  depends_on = [google_storage_bucket_iam_member.storage_object_admin]
  triggers = {
    datamigration_util_hashes = jsonencode({
      for fn in fileset(var.datamigration_utils, "**") :
      fn => filesha256("${var.datamigration_utils}/${fn}")
    })
  }
  provisioner "local-exec" {
    command = "gcloud storage cp --recursive ${var.datamigration_utils} ${data.google_composer_environment.composer_env.config.0.dag_gcs_prefix}/"
  }
}
