#
#Copyright 2023 Google LLC.
#This software is provided as-is, without warranty or representation for any use or purpose.
#Your use of it is subject to your agreement with Google.

#/************************* TERADATA TO GOOGLE BIGQUERY END TO END MIGRATION INFRASTRUCTURE*************************

locals {
  image_prefix              = "${var.artifact_registry_region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.dmt-repo.repository_id}"
  dvt_image_name            = "data-validation-tool"
  event_listener_image_name = "event_listener"
  oracle_lib_path_split     = split("/", var.oracle_instant_client_file_path)
}

resource "google_artifact_registry_repository" "dmt-repo" {
  location      = var.artifact_registry_region
  repository_id = "dmt-repo"
  description   = "Repository for all images needed to run dmt tool."
  format        = "DOCKER"
}

/* DVT Docker image creation */
resource "null_resource" "dvt_image_creation" {
  provisioner "local-exec" {
    working_dir = var.dvt_source_path
    command     = "gcloud builds submit --config cloudbuild_dvt.yaml --region=${var.artifact_registry_region} --project ${var.project_id} --substitutions _ORACLE_INSTANTCLIENT_FILE_PATH=${var.oracle_instant_client_file_path},_ORACLE_ODBC_VERSION_NUMBER=${var.oracle_odbc_version_number},_TAG=${local.image_prefix}/${local.dvt_image_name},_FILE_NAME=${local.oracle_lib_path_split[length(local.oracle_lib_path_split) - 1]}"
  }
  triggers = {
    file_hashes = jsonencode({
      for fn in fileset(var.dvt_source_path, "**") :
      fn => filesha256("${var.dvt_source_path}/${fn}")
    })
    region_change  = var.artifact_registry_region
    locals_changes = "${local.image_prefix}${local.dvt_image_name}${local.event_listener_image_name}"
  }
}

/* Event listener Docker image creation */
resource "null_resource" "event_listener_image_creation" {
  provisioner "local-exec" {
    working_dir = var.event_listener_source_path
    command     = "gcloud builds submit --region=${var.artifact_registry_region} --tag ${local.image_prefix}/${local.event_listener_image_name} . --project ${var.project_id}"
  }
  triggers = {
    file_hashes = jsonencode({
      for fn in fileset(var.event_listener_source_path, "**") :
      fn => filesha256("${var.event_listener_source_path}/${fn}")
    })
    region_change  = var.artifact_registry_region
    locals_changes = "${local.image_prefix}${local.dvt_image_name}${local.event_listener_image_name}"
  }
}
