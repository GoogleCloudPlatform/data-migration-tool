/**
 * Copyright 2023 Google LLC.
 *
 * This software is provided as-is, without warranty or representation for any use or purpose.
 * Your use of it is subject to your agreement with Google.
 */
/************************* TERADATA TO GOOGLE BIGQUERY END TO END MIGRATION INFRASTRUCTURE*************************/

# This resource requires the Service Usage API
# https://console.cloud.google.com/apis/library/serviceusage.googleapis.com
# to use.
resource "google_project_service" "service" {
  for_each = toset([
    "cloudresourcemanager.googleapis.com",
    "artifactregistry.googleapis.com",
    "iam.googleapis.com",
    "cloudbilling.googleapis.com",
    "compute.googleapis.com",
    "composer.googleapis.com",
    "bigquery.googleapis.com",
    "bigquerymigration.googleapis.com",
    "bigquerydatatransfer.googleapis.com",
    "bigquerystorage.googleapis.com",
    "storage.googleapis.com",
    "autoscaling.googleapis.com",
    "vpcaccess.googleapis.com",
    "pubsub.googleapis.com",
    "run.googleapis.com",
    "monitoring.googleapis.com",
    "logging.googleapis.com",
    "servicenetworking.googleapis.com",
    "secretmanager.googleapis.com",
    "storage-api.googleapis.com",
    "cloudapis.googleapis.com",
    "storage-component.googleapis.com",
    "storage-api.googleapis.com"
  ])

  service = each.key

  project            = var.project_id
  disable_on_destroy = false
}