/**
 * Copyright 2021 Google LLC.
 *
 * This software is provided as-is, without warranty or representation for any use or purpose.
 * Your use of it is subject to your agreement with Google.
 */

/******************************************
 Big Query table Creation
*****************************************/


/* Create Transfer Summary Logging Table with JSON schema */
resource "google_bigquery_table" "transfer_run_summary" {
  project             = var.project_id
  dataset_id          = var.logging_dataset
  table_id            = var.transfer_run_summary_table
  schema              = file("bq_schemas/transfer_run_summary_schema.json")
  clustering          = ["run_date", "transfer_config_id"]
  deletion_protection = var.bq_tables_deletion_protection
}

/* Create Transfer Jobwise Logging Table with JSON schema */
resource "google_bigquery_table" "transfer_run_jobwise_details" {
  project             = var.project_id
  dataset_id          = var.logging_dataset
  table_id            = var.transfer_run_jobwise_details_table
  schema              = file("bq_schemas/transfer_run_jobwise_details_schema.json")
  clustering          = ["run_date", "src_table_name", "transfer_config_id"]
  deletion_protection = var.bq_tables_deletion_protection
}

/* Create Transfer Tracking Logging Table with JSON schema */
resource "google_bigquery_table" "transfer_tracking" {
  project             = var.project_id
  dataset_id          = var.logging_dataset
  table_id            = var.transfer_id_tracking_table
  schema              = file("bq_schemas/transfer_tracking_schema.json")
  clustering          = ["transfer_config_id"]
  deletion_protection = var.bq_tables_deletion_protection
}