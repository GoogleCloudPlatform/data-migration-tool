/**
 * Copyright 2021 Google LLC.
 *
 * This software is provided as-is, without warranty or representation for any use or purpose.
 * Your use of it is subject to your agreement with Google.
 */

/******************************************
 Big Query table Creation
*****************************************/


/* Create BQ load audit table for Hive with JSON schema */
resource "google_bigquery_table" "flat_files_bq_load" {
  project             = var.project_id
  dataset_id          = var.logging_dataset
  table_id            = var.flat_files_data_load_logs
  schema              = file("bq_schemas/flat_files_data_load_logs.json")
  clustering          = ["load_dtm"]
  deletion_protection = var.bq_tables_deletion_protection
}

