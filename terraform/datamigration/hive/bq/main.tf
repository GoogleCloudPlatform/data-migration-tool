/**
 * Copyright 2021 Google LLC.
 *
 * This software is provided as-is, without warranty or representation for any use or purpose.
 * Your use of it is subject to your agreement with Google.
 */

/******************************************
 BigQuery Table Creation
*****************************************/


/* Create BQ load audit table for Hive with JSON schema */
resource "google_bigquery_table" "hive_bq_load" {
  project             = var.project_id
  dataset_id          = var.logging_dataset
  table_id            = var.hive_data_load_logs
  schema              = file("bq_schemas/bq_load_audit.json")
  clustering          = ["load_dtm"]
  deletion_protection = var.bq_tables_deletion_protection
}

