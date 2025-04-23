/**
 * Copyright 2021 Google LLC.
 *
 * This software is provided as-is, without warranty or representation for any use or purpose.
 * Your use of it is subject to your agreement with Google.
 */

/******************************************
 BigQuery Datasets Creation
*****************************************/

/* Create DVT Dataset */
resource "google_bigquery_dataset" "bq_dataset" {
  project    = var.project_id
  dataset_id = var.dmt_dataset
  location   = var.location
}

/* Create Logging Dataset */
resource "google_bigquery_dataset" "bq_logging_dataset" {
  project    = var.project_id
  dataset_id = var.logging_dataset
  location   = var.location
}

/* Create Translation Logging Table with JSON schema */
resource "google_bigquery_table" "translation_log" {
  depends_on = [google_bigquery_dataset.bq_logging_dataset]
  project    = var.project_id
  dataset_id = google_bigquery_dataset.bq_logging_dataset.dataset_id
  table_id   = var.translation_table
  schema     = file("bq_schemas/translation_results_schema.json")
  time_partitioning {
    field = "create_time"
    type  = "DAY"
  }
  deletion_protection = var.bq_tables_deletion_protection
}

/* Create Aggregated Translation Logging Table with JSON schema */
resource "google_bigquery_table" "agg_translation_log" {
  depends_on          = [google_bigquery_dataset.bq_logging_dataset]
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.bq_logging_dataset.dataset_id
  table_id            = var.aggregate_translation_table
  schema              = file("bq_schemas/translation_aggregated_results_schema.json")
  deletion_protection = var.bq_tables_deletion_protection
}

/* Create Schema Creation Logging Table with JSON schema */
resource "google_bigquery_table" "schema_log" {
  depends_on = [google_bigquery_dataset.bq_logging_dataset]
  project    = var.project_id
  dataset_id = google_bigquery_dataset.bq_logging_dataset.dataset_id
  table_id   = var.schema_table
  schema     = file("bq_schemas/schema_results_schema.json")
  time_partitioning {
    field = "execution_start_time"
    type  = "DAY"
  }
  deletion_protection = var.bq_tables_deletion_protection
}

/* Create Aggregated Schema Creation Logging Table with JSON schema */
resource "google_bigquery_table" "agg_schema_log" {
  depends_on          = [google_bigquery_dataset.bq_logging_dataset]
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.bq_logging_dataset.dataset_id
  table_id            = var.aggregate_schema_table
  schema              = file("bq_schemas/schema_aggregated_results_schema.json")
  deletion_protection = var.bq_tables_deletion_protection
}

/* Create sql_file_table_mapping Table with JSON schema */
resource "google_bigquery_table" "sql_file_table_mapping" {
  depends_on          = [google_bigquery_dataset.bq_logging_dataset]
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.bq_logging_dataset.dataset_id
  table_id            = var.sql_file_tbl_mapping_table
  schema              = file("bq_schemas/file_table_mapping_schema.json")
  deletion_protection = var.bq_tables_deletion_protection
}

/* Create DVT Table with JSON schema */
resource "google_bigquery_table" "dvt_log" {
  depends_on = [google_bigquery_dataset.bq_logging_dataset]
  project    = var.project_id
  dataset_id = google_bigquery_dataset.bq_logging_dataset.dataset_id
  table_id   = var.dvt_table
  schema     = file("bq_schemas/dvt_results_schema.json")
  time_partitioning {
    field = "start_time"
    type  = "DAY"
  }
  deletion_protection = var.bq_tables_deletion_protection
}

/* Create Aggregated DVT Table with JSON schema */
resource "google_bigquery_table" "agg_dvt_log" {
  depends_on          = [google_bigquery_dataset.bq_logging_dataset]
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.bq_logging_dataset.dataset_id
  table_id            = var.aggregate_dvt_table
  schema              = file("bq_schemas/dvt_aggregated_results_schema.json")
  deletion_protection = var.bq_tables_deletion_protection
}

/* Create DML Validation Logging Table with JSON schema */
resource "google_bigquery_table" "dml_validation_log" {
  depends_on = [google_bigquery_dataset.bq_logging_dataset]
  project    = var.project_id
  dataset_id = google_bigquery_dataset.bq_logging_dataset.dataset_id
  table_id   = var.dml_validation_table
  schema     = file("bq_schemas/dml_validation_results.json")
  time_partitioning {
    field = "execution_start_time"
    type  = "DAY"
  }
  deletion_protection = var.bq_tables_deletion_protection
}

/* Create Aggregated DML Validation Logging Table with JSON schema */
resource "google_bigquery_table" "agg_dml_validation_log" {
  depends_on          = [google_bigquery_dataset.bq_logging_dataset]
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.bq_logging_dataset.dataset_id
  table_id            = var.aggregate_dml_validation_table
  schema              = file("bq_schemas/dml_validation_aggregated_results.json")
  deletion_protection = var.bq_tables_deletion_protection
}

/* Create DDL Extraction Logging Table with JSON schema */
resource "google_bigquery_table" "ddl_extraction_log" {
  depends_on          = [google_bigquery_dataset.bq_logging_dataset]
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.bq_logging_dataset.dataset_id
  table_id            = var.extract_ddl_table
  schema              = file("bq_schemas/extract_ddl_results.json")
  deletion_protection = var.bq_tables_deletion_protection
}

/* Create Report Master Logging Table with JSON schema */
resource "google_bigquery_table" "dmt_reporting" {
  depends_on          = [google_bigquery_dataset.bq_logging_dataset]
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.bq_logging_dataset.dataset_id
  table_id            = var.dmt_reporting_table
  schema              = file("bq_schemas/dmt_report_table.json")
  deletion_protection = var.bq_tables_deletion_protection
}


/* Create DDL Extraction Logging Table for HIVE */
resource "google_bigquery_table" "hive_ddl_metadata" {
  depends_on          = [google_bigquery_dataset.bq_logging_dataset]
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.bq_logging_dataset.dataset_id
  table_id            = var.hive_ddl_metadata
  schema              = file("bq_schemas/hive_ddl_metadata.json")
  deletion_protection = var.bq_tables_deletion_protection
}


/* Create BQ Service Account */
resource "google_service_account" "bq" {
  project      = var.project_id
  account_id   = var.service_account_bq
  display_name = "Service Account for BigQuery tables"
}

/* Provide BQ roles to BQ Service Account */
resource "google_project_iam_member" "bq_roles" {
  depends_on = [google_service_account.bq]
  project    = var.project_id
  for_each   = toset(var.bq_roles)
  role       = each.value
  member     = google_service_account.bq.member
}
