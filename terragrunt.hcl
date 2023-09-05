locals {
  # Project ID where Cloud Run Environment is deployed.
  my_project_id = get_env("PROJECT_ID")
  # Name of your bucket to store state dat`a
  my_bucket_name = get_env("_MY_BUCKET_NAME", "${get_env("PROJECT_ID")}-dmt-state-bucket")
  # Name of the customer to append in all service names
  # Name must start with a lowercase letter followed by
  # up to 63 lowercase letters, numbers, or hyphens, and cannot end with a hyphen.
  my_customer_name = get_env("_MY_CUSTOMER_NAME", get_env("PROJECT_ID"))
  # For oracle data validation tool this 2 inputs are required
  # Oracle instant client .rpm file gcs bucket path
  oracle_instant_client_file_path = get_env("_ORACLE_INSTANTCLIENT_FILE_PATH", "")
  # Oracle instant client version number
  oracle_odbc_version_number = get_env("_ORACLE_ODBC_VERSION_NUMBER", "")
}

inputs = {
  project_id                             = local.my_project_id
  customer_name                          = local.my_customer_name
  bq_tables_deletion_protection          = get_env("_DELETE_BQ_TABLES", "false") == "true" ? false : true
  event_listener_source_path             = "${get_parent_terragrunt_dir()}/src/translation/event_listener"
  dvt_source_path                        = "${get_parent_terragrunt_dir()}/src/translation/dvt"
  translation_dag_source_path            = "${get_parent_terragrunt_dir()}/src/translation/dags"
  datamigration_hive_dag_source_path     = "${get_parent_terragrunt_dir()}/src/datamigration/dags/hive"
  datamigration_teradata_dag_source_path = "${get_parent_terragrunt_dir()}/src/datamigration/dags/teradata"
  datamigration_teradata_script          = "${get_parent_terragrunt_dir()}/src/datamigration/scripts/teradata"
  datamigration_redshift_dag_source_path = "${get_parent_terragrunt_dir()}/src/datamigration/dags/redshift"
  translation_teradata_script            = "${get_parent_terragrunt_dir()}/src/translation/scripts/teradata"
  translation_hive_script                = "${get_parent_terragrunt_dir()}/src/translation/scripts/hive"
  common_utils                           = "${get_parent_terragrunt_dir()}/src/common_utils"
  datamigration_utils                    = "${get_parent_terragrunt_dir()}/src/datamigration/dags/datamigration_utils"
  oracle_instant_client_file_path        = local.oracle_instant_client_file_path
  oracle_odbc_version_number             = local.oracle_odbc_version_number
}

remote_state {
  backend = "gcs"
  generate = {
    path      = "backend.tf"
    if_exists = "overwrite"
  }
  config = {
    /* Provide the bucket name to store state files.
   If bucket does not exist, you will receive a prompt and terraform will create it for you on confirmation */
    bucket   = local.my_bucket_name
    project  = local.my_project_id
    location = "us"
    prefix   = "${path_relative_to_include()}/"
  }
}

/* Generate providers.tf dynamically for each GCP service module */
generate "providers" {
  path      = "providers.tf"
  if_exists = "overwrite"
  contents  = <<EOF
terraform {
  required_version = ">= 0.13"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 3.53, < 5.0"
    }
  }
}
provider "google" {
 project 		= var.project_id
}
EOF
}