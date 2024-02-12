# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from airflow.api.common.trigger_dag import trigger_dag
from airflow.utils import timezone
from common_utils import parallelization_utils
from google.cloud import bigquery

AUDIT_LOG_DATASET = 'dmt_logs'
GCS_BQLOAD_AUDIT_TABLE = 'dmt_gcs_bqload_audit'
GET_TABLE_LIST_FOR_DVT = "select distinct concat('{db_name}.',tablename,' = {bq_dataset_name}.',tablename) as tablename FROM {bq_dataset_audit}.{bq_load_audit} where load_status = 'PASS' and load_dtm = (SELECT max(load_dtm) from {bq_dataset_audit}.{bq_load_audit})"

def audit_log_load_status(
    tablename, load_status, reason_for_failure, bq_job_id, config_params, op_load_dtm_str, op_run_id
):
    """
    Save BQ Load status in GCS Audit table
    """
    client = bigquery.Client()
    metadata_list = [
        {
            "load_dtm": op_load_dtm_str,
            "run_id": str(op_run_id),
            "db_name": config_params["db_name"],
            "bq_datatset": config_params["bq_dataset_id"],
            "tablename": tablename,
            "bq_job_id": bq_job_id,
            "load_status": load_status,
            "reason_for_failure": reason_for_failure,
        }
    ]

    client.insert_rows_json(
        f"{AUDIT_LOG_DATASET}.{GCS_BQLOAD_AUDIT_TABLE}", metadata_list
    )
    print("Audit table loaded for table: {}".format(tablename))


def file_format_subcmd_2(
    file_format, field_delimiter
):
    """
    Create subcommand to be include in the final BQ load command based on different formats
    """
    formatcmd = ""
    checkformat = 0
    droptable = 0
    text_tbl_schema_string = ""
    if file_format == "CSV":
        formatcmd = f" --field_delimiter='{field_delimiter}'"
        checkformat = 1
    elif file_format == "PARQUET":
        formatcmd = " --parquet_enable_list_inference=true "
        checkformat = 1
        droptable = 1
    elif file_format == "AVRO":
        formatcmd = " --use_avro_logical_types=true "
        checkformat = 1
    elif file_format == "ORC":
        checkformat = 1
    return formatcmd, checkformat, droptable, text_tbl_schema_string


def get_validation_dag_id(validation_mode):
    validation_gke_type = "gke"
    validation_crun_type = "cloudrun"
    validation_default_type = validation_gke_type
    validation_type_to_dag_id_mapping = {
        validation_crun_type: "validation_crun_dag",
        validation_gke_type: "validation_dag",
    }
    if validation_mode in validation_type_to_dag_id_mapping:
        return validation_type_to_dag_id_mapping[validation_mode]
    else:
        return validation_type_to_dag_id_mapping[validation_default_type]

def get_dvt_table_list(config):
    query = GET_TABLE_LIST_FOR_DVT.format(
        db_name=config['transfer_config']['params']["db_name"],
        bq_dataset_name=config['transfer_config']['params']["bq_dataset_id"],
        bq_dataset_audit=AUDIT_LOG_DATASET,
        bq_load_audit=GCS_BQLOAD_AUDIT_TABLE,
    )
    print(query)
    client = bigquery.Client()
    return client.query(query).to_dataframe()["tablename"].values.tolist()


def invoke_dvt_dag(ti):

    config = ti.xcom_pull(task_ids='set_required_vars', key='user_config')
    
    run_id_prefix = timezone.utcnow()

    validation_dag_id = get_validation_dag_id(config["validation_config"]["validation_mode"])

    batch_size = int(config["batchDistribution"])

    table_list = get_dvt_table_list(config)
    table_list = str(table_list)
    table_list = table_list.strip("][").split(", ")
    
    for batch_run_id, batch in parallelization_utils.make_run_batches(
        table_list, batch_size, run_id_prefix
    ):
        if table_list == []:
            logging.info(f"empty table list, skipping validation for {batch}")
            continue
        conf = {"config": config, "files": batch, "table_list": batch}
        trigger_dag(
            dag_id=validation_dag_id,
            run_id=batch_run_id,
            replace_microseconds=False,
            conf=conf,
        )
        logging.info(
            f"invoked {validation_dag_id} (run_id: {batch_run_id}) for files {batch}"
        )