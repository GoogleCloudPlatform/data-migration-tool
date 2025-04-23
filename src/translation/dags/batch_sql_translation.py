# Copyright 2022 Google LLC
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

import ast
import csv
import datetime
import logging
import os
from pathlib import Path

from airflow import models
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sensors.python import PythonSensor
from airflow.utils.trigger_rule import TriggerRule
from google.api_core.client_info import ClientInfo
from google.cloud import bigquery, storage
from translation_utils import csv_utils
from translation_utils import translation_stats_utils as stats_utils
from translation_utils.bigquery_migration_v2 import (
    create_single_task_migration_workflow,
    get_migration_workflow_state,
)

from common_utils import custom_user_agent, storage_utils
from common_utils.operators.reporting_operator import ReportingOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
TRANSLATION_REPORT_FILENAME = os.environ.get(
    "TRANSLATION_REPORT_FILENAME", "batch_translation_report.csv"
)

SKIP_VALIDATION_DIR_NAME = "skip_validation"
REPORT_FILENAME = "batch_translation_report.csv"
MAP_FILENAME = "consumed_name_map.json"
TRANSLATION_STATS_TABLE_ID = f"{PROJECT_ID}.dmt_logs.dmt_translation_results"

TRANSLATION_AGG_RESULTS_TABLE_ID = (
    f"{PROJECT_ID}.dmt_logs.dmt_translation_aggregated_results"
)

ERROR_FILTER_KEY = "translationErrorFilterRules"
TRANSLATION_CONFIG_KEY = "type"
TRANSLATION_CONFIG_DDL = "ddl"
TRANSLATION_CONFIG_SQL = "sql"
TRANSLATION_CONFIG_DML = "dml"
TRANSLATION_CONFIG_DEFAULT_VALUE = TRANSLATION_CONFIG_DDL

DDL_FOLDER = "ddl"

SCHEMA_DAG_ID = "schema_dag"
VALIDATION_GKE_TYPE = "gke"
VALIDATION_CRUN_TYPE = "cloudrun"
VALIDATION_DEFAULT_TYPE = VALIDATION_GKE_TYPE
VALIDATION_TYPE_TO_DAG_ID_MAPPING = {
    VALIDATION_CRUN_TYPE: "validation_crun_dag",
    VALIDATION_GKE_TYPE: "validation_dag",
}
VALIDATION_DAG_ID = "validation_dag"
VALIDATION_CRUN_DAG_ID = "validation_crun_dag"

DML_VALIDATION_DAG_ID = "dml_validation_dag"

gcs_util = storage_utils.StorageUtils()

default_dag_args = {"start_date": datetime.datetime(2022, 1, 1)}

storage_client = storage.Client(
    client_info=ClientInfo(user_agent=custom_user_agent.USER_AGENT)
)


def get_validation_dag_id(validation_mode):
    if validation_mode in VALIDATION_TYPE_TO_DAG_ID_MAPPING:
        return VALIDATION_TYPE_TO_DAG_ID_MAPPING[validation_mode]
    else:
        return VALIDATION_TYPE_TO_DAG_ID_MAPPING[VALIDATION_DEFAULT_TYPE]


def is_ddl_run(config):
    """
    Helper function to determine if the current run is a
    DDL run
    """
    return (
        TRANSLATION_CONFIG_KEY not in config
        or config[TRANSLATION_CONFIG_KEY].casefold() == "ddl"
    )


def _create_translation_workflow(ti, **kwargs) -> None:
    translation_config = ast.literal_eval(kwargs["dag_run"].conf["config"])["config"]
    response = create_single_task_migration_workflow(
        PROJECT_ID, translation_config["migrationTask"]
    )

    logging.info(f"workflow info: {response}")
    ti.xcom_push(key="workflow_info", value=response)
    ti.xcom_push(key="config", value=translation_config)


def _poll_workflow_state(ti) -> bool:
    [workflow_info] = ti.xcom_pull(
        key="workflow_info", task_ids=["create_translation_workflow"]
    )

    response = get_migration_workflow_state(workflow_info["name"])
    logging.info(f"retrieved workflow state. state: {response['state']}")

    if response["state"] == "COMPLETED":
        ti.xcom_push(key="workflow_info", value=response)
        return True
    else:
        return False


def _get_failed_files_from_csv(ti) -> None:
    config = ti.xcom_pull(key="config", task_ids="create_translation_workflow")
    error_row_filter_rules = (
        config[ERROR_FILTER_KEY] if ERROR_FILTER_KEY in config else None
    )
    error_row_filter = csv_utils.rules_filter(error_row_filter_rules)

    source_path = config["migrationTask"]["translationConfigDetails"]["gcsSourcePath"]
    target_path = config["migrationTask"]["translationConfigDetails"]["gcsTargetPath"]
    target_bucket, target_folder = gcs_util.parse_bucket_and_blob_from_path(target_path)

    gcs_hook = GCSHook()
    report_object_name = storage_utils.append_blob_name_to_path(
        target_folder, REPORT_FILENAME
    )
    report = gcs_hook.download_as_byte_array(target_bucket, report_object_name).decode(
        "utf-8"
    )
    csv_reader = csv.reader(report.splitlines())

    failed_files = set()
    errors = []
    headers = next(csv_reader)
    logging.info("scanning failed files:")
    for row in csv_reader:
        row_dict = csv_utils.row_to_dict(headers, row)

        file_path = ""
        if row_dict["FilePath"] is not None and row_dict["FilePath"] != "":
            file_path = os.path.relpath(row_dict["FilePath"], start=source_path)
        elif row_dict["FileName"] is not None and row_dict["FileName"] != "":
            file_path = row_dict["FileName"]
        else:
            logging.info("File path and name is blank in CSV file")
            continue  # If name and path both are blank skip the iteration

        failed_file_path = f"{target_folder}/{file_path}"
        if not error_row_filter or not error_row_filter(row_dict):
            failed_files.add(failed_file_path)
            logging.info(f"{failed_file_path} contains errors")
            errors.append(stats_utils.csv_row_to_record(row_dict, None, False))
        else:
            logging.info(f"{failed_file_path} contains errors (filtered)")
            errors.append(stats_utils.csv_row_to_record(row_dict, None, True))

    ti.xcom_push(key="files", value=list(failed_files))
    ti.xcom_push(key="errors", value=errors)


def _get_all_translated_files(ti) -> None:
    translation_config = ti.xcom_pull(
        key="config", task_ids="create_translation_workflow"
    )

    target_path = translation_config["migrationTask"]["translationConfigDetails"][
        "gcsTargetPath"
    ]
    target_bucket, target_folder = gcs_util.parse_bucket_and_blob_from_path(target_path)

    translated_files = set()
    for blob in storage_client.list_blobs(target_bucket, prefix=target_folder):
        file_full_path = Path(blob.name)
        filename = file_full_path.name
        if (
            blob.name[-1] != "/"
            and filename != REPORT_FILENAME
            and filename != MAP_FILENAME
            and SKIP_VALIDATION_DIR_NAME not in file_full_path.parts
        ):
            translated_files.add(blob.name)

    ti.xcom_push(key="files", value=list(translated_files))


def _get_successfully_translated_files(ti) -> None:
    [all_files, failed_files] = ti.xcom_pull(
        key="files",
        task_ids=["get_all_translated_files", "get_failed_files_from_csv"],
    )
    config = ti.xcom_pull(key="config", task_ids="create_translation_workflow")

    target_path = config["migrationTask"]["translationConfigDetails"]["gcsTargetPath"]
    _, target_folder = gcs_util.parse_bucket_and_blob_from_path(target_path)

    successfully_translated_files = set(all_files).difference(set(failed_files))
    if len(successfully_translated_files):
        ti.xcom_push(key="files", value=list(successfully_translated_files))
        file_names = []
        for filepath in successfully_translated_files:
            relative_path = os.path.relpath(filepath, start=target_folder)
            folder, _ = os.path.split(relative_path)
            if is_ddl_run(config) or folder != DDL_FOLDER:
                file_names.append(relative_path)
        ti.xcom_push(key="file_names", value=file_names)
    else:
        raise Exception("No files were successfully translated.")


"""
Method: _download_files
Description: This task definition download files from output GCS bucket and store it in airflow data gcs directory
Arguments: ti, **kwargs
"""


def _download_files(ti):
    config = ti.xcom_pull(key="config", task_ids="create_translation_workflow")
    file_list = ti.xcom_pull(
        key="file_names", task_ids="get_successfully_translated_files"
    )

    translation_target_path = config["migrationTask"]["translationConfigDetails"][
        "gcsTargetPath"
    ]
    bucket, folder = gcs_util.parse_bucket_and_blob_from_path(translation_target_path)

    logging.info(file_list)
    if len(file_list) == 0:
        logging.info("SQL File list is empty and there are no SQL files to download")
    else:
        gcs_hook = GCSHook()
        for filename in file_list:
            local_filename = f"/home/airflow/gcs/data/{filename}"
            object_name = f"{folder}/{filename}"
            # make sure local folder exists before downloading
            local_folder, _ = os.path.split(local_filename)
            if not os.path.exists(local_folder):
                os.makedirs(local_folder)
            # download file
            gcs_hook.download(
                bucket_name=bucket,
                object_name=object_name,
                filename=local_filename,
            )
        logging.info(os.listdir("/home/airflow/gcs/data"))


def _determine_next_dag(ti):
    config = ti.xcom_pull(key="config", task_ids="create_translation_workflow")
    run_type = (
        config[TRANSLATION_CONFIG_KEY]
        if TRANSLATION_CONFIG_KEY in config
        else TRANSLATION_CONFIG_DEFAULT_VALUE
    )
    if run_type == TRANSLATION_CONFIG_DDL:
        return "invoke_schema_dag"
    elif run_type == TRANSLATION_CONFIG_SQL:
        validation_mode = config["validation_config"].get("validation_mode")
        validation_dag_id = get_validation_dag_id(validation_mode)
        if validation_dag_id == VALIDATION_DAG_ID:
            return "invoke_validation_dag"
        else:
            return "invoke_validation_crun_dag"
    elif run_type == TRANSLATION_CONFIG_DML:
        return "invoke_dml_validation_dag"
    else:
        raise ValueError(f"invalid value for translation field: {run_type}")


def _save_stats(ti):
    translation_config = ti.xcom_pull(
        key="config", task_ids="create_translation_workflow"
    )
    all_translated_files = ti.xcom_pull(
        key="files", task_ids="get_all_translated_files"
    )
    successfully_translated_files = ti.xcom_pull(
        key="files", task_ids="get_successfully_translated_files"
    )
    failed_files = ti.xcom_pull(key="files", task_ids="get_failed_files_from_csv")
    translation_errors = ti.xcom_pull(
        key="errors", task_ids="get_failed_files_from_csv"
    )
    workflow_info = ti.xcom_pull(key="workflow_info", task_ids="poll_workflow_state")

    logging.info(f"workflow info: {workflow_info}")

    stats = [
        stats_utils.new_record(file.split("/")[-1], None, "OK")
        for file in successfully_translated_files
    ]
    stats += translation_errors

    for record in stats:
        record["unique_id"] = translation_config["unique_id"]
        record["create_time"] = workflow_info["createTime"]
        record["name"] = workflow_info["name"]
        translation_run = (
            translation_config[TRANSLATION_CONFIG_KEY]
            if TRANSLATION_CONFIG_KEY in translation_config
            else "ddl"
        )
        record["type"] = translation_run.casefold()

    bq_client = bigquery.Client(
        client_info=ClientInfo(user_agent=custom_user_agent.USER_AGENT)
    )

    if stats == []:
        logging.info("Translation stats are empty. Please check translation csv file. ")
    else:
        bq_client.insert_rows_json(TRANSLATION_STATS_TABLE_ID, stats)

    translation_summary_csv_path = f"https://console.cloud.google.com/storage/browser/_details/{translation_config['migrationTask']['translationConfigDetails']['gcsTargetPath'].split('//')[-1]}/batch_translation_report.csv;tab=live_object?{PROJECT_ID}"

    rows_json_list = [
        {
            "unique_id": translation_config["unique_id"],
            "total_files": len(all_translated_files),
            "successful_files": len(successfully_translated_files),
            "failed_files": len(failed_files),
            "input_files_path": translation_config["migrationTask"][
                "translationConfigDetails"
            ]["gcsSourcePath"],
            "output_files_path": translation_config["migrationTask"][
                "translationConfigDetails"
            ]["gcsTargetPath"],
            "translation_summary_csv_path": translation_summary_csv_path,
        }
    ]

    if rows_json_list == []:
        logging.info("Translation Aggregate Stats are empty. ")
    else:
        bq_client.insert_rows_json(TRANSLATION_AGG_RESULTS_TABLE_ID, rows_json_list)


with models.DAG(
    "batch_sql_translation",
    schedule=None,
    default_args=default_dag_args,
    render_template_as_native_obj=True,
) as dag:
    # tasks
    create_translation_workflow = PythonOperator(
        task_id="create_translation_workflow",
        python_callable=_create_translation_workflow,
        dag=dag,
    )
    get_failed_files_from_csv = PythonOperator(
        task_id="get_failed_files_from_csv",
        python_callable=_get_failed_files_from_csv,
        dag=dag,
    )
    get_all_translated_files = PythonOperator(
        task_id="get_all_translated_files",
        python_callable=_get_all_translated_files,
        dag=dag,
    )
    get_successfully_translated_files = PythonOperator(
        task_id="get_successfully_translated_files",
        python_callable=_get_successfully_translated_files,
        dag=dag,
    )
    save_stats = PythonOperator(
        task_id="save_stats", python_callable=_save_stats, dag=dag
    )
    download_files = PythonOperator(
        task_id="download_files", python_callable=_download_files, dag=dag
    )
    determine_next_dag = BranchPythonOperator(
        task_id="determine_next_dag",
        python_callable=_determine_next_dag,
        dag=dag,
    )

    invoke_schema_dag = TriggerDagRunOperator(
        task_id="invoke_schema_dag",
        trigger_dag_id=SCHEMA_DAG_ID,
        conf={
            "config": "{{ ti.xcom_pull(task_ids='create_translation_workflow', key='config') }}",
            "files": "{{ ti.xcom_pull(task_ids='get_successfully_translated_files', key='file_names') }}",
        },
        dag=dag,
    )
    invoke_validation_dag = TriggerDagRunOperator(
        task_id="invoke_validation_dag",
        trigger_dag_id=VALIDATION_DAG_ID,
        conf={
            "config": "{{ ti.xcom_pull(task_ids='create_translation_workflow', key='config') }}",
            "files": "{{ ti.xcom_pull(task_ids='get_successfully_translated_files', key='file_names') }}",
        },
        dag=dag,
    )
    invoke_validation_crun_dag = TriggerDagRunOperator(
        task_id="invoke_validation_crun_dag",
        trigger_dag_id=VALIDATION_CRUN_DAG_ID,
        conf={
            "config": "{{ ti.xcom_pull(task_ids='create_translation_workflow', key='config') }}",
            "files": "{{ ti.xcom_pull(task_ids='get_successfully_translated_files', key='file_names') }}",
        },
        dag=dag,
    )
    invoke_dml_validation_dag = TriggerDagRunOperator(
        task_id="invoke_dml_validation_dag",
        trigger_dag_id=DML_VALIDATION_DAG_ID,
        conf={
            "config": "{{ ti.xcom_pull(task_ids='create_translation_workflow', key='config') }}",
            "files": "{{ ti.xcom_pull(task_ids='get_successfully_translated_files', key='file_names') }}",
        },
        dag=dag,
    )

    end_task = EmptyOperator(task_id="end", dag=dag)

    # sensors
    poll_workflow_state = PythonSensor(
        task_id="poll_workflow_state",
        python_callable=_poll_workflow_state,
        dag=dag,
        poke_interval=15,
    )

    dag_report = ReportingOperator(
        task_id="dag_report",
        trigger_rule=TriggerRule.ALL_DONE,  # Ensures this task runs even if upstream fails
        configuration="{{ ti.xcom_pull(task_ids='create_translation_workflow', key='config') }}",
        dag=dag,
    )

    # dependencies
    create_translation_workflow >> poll_workflow_state
    (
        poll_workflow_state
        >> [get_failed_files_from_csv, get_all_translated_files]
        >> get_successfully_translated_files
    )
    get_successfully_translated_files >> [download_files, save_stats]
    (
        download_files
        >> determine_next_dag
        >> [
            invoke_schema_dag,
            invoke_validation_dag,
            invoke_validation_crun_dag,
            invoke_dml_validation_dag,
        ]
        >> end_task
        >> dag_report
    )
