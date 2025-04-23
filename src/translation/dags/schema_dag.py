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

import datetime
import logging
import os

from airflow import models
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
)
from airflow.utils.trigger_rule import TriggerRule
from google.api_core.client_info import ClientInfo
from google.cloud import bigquery

from common_utils import custom_user_agent, table_filter
from common_utils.bigquery_client_utils import ddl as ddl_utils
from common_utils.operators.reporting_operator import ReportingOperator

# DAG id constant variable
DAG_ID = "schema_dag"

# DAG arguments
execution_date = datetime.datetime(2022, 1, 1)
default_dag_args = {"start_date": execution_date}

## Variables used to store schema creation result in BQ table
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BQ_RESULT_DATASET_NAME = "dmt_logs"
SCHEMA_AGGREGATED_RESULTS_TABLE_ID = (
    f"{PROJECT_ID}.{BQ_RESULT_DATASET_NAME}.dmt_schema_aggregated_results"
)
SCHEMA_RESULTS_TABLE_ID = f"{PROJECT_ID}.{BQ_RESULT_DATASET_NAME}.dmt_schema_results"

## Config file keys
CUSTOM_RUN_ID_KEY = "unique_id"

LOCAL_DATA_DIRECTORY = "/home/airflow/gcs/data"

bq_client = bigquery.Client(
    client_info=ClientInfo(user_agent=custom_user_agent.USER_AGENT)
)

VALIDATION_GKE_TYPE = "gke"
VALIDATION_CRUN_TYPE = "cloudrun"
VALIDATION_DEFAULT_TYPE = VALIDATION_GKE_TYPE
VALIDATION_TYPE_TO_DAG_ID_MAPPING = {
    VALIDATION_CRUN_TYPE: "validation_crun_dag",
    VALIDATION_GKE_TYPE: "validation_dag",
}
VALIDATION_DAG_ID = "validation_dag"
VALIDATION_CRUN_DAG_ID = "validation_crun_dag"


def get_validation_dag_id(validation_mode):
    if validation_mode in VALIDATION_TYPE_TO_DAG_ID_MAPPING:
        return VALIDATION_TYPE_TO_DAG_ID_MAPPING[validation_mode]
    else:
        return VALIDATION_TYPE_TO_DAG_ID_MAPPING[VALIDATION_DEFAULT_TYPE]


"""
Method: _create_dataset
Description: This task definition is to create list of unique BigQuery dataset from config if not exist
Arguments: **kwargs
"""


def _create_dataset(**kwargs):
    if kwargs["dag_run"].conf is not None and "config" in kwargs["dag_run"].conf:
        logging.info("Configuration file is not empty")
        logging.info(kwargs["dag_run"].conf["config"])
        config = kwargs["dag_run"].conf["config"]
        nm_map_list = config["migrationTask"]["translationConfigDetails"][
            "nameMappingList"
        ]["name_map"]
        uq_dataset = set(d["target"]["schema"] for d in nm_map_list)

        for dataset in uq_dataset:
            execute_create_dataset = BigQueryCreateEmptyDatasetOperator(
                task_id=f"execute_create_dataset_{dataset}", dag=dag, dataset_id=dataset
            )

            execute_create_dataset.execute(context=kwargs)
    else:
        logging.error("Configuration file is empty")


"""
Method: _save_schema_creation_result
Description: This task definition is to insert Job Execution result in BigQuery
Arguments: ti
"""


def _save_schema_creation_result(ti):
    results = ti.xcom_pull(task_ids="exec", key="results")
    if results == []:
        logging.info("Schema Creation Stats are empty.")
    else:
        insert_result = bq_client.insert_rows_json(SCHEMA_RESULTS_TABLE_ID, results)
        logging.info(f"metric insertion result: {insert_result}")

    aggregated_results = ti.xcom_pull(task_ids="exec", key="aggregated_results")
    if aggregated_results == []:
        logging.info("Schema Creation Aggregate Stats are empty.")
    else:
        insert_result = bq_client.insert_rows_json(
            SCHEMA_AGGREGATED_RESULTS_TABLE_ID, aggregated_results
        )
        logging.info(f"aggregated metric insertion result: {insert_result}")


"""
Method: _execute_queries
Description: This task executes DDLs
Arguments: ti, dag_run
"""


def _execute_queries(ti, dag_run):
    config = dag_run.conf["config"]
    files = dag_run.conf["files"]

    local_files = [os.path.join(LOCAL_DATA_DIRECTORY, file) for file in files]
    unique_id = config[CUSTOM_RUN_ID_KEY]

    # Outcome handlers
    def success_handler(script):
        logging.info(f"file {script.filename} ran ok")

    def error_handler(script, exception):
        if exception.code in {404, 416} or exception.code in range(500, 600):
            script.mark_for_retry()
            action = "marked for retry"
        elif exception.code in {409}:
            script.mark_as_done()
            action = "marked as done"
        else:
            action = "marked as error"

        logging.error(
            f'error running {script.filename}, exception: {exception}.{" " + action if action else ""}'
        )

    # Execution
    start_time = datetime.datetime.utcnow()

    ran_scripts = ddl_utils.run_script_files(
        file_list=local_files,
        error_handler=error_handler,
        success_handler=success_handler,
        job_id_prefix=f"{unique_id}-",
    )

    # Metric collection
    def script_to_metric(script):
        return {
            "unique_id": config[CUSTOM_RUN_ID_KEY],
            "sql_file_name": script.filename[len(LOCAL_DATA_DIRECTORY) + 1 :],
            "job_id": script.get_job().job_id,
            "status": "success" if script.done() else "fail",
            "error_details": (
                str(script.get_job().exception()) if script.failed() else ""
            ),
            "execution_start_time": str(start_time),
            "gcs_source_path": config["migrationTask"]["translationConfigDetails"][
                "gcsSourcePath"
            ],
        }

    results = list(map(script_to_metric, ran_scripts))
    ti.xcom_push(key="results", value=results)

    aggregated_results = [
        {
            "unique_id": config[CUSTOM_RUN_ID_KEY],
            "total_files": len(ran_scripts),
            "successful_files": sum(map(ddl_utils.Script.done, ran_scripts)),
            "failed_files": sum(map(ddl_utils.Script.failed, ran_scripts)),
        }
    ]
    ti.xcom_push(key="aggregated_results", value=aggregated_results)


def _filter_tables(ti, dag_run):
    config = dag_run.conf["config"]
    files = ti.xcom_pull(task_ids="exec", key="results")
    filtered_tables = table_filter.filter(files, config)
    ti.xcom_push(key="filtered_tables", value=filtered_tables)


def _determine_validation_dag(dag_run):
    config = dag_run.conf["config"]
    validation_mode = config["validation_config"].get("validation_mode")
    validation_dag_id = get_validation_dag_id(validation_mode)
    if validation_dag_id == VALIDATION_DAG_ID:
        return "invoke_validation_dag"
    else:
        return "invoke_validation_crun_dag"


"""
DAG: Schema DAG to create BigQuery Schema from Ouput GCS Bucket SQL Translated files
     It trigger from sql_translation DAG and takes configuration details from kwargs(keyword arguments) variable
"""
with models.DAG(
    DAG_ID,
    schedule=None,
    default_args=default_dag_args,
    render_template_as_native_obj=True,
) as dag:
    create_dataset = PythonOperator(
        task_id="create_dataset",
        python_callable=_create_dataset,
        dag=dag,
    )

    exec_queries = PythonOperator(
        task_id="exec",
        python_callable=_execute_queries,
        dag=dag,
    )

    save_schema_result = PythonOperator(
        task_id="save_schema_creation_result",
        python_callable=_save_schema_creation_result,
        dag=dag,
    )

    filter_tables = PythonOperator(
        task_id="filtering_task",
        python_callable=_filter_tables,
        dag=dag,
    )

    determine_validation_dag = BranchPythonOperator(
        task_id="determine_validation_dag",
        python_callable=_determine_validation_dag,
        dag=dag,
    )

    invoke_validation_dag = TriggerDagRunOperator(
        task_id="invoke_validation_dag",
        trigger_dag_id="validation_dag",
        conf={
            "config": "{{ dag_run.conf['config'] }}",
            "files": "{{ ti.xcom_pull(task_ids='exec', key='results') }}",
            "table_list": "{{ ti.xcom_pull(task_ids='filtering_task', key='filtered_tables') }}",
        },
        dag=dag,
    )

    invoke_validation_crun_dag = TriggerDagRunOperator(
        task_id="invoke_validation_crun_dag",
        trigger_dag_id="validation_crun_dag",
        conf={
            "config": "{{ dag_run.conf['config'] }}",
            "files": "{{ ti.xcom_pull(task_ids='exec', key='results') }}",
            "table_list": "{{ ti.xcom_pull(task_ids='filtering_task', key='filtered_tables') }}",
        },
        dag=dag,
    )

    dag_report = ReportingOperator(
        task_id="dag_report",
        trigger_rule=TriggerRule.ALL_DONE,  # Ensures this task runs even if upstream fails
        configuration="{{ dag_run.conf['config'] }}",
        dag=dag,
    )

    # Dependency
    (
        create_dataset
        >> exec_queries
        >> [save_schema_result, filter_tables]
        >> determine_validation_dag
        >> [invoke_validation_dag, invoke_validation_crun_dag]
        >> dag_report
    )
