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

import json
import random
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datamigration_utils.hive_bq_load_utils_inc import (
    copy_inc_files,
    get_inc_gcs_files,
    get_inc_table_list,
    get_inc_table_list_for_copy,
    get_partition_clustering_info,
    get_table_info_from_metadata,
    get_text_format_schema,
    load_bq_tables,
)
from google.cloud import storage

bq_audit_dataset_id = "bigshift_logs"
bq_pubsub_audit_tbl = "hive_pubsub_audit"


def read_config_file():
    """
    Read Config file for hive_inc load from config bucket
    hive_config_bucket_id variable will be set by the batch load job
    """
    hive_config_bucket_id = Variable.get("hive_config_bucket_id")
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(hive_config_bucket_id)
    blob = bucket.blob("inc_data/hive_inc_load_config.json")
    data = json.loads(blob.download_as_string(client=None))
    schedule_time_str = data["transfer_config"]["params"]["schedule_time_str"]
    schedule_time_cron = data["transfer_config"]["params"]["schedule_time_cron"]
    Variable.set("hive_inc_load_schedule_runtime_cron", schedule_time_cron)
    Variable.set("hive_inc_batchDistribution", data["batchDistribution"])
    date_str = str(date.today())
    hive_inc_load_schedule_runtime = f"{date_str} {schedule_time_str}:00.000+00:00"
    Variable.set("hive_inc_load_schedule_runtime", hive_inc_load_schedule_runtime)
    Variable.set("hive_inc_load_config", data)
    Variable.set("op_run_id", random.randint(0, 999999))
    Variable.set("op_load_dtm", datetime.now())
    return data


default_args = {
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "hive_inc_data_load_dag",
    start_date=datetime(2023, 1, 1),
    max_active_runs=2,
    schedule=Variable.get("hive_inc_load_schedule_runtime_cron", "0 22 * * *"),
    default_args=default_args,
    catchup=False,
) as dag:
    read_config_file = PythonOperator(
        task_id="read_config_file",
        python_callable=read_config_file,
    )

    get_inc_gcs_files = PythonOperator(
        task_id="get_inc_gcs_files",
        python_callable=get_inc_gcs_files,
        op_kwargs={
            "bq_audit_dataset_id": bq_audit_dataset_id,
            "bq_pubsub_audit_tbl": bq_pubsub_audit_tbl,
            "schedule_runtime": Variable.get("hive_inc_load_schedule_runtime", ""),
        },
    )

    get_inc_table_list_for_copy = PythonOperator(
        task_id="get_inc_table_list_for_copy",
        python_callable=get_inc_table_list_for_copy,
        op_kwargs={
            "config": Variable.get("hive_inc_load_config", default_var=""),
            "inc_gcs_files_list": get_inc_gcs_files.output,
        },
    )

    copy_inc_files = PythonOperator.partial(
        task_id="copy_inc_files",
        python_callable=copy_inc_files,
        max_active_tis_per_dag=int(Variable.get("hive_inc_batchDistribution", 10)),
        op_kwargs={
            "config_data": Variable.get("hive_inc_load_config", default_var=""),
            "job_run_time": str(datetime.now()),
        },
    ).expand(op_args=get_inc_table_list_for_copy.output)

    get_inc_table_list = PythonOperator(
        task_id="get_inc_table_list",
        python_callable=get_inc_table_list,
        op_kwargs={"config": Variable.get("hive_inc_load_config", default_var="")},
    )

    get_table_info_from_metadata = PythonOperator(
        task_id="get_table_info_from_metadata",
        python_callable=get_table_info_from_metadata,
        op_kwargs={
            "config": Variable.get("hive_inc_load_config", default_var=""),
            "bq_audit_dataset_id": bq_audit_dataset_id,
        },
    )

    get_text_format_schema = PythonOperator(
        task_id="get_text_format_schema",
        python_callable=get_text_format_schema,
        op_kwargs={"config": Variable.get("hive_inc_load_config", default_var="")},
    )

    get_partition_clustering_info = PythonOperator(
        task_id="get_partition_clustering_info",
        python_callable=get_partition_clustering_info,
        op_kwargs={"config": Variable.get("hive_inc_load_config", default_var="")},
    )

    load_bq_tables = PythonOperator.partial(
        task_id="load_bq_tables",
        python_callable=load_bq_tables,
        max_active_tis_per_dag=int(
            Variable.get("hive_inc_batchDistribution", default_var=10)
        ),
        op_kwargs={
            "config": Variable.get("hive_inc_load_config", default_var=""),
            "op_load_dtm": Variable.get("op_load_dtm", default_var=""),
            "op_run_id": Variable.get("op_run_id", default_var=""),
        },
    ).expand(op_args=get_partition_clustering_info.output)

    (
        read_config_file
        >> get_inc_gcs_files
        >> get_inc_table_list_for_copy
        >> copy_inc_files
        >> get_inc_table_list
        >> get_table_info_from_metadata
        >> get_text_format_schema
        >> get_partition_clustering_info
        >> load_bq_tables
    )
