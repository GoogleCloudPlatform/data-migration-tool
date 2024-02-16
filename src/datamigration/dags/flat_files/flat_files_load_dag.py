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

import ast
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datamigration_utils.bq_load_utils import (
    get_job_status
)
from flat_files_utils.flat_files_load_utils import (
    audit_log_load_status,
    file_format_subcmd_2,
    invoke_dvt_dag
)

from common_utils.operators.reporting_operator import ReportingOperator
from common_utils import storage_utils

from google.cloud import storage
from google.api_core.client_info import ClientInfo
from common_utils import custom_user_agent
from pathlib import Path
import subprocess

storage_client = storage.Client(
    client_info=ClientInfo(user_agent=custom_user_agent.USER_AGENT)
)
gcs_util = storage_utils.StorageUtils()

FILE_FORMAT_LIST = ['parquet', 'csv', 'avro', 'orc']

def set_required_vars(ti, **kwargs):
    """
    TODO
    get bucket id from kwargs and then push as env variable
    Check parallelization_dag for reference
    """
    data_load_config = ast.literal_eval(kwargs["dag_run"].conf["config"])["config"]
    
    batch_distribution = data_load_config["batchDistribution"]
    Variable.set("batch_distribution", batch_distribution)

    ti.xcom_push(key="op_load_dtm_str", value=str(datetime.now()))
    ti.xcom_push(key="user_config", value=data_load_config)

def get_tables(ti):
    config = ti.xcom_pull(key="user_config", task_ids='set_required_vars')
    
    table_dict = []
    # Table dict contains table_name: '', database_name: '',  (in future we can add, Partition and other required params)
    gcs_data_bucket_name = config['transfer_config']['params']['gcs_staging_bucket_id']
    gcs_data_file_path = config['transfer_config']['params']['gcs_staging_path'] 
    # File path last directory should be source database name / schema name 
    source_schema_name = gcs_data_file_path.split('/')[-1]
    
    gcs_data_file_path_nest_len = len(gcs_data_file_path[:-1].split('/')) if gcs_data_file_path[-1] == "/" else len(gcs_data_file_path.split('/'))

    for blob in storage_client.list_blobs(gcs_data_bucket_name, prefix=gcs_data_file_path):
        if any(format in blob.name.lower() for format in FILE_FORMAT_LIST) and len(blob.name.split('/')) == gcs_data_file_path_nest_len+2:
            if blob.name.split('/')[len(blob.name.split('/'))-2] not in [table.get('table_name') for table in table_dict]:
                table_dict.append({
                    'table_name': blob.name.split('/')[len(blob.name.split('/'))-2],
                    'database_name': source_schema_name,
                    'format': blob.name.split('.')[1]
                })

    print(f'Data Migration Table list : {table_dict}')
    ti.xcom_push(key="table_list", value=table_dict)

    return [{'table':table.get('table_name')} for table in table_dict]

def truncate_or_drop_tbl(tbl, droptable, config_params):
    """
    DROP table if the format is PARQUET else TRUNCATE
    """
    if droptable == 0:
        print(f"\n\nTruncating table: {config_params['bq_dataset_id']}.{tbl}")
        truncate_cmd = f"bq query --use_legacy_sql=false --project_id={config_params['project_id']} 'truncate table {config_params['bq_dataset_id']}.{tbl}'"
        result = subprocess.run(
            truncate_cmd, capture_output=True, shell=True, encoding="utf-8"
        )
    else:
        print(f"Drop parquet table: {config_params['bq_dataset_id']}.{tbl}")
        drop_cmd = f"bq query --use_legacy_sql=false --project_id={config_params['project_id']} 'drop table {config_params['bq_dataset_id']}.{tbl}'"
        result = subprocess.run(
            drop_cmd, capture_output=True, shell=True, encoding="utf-8"
        )
    return result

def load_tables_using_bqload(table, ti, **kwargs):
    """
    Load BigQuery table using BQ load command
    """
    start = datetime.now()
    print(f"Loading Table: {table} Start Time: {str(start)}")

    config = ti.xcom_pull(key="user_config", task_ids='set_required_vars')
    
    op_load_dtm_str = ti.xcom_pull(key="op_load_dtm_str", task_ids='set_required_vars')
    
    op_run_id = config["unique_id"]
    
    table_list = ti.xcom_pull(key="table_list", task_ids='get_tables')
    table_obj = None
    for table_itm in table_list:
        if table_itm['table_name'] == table:
            table_obj = table_itm

    print(f'{table} details : {table_obj}')

    # TODO: Implement partition logic and get it details as an i/p
    partition_flag = 'N'  
    file_format = table_obj['format'].upper()

    field_delimiter = ''
    if file_format == "CSV":
        field_delimiter = ","

    gcs_data_bucket_name = config['transfer_config']['params']['gcs_staging_bucket_id']
    gcs_data_file_path = config['transfer_config']['params']['gcs_staging_path'] 
    table_gs_path = f"gs://{gcs_data_bucket_name}/{gcs_data_file_path}/{table}"

    # TODO filter out tables larger than 15 TB
    table_size_status = gcs_util.get_table_size(table, table_gs_path)
    if table_size_status:

        # TODO: Implement partition logic here
        if partition_flag == "N":
            pccmd = ""
            hive_partition_source_uri = ""
            print(f"Table {table} is not partitioned")

        (
            formatcmd,
            checkformat,
            droptable,
            text_tbl_schema_string,
        ) = file_format_subcmd_2(
            file_format, field_delimiter
        )
        if checkformat == 1:
            result = truncate_or_drop_tbl(table, droptable, config['transfer_config']['params'])
            bqloadcmd = (
                f"bq load --source_format={file_format} {formatcmd} "
                f"{pccmd} --project_id={config['transfer_config']['params']['project_id']} "
                f"{hive_partition_source_uri} {config['transfer_config']['params']['bq_dataset_id']}.{table} "
                f"{table_gs_path}/* "
                f"{text_tbl_schema_string}"
            )
            print(bqloadcmd)
            result = subprocess.run(
                bqloadcmd, capture_output=True, shell=True, encoding="utf-8"
            )
            load_status, reason_for_failure, bq_job_id = get_job_status(table, result)
            audit_log_load_status(
                table,
                load_status,
                reason_for_failure,
                bq_job_id,
                config['transfer_config']['params'],
                op_load_dtm_str,
                op_run_id,
            )
        else:
            print("Incorrect Source File Format for table: {}".format(table))
            audit_log_load_status(
                table,
                "FAIL",
                "Incorrect Source Format",
                "NA",
                config['transfer_config']['params'],
                op_load_dtm_str,
                op_run_id,
            )
        end = datetime.now()
        print("\nTotal Time Taken to load the table : {}".format(end - start))
    else:
        print(f"Skipping the table from load since it is more than 16TB: {table}")
        audit_log_load_status(
            table,
            "FAIL",
            "Cannot load table greater than 16TB",
            "NA",
            config,
            op_load_dtm_str,
            op_run_id,
        )

default_args = {
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "flat_files_load_dag",
    start_date=datetime(2021, 1, 1),
    max_active_runs=2,
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:
    
    set_required_vars = PythonOperator(
        task_id="set_required_vars",
        python_callable=set_required_vars,
        provide_context=True,
    )

    get_tables = PythonOperator(
        task_id="get_tables",
        python_callable=get_tables,
        provide_context=True,
    )

    load_tables_using_bqload = PythonOperator.partial(
        task_id="load_tables_using_bqload",
        python_callable=load_tables_using_bqload,
        max_active_tis_per_dag=int(Variable.get("batch_distribution", default_var=10)),
    ).expand(op_kwargs=get_tables.output)

    invoke_dvt_validation_dag = PythonOperator(
        task_id="invoke_dvt_dag",
        python_callable=invoke_dvt_dag,
        provide_context=True,
    )

    dag_report = ReportingOperator(
        task_id="dag_report",
        trigger_rule=TriggerRule.ALL_DONE,  # Ensures this task runs even if upstream fails
        configuration="{{ ti.xcom_pull(key='user_config', task_ids='set_required_vars') }}",
        dag=dag,
    )

    (
        set_required_vars
        >> get_tables
        >> load_tables_using_bqload
        >> invoke_dvt_validation_dag
        >> dag_report
    )
