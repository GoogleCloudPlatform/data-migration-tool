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
import datetime
import enum
import logging
import os

from airflow import models
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.providers.google.cloud.operators.cloud_composer import (
    CloudComposerGetEnvironmentOperator,
)
from airflow.utils.trigger_rule import TriggerRule
from google.api_core.client_info import ClientInfo
from google.cloud import bigquery
from kubernetes.client import models as k8s_models

from common_utils import custom_user_agent, storage_utils
from common_utils.operators.reporting_operator import ReportingOperator


class ValidationEntity(enum.Enum):
    Table = 1
    File = 2


project_id = os.environ.get("GCP_PROJECT_ID")

composer_k8s_cluster_region = os.environ.get("REGION")
composer_name = os.environ.get("COMPOSER_ENV")
dvt_image = os.environ.get("DVT_IMAGE")

client = bigquery.Client(
    client_info=ClientInfo(user_agent=custom_user_agent.USER_AGENT)
)
DVT_AGGREGATED_RESULTS_TABLE_ID = f"{project_id}.dmt_logs.dmt_dvt_aggregated_results"

gcs_util = storage_utils.StorageUtils()

# create source and target connection command,supports all type of connections,
# command option-val will taken as it is from config parameter key-val pair
create_connection = """data-validation connections add {source_conn_string} && \
        data-validation connections add {target_conn_string} && \
            data-validation connections list"""

# dvt schema validation command string
# stores result in <project_id>.dmt_logs.dmt_dvt_results BQ table (assumes it must be created)
schema_validation = "data-validation validate schema --source-conn {source_conn} --target-conn {target_conn} --tables-list {table} --bq-result-handler {project_id}.dmt_logs.dmt_dvt_results -l unique_id={unique_id},table_name={bq_table}"

# dvt column validation command string
# stores result in <project_id>.dmt_logs.dmt_dvt_results BQ table (assumes it must be created)
column_validation = "data-validation validate column --source-conn {source_conn} --target-conn {target_conn} --tables-list {table} --bq-result-handler {project_id}.dmt_logs.dmt_dvt_results -l unique_id={unique_id},table_name={bq_table}"

# dvt row validation command string
# stores result in <project_id>.dmt_logs.dmt_dvt_results BQ table (assumes it must be created)
row_validation = "data-validation validate row --source-conn {source_conn} --target-conn {target_conn} --tables-list {table} --bq-result-handler {project_id}.dmt_logs.dmt_dvt_results -l unique_id={unique_id},table_name={bq_table}"

copy_sql_files = 'gsutil cp "{source_gcs}/{sql_file}" "source_{sql_file}" & gsutil cp "{target_gcs}/{sql_file}" "target_{sql_file}"'

# dvt custom query column type command string
# stores result in <project_id>.dmt_logs.dmt_dvt_results BQ table (assumes it must be created)
custom_query_row_validation = 'data-validation validate custom-query row --source-conn {source_conn} --target-conn {target_conn} --source-query-file "source_{sql_file}" --target-query-file "target_{sql_file}" --bq-result-handler {project_id}.dmt_logs.dmt_dvt_results -l unique_id={unique_id},file_name={sql_file}'

custom_query_column_validation = 'data-validation validate custom-query column --source-conn {source_conn} --target-conn {target_conn} --source-query-file "source_{sql_file}" --target-query-file "target_{sql_file}" --bq-result-handler {project_id}.dmt_logs.dmt_dvt_results -l unique_id={unique_id},file_name={sql_file}'

schema_validation_supported_flags = [
    "filter-status",
    "exclusion-columns",
    "allow-list",
]
column_validation_supported_flags = [
    "filter-status",
    "filters",
    "count",
    "sum",
    "min",
    "max",
    "avg",
    "grouped-columns",
    "wildcard-include-string-len",
    "cast-to-bigint",
    "threshold",
]
row_validation_supported_flags = [
    "filter-status",
    "filters",
    "primary-keys",
    "hash",
    "concat",
    "use-random-row",
    "random-row-batch-size",
    "comparison-fields",
]
boolean_validation_flags = [
    "use-random-row",
    "wildcard-include-string-len",
    "cast-to-bigint",
]

validation_type_flags_mapping = {
    "schema": schema_validation_supported_flags,
    "column": column_validation_supported_flags,
    "row": row_validation_supported_flags,
}

default_dag_args = {"start_date": datetime.datetime(2022, 1, 1)}


def _get_config(ti, **kwargs):
    config = ast.literal_eval(str(kwargs["dag_run"].conf["config"]))
    ti.xcom_push(key="config", value=config)


def get_additional_validation_flags(
    validation_entity_type,
    validation_entity_name,
    validation_flags_list,
    validation_params_from_gcs,
):
    additonal_validation_flags = ""
    if validation_entity_type == ValidationEntity.Table:
        validation_entity_name = validation_entity_name.split("=")[0]
    validation_params = validation_params_from_gcs[validation_entity_name]
    for flag, flag_value in validation_params.items():
        if flag in validation_flags_list:
            if flag not in boolean_validation_flags and flag_value != "":
                additonal_validation_flags += f" --{flag} {flag_value}"
            elif flag in boolean_validation_flags and flag_value == "Y":
                additonal_validation_flags += f" --{flag}"
    return additonal_validation_flags


def pod_mem(config):
    pod_operator_mem = config["validation_config"].get("pod_operator_mem", "4000M")
    return pod_operator_mem


def pod_cpu(config):
    pod_operator_cpu = config["validation_config"].get("pod_operator_cpu", "1000m")
    return pod_operator_cpu


def connection_string(conn_config):
    conn_string = ""
    for key, val in conn_config:
        val = str(val)
        if key == "source_type" or key == "target_type":
            conn_type = val
            conn_name = val + "_CONN"
            continue
        conn_string = conn_string + f"--{key} '{val}' "
    if conn_type != "BigQuery":
        conn_string = (
            f"--secret-manager-type GCP --secret-manager-project-id {project_id} --connection-name {conn_name} {conn_type} "
            + conn_string
        )
    else:
        conn_string = f"--connection-name {conn_name} {conn_type} " + conn_string
    return conn_name, conn_string


def _save_dvt_aggregated_results(ti):
    config = ti.xcom_pull(key="config", task_ids="get_config")
    unique_id = config["unique_id"]
    failed_validations_query = f"""
        SELECT COUNT(*) as failed_count FROM\
            `{project_id}.dmt_logs.dmt_dvt_results` CROSS JOIN UNNEST(labels)\
                AS a where a.value="{unique_id}" and validation_status="fail";
    """
    query_job = client.query(failed_validations_query)
    failed_validations_count = [row["failed_count"] for row in query_job][0]
    successful_validations_query = f"""
        SELECT COUNT(*) as successful_count FROM\
            `{project_id}.dmt_logs.dmt_dvt_results` CROSS JOIN UNNEST(labels)\
                AS a where a.value="{unique_id}"\
                    and validation_status="success";
    """
    query_job = client.query(successful_validations_query)  # Make an API request
    successful_validations_count = [row["successful_count"] for row in query_job][0]
    total_validations_count = failed_validations_count + successful_validations_count
    op_type = config["type"]
    if op_type in ["ddl", "data", "dml"]:
        validation_type = config["validation_config"]["validation_type"]
    elif op_type == "sql":
        validation_type = (
            "custom query " + config["validation_config"]["validation_type"]
        )
    aggregated_results = [
        {
            "unique_id": unique_id,
            "validation_type": validation_type,
            "total_validations": total_validations_count,
            "successful_validations": successful_validations_count,
            "failed_validations": failed_validations_count,
        }
    ]
    if aggregated_results == []:
        logging.info("DVT Aggregate Stats are empty.")
    else:
        client.insert_rows_json(DVT_AGGREGATED_RESULTS_TABLE_ID, aggregated_results)


def get_dvt_cmd_ddl_validation(config, table, validation_params_from_gcs):
    cmd_errors = ""
    logging.info(f"Running validation for table: {table}")

    source_conn, source_conn_string = connection_string(
        config["validation_config"]["source_config"].items()
    )
    target_conn, target_conn_string = connection_string(
        config["validation_config"]["target_config"].items()
    )

    add_conn = create_connection.format(
        source_conn_string=source_conn_string, target_conn_string=target_conn_string
    )

    validation_type = config["validation_config"]["validation_type"]

    if validation_type == "schema":
        validation_command = schema_validation.format(
            source_conn=source_conn,
            target_conn=target_conn,
            table=table,
            bq_table=table.split("=")[-1],
            project_id=project_id,
            unique_id=config["unique_id"],
        )
    elif validation_type == "column":
        validation_command = column_validation.format(
            source_conn=source_conn,
            target_conn=target_conn,
            table=table,
            bq_table=table.split("=")[-1],
            project_id=project_id,
            unique_id=config["unique_id"],
        )
    elif validation_type == "row":
        validation_command = row_validation.format(
            source_conn=source_conn,
            target_conn=target_conn,
            table=table,
            bq_table=table.split("=")[-1],
            project_id=project_id,
            unique_id=config["unique_id"],
        )

    additional_validation_flags = get_additional_validation_flags(
        ValidationEntity.Table,
        table,
        validation_type_flags_mapping[validation_type],
        validation_params_from_gcs,
    )
    validation_command += additional_validation_flags
    dvt_bash = " ) && ( ".join(["( " + add_conn, validation_command + " )"])
    return dvt_bash, cmd_errors


def get_dvt_cmd_sql_validation(config, sql_file, validation_params_from_gcs):
    cmd_errors = ""
    logging.info(f"Running validation for sql file: {sql_file}")

    source_conn, source_conn_string = connection_string(
        config["validation_config"]["source_config"].items()
    )
    target_conn, target_conn_string = connection_string(
        config["validation_config"]["target_config"].items()
    )

    add_conn = create_connection.format(
        source_conn_string=source_conn_string, target_conn_string=target_conn_string
    )

    translated_config = config["migrationTask"]["translationConfigDetails"]
    source_gcs = translated_config["gcsSourcePath"]
    target_gcs = translated_config["gcsTargetPath"]
    copy_sql_command = copy_sql_files.format(
        source_gcs=source_gcs, target_gcs=target_gcs, sql_file=sql_file
    )
    custom_query_validation_type = config["validation_config"]["validation_type"]

    if custom_query_validation_type == "row":
        custom_validation_command = custom_query_row_validation.format(
            source_conn=source_conn,
            target_conn=target_conn,
            sql_file=sql_file,
            project_id=project_id,
            unique_id=config["unique_id"],
        )
    elif custom_query_validation_type == "column":
        custom_validation_command = custom_query_column_validation.format(
            source_conn=source_conn,
            target_conn=target_conn,
            sql_file=sql_file,
            project_id=project_id,
            unique_id=config["unique_id"],
        )
    else:
        print(
            f"Unknown validation type: {custom_query_validation_type} passed with config['type'] == 'sql'!"
        )
        cmd_errors += f"Unknown validation type: {custom_query_validation_type} passed with config['type'] == 'sql'!"
        raise AirflowFailException(cmd_errors)

    additional_validation_flags = get_additional_validation_flags(
        ValidationEntity.File,
        sql_file,
        validation_type_flags_mapping[custom_query_validation_type],
        validation_params_from_gcs,
    )
    custom_validation_command += additional_validation_flags
    dvt_bash = " ) && ( ".join(
        ["( " + add_conn, copy_sql_command, custom_validation_command + " )"]
    )
    return dvt_bash, cmd_errors


@task
def parallelize_dvt_tasks(input_json):
    bash_cmds_list = []
    config = ast.literal_eval(str(input_json["config"]))
    translation_type = config["type"]
    validation_type = config["validation_config"]["validation_type"]
    validation_only = config["validation_only"]
    validation_params_file_path = config["validation_config"][
        "validation_params_file_path"
    ]
    bucket_name, blob_name = gcs_util.parse_bucket_and_blob_from_path(
        validation_params_file_path
    )
    validation_params_from_gcs = gcs_util.get_validation_params_from_gcs(
        bucket_name, blob_name, translation_type, validation_type
    )
    if translation_type in ["ddl", "data", "dml"]:
        table_list = []
        if validation_only == "yes":
            for key in validation_params_from_gcs:
                source_table = validation_params_from_gcs[key]["source-table"]
                target_table = validation_params_from_gcs[key]["target-table"]
                table_list.append(source_table + "=" + target_table)
        else:
            table_list = input_json["table_list"]

        for table in table_list:
            bash_cmd = get_dvt_cmd_ddl_validation(
                config, table, validation_params_from_gcs
            )
            bash_cmds_list.append(bash_cmd)
    elif translation_type == "sql":
        files = []
        if validation_only == "yes":
            for key in validation_params_from_gcs:
                files.append(key)
        else:
            files = input_json["files"]
        for file in files:
            bash_cmd = get_dvt_cmd_sql_validation(
                config, file, validation_params_from_gcs
            )
            bash_cmds_list.append(bash_cmd)
    else:
        print(f"Unknown translation type: {translation_type}")
    return bash_cmds_list


with models.DAG(
    "validation_dag",
    schedule_interval=None,
    default_args=default_dag_args,
    render_template_as_native_obj=True,
    user_defined_macros={
        "pod_mem": pod_mem,
        "pod_cpu": pod_cpu,
    },
) as dag:
    get_env = CloudComposerGetEnvironmentOperator(
        task_id="get_env",
        project_id=project_id,
        region=composer_k8s_cluster_region,
        environment_id=composer_name,
        dag=dag,
    )
    config_change = PythonOperator(
        task_id="get_config",
        python_callable=_get_config,
        dag=dag,
    )
    dvt_validation = KubernetesPodOperator.partial(
        task_id="ex-dvt",
        name="ex-dvt",
        max_active_tis_per_dag=3,
        namespace="default",
        image=dvt_image,
        cmds=["bash", "-cx"],
        service_account_name="sa-k8s",
        container_resources=k8s_models.V1ResourceRequirements(
            limits={
                "memory": "{{ pod_mem(ti.xcom_pull(key='config', task_ids='get_config')) }}",
                "cpu": "{{ pod_cpu(ti.xcom_pull(key='config', task_ids='get_config')) }}",
            }
        ),
        startup_timeout_seconds=1800,
        get_logs=True,
        image_pull_policy="Always",
        config_file="/home/airflow/composer_kube_config",
        dag=dag,
    ).expand(arguments=(parallelize_dvt_tasks("{{ dag_run.conf }}")))

    save_dvt_aggregated_results = PythonOperator(
        task_id="save_dvt_aggregated_results",
        python_callable=_save_dvt_aggregated_results,
        trigger_rule="all_done",
        dag=dag,
    )
    dag_report = ReportingOperator(
        task_id="dag_report",
        trigger_rule=TriggerRule.ALL_DONE,  # Ensures this task runs even if upstream fails
        configuration="{{ dag_run.conf['config'] }}",
        dag=dag,
    )

    (
        config_change
        >> get_env
        >> dvt_validation
        >> save_dvt_aggregated_results
        >> dag_report
    )
