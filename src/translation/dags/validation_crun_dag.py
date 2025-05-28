import ast
import logging
import os
import re
from datetime import datetime
from http import HTTPStatus

import requests
from airflow import models
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.utils.trigger_rule import TriggerRule
from google.api_core.client_info import ClientInfo
from google.cloud import bigquery

from common_utils import custom_user_agent, storage_utils
from common_utils.operators.reporting_operator import ReportingOperator

project_id = os.environ.get("GCP_PROJECT_ID")
CUSTOMER_NAME = os.environ.get("CUSTOMER_NAME")
region = os.environ.get("REGION")

DESCRIBE_SERVICE = """
gcloud run services describe {service_name} --region={region} --project={project_id}
"""

gcs_util = storage_utils.StorageUtils()


def get_token():
    with os.popen("gcloud auth print-identity-token") as cmd:
        token = cmd.read().strip()
    return token


def get_cloud_run_url(service_name, project_id):
    describe_service = DESCRIBE_SERVICE.format(
        service_name=service_name, project_id=project_id, region=region
    )
    with os.popen(describe_service) as service:
        description = service.read()

    return re.findall("URL:.*\n", description)[0].split()[1].strip()


client = bigquery.Client(
    client_info=ClientInfo(user_agent=custom_user_agent.USER_AGENT)
)
DVT_AGGREGATED_RESULTS_TABLE_ID = f"{project_id}.dmt_logs.dmt_dvt_aggregated_results"

default_dag_args = {"start_date": datetime(2022, 1, 1)}


@task
def _get_table_or_file_list(input_json):
    file_or_table_list = []
    input_json = ast.literal_eval(str(input_json))
    config = ast.literal_eval(str(input_json["config"]))
    translation_type = config["type"]
    validation_type = config["validation_config"]["validation_type"]
    validation_only = config.get("validation_only", "no")
    validation_params_file_path = config["validation_config"][
        "validation_params_file_path"
    ]
    bucket_name, blob_name = gcs_util.parse_bucket_and_blob_from_path(
        validation_params_file_path
    )
    validation_params_from_gcs = gcs_util.get_validation_params_from_gcs(
        bucket_name, blob_name, translation_type, validation_type
    )
    common_request_json = {}
    common_request_json["config"] = config
    common_request_json["validation_params_from_gcs"] = validation_params_from_gcs
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
            request_json = common_request_json.copy()
            request_json["table"] = table
            file_or_table_list.append(request_json)
    elif translation_type == "sql":
        files = []
        if validation_only == "yes":
            for key in validation_params_from_gcs:
                files.append(key)
        else:
            files = input_json["files"]
        for file in files:
            request_json = common_request_json.copy()
            request_json["sql_file"] = file
            file_or_table_list.append(request_json)
    else:
        print(f"Unknown validation type: {translation_type}")
        raise AirflowFailException(f"Unknown translation type: {translation_type}")
    return file_or_table_list


@task
def _invoke_cloud_run(request_json):
    url = get_cloud_run_url(f"edw-dvt-tool-{CUSTOMER_NAME}", project_id)
    headers = {"Authorization": "Bearer " + get_token()}
    config = request_json["config"]
    validation_params_from_gcs = request_json["validation_params_from_gcs"]
    validation_type = config["type"]
    if validation_type in ["ddl", "data", "dml"]:
        table = request_json["table"]
        request_content = {
            "config": config,
            "table": table,
            "validation_params_from_gcs": validation_params_from_gcs,
        }
        print(f"Running validation for table: {table}")
    elif validation_type == "sql":
        sql_file = request_json["sql_file"]
        request_content = {
            "config": config,
            "sql_file": sql_file,
            "validation_params_from_gcs": validation_params_from_gcs,
        }
        print(f"Running validation for sql file: {sql_file}")
    else:
        print(f"Unknown validation type: {validation_type}")
        raise AirflowFailException("DVT CloudRun execution failed!")

    res = requests.post(url, headers=headers, json=request_content)
    print(f"Status Code received from DVT CloudRun: {res.status_code}")
    print("Received following response from DVT CloudRun")
    print(res.content.decode())
    if res.status_code == HTTPStatus.OK:
        print("Validation CloudRun execution successful")
    else:
        print(
            f"Validation CloudRun execution failed with status code: {res.status_code}"
        )
        raise AirflowFailException("DVT CloudRun execution failed!")


@task(trigger_rule="all_done")
def _save_dvt_aggregated_results(**kwargs):
    config = ast.literal_eval(str(kwargs["dag_run"].conf["config"]))
    unique_id = config["unique_id"]
    failed_validations_query = f"""
        SELECT COUNT(*) as failed_count FROM `{project_id}.dmt_logs.dmt_dvt_results` CROSS JOIN UNNEST(labels) AS a where a.value="{unique_id}" and validation_status="fail";
    """
    query_job = client.query(failed_validations_query)  # Make an API request
    failed_validations_count = [row["failed_count"] for row in query_job][0]
    successful_validations_query = f"""
        SELECT COUNT(*) as successful_count FROM `{project_id}.dmt_logs.dmt_dvt_results` CROSS JOIN UNNEST(labels) AS a where a.value="{unique_id}" and validation_status="success";
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


with models.DAG(
    "validation_crun_dag",
    schedule=None,
    default_args=default_dag_args,
):
    _save_dvt_aggregated_results = _save_dvt_aggregated_results()
    dag_report = ReportingOperator(
        task_id="dag_report",
        trigger_rule=TriggerRule.ALL_DONE,  # Ensures this task runs even if upstream fails
        configuration="{{ dag_run.conf['config'] }}",
    )

    (
        _invoke_cloud_run.partial().expand(
            request_json=(_get_table_or_file_list("{{ dag_run.conf }}"))
        )
        >> _save_dvt_aggregated_results
        >> dag_report
    )
