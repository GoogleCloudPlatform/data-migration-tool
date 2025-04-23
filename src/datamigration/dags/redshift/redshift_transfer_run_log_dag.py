import ast
import json
import logging
import os
import re
from datetime import datetime

from airflow import models
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from datamigration_utils import bq_result_tbl_utils, dts_logs_utils
from googleapiclient import _auth as auth
from googleapiclient.errors import HttpError

from common_utils import discovery_util, storage_utils, table_filter
from common_utils.bigquery_client_utils import utils as bq_utils
from common_utils.operators.reporting_operator import ReportingOperator

custom_user_agent_http = auth.authorized_http(auth.default_credentials())
bq_data_transfer_client = discovery_util.build_from_document_with_custom_http(
    "https://bigquerydatatransfer.googleapis.com/$discovery/rest?version=v1",
    custom_user_agent_http,
)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BQ_LOGGING_DATASET_NAME = "dmt_logs"
BQ_TRANSFER_SUMMARY_TABLE_NAME = (
    f"{PROJECT_ID}.{BQ_LOGGING_DATASET_NAME}.dmt_redshift_transfer_run_summary"
)
BQ_TRANSFER_JOB_STATS_TABLE_NAME = (
    f"{PROJECT_ID}.{BQ_LOGGING_DATASET_NAME}.dmt_redshift_transfer_run_jobwise_details"
)
BQ_TRANSFER_TRACKING_TABLE_NAME = (
    f"{PROJECT_ID}.{BQ_LOGGING_DATASET_NAME}.dmt_redshift_transfer_tracking"
)
LOGGING_BUCKET_ID_PREFIX = "dmt-redshift-datamigration-logging"

VALIDATION_GKE_TYPE = "gke"
VALIDATION_CRUN_TYPE = "cloudrun"
VALIDATION_DEFAULT_TYPE = VALIDATION_GKE_TYPE
VALIDATION_TYPE_TO_DAG_ID_MAPPING = {
    VALIDATION_CRUN_TYPE: "validation_crun_dag",
    VALIDATION_GKE_TYPE: "validation_dag",
}
VALIDATION_DAG_ID = "validation_dag"
VALIDATION_CRUN_DAG_ID = "validation_crun_dag"
default_dag_args = {"start_date": datetime(2022, 1, 1)}

gcs_util = storage_utils.StorageUtils()


def _load_parameters(ti, **kwargs) -> None:
    """
    Load necessary variables for downstream tasks
    """
    if kwargs["dag_run"].conf is not None:
        full_run_id = ast.literal_eval(kwargs["dag_run"].conf["config"])[
            "TRANSFER_RUN_ID"
        ]
        run_state = ast.literal_eval(kwargs["dag_run"].conf["config"])[
            "TRANSFER_RUN_STATE"
        ]

    else:
        logging.error(
            "Method _load_parameters: full_run_id not provided in DAG conf.\n \
            Please provide full_run_id in form 'projects/{projectId}/transferConfigs/{configId}/runs/{run_id}'\n \
            or 'projects/{projectId}/locations/{locationId}/transferConfigs/{configId}/runs/{run_id}'"
        )
        raise Exception(
            "Method _load_parameters: full_run_id not provided in DAG conf.\n \
            Please provide full_run_id in form 'projects/{projectId}/transferConfigs/{configId}/runs/{run_id}'\n \
            or 'projects/{projectId}/locations/{locationId}/transferConfigs/{configId}/runs/{run_id}'"
        )

    (
        _,
        _,
        transfer_config_id,
        transfer_run_id,
    ) = dts_logs_utils.parse_full_transfer_runID(full_run_id)
    tracking_info = dts_logs_utils.get_tracking_info(
        transfer_config_id, BQ_TRANSFER_TRACKING_TABLE_NAME
    )
    (unique_id, config_bucket_id, config_object_path,) = (
        tracking_info["unique_id"],
        tracking_info["config_bucket_id"],
        tracking_info["config_object_path"],
    )
    data_transfer_config_json = gcs_util.read_object_from_gcsbucket(
        config_bucket_id, config_object_path
    )

    job_stats_json = bq_result_tbl_utils.get_dts_run_job_stats_template(
        unique_id=unique_id,
        transfer_config_id=transfer_config_id,
        transfer_run_id=transfer_run_id,
    )
    dts_run_summary_json = bq_result_tbl_utils.get_dts_run_summary_template(
        unique_id=unique_id,
        transfer_config_id=transfer_config_id,
        transfer_run_id=transfer_run_id,
    )

    ti.xcom_push(key="unique_id", value=unique_id)
    ti.xcom_push(key="transfer_config_id", value=transfer_config_id)
    ti.xcom_push(key="transfer_run_id", value=transfer_run_id)
    ti.xcom_push(key="full_run_id", value=full_run_id)
    ti.xcom_push(key="run_state", value=run_state)
    ti.xcom_push(key="job_stats_json", value=job_stats_json)
    ti.xcom_push(key="dts_run_summary_json", value=dts_run_summary_json)
    ti.xcom_push(key="data_transfer_config_json", value=data_transfer_config_json)


def _get_transfer_run_summary(ti) -> None:
    """
    Calls DTS API to get transfer run summary, uses projects.transferConfigs.runs.get
    """
    transfer_config_id = ti.xcom_pull(
        key="transfer_config_id", task_ids="load_parameters"
    )
    run_id = ti.xcom_pull(key="transfer_run_id", task_ids="load_parameters")
    full_run_id = ti.xcom_pull(key="full_run_id", task_ids="load_parameters")
    unique_id = ti.xcom_pull(key="unique_id", task_ids="load_parameters")
    dts_run_summary_json = ti.xcom_pull(
        key="dts_run_summary_json", task_ids="load_parameters"
    )

    try:
        transfer_summary = (
            bq_data_transfer_client.projects()
            .locations()
            .transferConfigs()
            .runs()
            .get(name=full_run_id)
            .execute()
        )
        logging.info(
            f"Method _get_transfer_run_summary: received transfer run summary as {transfer_summary}"
        )
        dts_run_summary_json["unique_id"] = unique_id
        dts_run_summary_json["transfer_config_id"] = transfer_config_id
        dts_run_summary_json["transfer_run_id"] = run_id
        dts_run_summary_json["transfer_run_status"] = transfer_summary["state"]
        dts_run_summary_json["error_message"] = transfer_summary["errorStatus"].get(
            "message", None
        )
        dts_run_summary_json["start_time"] = transfer_summary["startTime"]
        dts_run_summary_json["end_time"] = transfer_summary["endTime"]
        ti.xcom_push(key="dts_run_summary_json", value=dts_run_summary_json)
    except HttpError as err:
        logging.error("Error while requesting transfer run summary")
        logging.exception(err)
        raise err


def get_transfer_run_logs(
    full_run_id: str,
    next_page_token=None,
    page_size: int = 1000,
    page_iteration: int = 1,
    transfer_logs=[],
):
    """
    Calls DTS API to get transfer run logs
    """

    if page_iteration != 1 and next_page_token is None:
        logging.info(
            f"Method get_transfer_run_logs page {page_iteration}: skipping the request as no more logs to fetch. "
        )
        return transfer_logs

    try:
        response = (
            bq_data_transfer_client.projects()
            .locations()
            .transferConfigs()
            .runs()
            .transferLogs()
            .list(parent=full_run_id, pageSize=page_size, pageToken=next_page_token)
            .execute()
        )
        logging.info(
            f"Method get_transfer_run_logs page {page_iteration}: request sent to fetch transfer logs for \
            transfer_run_id {full_run_id}"
        )
        logs = response["transferMessages"]
        transfer_logs.extend(logs)
        page_token = response.get("nextPageToken", None)
        return get_transfer_run_logs(
            full_run_id,
            next_page_token=page_token,
            page_size=page_size,
            page_iteration=page_iteration + 1,
            transfer_logs=transfer_logs,
        )
    except HttpError as err:
        logging.error("Error requesting transfer run log")
        logging.exception(err)
        raise err


def _get_transfer_run_logs(ti) -> None:
    """
    Calls relevant function which calls DTS API to get transfer run logs
    """
    full_run_id = ti.xcom_pull(key="full_run_id", task_ids="load_parameters")
    transfer_run_id = ti.xcom_pull(key="transfer_run_id", task_ids="load_parameters")
    transfer_run_logs = get_transfer_run_logs(full_run_id, page_size=10)
    if not transfer_run_logs:
        raise Exception(
            f"Method _get_transfer_run_logs: failed to retrieve transfer run logs for \
            transfer_run_id {transfer_run_id}"
        )
    ti.xcom_push(key="dts_run_logs", value=transfer_run_logs)


def get_logging_bucket_name():
    bucket_id = LOGGING_BUCKET_ID_PREFIX
    cust_name = os.environ.get("CUSTOMER_NAME")
    bucket_id = f"{bucket_id}-{cust_name}"
    logging.info(
        f"Method get_logging_bucket_name: retrieved logging bucket name as {bucket_id}"
    )
    return bucket_id


def _create_transfer_log_GCS(ti) -> None:
    """
    Creates DTS run log file in cloud storage bucket
    """
    bucket_id = get_logging_bucket_name()
    unique_id = ti.xcom_pull(key="unique_id", task_ids="load_parameters")
    transfer_run_id = ti.xcom_pull(key="transfer_run_id", task_ids="load_parameters")
    dts_run_logs = ti.xcom_pull(key="dts_run_logs", task_ids="get_transfer_run_logs")
    log_file_name = f"{unique_id}/{transfer_run_id}.json"
    formatted_logs = "\n".join(json.dumps(log) for log in dts_run_logs)
    log_file_path = gcs_util.write_object_in_gcsbucket(
        bucket_id, log_file_name, formatted_logs
    )
    logging.info(
        f"Method _create_transfer_log_GCS: written transfer run logs in GCS path: {log_file_path}"
    )
    ti.xcom_push(key="dts_log_file_path", value=log_file_path)


def _process_transfer_logs(ti) -> None:
    """
    Processes DTS logs and extracts relevant information for tables' migration
    """
    transfer_config_id = ti.xcom_pull(
        key="transfer_config_id", task_ids="load_parameters"
    )
    transfer_run_id = ti.xcom_pull(key="transfer_run_id", task_ids="load_parameters")
    unique_id = ti.xcom_pull(key="unique_id", task_ids="load_parameters")
    dts_run_summary_json = ti.xcom_pull(
        key="dts_run_summary_json", task_ids="get_transfer_run_summary"
    )
    job_stats_json_template = ti.xcom_pull(
        key="job_stats_json", task_ids="load_parameters"
    )
    dts_run_logs = ti.xcom_pull(key="dts_run_logs", task_ids="get_transfer_run_logs")

    job_stats_json_template["unique_id"] = unique_id
    job_stats_json_template["transfer_run_id"] = transfer_run_id
    job_stats_json_template["transfer_config_id"] = transfer_config_id

    job_stats_jsons = {}

    # TODO: add parsing logic for empty table migration
    # TODO: shorten error messages before inserting to bigquery
    for log_dict in dts_run_logs:
        log_message = log_dict["messageText"]
        try:
            # Parsing error messages
            if log_dict["severity"] == "ERROR":
                job_stats_json_template["job_status"] = "FAILED"
                if log_message.__contains__("Job"):
                    job_msg_match = re.match(r"Job (.*) \(table (.*?)\).*", log_message)
                    if job_msg_match:
                        table_name = job_msg_match.group(2)
                        if table_name not in job_stats_jsons:
                            job_stats_jsons[table_name] = job_stats_json_template.copy()
                        job_stats_jsons[table_name]["src_table_name"] = table_name
                        job_stats_jsons[table_name]["bq_job_id"] = job_msg_match.group(
                            1
                        )
                        job_stats_jsons[table_name]["message"] = (
                            job_stats_jsons[table_name]["message"] + " " + log_message
                        )
                else:
                    if not dts_run_summary_json.get("error_message"):
                        dts_run_summary_json["error_message"] = log_message
                continue

            elif log_dict["severity"] == "INFO":
                job_stats_json_template["job_status"] = "SUCCEEDED"

                if log_message.__contains__("Transfer load"):
                    run_date_match = re.match("Transfer.* ([0-9]{8})", log_message)
                    run_date = run_date_match.group(1)
                    dts_run_summary_json["run_date"] = run_date
                    continue

                # Done only to map source table_name with target table name
                # elif log_message.__contains__("data will be appended to existing table"):
                #     # For target table name
                #     target_summary_match = re.match(
                #         r"Table (.*) already exists in (.*);",
                #         log_message,
                #     )
                #     target_dataset_name = target_summary_match.group(2).split(".")[1]
                #     target_table_name = (
                #         target_dataset_name + "." + target_summary_match.group(1)
                #     )
                #     logging.info(target_table_name)
                #     continue

                # Done
                elif log_message.__contains__("Number of records"):
                    job_summary_match = re.match(
                        r"Job (.*) \(table (.*)\) .* records: (\d*),.* (\d*).",
                        log_message,
                    )
                    if job_summary_match:
                        table_name = job_summary_match.group(2)
                        if table_name not in job_stats_jsons:
                            job_stats_jsons[table_name] = job_stats_json_template.copy()
                        job_stats_jsons[table_name]["src_table_name"] = table_name
                        job_stats_jsons[table_name][
                            "bq_job_id"
                        ] = job_summary_match.group(1)
                        job_stats_jsons[table_name][
                            "success_records"
                        ] = job_summary_match.group(3)
                        job_stats_jsons[table_name][
                            "error_records"
                        ] = job_summary_match.group(4)
                    continue
                # Done
                elif log_message.__contains__("Summary:"):
                    run_summary_match = re.match(
                        r"^Summary: succeeded (\d*).*failed (\d*).*", log_message
                    )
                    dts_run_summary_json["succeeded_jobs"] = run_summary_match.group(1)
                    dts_run_summary_json["failed_jobs"] = run_summary_match.group(2)
                    continue

        except Exception as e:
            logging.error(
                f"Method _process_transfer_logs: failed to extract value from log \
                \n\tlog line: {log_message}\n\terror: {e}"
            )

    job_stats_json_rows = []
    for row_dict in job_stats_jsons.values():
        row_dict["run_date"] = run_date

        if not row_dict["message"]:
            row_dict["transfer_run_state"] = "SUCCEEDED"
        else:
            if row_dict["message"].__contains__("Skipping"):
                row_dict["transfer_run_state"] = "SKIPPED"
            else:
                row_dict["transfer_run_state"] = "FAILED"

        if row_dict["src_table_name"]:
            job_stats_json_rows.append(row_dict)

    logging.info(
        f"Method _process_transfer_logs: extracted {len(job_stats_json_rows)} transfer run job rows from logs"
    )

    ti.xcom_push(key="dts_run_summary_json", value=dts_run_summary_json)
    ti.xcom_push(key="job_stats_json_rows", value=job_stats_json_rows)


def _insert_bq_transfer_log_results(ti) -> None:
    """
    Inserts DTS run relevant information in bigquery dmt_logs tables
    """
    dts_run_summary_row = ti.xcom_pull(
        key="dts_run_summary_json", task_ids="process_transfer_logs"
    )
    job_stats_json_rows = ti.xcom_pull(
        key="job_stats_json_rows", task_ids="process_transfer_logs"
    )
    dts_log_file_path = ti.xcom_pull(
        key="dts_log_file_path", task_ids="create_transfer_log_GCS"
    )
    dts_run_summary_row["gcs_log_path"] = dts_log_file_path

    bq_utils.insert_bq_json_rows(BQ_TRANSFER_SUMMARY_TABLE_NAME, [dts_run_summary_row])
    if not job_stats_json_rows:
        logging.info(
            "Method _insert_bq_transfer_log_results: no job stats rows to populate. "
        )
    else:
        bq_utils.insert_bq_json_rows(
            BQ_TRANSFER_JOB_STATS_TABLE_NAME, job_stats_json_rows
        )


def _filter_tables(ti, **kwargs):
    data_transfer_config_json = ti.xcom_pull(
        task_ids="load_parameters", key="data_transfer_config_json"
    )
    translation_type = data_transfer_config_json["type"]
    validation_type = data_transfer_config_json["validation_config"]["validation_type"]
    validation_params_file_path = data_transfer_config_json["validation_config"][
        "validation_params_file_path"
    ]
    bucket_name, blob_name = gcs_util.parse_bucket_and_blob_from_path(
        validation_params_file_path
    )
    validation_params_from_gcs = gcs_util.get_validation_params_from_gcs(
        bucket_name, blob_name, translation_type, validation_type
    )

    config_table_mapping = []
    for source_table, source_table_params in validation_params_from_gcs.items():
        config_table_mapping.append(
            f"{source_table}={source_table_params['target-table']}"
        )

    src_schema = ast.literal_eval(kwargs["dag_run"].conf["config"])["SOURCE_SCHEMA"]
    logging.info(
        f"Method _filter_tables: received SOURCE_SCHEMA as {src_schema} from DAG config"
    )
    tables = ast.literal_eval(kwargs["dag_run"].conf["config"])["TABLE_NAME_PATTERN"]
    logging.info(
        f"Method _filter_tables: received TABLE_NAME_PATTERN as {tables} from DAG config"
    )
    valid_comparisons_list = table_filter.filter_valid_table_mappings(
        config_table_mapping, tables, src_schema
    )
    ti.xcom_push(key="dvt_table_list", value=valid_comparisons_list)


def _check_transfer_run_state(ti):
    run_state = ti.xcom_pull(task_ids="load_parameters", key="run_state")
    logging.info(f"TRANSFER_RUN_STATE: {run_state}")
    if run_state == "SUCCEEDED":
        return "filter_tables_for_dvt"
    else:
        logging.info("Skipping Validation")
        return "skip_validation_dag"


def _check_filtered_tables(ti):
    valid_comparisons_list = ti.xcom_pull(
        task_ids="filter_tables_for_dvt", key="dvt_table_list"
    )
    if not valid_comparisons_list:
        # if empty list, skip calling validation dag and call dummy task (to mark end of DAG's functionality)
        logging.info(
            "Method _check_filtered_tables: no valid dvt comparisons list found"
        )
        return "skip_validation_dag"
    else:
        # if non-empty list, call validation dag and skip invoking dummy task
        logging.info(
            f"Method _check_filtered_tables: valid dvt comparisons list - {valid_comparisons_list}"
        )
        return "determine_validation_dag"


def get_validation_dag_id(validation_mode):
    if validation_mode in VALIDATION_TYPE_TO_DAG_ID_MAPPING:
        return VALIDATION_TYPE_TO_DAG_ID_MAPPING[validation_mode]
    else:
        return VALIDATION_TYPE_TO_DAG_ID_MAPPING[VALIDATION_DEFAULT_TYPE]


def _determine_validation_dag(ti, **kwargs):
    config = ti.xcom_pull(task_ids="load_parameters", key="data_transfer_config_json")
    validation_mode = config["validation_config"].get("validation_mode")
    validation_dag_id = get_validation_dag_id(validation_mode)
    if validation_dag_id == VALIDATION_DAG_ID:
        return "invoke_validation_dag"
    else:
        return "invoke_validation_crun_dag"


with models.DAG(
    "redshift_transfer_run_log_dag",
    schedule=None,
    default_args=default_dag_args,
    render_template_as_native_obj=True,
) as dag:
    # tasks
    load_parameters = PythonOperator(
        task_id="load_parameters",
        python_callable=_load_parameters,
        dag=dag,
    )
    get_dts_run_summary = PythonOperator(
        task_id="get_transfer_run_summary",
        python_callable=_get_transfer_run_summary,
        dag=dag,
    )
    get_dts_run_logs = PythonOperator(
        task_id="get_transfer_run_logs",
        python_callable=_get_transfer_run_logs,
        dag=dag,
    )
    create_transfer_log_GCS = PythonOperator(
        task_id="create_transfer_log_GCS",
        python_callable=_create_transfer_log_GCS,
        dag=dag,
    )
    process_transfer_logs = PythonOperator(
        task_id="process_transfer_logs",
        python_callable=_process_transfer_logs,
        dag=dag,
    )
    insert_bq_transfer_log_results = PythonOperator(
        task_id="insert_bq_transfer_log_results",
        python_callable=_insert_bq_transfer_log_results,
        dag=dag,
    )

    filter_tables = PythonOperator(
        task_id="filter_tables_for_dvt",
        python_callable=_filter_tables,
        dag=dag,
    )
    # check transfer_run_state and decide whether to continue validation or not
    check_transfer_run_state = BranchPythonOperator(
        task_id="check_transfer_run_state",
        python_callable=_check_transfer_run_state,
        dag=dag,
    )
    # check filtered tables' list and decide whether to invoke validation DAG or not
    check_filtered_tables = BranchPythonOperator(
        task_id="check_filtered_tables",
        python_callable=_check_filtered_tables,
        dag=dag,
    )
    skip_validation_dag = EmptyOperator(task_id="skip_validation_dag", dag=dag)

    determine_validation_dag = BranchPythonOperator(
        task_id="determine_validation_dag",
        python_callable=_determine_validation_dag,
        dag=dag,
    )

    call_validation_dag = TriggerDagRunOperator(
        task_id="invoke_validation_dag",
        trigger_dag_id="validation_dag",
        conf={
            "config": "{{ ti.xcom_pull(task_ids='load_parameters', key='data_transfer_config_json') }}",
            "files": None,
            "table_list": "{{ ti.xcom_pull(task_ids='filter_tables_for_dvt', key='dvt_table_list') }}",
        },
        dag=dag,
    )

    call_validation_crun_dag = TriggerDagRunOperator(
        task_id="invoke_validation_crun_dag",
        trigger_dag_id="validation_crun_dag",
        conf={
            "config": "{{ ti.xcom_pull(task_ids='load_parameters', key='data_transfer_config_json') }}",
            "files": None,
            "table_list": "{{ ti.xcom_pull(task_ids='filter_tables_for_dvt', key='dvt_table_list') }}",
        },
        dag=dag,
    )

    dag_report = ReportingOperator(
        task_id="dag_report",
        trigger_rule=TriggerRule.ALL_DONE,  # Ensures this task runs even if upstream fails
        configuration="{{ ti.xcom_pull(task_ids='load_parameters', key='data_transfer_config_json') }}",
        dag=dag,
    )

    # dependencies
    (load_parameters)

    (
        load_parameters
        >> get_dts_run_summary
        >> get_dts_run_logs
        >> [create_transfer_log_GCS, process_transfer_logs]
        >> insert_bq_transfer_log_results
    )

    (
        load_parameters
        >> check_transfer_run_state
        >> [skip_validation_dag, filter_tables]
    )

    (
        filter_tables
        >> check_filtered_tables
        >> [skip_validation_dag, determine_validation_dag]
    )
    (
        determine_validation_dag
        >> [call_validation_dag, call_validation_crun_dag]
        >> dag_report
    )
