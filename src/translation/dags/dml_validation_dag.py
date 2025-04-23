import datetime
import logging
import os
import re

from airflow import models
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.trigger_rule import TriggerRule
from google.api_core.client_info import ClientInfo
from google.cloud import bigquery

from common_utils import custom_user_agent
from common_utils.operators.reporting_operator import ReportingOperator

# Constant variables

# DAG id
DAG_ID = "dml_validation_dag"

# DAG arguments
execution_date = datetime.datetime(2022, 1, 1)
default_dag_args = {"start_date": execution_date}

# Config file keys
CUSTOM_RUN_ID_KEY = "unique_id"

# Airflow data directory
DIRECTORY = "/home/airflow/gcs/data"

TRANSLATION_CONFIG_DML = "dml"
TRANSLATION_CONFIG_KEY = "type"
TRANSLATION_CONFIG_DEFAULT_VALUE = TRANSLATION_CONFIG_DML
# Variables used to store dml validation result in BQ table
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BQ_RESULT_DATASET_NAME = "dmt_logs"
BQ_RESULT_TABLE_NAME = "dmt_dml_validation_results"
DML_VALIDATION_AGGREGATED_RESULTS_TABLE_ID = (
    f"{PROJECT_ID}.dmt_logs.dmt_dml_validation_aggregated_results"
)

bq_client = bigquery.Client(
    client_info=ClientInfo(user_agent=custom_user_agent.USER_AGENT)
)
job_config = bigquery.QueryJobConfig(
    dry_run=True, use_query_cache=False, use_legacy_sql=False
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
Method: _dry_run
Description: DML query validation using BigQuery dry run task
Arguments: ti, **kwargs
"""


def _dry_run(ti, **kwargs):
    logging.info("Logging info for function _dry_run")

    if kwargs["dag_run"].conf is not None and "config" in kwargs["dag_run"].conf:
        logging.info("Configuration file is not empty")
        files = kwargs["dag_run"].conf["files"]

        if len(files) == 0:
            logging.error(
                "SQL File list is empty and \
                    there are no SQL files to execute SQL query"
            )
        else:
            config = kwargs["dag_run"].conf["config"]
            source = config["source"]
            if source == "teradata":
                mode = config["migrationTask"]["translationConfigDetails"][
                    "sourceDialect"
                ]["teradataDialect"]["mode"]
            else:
                mode = "sql"

            unique_id = config[CUSTOM_RUN_ID_KEY]
            gcsSourcePath = config["migrationTask"]["translationConfigDetails"][
                "gcsSourcePath"
            ]
            gcsTargetPath = config["migrationTask"]["translationConfigDetails"][
                "gcsTargetPath"
            ]
            skip_comment_expr = ["""--"""]
            remove_expr_bteq = """BEGIN"""
            skip_expr_bteq = ["""EXCEPTION WHEN ERROR""", """END"""]

            total_files_count = len(files)

            failed_query_count = 0
            successful_query_count = 0

            queryString = ""
            aggregated_results = []

            for filename in files:
                f = os.path.join(DIRECTORY, filename)
                logging.info(f)

                content = open(f, "r").read()
                statements = content.split(";")
                statements.pop()
                # Remove last item from list
                # (i.e. Last item will new line for ; last character)

                for stmt in statements:
                    stmt = stmt.strip()

                    # Default uncommentedStmtPresent is true means that line/statement doesn't have comments on starting
                    uncommentedStmtPresent = True
                    # Check if line/statement have comments on starting
                    if bool(
                        re.match(
                            r"(?=(" + "|".join(skip_comment_expr) + r"))", stmt, re.I
                        )
                    ):
                        # Set uncommentedStmtPresent to false as starting characters in line has comments
                        uncommentedStmtPresent = False
                        for lineStmt in stmt.splitlines():
                            # Check if all the lines has comments, if not update uncommentedStmtPresent flag value to True and break the loop
                            if (
                                bool(
                                    re.match(
                                        r"(?=(" + "|".join(skip_comment_expr) + r"))",
                                        lineStmt,
                                        re.I,
                                    )
                                )
                                is False
                            ):
                                uncommentedStmtPresent = True
                                logging.info(
                                    "In commented line separated by ; there is uncommented SQL statement which needs to process"
                                )
                                break

                    if mode == "SQL" and not uncommentedStmtPresent:
                        logging.info(
                            "SQL mode, skip the statement \
                                and goto next statement"
                        )
                        continue

                    elif mode == "BTEQ":
                        logging.info("BTEQ mode")

                        if (
                            bool(
                                re.match(
                                    r"(?=(" + "|".join(skip_expr_bteq) + r"))",
                                    stmt,
                                    re.I,
                                )
                            )
                            and not uncommentedStmtPresent
                        ):
                            logging.info(
                                "BTEQ mode, skip the statement \
                                    and goto next statement"
                            )
                            continue

                        if bool(re.match(remove_expr_bteq, stmt, re.I)):
                            logging.info(
                                "Statement start with begin block \
                                    hence remove BEGIN word"
                            )
                            stmt = re.split(remove_expr_bteq, stmt, flags=re.I)[1]

                    queryStr = ""
                    exec_time = datetime.datetime.now()

                    response_json = bq_client.query(
                        stmt, job_config=job_config
                    )._properties
                    # A dry run query completes immediately.
                    logging.debug(f"This query response json - {response_json}")

                    if response_json["status"]["state"] == "DONE":
                        successful_query_count += 1
                        statement_type = response_json["statistics"]["query"][
                            "statementType"
                        ]
                        bq_table_name = response_json["configuration"]["query"][
                            "destinationTable"
                        ]["tableId"]
                        logging.info("Nothing went wrong in _dry_run")
                        queryStr = f"INSERT INTO `{PROJECT_ID}.{BQ_RESULT_DATASET_NAME}.{BQ_RESULT_TABLE_NAME}`(unique_id,file_name,status,error_details,execution_start_time,gcs_input_path,gcs_output_path,bq_table_name,statement_type) VALUES ('{unique_id}','{filename}','success','','{exec_time}','{gcsSourcePath}','{gcsTargetPath}','{bq_table_name}','{statement_type}' )"
                    else:
                        failed_query_count += 1
                        logging.info("Dry run fail")
                        error_details = response_json["error"]["message"]
                        queryStr = f"INSERT INTO `{PROJECT_ID}.{BQ_RESULT_DATASET_NAME}.{BQ_RESULT_TABLE_NAME}`(unique_id,file_name,status,error_details,execution_start_time,gcs_input_path,gcs_output_path,bq_table_name,statement_type) VALUES ('{unique_id}','{filename}','fail','{error_details}','{exec_time}','{gcsSourcePath}','{gcsTargetPath}','', '')"

                    queryString = queryString + queryStr + ";"

            aggregated_results.append(
                {
                    "unique_id": unique_id,
                    "total_files": total_files_count,
                    "total_queries": successful_query_count + failed_query_count,
                    "successful_queries": successful_query_count,
                    "failed_queries": failed_query_count,
                }
            )

            results = {
                "unique_id": unique_id,
                "aggregated_results": aggregated_results,
                "queryString": queryString,
            }

            ti.xcom_push(key="dry_run_results", value=results)
            ti.xcom_push(key="config", value=kwargs["dag_run"].conf["config"])
            ti.xcom_push(
                key="validation_mode",
                value=config["validation_config"].get("validation_mode"),
            )

    else:
        logging.error("Configuration file is empty")


"""
Method: _save_dry_run_result
Description: Insert Dry run Job execution result in BigQuery
Arguments: ti, **kwargs
"""


def _save_dry_run_result(ti, **kwargs):
    logging.info("In save dry run results")
    results = ti.xcom_pull(task_ids="dry_run", key="dry_run_results")
    logging.info(f"Dry run execution results {results is None}")
    if results is None:
        logging.error(
            "None of the SQL file processed for dml validation\
                hence nothing to log"
        )
    else:
        try:
            bigquery_execute_multi_query = BigQueryInsertJobOperator(
                task_id="store_dry_run_result",
                dag=dag,
                configuration={
                    "query": {"query": results["queryString"], "useLegacySql": False}
                },
            )
            bigquery_execute_multi_query.execute(context=kwargs)

        except Exception as e:
            logging.info("Something went wrong in task _save_dry_run_result")
            logging.info(f"Exception is :: {str(e)}")
        else:
            logging.info("Dry run result stored successfully")

        aggregated_results = results["aggregated_results"]
        if aggregated_results == []:
            logging.info("DML Aggregation Stats are empty.")
        else:
            aggregated_insert = bq_client.insert_rows_json(
                DML_VALIDATION_AGGREGATED_RESULTS_TABLE_ID, aggregated_results
            )

        logging.info(
            f"Dry run aggregated result stored successfully\
                :: {aggregated_insert}"
        )

    logging.info("Save dry run result execution completed")


"""
DAG: DML validation DAG to validate DML statement\
    from DML translated files output GCS Bucket
"""


def _determine_next_dag(ti):
    validation_mode = ti.xcom_pull(key="validation_mode", task_ids="dry_run")
    validation_dag_id = get_validation_dag_id(validation_mode)
    if validation_dag_id == VALIDATION_DAG_ID:
        return "invoke_validation_dag"
    else:
        return "invoke_validation_crun_dag"


with models.DAG(
    DAG_ID,
    schedule=None,
    default_args=default_dag_args,
    render_template_as_native_obj=True,
) as dag:
    dry_run = PythonOperator(
        task_id="dry_run",
        python_callable=_dry_run,
        dag=dag,
    )

    save_dry_run_result = PythonOperator(
        task_id="save_dry_run_result",
        python_callable=_save_dry_run_result,
        dag=dag,
    )
    determine_next_dag = BranchPythonOperator(
        task_id="determine_next_dag",
        python_callable=_determine_next_dag,
        dag=dag,
    )
    invoke_validation_dag = TriggerDagRunOperator(
        task_id="invoke_validation_dag",
        trigger_dag_id=VALIDATION_DAG_ID,
        conf={
            "config": "{{ ti.xcom_pull(task_ids='dry_run', key='config') }}",
        },
        dag=dag,
    )
    invoke_validation_crun_dag = TriggerDagRunOperator(
        task_id="invoke_validation_crun_dag",
        trigger_dag_id=VALIDATION_CRUN_DAG_ID,
        conf={"config": "{{ ti.xcom_pull(task_ids='dry_run', key='config') }}"},
        dag=dag,
    )

    dag_report = ReportingOperator(
        task_id="dag_report",
        trigger_rule=TriggerRule.ALL_DONE,  # Ensures this task runs even if upstream fails
        configuration="{{ dag_run.conf['config'] }}",
        dag=dag,
    )
    (
        dry_run
        >> save_dry_run_result
        >> determine_next_dag
        >> [invoke_validation_dag, invoke_validation_crun_dag]
        >> dag_report
    )
