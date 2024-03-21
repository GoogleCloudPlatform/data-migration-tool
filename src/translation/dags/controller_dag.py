import base64
import datetime
import json
import os

from airflow import models
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from google.api_core.client_info import ClientInfo
from google.cloud import bigquery, storage

from common_utils import custom_user_agent
from common_utils.operators.reporting_operator import ReportingOperator

CUSTOM_RUN_ID_KEY = "unique_id"
DAG_ID = "controller_dag"
BATCH_TRANSLATOR_DAG_ID = "batch_sql_translation"
DATA_LOAD_TERADATA_DAG_ID = "teradata_data_load_dag"
DATA_LOAD_HIVE_DAG_ID = "hive_data_load_dag"
DATA_LOAD_HIVE_INC_DAG_ID = "hive_inc_data_load_dag"
DATA_LOAD_REDSHIFT_DAG_ID = "redshift_data_load_dag"
DATA_LOAD_SNOWFLAKE_DAG_ID = "snowflake_data_load_dag"
TERADATA_TRANSFER_RUN_LOG_DAG_ID = "teradata_transfer_run_log_dag"
REDSHIFT_TRANSFER_RUN_LOG_DAG_ID = "redshift_transfer_run_log_dag"
EXTRACT_DDL_DAG_ID = "extract_ddl_dag"

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")


client = bigquery.Client(
    client_info=ClientInfo(user_agent=custom_user_agent.USER_AGENT)
)


def _prepare_config_for_reporting(ti, **kwargs) -> None:
    event_json = kwargs["dag_run"].conf
    event_type = event_json["message"]["attributes"]["eventType"]
    if event_type == "OBJECT_FINALIZE":
        reporting_config = ti.xcom_pull(key="config", task_ids="load_config")
    elif event_type == "TRANSFER_RUN_FINISHED":
        fetch_unique_id_query = "select unique_id from {bq_transfer_tracking_table} where transfer_config_id = '{transfer_config_id}'"
        config = json.loads(base64.b64decode(event_json["message"]["data"]))
        transfer_config_id = config["name"].split("/")[-3]
        data_source_id = config["dataSourceId"]
        if data_source_id == "on_premises":
            source = "teradata"
            bq_transfer_tracking_table = (
                f"{PROJECT_ID}.dmt_logs.dmt_teradata_transfer_tracking"
            )
        elif data_source_id == "redshift":
            source = "redshift"
            bq_transfer_tracking_table = (
                f"{PROJECT_ID}.dmt_logs.dmt_redshift_transfer_tracking"
            )
        # get unique id from transfer tracking table, using transfer config id
        query_job = client.query(
            fetch_unique_id_query.format(
                bq_transfer_tracking_table=bq_transfer_tracking_table,
                transfer_config_id=transfer_config_id,
            )
        )
        unique_id = [row["unique_id"] for row in query_job][0]
        reporting_config = {"unique_id": unique_id, "source": source}
    ti.xcom_push(key="reporting_config", value=reporting_config)


def _load_config(ti, **kwargs) -> None:
    client = storage.Client(
        client_info=ClientInfo(user_agent=custom_user_agent.USER_AGENT)
    )
    event_json = kwargs["dag_run"].conf
    event_type = event_json["message"]["attributes"]["eventType"]
    if event_type == "OBJECT_FINALIZE":
        message = event_json["message"]
        print(f"message : {message}")
        bucket_id = message["attributes"]["bucketId"]
        Variable.set("hive_config_bucket_id", bucket_id)
        object_id = message["attributes"]["objectId"]
        print(f"bucket_id : {bucket_id}, object_id: {object_id}")
        bucket = client.get_bucket(bucket_id)
        blob = storage.Blob(object_id, bucket)
        raw_config = blob.download_as_bytes()
        config = json.loads(raw_config)
        if CUSTOM_RUN_ID_KEY not in config:
            config[CUSTOM_RUN_ID_KEY] = datetime.datetime.now().strftime("%x %H:%M:%S")
        ti.xcom_push(key="config", value=config)
        ti.xcom_push(key="bucket_id", value=bucket_id)
        ti.xcom_push(key="object_id", value=object_id)
    elif event_type == "TRANSFER_RUN_FINISHED":
        config = json.loads(base64.b64decode(event_json["message"]["data"]))
        ti.xcom_push(key="config", value=config)
    ti.xcom_push(key="event_type", value=event_type)


def _prepare_data_for_next_dag(ti, **kwargs):
    event_type = ti.xcom_pull(key="event_type", task_ids="load_config")
    if event_type == "OBJECT_FINALIZE":
        config = ti.xcom_pull(key="config", task_ids="load_config")
        bucket_id = ti.xcom_pull(key="bucket_id", task_ids="load_config")
        object_id = ti.xcom_pull(key="object_id", task_ids="load_config")
        op_type = config["type"]
        if op_type in ["ddl", "sql", "dml"]:
            data_source = config["source"]
            if data_source in ["teradata", "hive", "oracle", "redshift"]:
                next_dag_config = {"config": config}
            else:
                print(f"Unsupported data source : {data_source}")
                next_dag_config = None
        elif op_type == "data":
            next_dag_config = {
                "config": config,
                "bucket_id": bucket_id,
                "object_id": object_id,
            }
        else:
            print(f"Error: Unsupported operation type: {op_type}")
            next_dag_config = None
    elif event_type == "TRANSFER_RUN_FINISHED":
        config = ti.xcom_pull(key="config", task_ids="load_config")
        data_source = config["dataSourceId"]
        if data_source == "on_premises":
            next_dag_config = {
                "TRANSFER_RUN_ID": config["name"],
                "TRANSFER_RUN_STATE": config["state"],
                "SOURCE_SCHEMA": config["params"]["database_name"],
                "TABLE_NAME_PATTERN": config["params"]["table_name_patterns"],
            }
        elif data_source == "redshift":
            next_dag_config = {
                "TRANSFER_RUN_ID": config["name"],
                "TRANSFER_RUN_STATE": config["state"],
                "SOURCE_SCHEMA": config["params"]["redshift_schema"],
                "TABLE_NAME_PATTERN": config["params"]["table_name_patterns"],
            }
    else:
        print(f"Unsupported event type: {event_type}")
        next_dag_config = None

    ti.xcom_push(key="next_dag_config", value=next_dag_config)


def _determine_next_dag(ti, **kwargs):
    event_type = ti.xcom_pull(key="event_type", task_ids="load_config")
    if event_type == "OBJECT_FINALIZE":
        config = ti.xcom_pull(key="config", task_ids="load_config")
        op_type = config["type"]
        data_source = config["source"]
        if op_type in ["ddl", "sql", "dml"]:
            data_source = config["source"]
            if data_source in ["teradata", "oracle", "redshift"]:
                if (
                    "extract_ddl" in config
                    and config["extract_ddl"] == "yes"
                    and op_type not in ["sql", "dml"]
                ):
                    # call DDL extractor DAG
                    next_dag_id = EXTRACT_DDL_DAG_ID
                else:
                    # call batch_translator_dag
                    next_dag_id = BATCH_TRANSLATOR_DAG_ID
            elif data_source == "hive":
                # call DDL extractor DAG
                next_dag_id = EXTRACT_DDL_DAG_ID
            else:
                print(f"Error: Unsupported data source: {data_source}")
                next_dag_id = None
        elif op_type == "data":
            if data_source == "teradata":
                next_dag_id = DATA_LOAD_TERADATA_DAG_ID
            elif data_source == "hive":
                next_dag_id = DATA_LOAD_HIVE_DAG_ID
            elif data_source == "hive_inc":
                next_dag_id = DATA_LOAD_HIVE_INC_DAG_ID
            elif data_source == "redshift":
                next_dag_id = DATA_LOAD_REDSHIFT_DAG_ID
            elif data_source == "snowflake":
                next_dag_id = DATA_LOAD_SNOWFLAKE_DAG_ID
        else:
            print(f"Unsupported operation type: {op_type}")
            next_dag_id = None
    elif event_type == "TRANSFER_RUN_FINISHED":
        config = ti.xcom_pull(key="config", task_ids="load_config")
        data_source = config["dataSourceId"]
        if data_source == "on_premises":
            next_dag_id = TERADATA_TRANSFER_RUN_LOG_DAG_ID
        elif data_source == "redshift":
            next_dag_id = REDSHIFT_TRANSFER_RUN_LOG_DAG_ID
    else:
        print(f"Unsupported event type: {event_type}")
        next_dag_id = None

    if next_dag_id is None:
        next_task = "end_task"
    else:
        next_task = "determine_next_dag_task"

    ti.xcom_push(key="next_dag_id", value=next_dag_id)

    return next_task


with models.DAG(
    DAG_ID,
    schedule_interval=None,
    default_args={"start_date": datetime.datetime(2022, 1, 1)},
) as dag:
    load_config = PythonOperator(
        task_id="load_config",
        python_callable=_load_config,
        dag=dag,
    )

    prepare_config_for_reporting = PythonOperator(
        task_id="prepare_config_for_reporting",
        python_callable=_prepare_config_for_reporting,
        dag=dag,
    )

    prepare_data_for_next_dag = PythonOperator(
        task_id="prepare_data_for_next_dag",
        python_callable=_prepare_data_for_next_dag,
        dag=dag,
    )
    determine_next_dag = BranchPythonOperator(
        task_id="determine_next_dag_task", python_callable=_determine_next_dag, dag=dag
    )
    end_task = EmptyOperator(task_id="end_task", dag=dag)
    invoke_next_dag = TriggerDagRunOperator(
        task_id="invoke_next_dag",
        trigger_dag_id="{{ ti.xcom_pull(key='next_dag_id', task_ids='determine_next_dag_task') }}",
        conf={
            "config": "{{ ti.xcom_pull(key='next_dag_config', task_ids='prepare_data_for_next_dag') }}"
        },
        dag=dag,
    )
    dag_report = ReportingOperator(
        task_id="dag_report",
        trigger_rule=TriggerRule.ALL_DONE,  # Ensures this task runs even if upstream fails
        configuration="{{ ti.xcom_pull(key='reporting_config', task_ids='prepare_config_for_reporting') }}",
        dag=dag,
    )
    (
        load_config
        >> prepare_config_for_reporting
        >> prepare_data_for_next_dag
        >> determine_next_dag
        >> [invoke_next_dag, end_task]
        >> dag_report
    )
