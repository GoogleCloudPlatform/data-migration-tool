import ast
import json
import logging
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from requests.exceptions import HTTPError

from common_utils.sf_connector_utils import SfConnectorProxyUtils

SF_CONNECTOR_HOST = os.environ.get("SNOWFLAKE_CONNECTOR_HOST")

sf_connector = SfConnectorProxyUtils(SF_CONNECTOR_HOST)

default_dag_args = {"start_date": datetime(2022, 1, 1)}

BATCH_TRANSLATOR_DAG_ID = "batch_sql_translation"


def _extract_ddl(ti, **kwargs) -> None:
    """
    Initiates snowflake to bigquery translation
    :param ti: task instance parameter
    """
    jsonString = json.dumps(kwargs["dag_run"].conf["config"])
    ti.xcom_push(key="next_dag_config", value=jsonString)
    config = ast.literal_eval(kwargs["dag_run"].conf["config"])["config"]
    request_body = config["payload"]
    logging.info(f"[HOST] : {SF_CONNECTOR_HOST}")
    logging.info(f"[PAYLOAD] : ({type(request_body)}) : {request_body}")

    logging.info("Calling snowflake connector...")
    try:
        response = sf_connector.extract_ddl(request_body)
        logging.info(f"Snowflake migration job finished: {response}")
    except HTTPError as ex:
        logging.error(f"Snowflake migration job failed: {ex}")
        logging.exception(ex)


def build_snowflake_ddl_extraction_group(dag: DAG) -> TaskGroup:
    snowflake_extraction_taskgroup = TaskGroup(
        group_id="snowflake_extraction_taskgroup"
    )

    extract_ddl = PythonOperator(
        task_id="extract_ddl",
        python_callable=_extract_ddl,
        task_group=snowflake_extraction_taskgroup,
        dag=dag,
        provide_context=True,
    )

    invoke_batch_translator_dag = TriggerDagRunOperator(
        task_id="invoke_batch_translator_dag_task",
        trigger_dag_id=BATCH_TRANSLATOR_DAG_ID,
        conf={
            "config": "{{ ti.xcom_pull(key='next_dag_config', task_ids='snowflake_extraction_taskgroup.extract_ddl') }}"
        },
        task_group=snowflake_extraction_taskgroup,
        dag=dag,
    )
    extract_ddl >> invoke_batch_translator_dag
    return snowflake_extraction_taskgroup
