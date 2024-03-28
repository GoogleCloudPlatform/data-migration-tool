import ast
import logging
import os
from datetime import datetime

from airflow import models
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.trigger_rule import TriggerRule
from common_utils.sf_connector_utils import SfConnectorProxyUtils
from google.api_core.client_info import ClientInfo
from google.cloud import bigquery
from googleapiclient import _auth as auth
from requests.exceptions import HTTPError

from common_utils import custom_user_agent, storage_utils
from common_utils.operators.reporting_operator import ReportingOperator

# environment variables
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
CONTROLLER_TOPIC = os.environ.get("CONTROLLER_TOPIC")
SF_CONNECTOR_HOST = os.environ.get("SNOWFLAKE_CONNECTOR_HOST")

# task names
GET_CONFIG_TASK = "get_config"
GET_TABLE_LIST_TASK = "get_table_list"
MIGRATE_DATA_DAG_TASK = "migrate_data"

# config fields
VALIDATION_UNIQUE_ID_CONF = "unique_id"
TABLE_LIST_FILE_CONF = "table_list_file"

# XCom keys
VALIDATION_UNIQUE_ID_XCOM = "unique_id"
TABLE_LIST_XCOM = "table_list"
CONFIG_FILE_BUCKET_XCOM = "config_file_bucket"
CONFIG_FILE_OBJECT_XCOM = "config_file_object"
TABLE_LIST_FILE_XCOM = "table_list_file"

# BigQuery Logs
LOGS_DATASET = "dmt_logs"
DVT_TABLE_NAME = "dmt_dvt_results"

SECRET_PREFIX = "secret:"

TABLE_NAME_SEPARATOR = ","

DAG_ID = "snowflake_data_load_dag"

gcs_util = storage_utils.StorageUtils()
sf_connector = SfConnectorProxyUtils(SF_CONNECTOR_HOST)

custom_user_agent_http = auth.authorized_http(auth.default_credentials())

bq_client = bigquery.Client(
    client_info=ClientInfo(user_agent=custom_user_agent.USER_AGENT)
)

default_dag_args = {"start_date": datetime(2022, 1, 1)}


def _get_config(ti, **kwargs):
    bucket_name = ast.literal_eval(kwargs["dag_run"].conf["config"])["bucket_id"]
    object_name = ast.literal_eval(kwargs["dag_run"].conf["config"])["object_id"]

    user_config = ast.literal_eval(kwargs["dag_run"].conf["config"])["config"]
    logging.info(f"loaded config: {user_config}")

    ti.xcom_push(key="user_config", value=user_config)
    ti.xcom_push(
        key=VALIDATION_UNIQUE_ID_XCOM, value=user_config.get(VALIDATION_UNIQUE_ID_CONF)
    )
    ti.xcom_push(key=TABLE_LIST_FILE_XCOM, value=user_config.get(TABLE_LIST_FILE_CONF))
    ti.xcom_push(key=CONFIG_FILE_BUCKET_XCOM, value=bucket_name)
    ti.xcom_push(key=CONFIG_FILE_OBJECT_XCOM, value=object_name)


def _get_table_list(ti):
    unique_id = ti.xcom_pull(key=VALIDATION_UNIQUE_ID_XCOM, task_ids=GET_CONFIG_TASK)
    table_list_file = ti.xcom_pull(key=TABLE_LIST_FILE_XCOM, task_ids=GET_CONFIG_TASK)

    if table_list_file:
        gcs = GCSHook()
        bucket, blob = gcs_util.parse_bucket_and_blob_from_path(table_list_file)
        file_list_blob = gcs.download_as_byte_array(
            bucket_name=bucket, object_name=blob
        )

        table_list = file_list_blob.decode("utf-8").splitlines()

    elif unique_id:
        results = bq_client.query(
            f"""
      SELECT DISTINCT
        SPLIT(source_table_name, '.')[OFFSET(1)] as source_table_name
      FROM
        `{PROJECT_ID}.{LOGS_DATASET}.{DVT_TABLE_NAME}`
      CROSS JOIN
        UNNEST(labels) AS a
      WHERE
        validation_type='Schema'
        AND a.value='{unique_id}'
      GROUP BY source_table_name, run_id
      HAVING
        SUM(IF(validation_status='fail', 1, 0)) = 0;
      """
        )

        table_list = [row["source_table_name"] for row in results]
    else:
        raise Exception(
            f"{VALIDATION_UNIQUE_ID_CONF} or {TABLE_LIST_FILE_CONF} must be present on config file"
        )

    ti.xcom_push(key=TABLE_LIST_XCOM, value=table_list)


def _migrate_data(ti) -> None:
    """
    Initiates snowflake to bigquery migration
    :param ti: task instance parameter
    """
    user_config = ti.xcom_pull(key="user_config", task_ids=GET_CONFIG_TASK)

    sf_oauth_config = user_config["oauth_config"]
    client_id = sf_oauth_config["clientId"]
    if client_id.startswith(SECRET_PREFIX):
        client_id = Variable.get(client_id.split(SECRET_PREFIX)[1])
    client_secret = sf_oauth_config["clientSecret"]
    if client_secret.startswith(SECRET_PREFIX):
        client_secret = Variable.get(client_secret.split(SECRET_PREFIX)[1])
    refresh_token = sf_oauth_config["refreshToken"]
    if refresh_token.startswith(SECRET_PREFIX):
        refresh_token = Variable.get(refresh_token.split(SECRET_PREFIX)[1])

    sf_setup_config = user_config["setup_config"]

    sf_migration_config = user_config["migration_config"]
    table_list = ti.xcom_pull(key=TABLE_LIST_XCOM, task_ids=GET_TABLE_LIST_TASK)
    sf_migration_config["sourceTableName"] = TABLE_NAME_SEPARATOR.join(table_list)

    request_body = {
        "oauth_config": {
            "clientId": client_id,
            "clientSecret": client_secret,
            "refreshToken": refresh_token,
        },
        "setup_config": sf_setup_config,
        "migration_config": sf_migration_config,
    }

    logging.info("Calling snowflake connector...")
    try:
        response = sf_connector.migrate_data(request_body)
    except HTTPError as ex:
        logging.error(f"Snowflake migration job failed: {ex}")
        logging.exception(ex)

    logging.info(f"Snowflake migration job finished: {response}")


with models.DAG(
    DAG_ID,
    schedule_interval=None,
    default_args=default_dag_args,
    render_template_as_native_obj=True,
) as dag:
    get_config = PythonOperator(
        task_id=GET_CONFIG_TASK,
        python_callable=_get_config,
        dag=dag,
    )

    get_valid_tables = PythonOperator(
        task_id=GET_TABLE_LIST_TASK,
        python_callable=_get_table_list,
        dag=dag,
    )

    migrate_data = PythonOperator(
        task_id=MIGRATE_DATA_DAG_TASK,
        python_callable=_migrate_data,
        dag=dag,
    )

    dag_report = ReportingOperator(
        task_id="dag_report",
        trigger_rule=TriggerRule.ALL_DONE,  # Ensures this task runs even if upstream fails
        configuration="{{ ti.xcom_pull(key='user_config', task_ids='get_config') }}",
        dag=dag,
    )

    (get_config >> get_valid_tables >> migrate_data >> dag_report)
