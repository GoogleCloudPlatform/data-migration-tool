import ast
import base64
import json
import logging
import os
import time
import uuid
from datetime import datetime, timedelta

from airflow import XComArg, models
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils import timezone
from airflow.utils.trigger_rule import TriggerRule
from google.api_core.client_info import ClientInfo
from google.cloud import bigquery, pubsub_v1
from googleapiclient import _auth as auth
from googleapiclient.errors import HttpError

from common_utils import (
    custom_user_agent,
    discovery_util,
    parallelization_utils,
    storage_utils,
)
from common_utils.bigquery_client_utils import utils as bq_utils
from common_utils.operators.reporting_operator import ReportingOperator

# environment variables
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
CONTROLLER_TOPIC = os.environ.get("CONTROLLER_TOPIC")

# task names
GET_CONFIG_TASK = "get_config"
GET_TABLE_LIST_TASK = "get_table_list"
GENERATE_BATCHES_DAG_TASK = "generate_batches"

# config fields
VALIDATION_UNIQUE_ID_CONF = "unique_id"
BATCH_DISTRIBUTION_CONF = "batchDistribution"
TABLE_LIST_FILE_CONF = "table_list_file"

# XCom keys
VALIDATION_UNIQUE_ID_XCOM = "unique_id"
BATCH_DISTRIBUTION_XCOM = "batch_distribution"
TABLE_LIST_XCOM = "table_list"
CONFIG_FILE_BUCKET_XCOM = "config_file_bucket"
CONFIG_FILE_OBJECT_XCOM = "config_file_object"
TABLE_LIST_FILE_XCOM = "table_list_file"

# BigQuery Logs
LOGS_DATASET = "dmt_logs"
DVT_TABLE_NAME = "dmt_dvt_results"
TERADATA_MIGRATION_TRACKING_TABLE_NAME = "dmt_teradata_transfer_tracking"

# DTS DAG constants
DTS_DAG_ID = "start_data_transfer_dag"
CONFIG_BUCKET_ID_KEY = "config_bucket_id"
CONFIG_OBJECT_ID_KEY = "config_object_id"
BATCH_TABLE_NAMES_KEY = "batch_table_names"
DTS_TABLE_NAME_SEPARATOR = ";"

# Agents constants
AGENT_START_DELAY = 10

DAG_ID = "teradata_data_load_dag"

gcs_util = storage_utils.StorageUtils()

custom_user_agent_http = auth.authorized_http(auth.default_credentials())

bq_client = bigquery.Client(
    client_info=ClientInfo(user_agent=custom_user_agent.USER_AGENT)
)

publisher = pubsub_v1.PublisherClient(
    publisher_options=pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
)
topic_name = "projects/{project_id}/topics/{topic}".format(
    project_id=PROJECT_ID, topic=CONTROLLER_TOPIC
)

bq_data_transfer_client = discovery_util.build_from_document_with_custom_http(
    "https://bigquerydatatransfer.googleapis.com/$discovery/rest?version=v1",
    custom_user_agent_http,
)

default_dag_args = {"start_date": datetime(2022, 1, 1)}


def _get_config(ti, **kwargs):
    bucket_name = ast.literal_eval(kwargs["dag_run"].conf["config"])["bucket_id"]
    object_name = ast.literal_eval(kwargs["dag_run"].conf["config"])["object_id"]

    user_config = ast.literal_eval(kwargs["dag_run"].conf["config"])["config"]
    logging.info(f"loaded config: {user_config}")

    if "notificationPubsubTopic" not in user_config["transfer_config"]:
        raise AirflowFailException(
            "Task failed. notificationPubsubTopic key missing in transfer_config"
        )
    else:
        if "unique_id" not in user_config:
            user_config["unique_id"] = user_config["transfer_config"]["displayName"]

    ti.xcom_push(key="user_config", value=user_config)
    ti.xcom_push(
        key=VALIDATION_UNIQUE_ID_XCOM, value=user_config.get(VALIDATION_UNIQUE_ID_CONF)
    )
    ti.xcom_push(
        key=BATCH_DISTRIBUTION_XCOM, value=user_config.get(BATCH_DISTRIBUTION_CONF)
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


def _generate_batches(ti):
    batch_distribution = ti.xcom_pull(
        key=BATCH_DISTRIBUTION_XCOM, task_ids=GET_CONFIG_TASK
    )
    table_list = ti.xcom_pull(key=TABLE_LIST_XCOM, task_ids=GET_TABLE_LIST_TASK)

    batch_table_names_list = []
    run_id_prefix = timezone.utcnow()
    for batch_run_id, batch in parallelization_utils.make_run_batches(
        table_list, batch_distribution, run_id_prefix
    ):
        batch_table_names = DTS_TABLE_NAME_SEPARATOR.join(batch)
        logging.info(f"run_id: {batch_run_id} ==> tables: {batch_table_names}")
        batch_table_names_list.append(batch_table_names)
    ti.xcom_push(key="batch_table_names_list", value=batch_table_names_list)
    return [{"batch_idx": i} for i in range(len(batch_table_names_list))]


def _create_bq_transfer_config_json(batch_idx, ti) -> None:
    """
    This function will create the JSON required to create transfer config on BigQuery
    :param ti: task instance parameter
    """
    user_config = ti.xcom_pull(key="user_config", task_ids=GET_CONFIG_TASK)
    table_name = ti.xcom_pull(
        key="batch_table_names_list", task_ids=GENERATE_BATCHES_DAG_TASK
    )[batch_idx]
    timestamp = timezone.utcnow().isoformat().replace("+00:00", "Z") + "-UTC"
    user_config["transfer_config"]["displayName"] = (
        user_config["transfer_config"]["displayName"] + "-" + timestamp
    )
    user_config["transfer_config"]["params"]["table_name_patterns"] = table_name
    # To avoid running dts automatically when created
    user_config["transfer_config"]["scheduleOptions"] = {"disableAutoScheduling": True}
    ti.xcom_push(key="transfer_config", value=user_config)
    logging.info(
        "Successfully created config file which will be used to create transfer configuration"
    )


def _create_bq_transfer(batch_idx, ti) -> None:
    """
    This function will be used to create the transfer config on BigQuery
    :param ti: task instance parameter
    """
    config_json = ti.xcom_pull(
        key="transfer_config", task_ids="create_bq_transfer_config_json"
    )[batch_idx]

    try:
        response = (
            bq_data_transfer_client.projects()
            .transferConfigs()
            .create(
                parent=f"projects/{PROJECT_ID}", body=config_json.get("transfer_config")
            )
            .execute(num_retries=10)
        )
        resource_name = response["name"]
        ti.xcom_push(key="resource_name", value=resource_name)
        transfer_id = response["name"].split("/")[-1]
        ti.xcom_push(key="transfer_id", value=transfer_id)
        logging.info(
            f"Successfully created transfer configuration with resource name {response['name']}"
        )
    except HttpError as err:
        logging.error("Error while creating transfer configuration.")
        logging.exception(err)
        raise err


def _run_bq_transfer_config(batch_idx, ti, **kwargs) -> None:
    """
    This function will run the transfer config on BigQuery
    :param ti: task instance parameter
    :param kwargs: config parameters passed to this DAG
    """
    user_config = ti.xcom_pull(key="user_config", task_ids=GET_CONFIG_TASK)
    transfer_id = ti.xcom_pull(key="transfer_id", task_ids="create_bq_transfer")[
        batch_idx
    ]
    r_name = (
        f"projects/{user_config['agent_config']['transfer-configuration']['project-id']}/locations/"
        f"{user_config['agent_config']['transfer-configuration']['location']}/transferConfigs/{transfer_id}"
    )

    current_time = datetime.now(timezone.utc)
    run_time = current_time + timedelta(seconds=10)
    run_time = run_time.isoformat().replace("+00:00", "Z")

    try:
        _ = (
            bq_data_transfer_client.projects()
            .locations()
            .transferConfigs()
            .startManualRuns(parent=r_name, body={"requestedRunTime": run_time})
            .execute(num_retries=10)
        )
        logging.info(f"Started transfer job {r_name} successfully")
    except HttpError as err:
        logging.error("Error running transfer config.")
        logging.exception(err)
        raise err


def _create_agent_config(batch_idx, ti, **kwargs) -> None:
    """
    This function will create the JSON required to run agent on server
    :param ti: task instance parameter
    """
    transfer_id = ti.xcom_pull(key="transfer_id", task_ids="create_bq_transfer")[
        batch_idx
    ]
    config_bucket_id = ti.xcom_pull(
        key=CONFIG_FILE_BUCKET_XCOM, task_ids=GET_CONFIG_TASK
    )
    config_object_id = ti.xcom_pull(
        key=CONFIG_FILE_OBJECT_XCOM, task_ids=GET_CONFIG_TASK
    )
    agent_uuid = str(uuid.uuid4())
    agent_run_config = gcs_util.read_object_from_gcsbucket(
        config_bucket_id, config_object_id
    )
    agent_run_config["agent_config"]["transfer-configuration"]["id"] = transfer_id
    agent_run_config["agent_config"]["agent-id"] = agent_uuid
    agent_config = agent_run_config["agent_config"]
    ti.xcom_push(key="agent_config", value=agent_config)
    ti.xcom_push(key="transfer_id", value=transfer_id)
    logging.info(
        f"Successfully created agent config for transfer config with id {transfer_id}"
    )


def _setup_dts_agent(batch_idx, ti) -> None:
    """
    This function will trigger the command on agent VM to run agent in background
    :param ti: task instance parameter
    """
    transfer_id = ti.xcom_pull(key="transfer_id", task_ids="create_bq_transfer")[
        batch_idx
    ]
    agent_config = ti.xcom_pull(task_ids="create_agent_config", key="agent_config")[
        batch_idx
    ]
    agent_config = json.dumps(agent_config).encode("utf-8")
    data = {
        "time": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "transfer_id": transfer_id,
        "params": {"agent_config": base64.b64encode(agent_config).decode("utf-8")},
        "action": "setup",
    }
    payload = json.dumps(data).encode("utf-8")
    future = publisher.publish(topic_name, payload, ordering_key=transfer_id)
    future.result()


def _run_dts_agent(batch_idx, ti) -> None:
    """
    This function will trigger the command on agent VM to run agent in background
    :param ti: task instance parameter
    """
    time.sleep(AGENT_START_DELAY)
    transfer_id = ti.xcom_pull(key="transfer_id", task_ids="create_bq_transfer")[
        batch_idx
    ]
    data = {
        "time": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "transfer_id": transfer_id,
        "params": {},
        "action": "run",
    }
    payload = json.dumps(data).encode("utf-8")
    future = publisher.publish(topic_name, payload, ordering_key=transfer_id)
    future.result()


def _send_data_to_bq(batch_idx, ti) -> None:
    """
    This DAG will insert data in BQ table to track transfer config details
    :param ti: task instance parameter
    """
    user_config = ti.xcom_pull(key="user_config", task_ids=GET_CONFIG_TASK)
    transfer_id = ti.xcom_pull(key="transfer_id", task_ids="create_bq_transfer")[
        batch_idx
    ]
    config_bucket_id = ti.xcom_pull(
        key=CONFIG_FILE_BUCKET_XCOM, task_ids=GET_CONFIG_TASK
    )
    config_object_id = ti.xcom_pull(
        key=CONFIG_FILE_OBJECT_XCOM, task_ids=GET_CONFIG_TASK
    )
    agent_config = ti.xcom_pull(key="agent_config", task_ids="create_agent_config")[
        batch_idx
    ]
    agent_id = agent_config["agent-id"]
    unique_id = user_config["unique_id"]
    full_table_name = (
        f"{PROJECT_ID}.{LOGS_DATASET}.{TERADATA_MIGRATION_TRACKING_TABLE_NAME}"
    )
    query = (
        f"INSERT INTO `{full_table_name}` (unique_id, transfer_config_id , "
        f"agent_id, config_bucket_id, config_object_path) VALUES ('{unique_id}', '{transfer_id}', '{agent_id}',"
        f" '{config_bucket_id}', '{config_object_id}');"
    )
    _ = bq_utils.run_query_on_bq(query, PROJECT_ID)
    logging.info(
        f"Added unique_id, transfer_config_id and agent_id in BQ {full_table_name}"
    )


with models.DAG(
    DAG_ID,
    schedule=None,
    default_args=default_dag_args,
    render_template_as_native_obj=True,
) as dag:
    get_unique_id_from_config = PythonOperator(
        task_id=GET_CONFIG_TASK,
        python_callable=_get_config,
        dag=dag,
    )

    get_valid_tables = PythonOperator(
        task_id=GET_TABLE_LIST_TASK,
        python_callable=_get_table_list,
        dag=dag,
    )

    generate_batches = PythonOperator(
        task_id=GENERATE_BATCHES_DAG_TASK,
        python_callable=_generate_batches,
        dag=dag,
    )

    create_bq_transfer_config_json = PythonOperator.partial(
        task_id="create_bq_transfer_config_json",
        python_callable=_create_bq_transfer_config_json,
        dag=dag,
    ).expand(op_kwargs=XComArg(generate_batches, key="return_value"))

    create_bq_transfer = PythonOperator.partial(
        task_id="create_bq_transfer", python_callable=_create_bq_transfer, dag=dag
    ).expand(op_kwargs=XComArg(generate_batches, key="return_value"))

    run_bq_transfer_config = PythonOperator.partial(
        task_id="run_bq_transfer_config",
        python_callable=_run_bq_transfer_config,
        dag=dag,
    ).expand(op_kwargs=XComArg(generate_batches, key="return_value"))

    create_agent_config = PythonOperator.partial(
        task_id="create_agent_config", python_callable=_create_agent_config, dag=dag
    ).expand(op_kwargs=XComArg(generate_batches, key="return_value"))

    setup_dts_agent = PythonOperator.partial(
        task_id="setup_dts_agent",
        python_callable=_setup_dts_agent,
        dag=dag,
    ).expand(op_kwargs=XComArg(generate_batches, key="return_value"))

    run_dts_agent = PythonOperator.partial(
        task_id="run_dts_agent",
        python_callable=_run_dts_agent,
        dag=dag,
        max_active_tis_per_dag=1,
    ).expand(op_kwargs=XComArg(generate_batches, key="return_value"))

    send_data_to_bq = PythonOperator.partial(
        task_id="send_data_to_bq", python_callable=_send_data_to_bq, dag=dag
    ).expand(op_kwargs=XComArg(generate_batches, key="return_value"))

    dag_report = ReportingOperator(
        task_id="dag_report",
        trigger_rule=TriggerRule.ALL_DONE,  # Ensures this task runs even if upstream fails
        configuration="{{ ti.xcom_pull(key='user_config', task_ids='get_config') }}",
        dag=dag,
    )

    (
        get_unique_id_from_config
        >> get_valid_tables
        >> generate_batches
        >> create_bq_transfer_config_json
        >> create_bq_transfer
        >> run_bq_transfer_config
        >> create_agent_config
        >> setup_dts_agent
        >> send_data_to_bq
        >> run_dts_agent
        >> dag_report
    )
