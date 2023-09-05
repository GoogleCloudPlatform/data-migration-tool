import logging

from google.api_core.client_info import ClientInfo
from google.cloud import bigquery

from common_utils import custom_user_agent


def insert_bq_json_rows(table_name: str, json_rows: list):
    bq_client = bigquery.Client(
        client_info=ClientInfo(user_agent=custom_user_agent.USER_AGENT)
    )
    errors = bq_client.insert_rows_json(
        table_name, json_rows, ignore_unknown_values=True
    )
    if errors:
        logging.error(
            f"Method insert_summary_bigquery: error inserting json_rows into table {table_name}, \
            \n\t error: {errors}"
        )
        raise Exception(
            f"Method insert_summary_bigquery: error inserting json_rows into table {table_name}, \
            \n\terror: {errors}"
        )
    logging.info(
        f"Method insert_summary_bigquery: {len(json_rows)} rows inserted into table {table_name}"
    )


def run_query_on_bq(query: str, project_id: str):
    client = bigquery.Client(project_id)
    query_job = client.query(query)
    logging.info(
        f"Method run_query_on_bq: query: {query}, received {len(list(query_job))} rows"
    )
    return query_job
