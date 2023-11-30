import os
import re

from google.api_core.client_info import ClientInfo
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from common_utils import custom_user_agent, storage_utils

## Config file keys
CUSTOM_RUN_ID_KEY = "unique_id"

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BQ_RESULT_DATASET_NAME = "dmt_logs"

DMT_SQLFILE_TABLE_MAPPPING_BQ_TABLE_ID = (
    f"{PROJECT_ID}.{BQ_RESULT_DATASET_NAME}.dmt_file_table_mapping"
)

bq_client = bigquery.Client(
    client_info=ClientInfo(user_agent=custom_user_agent.USER_AGENT)
)
gcs_util = storage_utils.StorageUtils()


def filter(files, config):
    translation_type = config["type"]
    validation_type = config["validation_config"]["validation_type"]
    if translation_type == "sql":
        return list()
    exp = r"\b(?i:(CREATE|REPLACE) (OR REPLACE)*\s?(TABLE|VIEW) (IF NOT EXISTS)*\s?`?([\w-]+)`?\.([\w-]+)\.(\w+)[a-zA-Z]*)\b"
    remove_expr_bteq = """BEGIN"""
    skip_expr_bteq = ["""EXCEPTION WHEN ERROR""", """END"""]

    if (
        "teradataDialect"
        in config["migrationTask"]["translationConfigDetails"]["sourceDialect"]
    ):
        mode = config["migrationTask"]["translationConfigDetails"]["sourceDialect"][
            "teradataDialect"
        ]["mode"]
    else:
        mode = "SQL"
    directory = "/home/airflow/gcs/data"

    all_tables_set = set()
    file_table_mapping_list = []
    for file_details in files:
        filename = file_details["sql_file_name"]
        file_status = file_details["status"]
        f = os.path.join(directory, filename)
        content = open(f, "r").read()
        statements = content.split(";")
        statements.pop()  # Remove last item from list (i.e. Last item will new line for ; last character)
        for stmt in statements:
            stmt = stmt.strip()
            if mode == "BTEQ":
                if bool(
                    re.match(r"(?=(" + "|".join(skip_expr_bteq) + r"))", stmt, re.I)
                ):
                    # print("BTEQ mode, skip the statment and goto next statement")
                    continue

                if bool(re.match(remove_expr_bteq, stmt, re.I)):
                    # print('Statement start with begin block hence remove BEGIN word')
                    stmt = re.split(remove_expr_bteq, stmt, flags=re.I)[1]
            matched_query_tokens_list = re.findall(exp, stmt)
            if file_status == "success":
                for matched_query_tokens in matched_query_tokens_list:
                    parsed_table_name = (
                        f"{matched_query_tokens[5]}.{matched_query_tokens[6]}"
                    )
                    all_tables_set.add(parsed_table_name)
                    file_table_mapping_list.append(
                        {
                            f"{CUSTOM_RUN_ID_KEY}": config[CUSTOM_RUN_ID_KEY],
                            "sql_file_name": filename,
                            "table_name": parsed_table_name,
                            "table_creation_status": "success",
                        }
                    )
            elif file_status == "fail":
                for matched_query_tokens in matched_query_tokens_list:
                    parsed_table_name = (
                        f"{matched_query_tokens[5]}.{matched_query_tokens[6]}"
                    )
                    table_id = f"{PROJECT_ID}.{parsed_table_name}"
                    try:
                        bq_client.get_table(
                            table_id
                        )  # Make an API request to check if this table actually exists
                        print(f"Table {table_id} already exists.")
                        all_tables_set.add(parsed_table_name)
                        file_table_mapping_list.append(
                            {
                                f"{CUSTOM_RUN_ID_KEY}": config[CUSTOM_RUN_ID_KEY],
                                "sql_file_name": filename,
                                "table_name": parsed_table_name,
                                "table_creation_status": "success",
                            }
                        )
                    except NotFound:
                        print(f"Table {table_id} not found.")
                        file_table_mapping_list.append(
                            {
                                f"{CUSTOM_RUN_ID_KEY}": config[CUSTOM_RUN_ID_KEY],
                                "sql_file_name": filename,
                                "table_name": parsed_table_name,
                                "table_creation_status": "fail",
                            }
                        )
    # add file_table mapping details to BQ Table
    if file_table_mapping_list == []:
        print("Empty File Table Mapping")
    else:
        insert_result = bq_client.insert_rows_json(
            DMT_SQLFILE_TABLE_MAPPPING_BQ_TABLE_ID, file_table_mapping_list
        )
        print(f"File Table Mapping Insertion Result: {insert_result}")

    tables_set_from_gcs = set()
    validation_params_file_path = config["validation_config"][
        "validation_params_file_path"
    ]
    bucket_name, blob_name = gcs_util.parse_bucket_and_blob_from_path(
        validation_params_file_path
    )
    validation_params_from_gcs = gcs_util.get_validation_params_from_gcs(
        bucket_name, blob_name, translation_type, validation_type
    )

    print(f"validation_params_from_gcs: {validation_params_from_gcs}")
    for source_table, source_table_params in validation_params_from_gcs.items():
        tables_set_from_gcs.add(f"{source_table}={source_table_params['target-table']}")

    print(f"all_tables_set : {all_tables_set}")
    print(f"tables_set_from_gcs: {tables_set_from_gcs}")
    valid_comparisons_list = []
    for fqtn in tables_set_from_gcs:
        tbl_name = fqtn.split("=")[-1]
        if tbl_name in all_tables_set:
            valid_comparisons_list.append(fqtn)

    print(f"VCL: {valid_comparisons_list}")
    return valid_comparisons_list


def filter_valid_table_mappings(config_table_mapping: list, tables: str, schema: str):
    """
    Returns available table mappings from config for the tables list in data transfer config.
    Parameters:
        config_table_list: DVT source-target table name mapping, provided by user in data transfer config json.
                        Eg. ["testdb.EMPLOYEE=testdb.EMPLOYEE","testdb.DEPARTMENT=testdb.DEPARTMENT","testdb.EMPLOYEE5=testdb.EMPLOYEE5"]
        tables: Semicolon(;) separated tables list present in the data transfer config.
                        Eg. EMPLOYEE;DEPARTMENT;EMPLOYEE5
        schema: source schema for the tables in data transfer config
                        Eg. dvt
    """
    config = {}
    filtered_dvt_mappings = []

    for table_mapping in config_table_mapping:
        split_str = table_mapping.split("=")
        config[split_str[0]] = split_str[1]

    tables_list = tables.split(";")
    for table in tables_list:
        src_table_name = schema + "." + table
        if config.get(src_table_name, None):
            filtered_dvt_mappings.append(src_table_name + "=" + config[src_table_name])

    return filtered_dvt_mappings
