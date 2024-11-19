# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import enum
import os
from http import HTTPStatus
from shlex import split
from subprocess import STDOUT, CalledProcessError, check_output

import flask


class ValidationEntity(enum.Enum):
    Table = 1
    File = 2


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
COMPOSER_GCS_BUCKET = os.environ.get("COMPOSER_GCS_BUCKET")
SECRET_PREFIX = "secret-"


# create source and traget connection command, supports all type of connections,
# comand option-val will taken as it is from config parameter key-val pair
create_connection = """data-validation connections add {source_conn_string} && data-validation connections add {target_conn_string} && data-validation connections list"""

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

app = flask.Flask(__name__)


def _get_request_content(request):
    return request.json


def get_db_password(secret_name):
    return os.getenv(secret_name)


def connection_string(conn_config):
    conn_string = ""
    for key, val in conn_config:
        val = str(val)
        if key == "source_type" or key == "target_type":
            conn_type = val
            conn_name = val + "_CONN"
            continue
        if val.startswith(SECRET_PREFIX):
            val = get_db_password(val.removeprefix(SECRET_PREFIX))
        conn_string = conn_string + f"--{key} '{val}' "
    if conn_type != "BigQuery":
        conn_string = (
            f"--secret-manager-type GCP --secret-manager-project-id {PROJECT_ID} --connection-name {conn_name} {conn_type} "
            + conn_string
        )
    else:
        conn_string = f"--connection-name {conn_name} {conn_type} " + conn_string
    return conn_name, conn_string


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


def get_dvt_cmd_ddl_validation(config, table, validation_params_from_gcs):
    cmd_errors = ""
    print(f"Running validation for table: {table}")

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
            project_id=PROJECT_ID,
            unique_id=config["unique_id"],
        )

    elif validation_type == "column":
        validation_command = column_validation.format(
            source_conn=source_conn,
            target_conn=target_conn,
            table=table,
            bq_table=table.split("=")[-1],
            project_id=PROJECT_ID,
            unique_id=config["unique_id"],
        )

    elif validation_type == "row":
        validation_command = row_validation.format(
            source_conn=source_conn,
            target_conn=target_conn,
            table=table,
            bq_table=table.split("=")[-1],
            project_id=PROJECT_ID,
            unique_id=config["unique_id"],
        )
    additional_validation_flags = get_additional_validation_flags(
        ValidationEntity.Table,
        table,
        validation_type_flags_mapping[validation_type],
        validation_params_from_gcs,
    )
    validation_command += additional_validation_flags
    print(f"DVT command to be executed: {validation_command}")
    dvt_bash = " ) && ( ".join(["( " + add_conn, validation_command + " )"])
    return dvt_bash, cmd_errors


def get_dvt_cmd_sql_validation(config, sql_file, validation_params_from_gcs):
    cmd_errors = ""
    print(f"Running validation for sql file: {sql_file}")

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
            project_id=PROJECT_ID,
            unique_id=config["unique_id"],
        )

    elif custom_query_validation_type == "column":
        custom_validation_command = custom_query_column_validation.format(
            source_conn=source_conn,
            target_conn=target_conn,
            sql_file=sql_file,
            project_id=PROJECT_ID,
            unique_id=config["unique_id"],
        )

    else:
        print(
            f"Unknown validation type: {custom_query_validation_type} passed with config['type'] == 'sql'!"
        )
        cmd_errors += f"Unknown validation type: {custom_query_validation_type} passed with config['type'] == 'sql'!"
        return add_conn, cmd_errors

    additional_validation_flags = get_additional_validation_flags(
        ValidationEntity.File,
        sql_file,
        validation_type_flags_mapping[custom_query_validation_type],
        validation_params_from_gcs,
    )
    custom_validation_command += additional_validation_flags
    print(f"DVT command to be executed: {custom_validation_command}")
    dvt_bash = " ) && ( ".join(
        ["( " + add_conn, copy_sql_command, custom_validation_command + " )"]
    )
    return dvt_bash, cmd_errors


@app.route("/", methods=["POST"])
def run():
    response, response_code = "Empty Response", HTTPStatus.INTERNAL_SERVER_ERROR
    request_content = _get_request_content(flask.request)
    config = request_content["config"]
    validation_params_from_gcs = request_content["validation_params_from_gcs"]
    validation_type = config["type"]
    if validation_type in ["ddl", "data", "dml"]:
        table = request_content["table"]
        dvt_cmd, cmd_errors = get_dvt_cmd_ddl_validation(
            config, table, validation_params_from_gcs
        )
    else:
        sql_file = request_content["sql_file"]
        dvt_cmd, cmd_errors = get_dvt_cmd_sql_validation(
            config, sql_file, validation_params_from_gcs
        )
    if cmd_errors != "":
        response = cmd_errors
    else:
        wrapper_cmd = split("/bin/sh -c")
        wrapper_cmd.append(dvt_cmd)
        try:
            output = check_output(wrapper_cmd, stderr=STDOUT)
            print("DVT validation successful!")
            response, response_code = output.decode(), HTTPStatus.OK
        except CalledProcessError as exc:
            print("Encountered error in DVT validation!")
            response, response_code = (
                exc.output.decode(),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    print(f"Response code sent back to DAG: {response_code}")
    print("Sent following response back to DAG")
    print(response)
    return response, response_code


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
