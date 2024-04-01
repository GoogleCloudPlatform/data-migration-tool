# Note: This is a temporary stopgap solution until snowflake connector is released with run-time setup feature

import logging
import os
import socket
import subprocess
import time
from http import HTTPStatus

import jinja2
from flask import Flask, request

from sf_connector_utils import SfConnectorUtils

_APP_ROOT = os.path.dirname(os.path.abspath(__file__))
_JAR_DIR = os.path.join(_APP_ROOT, "jar/")
_JAR_PATH = os.path.join(_JAR_DIR, "snowflake-to-bq-data-transfer.jar")
_JAR_RESOURCES_TEMPLATES_DIR = os.path.join(_JAR_DIR, "resources/")
_RUNS_BASE_DIR = os.path.join(_APP_ROOT, "runs/")

_SF_CONNECTOR_HOST = "http://localhost"

_TEMPLATE_LOADER = jinja2.FileSystemLoader(searchpath=_JAR_RESOURCES_TEMPLATES_DIR)
_TEMPLATE_ENV = jinja2.Environment(loader=_TEMPLATE_LOADER)

_REQUEST_BODY_TEMPLATE_FILE = "snowflake_request_body.json.j2"
_APP_PROPERTIES_TEMPLATE_FILE = "application.properties.j2"

APP = Flask(__name__)
logging.basicConfig(level=logging.INFO)


def get_open_port():
    """
    Find and return an open port
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    s.listen(1)
    port = s.getsockname()[1]
    s.close()
    return port


def start(app_properties_file_path, port):
    """
    Start snowflake connector jar

    Args:
        app_properties_file_path (str): absolute path of application properties file
        port (int): server port for snowflake connector

    Returns:
        Popen instance for snowflake connector jar process
    """
    start_cmd = f"java -jar -Dspring.config.location={app_properties_file_path} -Dserver.port={port} {_JAR_PATH}"
    p = subprocess.Popen([start_cmd], shell=True)
    logging.info("Sleeping 100s ...")
    time.sleep(100)
    if p.poll():
        raise Exception("Snowflake connector jar startup failed")
    else:
        logging.info(f"Snowflake connector jar running on port: {port}\n")
    return p


@APP.route("/migrate-data", methods=["POST"])
def migrate_data():
    """
    Setup and start snowflake connector jar, call migrate-data api of the jar, and finally terminate the jar
    """
    try:
        params = request.get_json()
        setup_config = params["setup_config"]
        oauth_config = params["oauth_config"]
        migration_config = params["migration_config"]

        port = get_open_port()
        sf_connector = SfConnectorUtils(_SF_CONNECTOR_HOST, port)
        run_dir = f"{_RUNS_BASE_DIR}/{port}"
        os.makedirs(run_dir, exist_ok=True)

        request_body_template_vars = {
            "warehouse": setup_config["warehouse"],
            "storageIntegration": setup_config["storageIntegration"],
            "database": migration_config["sourceDatabaseName"],
            "schema": migration_config["sourceSchemaName"],
        }
        request_body_template = _TEMPLATE_ENV.get_template(_REQUEST_BODY_TEMPLATE_FILE)
        request_body_output = request_body_template.render(request_body_template_vars)
        request_body_file_path = os.path.join(run_dir, "snowflake_request_body.json")
        with open(request_body_file_path, "w") as f:
            f.write(request_body_output)

        query_mapping_file_path = os.path.join(
            run_dir, "snowflake_table_query_mapping.json"
        )
        with open(query_mapping_file_path, "w") as f:
            if (
                "table_query_mapping" in setup_config
                and setup_config["table_query_mapping"]
            ):
                f.write(setup_config["table_query_mapping"])
            else:
                f.write("{}")

        app_properties_template_vars = {
            "jdbc_url": setup_config["jdbcUrl"],
            "account_url": setup_config["accountUrl"],
            "request_body_file": request_body_file_path,
            "query_mapping_file": query_mapping_file_path,
        }
        app_properties_template = _TEMPLATE_ENV.get_template(
            _APP_PROPERTIES_TEMPLATE_FILE
        )
        app_properties_output = app_properties_template.render(
            app_properties_template_vars
        )
        app_properties_file_path = os.path.join(run_dir, "application.properties")
        with open(app_properties_file_path, "w") as f:
            f.write(app_properties_output)

        jar_process = start(app_properties_file_path, port)
        try:
            sf_connector.save_oauth_values(
                client_id=oauth_config["clientId"],
                client_secret=oauth_config["clientSecret"],
                refresh_token=oauth_config["refreshToken"],
            )
            res = sf_connector.migrate_data(migration_config)
            logging.info(f"Connector Response: {res}")
        finally:
            logging.info("Stopping snowflake connector jar")
            jar_process.kill()
        return res, HTTPStatus.OK
    except KeyError as key:
        return {"msg": f"Missing key in request body: {key}"}, HTTPStatus.BAD_REQUEST
    except Exception as e:
        return {"msg": str(e)}, HTTPStatus.BAD_REQUEST


@APP.route("/extract-ddl", methods=["POST"])
def extract_ddl():
    """
    Setup and start snowflake connector jar, call extract-ddl api of the jar, and finally terminate the jar
    """
    try:
        params = request.get_json()
        setup_config = params["setup_config"]
        oauth_config = params["oauth_config"]
        translation_config = params["translation_config"]

        port = get_open_port()
        sf_connector = SfConnectorUtils(_SF_CONNECTOR_HOST, port)
        run_dir = f"{_RUNS_BASE_DIR}/{port}"
        os.makedirs(run_dir, exist_ok=True)

        request_body_template_vars = {
            "warehouse": setup_config["warehouse"],
            "storageIntegration": setup_config["storageIntegration"],
            "database": translation_config["sourceDatabaseName"],
            "schema": translation_config["sourceSchemaName"],
        }
        request_body_template = _TEMPLATE_ENV.get_template(_REQUEST_BODY_TEMPLATE_FILE)
        request_body_output = request_body_template.render(request_body_template_vars)
        request_body_file_path = os.path.join(run_dir, "snowflake_request_body.json")
        with open(request_body_file_path, "w") as f:
            f.write(request_body_output)

        query_mapping_file_path = os.path.join(
            run_dir, "snowflake_table_query_mapping.json"
        )
        with open(query_mapping_file_path, "w") as f:
            if (
                "table_query_mapping" in setup_config
                and setup_config["table_query_mapping"]
            ):
                f.write(setup_config["table_query_mapping"])
            else:
                f.write("{}")

        app_properties_template_vars = {
            "jdbc_url": setup_config["jdbcUrl"],
            "account_url": setup_config["accountUrl"],
            "request_body_file": request_body_file_path,
            "query_mapping_file": query_mapping_file_path,
        }
        app_properties_template = _TEMPLATE_ENV.get_template(
            _APP_PROPERTIES_TEMPLATE_FILE
        )
        app_properties_output = app_properties_template.render(
            app_properties_template_vars
        )
        app_properties_file_path = os.path.join(run_dir, "application.properties")
        with open(app_properties_file_path, "w") as f:
            f.write(app_properties_output)

        jar_process = start(app_properties_file_path, port)
        try:
            sf_connector.save_oauth_values(
                client_id=oauth_config["clientId"],
                client_secret=oauth_config["clientSecret"],
                refresh_token=oauth_config["refreshToken"],
            )
            res = sf_connector.extract_ddl(translation_config)
            logging.info(f"Connector Response: {res}")
        finally:
            logging.info("Stopping snowflake connector jar")
            jar_process.kill()
        return res, HTTPStatus.OK
    except KeyError as key:
        return {"msg": f"Missing key in request body: {key}"}, HTTPStatus.BAD_REQUEST
    except Exception as e:
        return {"msg": str(e)}, HTTPStatus.BAD_REQUEST


if __name__ == "__main__":
    APP.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=True)
