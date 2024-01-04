import os
from http import HTTPStatus

from flask import Flask, request

from translation.event_listener import composer, errors, routing

app = Flask(__name__)


@app.route("/", methods=["POST"])
def index():
    valid_subfolders_list = ["ddl", "dml", "sql", "data"]

    if not request.is_json:
        print("pushed message without body")
        return errors.NO_BODY
    else:
        event_json = request.get_json()
        print(f"event_json: {event_json}")

    if not event_json:
        print("pushed message without body")
        return errors.NO_BODY

    if not isinstance(event_json, dict):
        print("invalid message format")
        return errors.INVALID_MESSAGE_FORMAT

    if "subscription" not in event_json or "message" not in event_json:
        print("pushed message must contain subscription and message field")
        return errors.SUBSCRIPTION_AND_MESSAGE_REQUIRED

    subscription = event_json["subscription"]
    next_dag_id = routing.get_dag_id(subscription)

    if not next_dag_id:
        print(f"no routing rule found to handle message from {subscription}")
        return errors.NO_ROUTING_RULE_FOUND

    event_type = event_json["message"]["attributes"]["eventType"]
    print(f"event type: {event_type}")

    if event_type == "OBJECT_FINALIZE":
        object_id = event_json["message"]["attributes"]["objectId"]
        subfolder = object_id.split("/")[0]
        file_name = os.path.basename(object_id)
        file_extension = file_name.split(".")[-1]
        if subfolder not in valid_subfolders_list or file_extension != "json":
            print(
                "Please ensure you upload only JSON configuration files to trigger DMT workflow in the folders config/[ddl, dml, sql,data] paths only"
            )
            print(
                f"File with extension: {file_extension} uploaded in subfolder: {subfolder}"
            )
            return errors.INVALID_FORMAT_OR_SUBFOLDER_FOUND

    dag_config = event_json
    status = composer.run_dag(routing.get_composer_env_url(), next_dag_id, dag_config)
    print(f"composer dag return status: {status}")

    if status >= 200 and status < 300:
        return {}, HTTPStatus.OK
    elif status >= 400 and status < 500:
        return {}, HTTPStatus.BAD_REQUEST
    elif status >= 500 and status < 600:
        return {}, HTTPStatus.BAD_GATEWAY


if __name__ == "__main__":
    PORT = int(os.getenv("PORT")) if os.getenv("PORT") else 8080
    app.run(host="127.0.0.1", port=PORT, debug=True)
