import base64
import json


def transform_file_notification(event_json):
    return event_json


def transform_transfer_notification(event_json):
    data = json.loads(base64.b64decode(event_json["message"]["data"]))
    return {
        "TRANSFER_RUN_ID": data["name"],
        "TRANSFER_RUN_STATE": data["state"],
        "DATA_BUCKET_ID": data["params"]["bucket"],
        "SOURCE_SCHEMA": data["params"]["database_name"],
        "TABLE_NAME_PATTERN": data["params"]["table_name_patterns"],
    }


event_type_to_transformation_mapping = {
    "OBJECT_FINALIZE": transform_file_notification,
    "TRANSFER_RUN_FINISHED": transform_transfer_notification,
}


def get(event_type):
    if event_type in event_type_to_transformation_mapping:
        return event_type_to_transformation_mapping[event_type]
    return None
