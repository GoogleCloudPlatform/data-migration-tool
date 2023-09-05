import csv
import json
from pathlib import Path

import pandas as pd
from google.api_core.client_info import ClientInfo
from google.cloud import storage
from google.cloud.storage.blob import Blob

from common_utils import custom_user_agent

validation_csv_header_fields = [
    "translation-type",
    "validation-type",
    "source-table",
    "target-table",
    "source-query-file",
    "target-query-file",
    "filter-status",
    "primary-keys",
    "filters",
    "exclusion-columns",
    "allow-list",
    "count",
    "sum",
    "min",
    "max",
    "avg",
    "grouped-columns",
    "wildcard-include-string-len",
    "cast-to-bigint",
    "threshold",
    "hash",
    "concat",
    "comparison-fields",
    "use-random-row",
    "random-row-batch-size",
]


class StorageUtils:
    def __init__(self) -> None:
        self.client = storage.Client(
            client_info=ClientInfo(user_agent=custom_user_agent.USER_AGENT)
        )

    def create_bucket_path_notification(self, path, blob_name, topic_name):
        bucket_id, path_prefix = self.parse_bucket_and_blob_from_path(path)

        bucket = self.client.bucket(bucket_id)

        notification = bucket.notification(
            topic_name=topic_name,
            blob_name_prefix=append_blob_name_to_path(path_prefix, blob_name),
            event_types=["OBJECT_FINALIZE"],
        )
        notification.create()

    def read_object_from_gcsbucket(self, bucket_id, object_id):
        bucket = self.client.get_bucket(bucket_id)
        blob = storage.Blob(object_id, bucket)
        raw_config = blob.download_as_bytes()
        config = json.loads(raw_config)
        return config

    def write_object_in_gcsbucket(
        self, bucket_id: str, object_name: str, object_content: str
    ):
        bucket = self.client.bucket(bucket_id)
        blob = bucket.blob(object_name)
        blob.upload_from_string(object_content)
        gs_object_path = f"gs://{bucket_id}/{object_name}"
        return gs_object_path

    def parse_bucket_and_blob_from_path(self, path):
        blob = Blob.from_string(path, client=self.client)
        return blob.bucket.name, blob.name

    def check_object_exist_in_bucket(self, bucket_id: str, object_name: str):
        bucket = self.client.bucket(bucket_id)
        blob = bucket.blob(object_name)
        return blob.exists()

    def get_validation_params_from_gcs(
        self, bucket_id, object_name, translation_type, validation_type
    ):
        temp_file = "temporary_file.xlsx"
        dest_file = "validation_params.csv"
        bucket = self.client.get_bucket(bucket_id)
        blob = bucket.blob(object_name)
        file_extension = Path(object_name).suffix
        if file_extension == ".xlsx":
            blob.download_to_filename(temp_file)
            excel_file = pd.read_excel(temp_file)
            excel_file.to_csv(dest_file, index=None, header=True)
        else:
            blob.download_to_filename(dest_file)

        validation_params_key = (
            "source-table"
            if translation_type in ["ddl", "data"]
            else "source-query-file"
        )
        validation_type = (
            f"custom query {validation_type}"
            if translation_type == "sql"
            else validation_type
        )
        validation_params = {}
        with open(dest_file, encoding="utf-8") as csvf:
            csvReader = csv.DictReader(csvf, fieldnames=validation_csv_header_fields)
            next(csvReader)
            next(csvReader)  # skip the first two lines from csv as headers
            for rows in csvReader:
                key = rows[validation_params_key]
                if (
                    translation_type == rows["translation-type"]
                    and validation_type == rows["validation-type"]
                ):
                    validation_params[key] = rows
        return validation_params


def append_blob_name_to_path(path, blob_name):
    if path[-1] != "/":
        return f"{path}/{blob_name}"
    return f"{path}{blob_name}"
