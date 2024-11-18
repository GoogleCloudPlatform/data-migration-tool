# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow.exceptions import AirflowFailException
from google.api_core.exceptions import NotFound
from google.cloud import secretmanager_v1 as secretmanager
from google.cloud import storage

from common_utils import constants


def check_gcs_bucket_exists(gs_path: str) -> bool:
    bucket_name, _ = split_gcs_path(gs_path)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    if bucket.exists():
        return True
    else:
        return False


def check_gcs_file_exists(gs_path: str) -> bool:
    bucket_name, file_path = split_gcs_path(gs_path)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    return blob.exists()


def check_gcs_directory_not_empty(gs_path: str) -> bool:
    bucket_name, directory_path = split_gcs_path(gs_path)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    if not bucket.exists():
        return False

    blobs = bucket.list_blobs(prefix=directory_path)
    for _ in blobs:
        return True
    return False


def check_secret_access(project_id: str, secret_name: str) -> bool:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    try:
        client.access_secret_version(request={"name": f"{name}"})
        return True
    except NotFound:
        return False
    except Exception as e:
        raise e


def split_gcs_path(gcs_path):
    # Splits a GCS path into bucket name and directory path.
    # Input: "gs://mybucket/dir1/dir2/file1.csv"
    # Output: "mybucket", "dir1/dir2/file1.csv"
    path_parts = gcs_path.removeprefix("gs://").split("/", 1)
    bucket_name = path_parts[0]
    directory_path = path_parts[1] if len(path_parts) > 1 else ""
    return bucket_name, directory_path


def normalize_and_validate_config(project_id: str, config: dict) -> dict:
    # Normalize source name
    if "source" in config:
        config["source"] = config["source"].lower()

    if "migrationTask" in config:
        # Normalize GCS paths in the config by removing trailing slashes.
        translationConfigDetails = config["migrationTask"]["translationConfigDetails"]
        translationConfigDetails["gcsSourcePath"] = translationConfigDetails[
            "gcsSourcePath"
        ].rstrip("/")
        translationConfigDetails["gcsTargetPath"] = translationConfigDetails[
            "gcsTargetPath"
        ].rstrip("/")

        # Check that translation input directory is not empty.
        if not check_gcs_directory_not_empty(translationConfigDetails["gcsSourcePath"]):
            raise AirflowFailException(
                f'No translation input files found at gcsSourcePath={translationConfigDetails["gcsSourcePath"]}.'
            )

        # Check that translation output bucket exists.
        if not check_gcs_bucket_exists(translationConfigDetails["gcsTargetPath"]):
            raise AirflowFailException(
                f'Translation output bucket does not exist at gcsTargetPath={translationConfigDetails["gcsTargetPath"]}.'
            )

    if "validation_config" in config:
        # Note: DVT source names are case-sensitive at present, so it's not easy to normalize them without maintaining a copy of their source name mapping here.

        validation_params_file = config["validation_config"][
            "validation_params_file_path"
        ]

        # Check that validation params file exists.
        if not check_gcs_file_exists(validation_params_file):
            raise AirflowFailException(
                f"Validation config parameters file not found at validation_params_file_path={validation_params_file}."
            )

        # Check that secrets exist in Secret Manager.
        if "password" in config["validation_config"]["source_config"] and config[
            "validation_config"
        ]["source_config"]["password"].startswith(constants.SECRET_PREFIX):
            src_pw_secret = config["validation_config"]["source_config"]["password"]
            if not check_secret_access(project_id, src_pw_secret):
                raise AirflowFailException(
                    f"Secret not found in Secret Manager with the name {src_pw_secret}."
                )

        if "password" in config["validation_config"]["target_config"] and config[
            "validation_config"
        ]["target_config"]["password"].startswith(constants.SECRET_PREFIX):
            tgt_pw_secret = config["validation_config"]["target_config"]["password"]
            if not check_secret_access(project_id, tgt_pw_secret):
                raise AirflowFailException(
                    f"Secret not found in Secret Manager with the name {tgt_pw_secret}."
                )

    return config
