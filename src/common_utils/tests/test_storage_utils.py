import os
import shutil
from pathlib import Path
from unittest import mock
from unittest.mock import Mock

import pytest

from common_utils import storage_utils

csv_column_mappings = [
    ("ddl", "schema", "dmt.EMPLOYEE1"),
    ("ddl", "column", "dmt.EMPLOYEE2"),
    ("ddl", "column", "dmt.EMPLOYEE3"),
    ("ddl", "row", "dmt.EMPLOYEE4"),
    ("ddl", "row", "dmt.EMPLOYEE5"),
    ("data", "column", "dmt.EMPLOYEE6"),
    ("data", "row", "dmt.EMPLOYEE7"),
]


@pytest.mark.parametrize(
    "translation_type, validation_type, source_table", csv_column_mappings
)
@mock.patch("common_utils.storage_utils.storage")
def test_get_validation_params_from_gcs(
    mock_storage, translation_type, validation_type, source_table
):
    shutil.copy(
        Path(__file__).parent / "mock/validation_params.csv", os.getcwd()
    )  # copy mock file to location required by source function code
    storage_utils_object = storage_utils.StorageUtils()
    mock_gcs_client = mock_storage.Client.return_value
    mock_bucket = Mock()
    mock_gcs_client.bucket.return_value = mock_bucket
    validation_params = storage_utils_object.get_validation_params_from_gcs(
        "bucket_id", "object_name", translation_type, validation_type
    )
    assert source_table in validation_params.keys()
    assert validation_params[source_table]["translation-type"] == translation_type
    assert validation_params[source_table]["validation-type"] == validation_type
    os.remove(
        f"{os.getcwd()}/validation_params.csv"
    )  # delete temp file created for unit test


sample_paths_with_blob_names = [
    ("sample/path1/sample_blob_name1", "sample/path1", "sample_blob_name1"),
    ("sample/path2/sample_blob_name2", "sample/path2", "sample_blob_name2"),
]


@pytest.mark.parametrize("appended_path, path, blob_name", sample_paths_with_blob_names)
def test_append_blob_name_to_path(appended_path, path, blob_name):
    result = storage_utils.append_blob_name_to_path(path, blob_name)
    assert result == appended_path


sample_gcs_paths_with_blob_names = [
    (
        "gs://demo-bucket-1/output/ddl/sample-folder-1",
        "demo-bucket-1",
        "output/ddl/sample-folder-1",
    ),
    (
        "gs://demo-bucket-2/output/ddl/sample-folder-2",
        "demo-bucket-2",
        "output/ddl/sample-folder-2",
    ),
]


@pytest.mark.parametrize("path, bucket, blob", sample_gcs_paths_with_blob_names)
def test_parse_bucket_and_blob_from_path(path, bucket, blob):
    storage_utils_object = storage_utils.StorageUtils()
    bucket_name, blob_name = storage_utils_object.parse_bucket_and_blob_from_path(path)
    assert bucket_name == bucket
    assert blob_name == blob
