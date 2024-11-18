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

from unittest import mock

import pytest
from google.api_core.exceptions import NotFound

from translation.dags.translation_utils import input_validation_utils

# python -m pytest tests/unit/input_validation_utils_test.py


@pytest.mark.parametrize(
    "gs_path, expected_result",
    [
        ("gs://mybucket", True),
        ("gs://mybucket/prefix", True),
        ("gs://mybucket/prefix/file.txt", True),
        ("gs://nonexistentbucket", False),
    ],
)
@mock.patch("translation.dags.translation_utils.input_validation_utils.storage")
def test_check_gcs_bucket_exists(mock_storage, gs_path, expected_result):
    mock_bucket = mock.MagicMock()
    mock_bucket.exists.return_value = expected_result
    mock_storage.Client.return_value.bucket.return_value = mock_bucket

    assert input_validation_utils.check_gcs_bucket_exists(gs_path) == expected_result
    mock_storage.Client.return_value.bucket.assert_called_once_with(
        gs_path.split("/")[2]
    )


@pytest.mark.parametrize(
    "gs_path, expected_result",
    [
        ("gs://mybucket/dir1/file1.csv", True),
        ("gs://mybucket/file1.csv", True),
        ("gs://mybucket/nonexistentfile.csv", False),
        ("gs://nonexistentbucket/dir1/file1.csv", False),
    ],
)
@mock.patch("translation.dags.translation_utils.input_validation_utils.storage")
def test_check_gcs_file_exists(mock_storage, gs_path, expected_result):
    mock_blob = mock.MagicMock()
    mock_blob.exists.return_value = expected_result
    mock_storage.Client.return_value.bucket.return_value.blob.return_value = mock_blob

    assert input_validation_utils.check_gcs_file_exists(gs_path) == expected_result
    mock_storage.Client.return_value.bucket.assert_called_once()
    mock_storage.Client.return_value.bucket.return_value.blob.assert_called_once()


@pytest.mark.parametrize(
    "gs_path, expected_result",
    [
        ("gs://mybucket/dir1/", True),
        ("gs://mybucket/dir2/", False),
        ("gs://nonexistentbucket/dir1/", False),
    ],
)
@mock.patch("translation.dags.translation_utils.input_validation_utils.storage")
def test_check_gcs_directory_not_empty(mock_storage, gs_path, expected_result):
    mock_bucket = mock.MagicMock()
    mock_bucket.exists.return_value = True
    mock_blob = mock.MagicMock()
    mock_bucket.list_blobs.return_value = [mock_blob] if expected_result else []
    mock_storage.Client.return_value.bucket.return_value = mock_bucket

    assert (
        input_validation_utils.check_gcs_directory_not_empty(gs_path) == expected_result
    )
    mock_storage.Client.return_value.bucket.assert_called_once()
    mock_bucket.list_blobs.assert_called_once()


@pytest.mark.parametrize(
    "project_id, secret_name, expected_result",
    [
        ("myproject", "mysecret", True),
        ("myproject", "nonexistentsecret", False),
    ],
)
@mock.patch("translation.dags.translation_utils.input_validation_utils.secretmanager")
def test_check_secret_access(
    mock_secretmanager, project_id, secret_name, expected_result
):
    if expected_result is True:
        mock_secretmanager.SecretManagerServiceClient.return_value.access_secret_version.return_value = (
            mock.MagicMock()
        )
    else:
        mock_secretmanager.SecretManagerServiceClient.return_value.access_secret_version.side_effect = NotFound(
            "testing"
        )

    assert (
        input_validation_utils.check_secret_access(project_id, secret_name)
        == expected_result
    )
    mock_secretmanager.SecretManagerServiceClient.return_value.access_secret_version.assert_called_once_with(
        request={"name": f"projects/{project_id}/secrets/{secret_name}/versions/latest"}
    )


@pytest.mark.parametrize(
    "gcs_path, expected_bucket_name, expected_directory_path",
    [
        (
            "gs://mybucket/dir1/dir2/file1.csv",
            "mybucket",
            "dir1/dir2/file1.csv",
        ),
        ("gs://mybucket/file1.csv", "mybucket", "file1.csv"),
        ("gs://mybucket/", "mybucket", ""),
    ],
)
def test_split_gcs_path(gcs_path, expected_bucket_name, expected_directory_path):
    bucket_name, directory_path = input_validation_utils.split_gcs_path(gcs_path)
    assert bucket_name == expected_bucket_name
    assert directory_path == expected_directory_path
