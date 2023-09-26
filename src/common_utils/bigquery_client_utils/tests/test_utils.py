import pytest
from testfixtures import LogCapture

from common_utils.bigquery_client_utils import utils


def test_insert_bq_json_rows_no_error(mocker):
    mocker.patch("google.cloud.bigquery.Client.insert_rows_json", return_value=[])
    table_name = "table1"
    json_rows = [{"row1": "abc"}, {"row2": "bcd"}]
    with LogCapture() as logs:
        utils.insert_bq_json_rows(table_name, json_rows)
        assert (
            "Method insert_summary_bigquery: 2 rows inserted into table table1"
            in str(logs)
        )


def test_insert_bq_json_rows_with_error(mocker):
    error = ["some error"]
    mocker.patch("google.cloud.bigquery.Client.insert_rows_json", return_value=error)
    table_name = "table2"
    json_rows = [{"row1": "abc"}, {"row2": "bcd"}]
    with pytest.raises(Exception) as exc_info:
        utils.insert_bq_json_rows(table_name, json_rows)
    assert (
        str(exc_info.value)
        == "Method insert_summary_bigquery: error inserting json_rows into table table2, \
            \n\terror: ['some error']"
    )


def test_run_query_on_bq(mocker):
    mocker.patch("google.cloud.bigquery.Client.query", return_value="query_job")
    query = "select * from table1"
    project_id = "dummy_project_id"
    expected_result = "query_job"
    with LogCapture() as logs:
        results = utils.run_query_on_bq(query, project_id)
        assert "Method run_query_on_bq" in str(logs)
        assert results == expected_result
