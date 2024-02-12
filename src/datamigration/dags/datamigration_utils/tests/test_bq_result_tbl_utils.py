from datamigration_utils import bq_result_tbl_utils


def test_get_dts_run_job_stats_template_success():
    result = bq_result_tbl_utils.get_dts_run_job_stats_template(
        "unique_id_dummy",
        "transfer_config_id_dummy",
        "transfer_run_id_dummy",
        job_status="Success",
        src_table_name="dummy_table_1",
        bq_job_id="dummy_job_id_1",
        success_records=10,
        error_records=0,
        message="Job Successful",
    )
    assert result == {
        "unique_id": "unique_id_dummy",
        "transfer_config_id": "transfer_config_id_dummy",
        "transfer_run_id": "transfer_run_id_dummy",
        "job_status": "Success",
        "run_date": None,
        "src_table_name": "dummy_table_1",
        "bq_job_id": "dummy_job_id_1",
        "success_records": 10,
        "error_records": 0,
        "message": "Job Successful",
    }


def test_get_dts_run_job_stats_template_fail():
    result = bq_result_tbl_utils.get_dts_run_job_stats_template(
        "unique_id_dummy",
        "transfer_config_id_dummy",
        "transfer_run_id_dummy",
        job_status="Fail",
        src_table_name="dummy_table_2",
        bq_job_id="dummy_job_id_2",
        success_records=0,
        error_records=10,
        message="Job Failed",
    )
    assert result == {
        "unique_id": "unique_id_dummy",
        "transfer_config_id": "transfer_config_id_dummy",
        "transfer_run_id": "transfer_run_id_dummy",
        "job_status": "Fail",
        "run_date": None,
        "src_table_name": "dummy_table_2",
        "bq_job_id": "dummy_job_id_2",
        "success_records": 0,
        "error_records": 10,
        "message": "Job Failed",
    }


def test_get_dts_run_summary_template_success():
    result = bq_result_tbl_utils.get_dts_run_summary_template(
        "unique_id_dummy",
        "transfer_config_id_dummy",
        "transfer_run_id_dummy",
        transfer_run_status="Success",
        succeeded_jobs=100,
        failed_jobs=0,
        error_message="NA",
    )
    assert result == {
        "unique_id": "unique_id_dummy",
        "transfer_config_id": "transfer_config_id_dummy",
        "transfer_run_id": "transfer_run_id_dummy",
        "run_date": None,
        "transfer_run_status": "Success",
        "start_time": None,
        "end_time": None,
        "succeeded_jobs": 100,
        "failed_jobs": 0,
        "error_message": "NA",
    }


def test_get_dts_run_summary_template_fail():
    result = bq_result_tbl_utils.get_dts_run_summary_template(
        "unique_id_dummy",
        "transfer_config_id_dummy",
        "transfer_run_id_dummy",
        transfer_run_status="Fail",
        succeeded_jobs=0,
        failed_jobs=100,
        error_message="Error occured. Please check DTS logs for troubleshooting",
    )
    assert result == {
        "unique_id": "unique_id_dummy",
        "transfer_config_id": "transfer_config_id_dummy",
        "transfer_run_id": "transfer_run_id_dummy",
        "run_date": None,
        "transfer_run_status": "Fail",
        "start_time": None,
        "end_time": None,
        "succeeded_jobs": 0,
        "failed_jobs": 100,
        "error_message": "Error occured. Please check DTS logs for troubleshooting",
    }
