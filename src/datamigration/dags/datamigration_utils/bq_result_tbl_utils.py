def get_dts_run_job_stats_template(
    unique_id, transfer_config_id: str, transfer_run_id: str, **kwargs
):
    template = {
        "unique_id": unique_id,
        "transfer_config_id": transfer_config_id,
        "transfer_run_id": transfer_run_id,
        "job_status": None,
        "run_date": None,
        "src_table_name": None,
        "bq_job_id": None,
        "success_records": None,
        "error_records": None,
        "message": "",
    }
    for key, value in kwargs.items():
        template[key] = value
    return template


def get_dts_run_summary_template(
    unique_id: str, transfer_config_id: str, transfer_run_id: str, **kwargs
):
    template = {
        "unique_id": unique_id,
        "transfer_config_id": transfer_config_id,
        "transfer_run_id": transfer_run_id,
        "run_date": None,
        "transfer_run_status": None,
        "start_time": None,
        "end_time": None,
        "succeeded_jobs": 0,
        "failed_jobs": 0,
        "error_message": None,
    }
    for key, value in kwargs.items():
        template[key] = value
    return template
