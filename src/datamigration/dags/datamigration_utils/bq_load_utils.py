def get_job_status(tbl, result):
    """
    Get Job status of BQ load command
    """
    if result.returncode == 0:
        print("Loaded table: {} ".format(tbl))
        print("BigQuery job id: {}".format(result.stderr.split(" ")[2]))
        load_status = "PASS"
        reason_for_failure = "NA"
        if len(result.stderr.split(" ")) > 2:
            bq_job_id = result.stderr.split(" ")[2]
        elif len(result.stdout.split(":")) > 2:
            bq_job_id = result.stdout.split(":")[2].replace("'", "")
        else:
            bq_job_id = "NA"
    else:
        print("Error Ocuured while loading table: {table} ".format(table=tbl))
        reason_for_failure = result.stdout.strip()
        load_status = "FAIL"
        if len(result.stderr.split(" ")) > 2:
            bq_job_id = result.stderr.split(" ")[2]
        elif len(result.stdout.split(":")) > 2:
            bq_job_id = result.stdout.split(":")[2].replace("'", "")
        else:
            bq_job_id = "NA"
        print("Reason: {}".format(reason_for_failure))
        print("Printing STD ERROR")
        print(result.stderr)
        print("BigQuery job id: {}".format(bq_job_id))
    return load_status, reason_for_failure, bq_job_id
