import logging
import os
import re

from common_utils.bigquery_client_utils import utils as bq_utils

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")


def parse_full_transfer_runID(full_run_id: str):
    RUN_ID_PATTERN1 = r"projects/(.*)/locations/(.*)/transferConfigs/(.*)/runs/(.*)"
    RUN_ID_PATTERN2 = r"projects/(.*)/transferConfigs/(.*)/runs/(.*)"

    if "locations" in full_run_id:
        pattern_match = re.match(RUN_ID_PATTERN1, full_run_id)
        location = pattern_match.group(2)
        transfer_config_id = pattern_match.group(3)
        run_id = pattern_match.group(4)
    else:
        pattern_match = re.match(RUN_ID_PATTERN2, full_run_id)
        location = None
        transfer_config_id = pattern_match.group(2)
        run_id = pattern_match.group(3)
    project_id = pattern_match.group(1)
    return project_id, location, transfer_config_id, run_id


def get_tracking_info(transfer_config_id: str, tracking_table: str, cols=None):
    if cols is None:
        select_cols = "*"
    else:
        if isinstance(cols, list):
            select_cols = ", ".join(cols)
        else:
            raise TypeError(
                "unsupported argument type for 'cols', expected type: 'list'"
            )
    mapping_query = f"select {select_cols} from `{tracking_table}` \
        where transfer_config_id = '{transfer_config_id}'"
    mapping_record = bq_utils.run_query_on_bq(mapping_query, PROJECT_ID)
    for row in mapping_record:
        logging.info(
            f"Method get_tracking_info: tracking parameters loaded for transfer_config_id {transfer_config_id}: \n{row}"
        )
        return row
    raise Exception(
        f"Method read_dts_config_mappings: No entry in `{tracking_table}` \
        for transfer_config_id: {transfer_config_id} Please ensure that the required details are updated in the table to proceed"
    )
