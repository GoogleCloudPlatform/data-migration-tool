import ast
import logging
import os

from airflow.configuration import conf
from airflow.exceptions import AirflowFailException
from airflow.models.baseoperator import BaseOperator
from airflow.utils.log.gcs_task_handler import GCSTaskHandler
from airflow.utils.state import State
from google.api_core.client_info import ClientInfo
from google.cloud import bigquery

from common_utils import custom_user_agent

SUCCESS_STATUS = "Success"
FAILED_STATUS = "Failed"
PARTIAL_SUCCESS = "Partial Success"

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
REPORT_RESULTS_TABLE_ID = f"{PROJECT_ID}.dmt_logs.dmt_report_table"


class ReportingOperator(BaseOperator):
    template_fields = ("config",)

    def __init__(self, configuration=None, **kwargs) -> None:
        super().__init__(**kwargs)
        if configuration is None:
            self.config = None
        else:
            self.config = configuration

    def __getErrorMessage(self, log):
        errMsg = ""
        for logline in log[0].split("\n"):
            if "ERROR" in logline and "standard_task_runner.py" in logline:
                errMsg = logline.split("ERROR - ")[1]
                break
        return errMsg

    def __saveDAGReport(self, dagReportRecord):
        logging.error(f"DAG reporting insertion record - {dagReportRecord}")

        bq_client = bigquery.Client(
            client_info=ClientInfo(user_agent=custom_user_agent.USER_AGENT)
        )

        insertion_status = bq_client.insert_rows_json(
            REPORT_RESULTS_TABLE_ID, dagReportRecord
        )

        logging.info(f"insertion status :: {insertion_status}")

    def execute(self, context):
        if self.config is None or self.config == dict():
            logging.info("Configuration is not given in operator")
            config = (
                ast.literal_eval(context["dag_run"].conf["config"])
                if (isinstance(context["dag_run"].conf["config"], str))
                else context["dag_run"].conf["config"]
            )
        else:
            config = (
                ast.literal_eval(self.config)
                if (isinstance(self.config, str))
                else self.config
            )

        failedTaskArr = []
        dynamicFailedTaskCnt = 0

        upstreamFailedTaskArr = []

        remote_base_log_folder = conf.get("logging", "remote_base_log_folder")

        contextTaskId = context["task_instance"].task_id
        logging.info(f"context task_id - {contextTaskId}")

        for task_instance in context["dag_run"].get_task_instances():
            logging.info(
                f"ti task_id - {task_instance.task_id} and its status {task_instance.state}"
            )

            if (
                task_instance.state != State.SUCCESS
                and task_instance.task_id != context["task_instance"].task_id
            ):
                if (
                    task_instance.state == State.FAILED
                    and task_instance.task_id != contextTaskId
                ):
                    # read failed task log
                    fth = GCSTaskHandler(
                        base_log_folder="log",
                        gcs_log_folder=f"{remote_base_log_folder}",
                    )
                    actual = fth._read(
                        ti=task_instance, try_number=task_instance.try_number - 1
                    )

                    # get error message from log
                    errorMessage = self.__getErrorMessage(actual)

                    logging.info(
                        f"Task {task_instance.task_id} is failed with error {errorMessage} and in {task_instance.try_number-1} total tries"
                    )

                    failedTaskArr.append(
                        {
                            "task_name": task_instance.task_id,
                            "error_message": errorMessage,
                            "log_link": task_instance.log_url,
                        }
                    )

                    # Task instance with map index not a -1 means task is Dynamically mapped task
                    if task_instance.map_index != -1:
                        logging.info(
                            f"Failed task {task_instance.task_id} is part of Dynamic Task Mapping"
                        )
                        dynamicFailedTaskCnt = dynamicFailedTaskCnt + 1

                elif (
                    task_instance.state == State.UPSTREAM_FAILED
                    and task_instance.task_id != contextTaskId
                ):
                    upstreamFailedTaskArr.append(task_instance.task_id)
            else:
                logging.info(f"Task {task_instance.task_id} is successful")

        dagStatus = FAILED_STATUS
        if len(failedTaskArr) == 0 and len(upstreamFailedTaskArr) == 0:
            dagStatus = SUCCESS_STATUS
        elif (
            len(failedTaskArr) > 0
            and len(failedTaskArr) == dynamicFailedTaskCnt
            and len(upstreamFailedTaskArr) == 0
        ):
            dagStatus = PARTIAL_SUCCESS

        reportRecord = [
            {
                "unique_id": config["unique_id"],
                "dag_name": getattr(context["dag"], "dag_id"),
                "execution_time": str(context["execution_date"]),
                "dag_status": dagStatus,
                "source_db": config["source"],
                "Error": failedTaskArr,
            }
        ]

        self.__saveDAGReport(reportRecord)

        if dagStatus is FAILED_STATUS:
            logging.info(
                "Task {} failed. Failing this DAG run".format(
                    [errorItm["task_name"] for errorItm in reportRecord[0]["Error"]]
                )
            )
            raise AirflowFailException(
                "Task {} failed. Failing this DAG run".format(
                    [errorItm["task_name"] for errorItm in reportRecord[0]["Error"]]
                )
            )
