import concurrent.futures
import logging
from enum import Enum

from google.api_core.client_info import ClientInfo
from google.cloud import bigquery

from common_utils import custom_user_agent


class ScriptStatus(Enum):
    PENDING = 1
    STARTED = 2
    DONE = 3
    FAILED = 4
    RETRY = 5

    @property
    def is_terminal(self) -> bool:
        return self in (ScriptStatus.DONE, ScriptStatus.FAILED)


class RunScriptException(Exception):
    pass


class Script:
    bq_client = None

    @classmethod
    def get_bq_client(cls):
        if not Script.bq_client:
            Script.bq_client = bigquery.Client(
                client_info=ClientInfo(user_agent=custom_user_agent.USER_AGENT)
            )
        return Script.bq_client

    def __init__(self, filename):
        self.filename = filename
        self.status = ScriptStatus.PENDING
        self.retries = 0
        self.script = None
        self.job = None

    def get_script(self) -> str:
        if not self.script:
            f = open(self.filename, "r")
            self.script = f.read()
            f.close()

        return self.script

    def get_status(self) -> ScriptStatus:
        return self.status

    def get_job(self) -> bigquery.QueryJob:
        return self.job

    def run(self, executor, job_id_prefix=None):
        if not self.is_runnable():
            raise RunScriptException(f"script with status {self.status} can't be ran")

        if self.status == ScriptStatus.RETRY:
            self.retries += 1

        self.job = Script.get_bq_client().query(
            self.get_script(), job_id_prefix=job_id_prefix
        )

        def async_task():
            try:
                self.job.result()
            finally:
                return self

        future_script = executor.submit(async_task)
        self.status = ScriptStatus.STARTED

        return future_script

    def is_runnable(self) -> bool:
        return not self.status.is_terminal and self.status != ScriptStatus.STARTED

    def done(self) -> bool:
        return self.status == ScriptStatus.DONE

    def failed(self) -> bool:
        return self.status == ScriptStatus.FAILED

    def mark_for_retry(self):
        self.status = ScriptStatus.RETRY

    def mark_as_failed(self):
        self.status = ScriptStatus.FAILED

    def mark_as_done(self):
        self.status = ScriptStatus.DONE

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"{{ filename: {self.filename}, status: {self.status}, retries: {self.retries} }}"


def run_script_files(file_list, error_handler, success_handler, job_id_prefix=None):
    scripts = []
    for file in file_list:
        if file.endswith(".sql"):
            scripts.append(Script(file))
    marked_for_retry_count = 0
    with concurrent.futures.ThreadPoolExecutor() as executor:
        while True:
            # Run all runnable scripts async
            future_scripts = map(
                lambda script: script.run(executor, job_id_prefix),
                filter(Script.is_runnable, scripts),
            )

            # Handle results
            for future_script in concurrent.futures.as_completed(future_scripts):
                script = future_script.result()
                job_exception = script.get_job().exception()
                if not job_exception:
                    script.mark_as_done()
                    success_handler(script)
                else:
                    script.mark_as_failed()
                    error_handler(script, job_exception)

            marked_for_retry = list(
                filter(
                    lambda script: script.get_status() == ScriptStatus.RETRY, scripts
                )
            )
            if len(marked_for_retry) == marked_for_retry_count:
                logging.info(
                    f"stopped retrying, retried {len(marked_for_retry)} files but none succeed, marking them as FAILED"
                )
                for script in marked_for_retry:
                    script.mark_as_failed()
                break
            else:
                marked_for_retry_count = len(marked_for_retry)

            logging.info(
                f"finished iteration, marked {marked_for_retry_count} for retry"
            )

            if not marked_for_retry_count:
                break

        return scripts
