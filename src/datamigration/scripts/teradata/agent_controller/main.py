import json
import logging
import os
from json.decoder import JSONDecodeError
from logging.handlers import TimedRotatingFileHandler

from config import config
from controller import Controller
from google.cloud import pubsub_v1

_PROJECT_ID = config["project_id"]
_SUBSCRIPTION_ID = config["subscription_id"]
_SUBSCRIPTION_NAME = "projects/{project_id}/subscriptions/{sub}".format(
    project_id=_PROJECT_ID,
    sub=_SUBSCRIPTION_ID,
)

_LOGGING_ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
_LOGGING_MAX_BACKUP_DAYS = 30
_LOGGING_LOG_FILE_NAME = "agent-controller.log"

_LOGGER = logging.getLogger(__name__)


def _init_logger():
    """
    Updates logging config and handlers

    Returns:
        None
    """
    logs_dir = os.path.join(_LOGGING_ROOT_DIR, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    log_file = os.path.join(logs_dir, _LOGGING_LOG_FILE_NAME)

    # Create and configure logger
    logging.basicConfig(
        format="[%(asctime)s] | %(levelname)s |  %(message)s",
    )
    logger = logging.getLogger()
    logger.addHandler(logging.StreamHandler())
    logger.addHandler(
        TimedRotatingFileHandler(
            log_file, when="D", interval=1, backupCount=_LOGGING_MAX_BACKUP_DAYS
        )
    )

    # Setting the threshold of logger to INFO
    logger.setLevel(logging.INFO)


def callback(message):
    """
    Callback function to invoke controller whenever pub/sub payload is received from subscription

    Args:
        message: pub/sub message

    Returns:
        None
    """
    data = message.data.decode("utf-8")
    _LOGGER.info(f"PUB/SUB Payload received:\n{data}")
    try:
        data = json.loads(data)
        controller = Controller(data)
        controller.run_action()
    except JSONDecodeError as e:
        _LOGGER.error(f"Invalid JSON payload: {e}")
    except Exception as e:
        _LOGGER.exception(e)
    finally:
        message.ack()
        _LOGGER.info("Payload Acknowledged")
        _LOGGER.info("--\n")


if __name__ == "__main__":
    _init_logger()
    _LOGGER.info("Starting controller")
    with pubsub_v1.SubscriberClient() as subscriber:
        future = subscriber.subscribe(_SUBSCRIPTION_NAME, callback)
        try:
            result = future.result()
        except KeyboardInterrupt:
            future.cancel()
            _LOGGER.info("Controller Stopped")
