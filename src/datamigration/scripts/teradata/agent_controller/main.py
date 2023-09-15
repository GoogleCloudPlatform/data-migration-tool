import json
import logging
import os
from json.decoder import JSONDecodeError
from logging.handlers import TimedRotatingFileHandler

from google.cloud import pubsub_v1

from config import config
from controller import Controller

PROJECT_ID = config["project_id"]
SUBSCRIPTION_ID = config["subscription_id"]
SUBSCRIPTION_NAME = "projects/{project_id}/subscriptions/{sub}".format(
    project_id=PROJECT_ID,
    sub=SUBSCRIPTION_ID,
)

LOGGING_ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
LOGGING_MAX_BACKUP_DAYS = 30
LOGGING_LOG_FILE_NAME = "agent-controller.log"

_LOGGER = logging.getLogger(__name__)


def _init_logger():
    """
    Updates logging config and handlers

    Returns:
        None
    """
    logs_dir = os.path.join(LOGGING_ROOT_DIR, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    log_file = os.path.join(logs_dir, LOGGING_LOG_FILE_NAME)

    # Create and configure logger
    logging.basicConfig(
        format="[%(asctime)s] | %(levelname)s |  %(message)s",
    )
    logger = logging.getLogger()
    logger.addHandler(logging.StreamHandler())
    logger.addHandler(
        TimedRotatingFileHandler(
            log_file, when="D", interval=1, backupCount=LOGGING_MAX_BACKUP_DAYS
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
        future = subscriber.subscribe(SUBSCRIPTION_NAME, callback)
        try:
            result = future.result()
        except KeyboardInterrupt:
            future.cancel()
            _LOGGER.info("Controller Stopped")
