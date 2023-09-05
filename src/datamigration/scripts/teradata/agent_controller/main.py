import json
from json.decoder import JSONDecodeError

from config import config
from controller import Controller
from google.cloud import pubsub_v1
from logger import logger

PROJECT_ID = config["project_id"]
SUBSCRIPTION_ID = config["subscription_id"]
SUBSCRIPTION_NAME = "projects/{project_id}/subscriptions/{sub}".format(
    project_id=PROJECT_ID,
    sub=SUBSCRIPTION_ID,
)


def callback(message):
    """
    Callback function to invoke controller whenever pub/sub payload is received from subscription

    Args:
        message: pub/sub message

    Returns:
        None
    """
    data = message.data.decode("utf-8")
    logger.info(f"PUB/SUB Payload received:\n{data}")
    try:
        data = json.loads(data)
        controller = Controller(data)
        controller.run_action()
    except JSONDecodeError as e:
        logger.error(f"Invalid JSON payload: {e}")
    except Exception as e:
        logger.exception(e)
    finally:
        message.ack()
        logger.info("Payload Acknowledged")
        logger.info("--\n")


if __name__ == "__main__":
    logger.info("Starting controller")
    with pubsub_v1.SubscriberClient() as subscriber:
        future = subscriber.subscribe(SUBSCRIPTION_NAME, callback)
        try:
            result = future.result()
        except KeyboardInterrupt:
            future.cancel()
            logger.info("Controller Stopped")
