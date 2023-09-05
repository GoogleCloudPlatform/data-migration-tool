import logging
import os
from logging.handlers import TimedRotatingFileHandler

"""
Initializes logging instance
"""

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
MAX_BACKUP_DAYS = 30
LOG_FILE_NAME = "agent-controller.log"

logs_dir = os.path.join(ROOT_DIR, "logs")
os.makedirs(logs_dir, exist_ok=True)
log_file = os.path.join(logs_dir, LOG_FILE_NAME)

# Create and configure logger
logging.basicConfig(
    format="[%(asctime)s] | %(levelname)s |  %(message)s",
)
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.addHandler(
    TimedRotatingFileHandler(
        log_file, when="D", interval=1, backupCount=MAX_BACKUP_DAYS
    )
)

# Setting the threshold of logger to DEBUG
logger.setLevel(logging.INFO)
