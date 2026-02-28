import logging
from datetime import datetime
from pathlib import Path
from logging.handlers import RotatingFileHandler
from stockify.config import get_logs_path

# Logs directory
LOG_DIR = get_logs_path()
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Timestamped log file
LOG_FILE = datetime.now().strftime("%m_%d_%Y_%H_%M_%S.log")
LOG_FILE_PATH = LOG_DIR / LOG_FILE

# File logger
logger = logging.getLogger("app_logger")
logger.setLevel(logging.INFO)

if not logger.handlers:
    file_handler = RotatingFileHandler(
        LOG_FILE_PATH,
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=5
    )
    file_handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | "
    "%(filename)s:%(lineno)d | %(funcName)s | %(message)s"
)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

# Terminal logger
logger_terminal = logging.getLogger("ETL-Logs")
logger_terminal.setLevel(logging.INFO)

if not logger_terminal.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)
    logger_terminal.addHandler(console_handler)
