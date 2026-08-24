"""
AlphaCandle - Isolated logging setup. Writes to its own logs/ folder inside
this project directory - never touches Ali's original log path.
"""
import logging
from logging.handlers import RotatingFileHandler
import os
from datetime import datetime
import config


def setup_logging(level=logging.INFO, console_level=logging.INFO):
    os.makedirs(config.LOG_DIR, exist_ok=True)
    today = datetime.now(config.TIME_ZONE).date()
    log_path = os.path.join(config.LOG_DIR, f"alphacandle_{today}.log")

    formatter = logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s")

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    if root_logger.handlers:
        return

    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(formatter)

    file_handler = RotatingFileHandler(log_path, maxBytes=5 * 1024 * 1024, backupCount=5)
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
