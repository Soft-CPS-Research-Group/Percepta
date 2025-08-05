import sys
import os
from loguru import logger

class LoggingUtils():
    def __init__(self, name, configurations):
        max_size = configurations.get('LogFiles').get('maxSize')
        file_path = os.path.join("logs", f"{name}/{name}.log")
        # Extracts the directory from the given path
        log_dir = os.path.dirname(file_path)
        # Creates the directory if it doesn't exist
        if not os.path.exists(log_dir):
            os.makedirs(log_dir, mode=0o777, exist_ok=True)
        # Removes default handlers
        logger.remove()
        logger.add(
            file_path,
            # Rotates log when it reaches max_size (e.g., 10MB)
            rotation=max_size,
            # Keeps logs for 10 days
            retention="10 days",
            # Compresses old logs to save space
            compression="zip",
            level="WARNING",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
            enqueue=True
        )
        logger.add(sys.stderr, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")

    def info(self, message):
        logger.info(message)

    def debug(self, message):
        logger.debug(message)

    def error(self, message):
        logger.error(message)

    def warning(self, message):
        logger.warning(message)

    def critical(self, message):
        logger.critical(message)