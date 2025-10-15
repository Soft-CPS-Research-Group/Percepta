import logging
import logging.handlers
import time
import os

class LoggingUtils:
    """
    Thread-safe logging utility built on Python's standard logging module.

    Each instance creates a dedicated logger for a given component and environment,
    with independent handlers to ensure that logs from different components or
    environments do not mix.
    """

    def __init__(self, component_name: str, configurations: dict, environment: str = None):
        """
        Initializes the LoggingUtils instance.

        Args:
            component_name (str): Name of the component (e.g., accumulator, predictor).
            configurations (dict): Dictionary containing log configuration. Must include 'log_files' -> 'max_size'.
            environment (str, optional): Environment name. If not provided, logs are stored under 'general'.
        """
        self._environment = environment if environment else "general"
        self._component = component_name

        max_size = int(configurations["log_files"]["max_size"])

        # Build the log directory structure
        log_dir = os.path.join("logs", self._environment, component_name)
        os.makedirs(log_dir, exist_ok=True)
        file_path = os.path.join(log_dir, f"{component_name}.log")

        # Create a dedicated logger
        logger_name = f"{self._environment}-{component_name}"
        self._logger = logging.getLogger(logger_name)
        self._logger.setLevel(logging.DEBUG)
        self._logger.propagate = False

        # File handler
        file_handler = logging.handlers.RotatingFileHandler(
            file_path, maxBytes=max_size, backupCount=5, encoding="utf-8"
        )
        format = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(environment)s | %(component)s | %(message)s"
        )
        file_handler.setFormatter(format)
        file_handler.setLevel(logging.WARNING)

        # Console handler
        console_handler = logging.StreamHandler()

        console_handler.setFormatter(format)
        console_handler.setLevel(logging.DEBUG)

        # Add handlers if not already added
        if not self._logger.handlers:
            self._logger.addHandler(file_handler)
            self._logger.addHandler(console_handler)

    # Internal method to add extra fields
    def _log(self, level: str, message: str):
        self._logger.log(level, message, extra={"environment": self._environment, "component": self._component})

    def info(self, message: str):
        """Logs an INFO-level message."""
        self._log(logging.INFO, message)

    def debug(self, message: str):
        """Logs a DEBUG-level message."""
        self._log(logging.DEBUG, message)

    def error(self, message: str):
        """Logs an ERROR-level message."""
        self._log(logging.ERROR, message)

    def warning(self, message: str):
        """Logs a WARNING-level message."""
        self._log(logging.WARNING, message)

    def critical(self, message: str):
        """Logs a CRITICAL-level message."""
        self._log(logging.CRITICAL, message)
