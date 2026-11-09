import logging
import logging.handlers
from typing import Union, Tuple
import os
import re


class _RedactingFormatter(logging.Formatter):
    """Formatter that hides secrets (tokens, passwords) in the whole log line, including tracebacks.

    The secrets are hidden here, at the last step, so that it does not matter which component wrote them (a URL with a
    token in the query string, a request body with a password, an exception message that repeats either of them).
    """

    _MASK: str = "***"
    _PATTERNS: tuple = (
        # A JSON Web Token (e.g., the access and refresh tokens): three base64url parts separated by dots
        (re.compile(r"eyJ[\w-]{5,}\.[\w-]{5,}\.[\w-]*"), _MASK),
        # A secret in a query string or in key=value form: token=..., refresh_token=..., password=...
        (re.compile(r"(?i)\b((?:access_|refresh_)?token|password|passwd|secret|api_?key)=[^&\s'\"]+"), r"\1=" + _MASK),
        # A secret in a JSON or Python dictionary: "password": "...", 'refresh_token': '...'
        (
            re.compile(r"(?i)([\"']\s*(?:(?:access_|refresh_)?token|password|passwd|secret|api_?key)\s*[\"']\s*:\s*)([\"']).*?\2"),
            r"\1\2" + _MASK + r"\2"
        ),
    )

    def format(self, record: logging.LogRecord) -> str:
        """Returns the formatted line with the secrets replaced by '***'."""
        line = super().format(record)
        for pattern, replacement in self._PATTERNS:
            line = pattern.sub(replacement, line)
        return line


class _ConsoleHandler(logging.StreamHandler):
    """Console handler that leaves a blank line after every log line, to make the console easier to read."""

    terminator = "\n\n"


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
                May include 'console_level' (DEBUG, INFO, WARNING, ERROR or CRITICAL; defaults to INFO).
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
        format = _RedactingFormatter(
            "%(asctime)s | %(levelname)s | %(environment)s | %(component)s | %(message)s"
        )
        file_handler.setFormatter(format)
        file_handler.setLevel(logging.DEBUG)

        # Console handler
        console_handler = _ConsoleHandler()

        console_handler.setFormatter(format)
        # Console shows INFO and above (optional "console_level" in the configurations, e.g. "DEBUG");
        # the log file always keeps everything, including DEBUG.
        console_level = str(configurations.get("console_level", "INFO")).upper()
        console_handler.setLevel(getattr(logging, console_level, logging.INFO))

        # Add handlers if not already added
        if not self._logger.handlers:
            self._logger.addHandler(file_handler)
            self._logger.addHandler(console_handler)

    def _log(self, level: int, message: str, exc_info: Union[bool, BaseException, tuple] = False):
        self._logger.log(
            level,
            message,
            extra={"environment": self._environment, "component": self._component},
            exc_info=exc_info,
        )

    def info(self, message: str):
        """Logs an INFO-level message."""
        self._log(logging.INFO, message)

    def debug(self, message: str):
        """Logs a DEBUG-level message."""
        self._log(logging.DEBUG, message)

    def error(self, message: str, exc_info: Union[bool, BaseException, tuple] = False):
        """Logs an ERROR-level message."""
        self._log(logging.ERROR, message, exc_info=exc_info)

    def warning(self, message: str, exc_info: bool = False):
        """Logs a WARNING-level message."""
        self._log(logging.WARNING, message, exc_info=exc_info)

    def critical(self, message: str, exc_info: bool = False):
        """Logs a CRITICAL-level message."""
        self._log(logging.CRITICAL, message, exc_info=exc_info)
