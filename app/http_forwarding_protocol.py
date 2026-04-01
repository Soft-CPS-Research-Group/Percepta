import threading
from typing import Any
from app.connectors.http_connector import HTTPConnector
from app.utils.logger import LoggingUtils
from app.utils.retry import with_retries


class HTTPForwardingProtocol:
    """
    Handles the creation and dispatch of runtime requests via RabbitMQ.
    This class manages initialization of the messaging service, sending requests,
    and handling responses from RabbitMQ.
    """

    _server: dict  # Server configuration dictionary containing environment-specific settings
    _publisher_connector: HTTPConnector  # RabbitMQ connector for publishing
    _logger: LoggingUtils  # Logger instance
    _message: dict  # Runtime request message, stored as dict before serialization

    def __init__(self, server_config: dict, logger: LoggingUtils) -> None:
        """
        Initialize the ICRuntimeRequest instance.

        Args:
            configurations (dict): Configuration dictionary including server and frequency settings.
            logger (LoggingUtils): Logger instance for logging messages.
        """
        self._server = server_config
        self._logger = logger

        self._stop_event: threading.Event = threading.Event()

        with_retries(self._setup_publisher_service, logger=self._logger)

    def _setup_publisher_service(self) -> None:
        """Initialize RabbitMQ connection for publishing."""
        self._publisher_connector = HTTPConnector(self._server.get('url'))
        self._logger.info(f"Connection successfully established.")


    def stop(self) -> None:
        self._stop_event.set()

        # Close connections
        self._publisher_connector.close()


    def send_message(self, message, endpoint, header = None) -> None:
        """
        Send the prepared runtime request message to the RabbitMQ broker.

        - Serializes the message into JSON format.
        - Sets message properties including reply queue and unique message ID.
        - Waits briefly before publishing to ensure the consumer is ready.
        """
        if header is not None:
            self._publisher_connector.update_headers(header)
            self._logger.info(f"Header {header}.")

        result = self._publisher_connector.post(endpoint, message)
        self._logger.info(f"Message successfully sent to broker {endpoint}. Result {result}.")

