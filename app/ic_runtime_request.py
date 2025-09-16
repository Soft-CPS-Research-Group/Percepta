import json
import uuid
import time
import threading
from typing import Dict, Any
from app.connectors.rabbitmq_connector import RabbitMQConnector
from app.utils.data import DataSet
from app.utils.logger import LoggingUtils
from app.utils.retry import with_retries


class ICRuntimeRequest:
    """
    Handles the creation and dispatch of runtime requests via RabbitMQ.
    This class manages initialization of the messaging service, sending requests,
    and handling responses from RabbitMQ.
    """

    _server: dict # Server configuration dictionary containing environment-specific settings
    _rabbitmq_connector: RabbitMQConnector  # RabbitMQ connector instance for handling messaging
    _logger: LoggingUtils  # Logger instance
    _message: dict  # Runtime request message, stored as dict before serialization
    _return_queue_name: str  # Name of the return queue created for responses

    def __init__(self, environments: dict, configurations: dict, logger: LoggingUtils) -> None:
        """
        Initialize the ICRuntimeRequest instance.

        Args:
            environments (dict): Dictionary of environment identifiers.
            configurations (dict): Configuration dictionary including server and frequency settings.
            logger (LoggingUtils): Logger instance for logging messages.
        """
        self._server = configurations.get('i-charging').get('receiver_server')

        self._logger = logger

        self._message = {
            "type": "runtime",
            "value": {
                "installations": [list(environments.keys())],
                "frequency": DataSet.calculate_interval(configurations.get('frequency'))
            }
        }

        # Start the messaging service by establishing a connection to RabbitMQ with retries
        with_retries(self._start_messaging_service, logger=self._logger)

    def _start_messaging_service(self) -> None:
        """
        Establish connection to RabbitMQ with retry logic.
        Declares the environment-specific queue and logs connection status.

        Raises:
            Exception: If maximum reconnection attempts are reached.
        """
        self._rabbitmq_connector: RabbitMQConnector = RabbitMQConnector(self._server)
        self._rabbitmq_connector.connect()
        self._rabbitmq_connector.declare_queue('RPC')
        self._return_queue_name: str = self._rabbitmq_connector.declare_queue(exclusive=True)

        self._logger.info("IC Runtime Request: Connection successfully established.")

    def init(self) -> None:
        """
        Initialize the runtime request by sending the initial message.
        Uses retry logic to ensure the message is delivered.
        """
        with_retries(self._send_message, logger=self._logger)

    def _start_service(self) -> None:
        """
        Start the runtime request service.

        - Launches a separate thread to send the initial message.
        - Begins consuming messages from the return queue to process responses.
        """
        sender_thread: threading.Thread = threading.Thread(target=self._send_message, daemon=True)
        sender_thread.start()

        self._rabbitmq_connector.consume(self._return_queue_name, self._on_response)

    def _send_message(self) -> None:
        """
        Send the prepared runtime request message to the RabbitMQ broker.

        - Serializes the message into JSON format.
        - Sets message properties including reply queue and unique message ID.
        - Waits briefly before publishing to ensure readiness of the consumer.
        """
        self._message = json.dumps(self._message)

        _properties: Dict[str, str] = {
            "reply_to": self._return_queue_name,
            "message_id": str(uuid.uuid4())
        }

        time.sleep(1)
        self._rabbitmq_connector.publish(self._message, _properties)

    def _on_response(self, ch: Any, method: Any, properties: Any, body: bytes) -> None:
        """
        Callback function to handle responses received from RabbitMQ.

        Args:
            ch (Any): The channel object.
            method (Any): Delivery method information.
            properties (Any): Message properties.
            body (bytes): The response payload received from RabbitMQ.
        """
        self._logger.info(f"Received response: {body.decode()}")
        self._rabbitmq_connector.close()
