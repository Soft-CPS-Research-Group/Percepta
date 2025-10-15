import uuid
import time
import threading
from typing import Any
from app.connectors.rabbitmq_connector import RabbitMQConnector
from app.utils.data import DataSet
from app.utils.logger import LoggingUtils
from app.utils.retry import with_retries


class ICActuationRequest:
    """
    Handles the creation and dispatch of runtime requests via RabbitMQ.
    This class manages initialization of the messaging service, sending requests,
    and handling responses from RabbitMQ.
    """

    _server: dict  # Server configuration dictionary containing environment-specific settings
    _publisher_connector: RabbitMQConnector  # RabbitMQ connector for publishing
    _consumer_connector: RabbitMQConnector   # RabbitMQ connector for consuming
    _logger: LoggingUtils  # Logger instance
    _message: dict  # Runtime request message, stored as dict before serialization
    _return_queue_name: str  # Name of the return queue created for responses

    def __init__(self, configurations: dict, logger: LoggingUtils) -> None:
        """
        Initialize the ICRuntimeRequest instance.

        Args:
            configurations (dict): Configuration dictionary including server and frequency settings.
            logger (LoggingUtils): Logger instance for logging messages.
        """
        self._server = configurations.get("i-charging").get("receiver_server")
        self._logger = logger

        self._serial_number = "AC000012"
        self._plug = 1
        self._power = 1.9

        self._message = {
            "type": "setlimit",
            "value": {
                "serialnumber": self._serial_number,
                "plug": self._plug,
                "power": self._power
            }
        }

        print(self._message)

        self._logger.info(f"Request to set limit for {self._serial_number}_{self._plug} charger to {self._power} kW\n")

        self._response_event: threading.Event = threading.Event()

        # Initialize both publisher and consumer connectors with retries
        with_retries(self._setup_consumer_service, logger=self._logger)
        with_retries(self._setup_publisher_service, logger=self._logger)


    def _setup_publisher_service(self) -> None:
        """Initialize RabbitMQ connection for publishing."""
        self._publisher_connector = RabbitMQConnector(self._server)
        self._publisher_connector.connect()
        self._publisher_connector.declare_queue("RPC")
        self._logger.info("IC Runtime Request: Publisher connection established.")

    def _setup_consumer_service(self) -> None:
        """Initialize RabbitMQ connection for consuming responses."""
        self._consumer_connector = RabbitMQConnector(self._server)
        self._consumer_connector.connect()
        self._return_queue_name: str = self._consumer_connector.declare_queue(exclusive=True)
        self._logger.info("IC Runtime Request: Consumer connection established.")

    def start_service(self) -> None:
        """
        Start the runtime request service.
        """

        # Thread to consume messages
        consumer_thread = threading.Thread(
            target=self._consumer_connector.consume,
            args=(self._return_queue_name, self._on_response),
            daemon=True,
        )
        consumer_thread.start()

        # Method to send the message
        self._send_message()

        # Wait until the response arrives
        self._response_event.wait()

        # Close connections
        self._consumer_connector.close()
        self._publisher_connector.close()

        consumer_thread.join()

    def _send_message(self) -> None:
        """
        Send the prepared runtime request message to the RabbitMQ broker.

        - Serializes the message into JSON format.
        - Sets message properties including reply queue and unique message ID.
        - Waits briefly before publishing to ensure the consumer is ready.
        """
        time.sleep(1)
        self._publisher_connector.publish(
            "RPC",
            self._message,
            properties={
                "reply_to": self._return_queue_name
            },
        )
        self._logger.info("Actuation request message published.")

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

        # Stop the consuming loop once the response is received
        self._consumer_connector.stop_consuming()
        self._response_event.set()
