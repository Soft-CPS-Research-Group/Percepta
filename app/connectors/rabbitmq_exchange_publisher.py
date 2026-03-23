import threading
from typing import Any
from app.connectors.rabbitmq_connector_2 import RabbitMQConnector
from app.utils.logger import LoggingUtils
from app.utils.retry import with_retries


class RabbitMQForwardingProtocol:
    """
    Handles the creation and dispatch of runtime requests via RabbitMQ.
    This class manages initialization of the messaging service, sending requests,
    and handling responses from RabbitMQ.
    """

    _publisher_connector: RabbitMQConnector  # RabbitMQ connector for publishing
    _consumer_connector: RabbitMQConnector   # RabbitMQ connector for consuming
    _logger: LoggingUtils  # Logger instance
    _message: dict  # Runtime request message, stored as dict before serialization
    _reply_to_queue_name: str  # Name of the return queue created for responses

    def __init__(self, server_config: dict, logger: LoggingUtils) -> None:
        """
        Initialize the ICRuntimeRequest instance.

        Args:
            configurations (dict): Configuration dictionary including server and frequency settings.
            logger (LoggingUtils): Logger instance for logging messages.
        """
        self._logger = logger

        self._connection_params = server_config.get('connection_params', {})
        self._exchange_conf = server_config.get("exchange_conf", {})
        self._reply_to_queue_conf = server_config.get("reply_to_queue_conf", {})

        self._stop_event: threading.Event = threading.Event()

        # Initialize both publisher and consumer connectors with retries
        with_retries(self._setup_publisher_service, logger=self._logger)

        if self._reply_to_queue_conf is not None:
            with_retries(self._setup_consumer_service, logger=self._logger)
            self._start_service()

    def _setup_publisher_service(self) -> None:
        """Initialize RabbitMQ connection for publishing."""
        self._publisher_connector = RabbitMQConnector(self._connection_params)
        self._publisher_connector.connect()

        self._logger.info("IC Runtime Request: Publisher connection established.")

    def _setup_consumer_service(self) -> None:
        """Initialize RabbitMQ connection for consuming responses."""
        self._consumer_connector = RabbitMQConnector(self._connection_params)

        self._consumer_connector.connect()

        self._consumer_connector.declare_queue(kwargs=self._reply_to_queue_conf)

        self._consumer_connector.setup_consumer(
            queue_name=self._reply_to_queue_name,
            callback=self._on_response
        )
        self._logger.info("IC Runtime Request: Consumer connection established.")

    def _start_service(self) -> None:
        """
        Start the runtime request service.
        """

        # Thread to consume messages
        self._consumer_thread = threading.Thread(
            target=self._consumer_connector.start_listening,
            daemon=False,
        )
        self._consumer_thread.start()

    def stop(self) -> None:
        self._stop_event.set()

        self._consumer_thread.join()

        # Close connections
        self._publisher_connector.close()
        if self._consumer_connector is not None:
            self._consumer_connector.close()

    def send_message(self, message) -> None:
        """
        Send the prepared runtime request message to the RabbitMQ broker.

        - Serializes the message into JSON format.
        - Sets message properties including reply queue and unique message ID.
        - Waits briefly before publishing to ensure the consumer is ready.
        """

        self._publisher_connector.publish(
            "RPC", # Aqui não pode ser RPC tenho de generalizar
            message,
            properties={
                "reply_to": self._reply_to_queue_name,
                "expiration": "10000"
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

        if self._stop_event.is_set():
            self._consumer_connector.stop_consuming()

