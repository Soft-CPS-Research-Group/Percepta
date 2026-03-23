import threading
from typing import Any
from app.connectors.rabbitmq_connector import RabbitMQConnector
from app.manager.manager import Manager
from app.utils.logger import LoggingUtils
from app.utils.retry import with_retries

class Accumulator(threading.Thread):
    """
    Accumulator thread that listens to an environment-specific RabbitMQ queue,
    processes incoming messages via a Manager, and acknowledges or rejects messages accordingly.
    """

    _rabbitmq_connector: RabbitMQConnector  # RabbitMQ connector instance
    _manager: Manager  # Manager instance for processing messages
    _environment: str  # Environment name for queue identification
    _configurations: dict # Configuration dictionary
    _logger: LoggingUtils  # Logger instance
    _stop_event: threading.Event  # Signals the thread to stop
    _internal_message_hub_server: dict  # Internal RabbitMQ server configuration

    def __init__(self, environment: str, manager: Manager, configurations: dict, logger: LoggingUtils) -> None:
        """
        Initializes the Accumulator thread.

        Args:
            environment (str): The environment name used to identify the RabbitMQ queue.
            manager (Manager): Manager instance for processing messages.
            configurations (dict): Configuration dictionary containing RabbitMQ server details.
            logger (LoggingUtils): Logger instance for logging messages.
        """

        threading.Thread.__init__(self)
        self._logger = logger
        # TODO: If the configuration file key changes, update this as well.
        self._internal_message_hub_server = configurations.get('internal_amqp_server').get('server')
        self._rabbitmq_connector = RabbitMQConnector(self._internal_message_hub_server)
        self._environment = environment
        self._manager = manager
        self._configurations = configurations
        self._thread = None

        self._stop_event = threading.Event()

    def _start_messaging_service(self) -> None:
        """
        Establishes a connection to RabbitMQ with retry logic.
        Declares the environment-specific queue and starts consuming messages.
        """
        def _start_messaging_service_auxiliar() -> None:
            """
            Helper function to initialize RabbitMQ connector, declare the queue,
            and start consuming messages.
            """
            self._rabbitmq_connector.connect()
            self._rabbitmq_connector.declare_queue(self._environment)

            self._logger.info(f"Connection successfully established. ACC")
            self._rabbitmq_connector.setup_consumer(
                queue_name=self._environment,
                callback=self._callback,
                auto_ack=False
            )

            self._rabbitmq_connector.start_listening()

        with_retries(
            _start_messaging_service_auxiliar,
            error_msg=f"RabbitMQ connection failed",
            logger=self._logger
        )

    def stop(self) -> None:
        """
        Signals the thread to stop and closes the RabbitMQ connection.
        """
        self._stop_event.set()
        self._rabbitmq_connector.stop_consuming_safely()

        if self._thread and self._thread.is_alive():
            self._thread.join()

        self._rabbitmq_connector.close()

    def _callback(self, ch: Any, method: Any, properties: Any, body: bytes) -> None:
        """
        Callback function executed when a message is received from RabbitMQ.
        Processes the message using the Manager and acknowledges or rejects it.

        Args:
            ch: Channel object (provided by pika).
            method: Delivery method containing delivery_tag.
            properties: Message properties.
            body: Message body in bytes.
        """
        if not self._stop_event.is_set():
            if self._manager.new_message(body):
                self._rabbitmq_connector.ack(method.delivery_tag)
            else:
                self._rabbitmq_connector.nack(method.delivery_tag)
                self._logger.warning("Error processing RabbitMQ message.")

    def run(self) -> None:
        """
        Main thread execution method.
        Starts the messaging service with retry logic.
        """
        self._thread = threading.current_thread()
        with_retries(func=self._start_messaging_service, logger=self._logger)
