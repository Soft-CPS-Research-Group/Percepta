import threading
from abc import abstractmethod
from app.receivers.receiver_base import ReceiverBase
from app.connectors.rabbitmq_connector import RabbitMQConnector
from app.utils.logger import LoggingUtils
from app.utils.retry import with_retries

class ReceiverRabbitMQBase(ReceiverBase):
    """
    Generic base class for RabbitMQ receivers in Percepta.

    This class manages RabbitMQ connections, channels, and message consumption.
    It is designed for the specific context where each environment has a single fanout exchange,
    and all data for that environment is published to that exchange.

    Subclasses are responsible for defining how received messages are processed,
    but the connection, queue declaration, and message acknowledgement are handled here.
    """

    _server: dict # Server configuration dictionary containing environment-specific settings
    _rabbitmq_connector: RabbitMQConnector  # RabbitMQ connector instance for handling messaging
    _stop_event: threading.Event # Signals that the thread will stop

    def __init__(self, environment_name: str, environment_specs: dict, configurations: dict, logger: LoggingUtils):
        """
        Initialize the receiver with environment settings, HTTP connector, and scheduling interval.

        Args:
            environment_name (str): Current environment (e.g., production, staging).
            environment_specs (dict): Environment-specific configurations.
            configurations (dict): General configurations including provider info and frequency.
            logger (LoggingUtils): Logger instance for logging messages.
        """
        super().__init__(environment_name, environment_specs, configurations, logger)

        self._server = self._provider_configurations.get('receiver_server')
        self._rabbitmq_connector = RabbitMQConnector(self._server)
        self._stop_event = threading.Event()
        self._thread = None

    @abstractmethod
    def _process_message(self, body: bytes, source : str):
        raise NotImplementedError

    def _start_messaging_service(self):
        """
           Establish connection to RabbitMQ with retry logic.
           Declares the environment-specific queue and logs connection status.
           Raises an exception if maximum reconnection attempts are reached.
        """
        # Initialize the RabbitMQ connector with the internal message hub server

        self._rabbitmq_connector.connect()

        for _, ex_name in self._resources_rules.items():
            self._rabbitmq_connector.declare_exchange(ex_name)

            real_queue_name = self._rabbitmq_connector.declare_queue(
                self._environment_name,
                ex_name
            )


            self._rabbitmq_connector.setup_consumer(real_queue_name, self._callback)

        self._rabbitmq_connector.start_listening()

    def _callback(self, ch, method, properties, body):
        """
        Rabbitmq callback.
        """
        self._process_message(body, method.exchange)
        self._rabbitmq_connector.ack(method.delivery_tag)

    def stop(self):
        """
        Stop the scheduled job and close the HTTP session.

        This method shuts down the scheduler and closes the underlying HTTP connection.
        """
        self._stop_event.set()

        self._rabbitmq_connector.stop_consuming_safely()

        if self._thread and self._thread.is_alive():
            self._thread.join()

        self._rabbitmq_connector.close()

    def run(self):
        self._thread = threading.current_thread()
        with_retries(func=self._start_messaging_service, logger=self._logger)



