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
    _exchange_name : str

    def __init__(self, environment: str, environment_specs: dict, configurations: dict, logger: LoggingUtils):
        """
        Initialize the receiver with environment settings, HTTP connector, and scheduling interval.

        Args:
            environment (str): Current environment (e.g., production, staging).
            environment_specs (dict): Environment-specific configurations.
            configurations (dict): General configurations including provider info and frequency.
            logger (LoggingUtils): Logger instance for logging messages.
        """
        super().__init__(environment, environment_specs, configurations, logger)

        self._server = self._provider_configurations.get('receiver_server')
        self._rabbitmq_connector = RabbitMQConnector(self._server)
        self._stop_event = threading.Event()
        self._thread = None

    @abstractmethod
    def _process_message(self, body: bytes):
        raise NotImplementedError

    def _start_messaging_service(self):
        """
           Establish connection to RabbitMQ with retry logic.
           Declares the environment-specific queue and logs connection status.
           Raises an exception if maximum reconnection attempts are reached.
        """
        # Initialize the RabbitMQ connector with the internal message hub server
        self._rabbitmq_connector.connect()
        self._rabbitmq_connector.declare_exchange(self._exchange_name)
        queue_name = self._rabbitmq_connector.declare_queue(queue_name= f"percepta_local_{self.provider}_{self._environment}",exchange_name=self._exchange_name)
        self._rabbitmq_connector.consume(queue_name, self._callback)

    def _callback(self, ch, method, properties, body):
        """
        Rabbitmq callback.
        """
        self._process_message(body)
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



