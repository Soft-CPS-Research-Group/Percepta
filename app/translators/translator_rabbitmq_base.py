from app.connectors.rabbitmq_connector import RabbitMQConnector
from app.translators.translator_base import TranslatorBase
from app.utils.logger import LoggingUtils
from app.utils.retry import with_retries

class TranslatorRabbitMQBase(TranslatorBase):
    """
    Abstract base class for implementing a RabbitMQ-based translator.

    Handles connection management, message sending, reconnection retries,
    and provides a common interface for translating data to a specific environment queue.
    """
    _rabbitmq_connector: RabbitMQConnector  # RabbitMQ connector instance for handling messaging

    def __init__(self, environment: str, configurations: dict, logger: LoggingUtils):
        """
        Initialize the translator with environment and configuration details.

        Args:
            environment (str): String to identify the environment which the data belongs.
            configurations (dict): General configurations passed to the translator.
            logger (LoggingUtils): Logger instance for structured logging.
        """

        super().__init__(environment, configurations, logger)

        # Start the messaging service by establishing a connection to RabbitMQ
        with_retries(self._start_messaging_service, error_msg=f"RabbitMQ Translator - {self._environment}: RabbitMQ connection failed", logger = self._logger)

    def _start_messaging_service(self) -> None:
        """
        Establishes connection to RabbitMQ using the appropriate connector.
        """

        # Create a new RabbitMQConnector instance using the internal message hub server
        self._rabbitmq_connector = RabbitMQConnector(self._internal_message_hub_server)

        # Establish the connection to RabbitMQ
        self._rabbitmq_connector.connect()

        # Declare a queue named after the current environment
        self._rabbitmq_connector.declare_queue(self._environment)

        self._logger.info(f"RabbitMQ Translator - {self._environment}: Connection successfully established.")

    def send_message_to_environment_queue(self, message) -> None:
        """
        Send a message to the environment-specific RabbitMQ queue with retry logic.
        Reconnects automatically if the connection is lost.
        Raises an exception if maximum sending attempts are reached.

        Args:
        message (str or dict): The message to send. Can be a string or JSON-serializable object.
        """
        def send_message_to_environment_queue_auxiliar():
            if not self._rabbitmq_connector.is_connected():
                # Close existing connection if any, then reconnect
                try:
                    self._rabbitmq_connector.close()
                except Exception:
                    pass

                self._start_messaging_service()

            try:
                self._rabbitmq_connector.publish(self._environment, message)
            except Exception as e:
                self._rabbitmq_connector.close()
                raise e

        with_retries(send_message_to_environment_queue_auxiliar,error_msg=f"RabbitMQ Translator - {self._environment}: Sending message failed",logger = self._logger)

    def stop(self) -> None:
        """
        Stop the messaging service by closing the RabbitMQ connection.
        Raises an exception if closing the connection fails.
        """
        try:
            self._rabbitmq_connector.close()
        except Exception as e:
            raise Exception(f"Failed to close RabbitMQ connection/channel: {e}") from e
