from app.utils.logger import LoggingUtils


class TranslatorBase:
    """
    Abstract base class for implementing a translator service.
    Provides a common interface for handling message hub connections,
    reconnection policies, and structured logging.
    """
    _environment : str # string to identify the environment which the data will be translated
    _internal_message_hub_server: dict  # Configuration details for the internal message hub (AMQP server)
    _max_reconnect_attempts: int  # Maximum number of reconnection attempts allowed
    _configurations: dict  # General configurations passed to the translator
    _logger: LoggingUtils  # Logger instance for structured logging

    def __init__(self, environment : str, configurations: dict, logger: LoggingUtils):
        """Initialize the translator with configuration and logging details.

        Args:
            environment (str): Name of the environment the translator operates in.
            configurations (dict): General configurations for the translator,
                                   including message hub server and retry attempts.
            logger (LoggingUtils): Logger instance for structured logging.
        """
        self._environment = environment
        self._internal_message_hub_server = configurations.get('internal_amqp_server').get('server')
        self._max_reconnect_attempts = configurations.get('max_reconnect_attempts')
        self._configurations = configurations
        self._logger = logger

    def period_harmonizer(self):
        """Generic harmonizer to resample or aggregate data across different time periods.

        For example, this function can take data points recorded every 5 minutes
        and aggregate them into 15-minute intervals. It is meant to provide a
        standardized temporal resolution for downstream processing.

        This method should be overridden by subclasses with the specific
        aggregation or resampling logic required for the data.

        Raises:
            NotImplementedError: If not implemented by the subclass.
        """
        pass
