from abc import abstractmethod

from app.utils.logger import LoggingUtils
from typing import Any


class TranslatorBase:
    """
    Abstract base class for implementing a translator service.
    """
    _environment : str # string to identify the environment which the data belongs
    _internal_message_hub_server: dict  # Configuration details for the internal message hub (for example, AMQP server)
    _configurations: dict  # General configurations passed to the translator
    _logger: LoggingUtils  # Logger instance for structured logging

    def __init__(self, environment : str, configurations: dict, logger: LoggingUtils):
        """Initializes the translator with configuration and logging details.

        Args:
            environment (str): String to identify the environment which the data belongs.
            configurations (dict): General configurations passed to the translator.
            logger (LoggingUtils): Logger instance for structured logging.
        """
        self._environment = environment
        self._internal_message_hub_server = configurations.get('internal_amqp_server').get('server')
        self._configurations = configurations
        self._logger = logger

    @abstractmethod
    def translate(self, data : Any) -> None:
        """
           Translate incoming device messages into a standardized format.

           Args:
               data (Any): Raw data from the source system.
                   Each subclass is responsible for handling its expected type:
                   - bytes (JSON encoded string with "observation")
                   - dict with "messages", "label", "entity_id"
           """
        raise NotImplementedError()

    def generic_period_harmonizer(self):
        """Generic harmonizer to resample or aggregate data across different time periods.

        For example, this function can take data points recorded every 5 minutes
        and aggregate them into 15-minute intervals. It is meant to provide a
        standardized temporal resolution for downstream processing.

        This method can be overridden by subclasses with the specific
        aggregation or resampling logic required for the data.
        """
        pass
