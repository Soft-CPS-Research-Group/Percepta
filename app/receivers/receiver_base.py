import threading
from abc import ABC, abstractmethod
from app.utils.logger import LoggingUtils

class ReceiverBase(ABC, threading.Thread):
    """
    Abstract base class for implementing a data receiver running as a thread.
    """

    provider: str  # Identifier of the data provider (to be set by subclasses)
    _environment: str # String to identify the environment which the data belongs
    _entities: dict  # Entities configuration extracted from environment specifications
    _provider_configuration: dict # Provider-specific configurations
    _configurations: dict  # General configurations passed to the receiver
    _logger: LoggingUtils  # Logger instance for structured logging

    def __init__(self, environment : str, environment_specs : dict, configurations : dict, logger : LoggingUtils):
        """Initializes the receiver with environment and configuration details.

        Args:
            environment (str): String to identify the environment which the data belongs.
            environment_specs (dict): Specifications for the environment, including entities.
            configurations (dict): General configurations passed to the translator.
            logger (LoggingUtils): Logger instance for structured logging.
        """

        threading.Thread.__init__(self)
        self._environment = environment
        self._entities = environment_specs.get('entities')
        self._configurations = configurations
        self._provider_configurations = configurations.get(self.provider)
        self._logger = logger

    @abstractmethod
    def run(self) -> None:
        """Main execution loop for the receiver thread."""
        raise NotImplementedError

    @abstractmethod
    def stop(self) -> None:
        """Stops the receiver thread."""
        raise NotImplementedError

    @classmethod
    def pre_start(cls, configurations: dict, logger: LoggingUtils) -> None:
        """
        Optional hook executed before the receiver starts.
        Defaults to a no-op.

        Args:
            configurations (dict): General configurations for the receiver.
            logger (LoggingUtils): Logger instance for structured logging.
        """
        pass

    @classmethod
    def post_start(cls, environments: dict, configurations: dict, logger: LoggingUtils) -> None:
        """
        Optional hook executed after the receiver starts.
        Defaults to a no-op.

        Args:
            environments (dict): Dict of all provider environments.
            configurations (dict): General configurations for the receiver.
            logger (LoggingUtils): Logger instance for structured logging.
        """
        pass