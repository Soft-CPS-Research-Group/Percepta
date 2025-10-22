from typing import Dict, Any
from app.translators.pc_translator import PCTranslator
from app.receivers.receiver_rabbitmq_base import ReceiverRabbitMQBase
from app.utils.logger import LoggingUtils
from app.utils.providers import Provider

class PCReceiver(ReceiverRabbitMQBase):
    """
    ICReceiver is responsible for retrieving raw data from the i-charging API
    for specific environments.

    Note:
        Translation of provider-specific data into system-specific format
        is handled by ICTranslator.
    """

    provider = Provider.PULSECHARGE.value
    _translator: PCTranslator

    def __init__(self, environment: str, environment_specs: Dict[str, Any], configurations: Dict[str, Any], logger: LoggingUtils) -> None:
        """
        Initialize the ICReceiver instance.

        Args:
            environment (str): Environment name.
            environment_specs (dict): Environment-specific configurations.
            configurations (dict): General configurations for the receiver.
            logger (LoggingUtils): Logger instance for logging events.
        """
        self._exchange_name: str = f"building_{environment.replace(' ', '_')}"
        print(f"INICIEI {self._exchange_name}\n")
        super().__init__(environment, environment_specs, configurations, logger)
        self._translator = PCTranslator(environment, environment_specs, configurations, logger)

    def stop(self) -> None:
        """
        Stop the receiver and translator threads gracefully.
        Ensures proper cleanup and logs stop events.
        """
        self._logger.info(f"{self.provider} | Stopping thread {self._environment}...")
        super().stop()
        self._translator.stop()
        self._logger.info(f"{self.provider} | Thread {self._environment} stopped.")

    def _process_message(self, body: Any) -> None:
        """
        Process an incoming message by sending it to the translator.

        Args:
            body (Any): The message payload received from RabbitMQ.
                        Can be a dict, str, or serialized data structure.
        """
        if not self._stop_event.is_set():
            self._translator.translate(body)