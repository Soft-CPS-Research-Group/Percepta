from typing import Any
from app.translators.cw_2_translator import CW2Translator
from app.receivers.receiver_rabbitmq_base import ReceiverRabbitMQBase
from app.utils.logger import LoggingUtils
from app.utils.providers import Provider

class CW2Receiver(ReceiverRabbitMQBase):
    """
    CWReceiver2 is responsible for retrieving raw data from the Cleanwatts RabbitMQ API
    for configured entities and parameters, and maintaining session management
    using CWSession.

    Note:
        Translation of provider-specific data into Percepta-specific format
        is handled by CWTranslator.
    """
    provider = Provider.CLEANWATTS_2.value     # Provider ID

    _translator: CW2Translator   # Translator which translates Cleanwatts-specific format into Percepta-specific format

    def __init__(self, environment_name: str, environment_specs: dict, configurations: dict, logger: LoggingUtils):
        """
        Initializes the CWReceiver instance.

        Args:
            environment_name (str): Name of the environment the receiver will operate in.
            environment_specs (dict): Specifications for the environment, including entities.
            configurations (dict): General configurations for the receiver, e.g., max reconnect attempts, frequency.
            logger (LoggingUtils): Logger instance for structured logging.
        """
        super().__init__(environment_name, environment_specs, configurations, logger)

        # Translator instance is created
        self._translator = CW2Translator(environment_name, environment_specs, configurations, logger)

        self._source_to_entity = {v: k for k, v in self._resources_rules.items()}

        self._entities = environment_specs.get('entities', {})

    def stop(self) -> None:
        """
        Stop the receiver and translator threads gracefully.
        Ensures proper cleanup and logs stop events.
        """
        self._logger.info(f"Cleanwatts | Stopping thread {self._environment_name}...")
        super().stop()
        self._translator.stop()
        self._logger.info(f"Cleanwatts | Thread {self._environment_name} stopped.")

    def _process_message(self, body: Any, source : str) -> None:
        """
        Process an incoming message by sending it to the translator.

        Args:
            body (Any): The message payload received from RabbitMQ.
                        Can be a dict, str, or serialized data structure.
        """
        if not self._stop_event.is_set():
            entity_id = self._source_to_entity[source]
            self._translator.translate(
                {
                    "entity_id": entity_id,
                    "label": self._entities.get(entity_id, {}).get('label',''),
                    "message": body
                })

    @classmethod
    def launch(cls, environments: dict, configurations: dict):
        threads = []

        for environment, environment_specs in environments.items():
            logger_per_environment = LoggingUtils(f"{cls.provider}_receiver", configurations, environment)
            receiver = cls(environment, environment_specs, configurations, logger_per_environment)
            receiver.start()
            threads.append(receiver)

        return threads