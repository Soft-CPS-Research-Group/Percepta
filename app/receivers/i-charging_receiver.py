import threading
import time
from typing import Dict, Any
from app.ic_runtime_request import ICRuntimeRequest
from app.translators.ic_translator import ICTranslator
from app.receivers.receiver_rabbitmq_base import ReceiverRabbitMQBase
from app.utils.logger import LoggingUtils
from app.utils.providers import Provider
from app.utils.data import DataSet


class ICReceiver(ReceiverRabbitMQBase):
    """
    ICReceiver is responsible for retrieving raw data from the i-charging API
    for specific environments.

    Note:
        Translation of provider-specific data into system-specific format
        is handled by ICTranslator.
    """

    provider: str = Provider.ICHARGING.value
    _translator: ICTranslator
    _time_interval: int # Interval in seconds for scheduling the periodic job

    def __init__(self, environment: str, environment_specs: Dict[str, Any], configurations: Dict[str, Any], logger: LoggingUtils) -> None:
        """
        Initialize the ICReceiver instance.

        Args:
            environment (str): Environment name.
            environment_specs (dict): Environment-specific configurations.
            configurations (dict): General configurations for the receiver.
            logger (LoggingUtils): Logger instance for logging events.
        """
        self._exchange_name : str = environment

        super().__init__(environment, environment_specs, configurations, logger)

        self._translator = ICTranslator(environment, environment_specs, configurations, logger)

        self._time_interval = DataSet.calculate_interval(configurations.get('frequency'))

        self._first_message = False

        messages_monitor = threading.Thread(
            target=self._messages_monitor,
            daemon=True
        )

        messages_monitor.start()

    def _messages_monitor(self) -> None:
        ic_runtime_request = ICRuntimeRequest([self._environment], self._configurations, self._logger)
        ic_runtime_request.start_service()

        if self._first_message:
            while True:
                if time.time() - self._arrival_time > self._time_interval*2:
                    ic_runtime_request.start_service()
                    self._logger.warning("i-charging receiver isn't communicating correctly. Communication will be restarted.")
                else:
                    self._logger.info("i-charging receiver is communicating correctly.")
                time.sleep(self._time_interval)

    def stop(self) -> None:
        """
        Stop the receiver and translator threads gracefully.
        Ensures proper cleanup and logs stop events.
        """
        self._logger.info(f"i-charging | Stopping thread {self._environment}...")
        super().stop()
        self._translator.stop()
        self._logger.info(f"i-charging | Thread {self._environment} stopped.")

    def _process_message(self, body: Any) -> None:
        """
        Process an incoming message by sending it to the translator.

        Args:
            body (Any): The message payload received from RabbitMQ.
                        Can be a dict, str, or serialized data structure.
        """
        self._arrival_time = time.time()
        self._first_message = True
        if not self._stop_event.is_set():
            self._translator.translate(body)

    @classmethod
    def launch(cls, environments: dict, configurations: dict):
        threads = []

        for environment, environment_specs in environments.items():
            logger_per_environment = LoggingUtils(f"{cls.provider}_receiver", configurations, environment)
            receiver = cls(environment, environment_specs, configurations, logger_per_environment)
            receiver.start()
            threads.append(receiver)

        return threads