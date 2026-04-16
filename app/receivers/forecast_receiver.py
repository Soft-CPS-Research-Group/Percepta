import threading
import time
from typing import Dict, Any
from app.translators.forecast_translator import ForecastTranslator
from app.receivers.receiver_rabbitmq_base import ReceiverRabbitMQBase
from app.utils.logger import LoggingUtils
from app.utils.providers import Provider
from app.utils.data import DataSet


class ForecastReceiver(ReceiverRabbitMQBase):
    """
    ForecastReceiver is responsible for retrieving raw data from the i-charging API
    for specific environments.

    Note:
        Translation of provider-specific data into system-specific format
        is handled by ForecastTranslator.
    """

    provider: str = Provider.FORECAST.value
    _translator: ForecastTranslator
    _time_interval: int # Interval in seconds for scheduling the periodic job
    _first_message: threading.Event

    def __init__(self, environment_name: str, environment_specs: Dict[str, Any], configurations: Dict[str, Any], logger: LoggingUtils) -> None:
        """
        Initialize the ForecastReceiver instance.

        Args:
            environment_name (str): Environment name.
            environment_specs (dict): Environment-specific configurations.
            configurations (dict): General configurations for the receiver.
            logger (LoggingUtils): Logger instance for logging events.
        """
        self._exchange_name : str = environment_name

        super().__init__(environment_name, environment_specs, configurations, logger)

        self._translator = ForecastTranslator(environment_name, environment_specs, configurations, logger)

        self._time_interval = DataSet.calculate_interval(configurations.get('frequency'))

    def stop(self) -> None:
        """
        Stop the receiver and translator threads gracefully.
        Ensures proper cleanup and logs stop events.
        """
        self._logger.info(f"forecast | Stopping thread {self._environment_name}...")
        super().stop()
        self._translator.stop()
        self._logger.info(f"forecast | Thread {self._environment_name} stopped.")

    def _process_message(self, body: Any, source : str) -> None:
        """
        Process an incoming message by sending it to the translator.

        Args:
            body (Any): The message payload received from RabbitMQ.
                        Can be a dict, str, or serialized data structure.
        """
        self._arrival_time = time.time()

        if not self._stop_event.is_set():
            time.sleep(self._time_interval)
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