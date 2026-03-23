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
    _first_message: threading.Event

    def __init__(self, environment_name: str, environment_specs: Dict[str, Any], configurations: Dict[str, Any], logger: LoggingUtils) -> None:
        """
        Initialize the ICReceiver instance.

        Args:
            environment_name (str): Environment name.
            environment_specs (dict): Environment-specific configurations.
            configurations (dict): General configurations for the receiver.
            logger (LoggingUtils): Logger instance for logging events.
        """
        self._exchange_name : str = environment_name

        super().__init__(environment_name, environment_specs, configurations, logger)

        self._translator = ICTranslator(environment_name, environment_specs, configurations, logger)

        self._time_interval = DataSet.calculate_interval(configurations.get('frequency'))

        self._first_message = threading.Event()

        messages_monitor = threading.Thread(
            target=self._messages_monitor,
            daemon=True
        )

        messages_monitor.start()

    def _messages_monitor(self) -> None:
        ic_runtime_request = ICRuntimeRequest([self._environment_name], self._configurations, self._time_interval, self._logger)

        # The ICRuntimeRequest is designed to request real-time data from i-charging at a specific frequency.
        # The request fails if no response is received within one minute.
        # This loop retries the request until a successful response is received and
        # the service starts sending observations. The sleep interval prevents excessive CPU usage.
        while not ic_runtime_request.start_service():
            self._logger.warning("i-charging Runtime Request failed. Retrying in 1 second...")
            time.sleep(1)

        self._first_message.wait()

        while True:
            if time.time() - self._arrival_time > self._time_interval*2:
                self._logger.warning("i-charging receiver isn't communicating correctly. Communication will be restarted.")
                if not ic_runtime_request.start_service():
                    continue
            else:
                self._logger.info("i-charging receiver is communicating correctly.")
            time.sleep(self._time_interval*2) # Allow time for a new observation to come in.

    def stop(self) -> None:
        """
        Stop the receiver and translator threads gracefully.
        Ensures proper cleanup and logs stop events.
        """
        self._logger.info(f"i-charging | Stopping thread {self._environment_name}...")
        super().stop()
        self._translator.stop()
        self._logger.info(f"i-charging | Thread {self._environment_name} stopped.")

    def _process_message(self, body: Any, source : str) -> None:
        """
        Process an incoming message by sending it to the translator.

        Args:
            body (Any): The message payload received from RabbitMQ.
                        Can be a dict, str, or serialized data structure.
        """
        self._arrival_time = time.time()

        if not self._first_message.is_set():
            self._first_message.set()

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