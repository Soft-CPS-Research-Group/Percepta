import datetime
import time
from apscheduler.schedulers.blocking import BlockingScheduler
from app.connectors.rabbitmq_connector import RabbitMQConnector
from app.utils.logger import LoggingUtils
from app.rabbitMQ_publisher import RabbitMQPublisher


class ICRuntimeRequest:
    """
    Handles the creation and dispatch of runtime requests via RabbitMQ.
    This class manages initialization of the messaging service, sending requests,
    and handling responses from RabbitMQ.
    """
    _LOG_PREFIX = "i-charging Runtime Request |"

    _publisher_config: dict  # Server configuration dictionary containing environment-specific settings
    _logger: LoggingUtils  # Logger instance
    _message: dict  # Runtime request message, stored as dict before serialization

    def __init__(self, environments: list, configurations: dict, time_interval : int, logger: LoggingUtils) -> None:
        """
        Initialize the ICRuntimeRequest instance.

        Args:
            environments (dict): Dictionary of environment identifiers.
            configurations (dict): Configuration dictionary including server and frequency settings.
            logger (LoggingUtils): Logger instance for logging messages.
        """
        self._publisher_config = configurations.get("i-charging", {}).get("publisher_settings", {})
        self._logger = logger
        self._time_interval = time_interval

        self._publisher = RabbitMQPublisher(self._publisher_config, self._logger)

        self._final_state = False

        self._message = {
            "type": "runtime",
            "value": {
                "installations": environments,
                "frequency": self._time_interval,
            },
        }
        self._logger.info(f"{self._LOG_PREFIX} Request initialized for: {environments}")

    def _calculate_next_run_time(self) -> datetime.datetime:
        """
        Calculates the next execution time based on the time_interval.
        Aligns the schedule to the next 'round' multiple of the interval (e.g., top of the hour).
        """
        now = datetime.datetime.now()

        if self._time_interval <= 0:
            return now.replace(microsecond=0)

        now_ts = now.timestamp()

        # Calculate seconds until the next multiple of the interval
        remainder = now_ts % self._time_interval
        seconds_to_wait = self._time_interval - remainder

        # Return the precise datetime for the next run, stripped of microseconds
        return (now + datetime.timedelta(seconds=seconds_to_wait)).replace(microsecond=0)

    def start_service(self) -> bool:
        """
        Start the runtime request service on-demand.
        """
        self._logger.info(f"{self._LOG_PREFIX} Starting service execution...")

        try:
            self._scheduler = BlockingScheduler()

            next_run = self._calculate_next_run_time()
            self._logger.info(f"{self._LOG_PREFIX} Message dispatch scheduled for: {next_run}")

            # O APScheduler chama o _send_message na hora certa
            self._scheduler.add_job(
                self._send_message,
                trigger='date',
                run_date=next_run,
                id='send_runtime_request',
                misfire_grace_time=10
            )

            if not self._scheduler.running:
                self._scheduler.start()

        except Exception as e:
            self._logger.error(f"{self._LOG_PREFIX} Error during service execution: {e}")
        finally:
            return self._final_state

    def _send_message(self) -> None:
        """
        Dispatches the message and waits for the RPC response.
        """
        self._logger.info(f"{self._LOG_PREFIX} Dispatching message via Publisher RPC...")

        try:
            response = self._publisher.send_message(self._message)

            if response and "error" not in response:
                self._logger.info(f"{self._LOG_PREFIX} RPC Success! Response: {response}")
                self._final_state = True
            else:
                self._logger.error(f"{self._LOG_PREFIX} RPC Failed or Timed out: {response}")

        finally:
            self._scheduler.shutdown(wait=False)

    def stop(self) -> None:
        """Cleanup resources."""
        self._logger.info(f"{self._LOG_PREFIX} Cleaning up...")
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)
        self._publisher.stop()