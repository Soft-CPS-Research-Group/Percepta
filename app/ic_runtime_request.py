import datetime
import uuid
import time
import threading
import json
from apscheduler.schedulers.background import BackgroundScheduler
from typing import Any
from app.connectors.rabbitmq_connector import RabbitMQConnector
from app.utils.data import DataSet
from app.utils.logger import LoggingUtils
from app.utils.retry import with_retries


class ICRuntimeRequest:
    """
    Handles the creation and dispatch of runtime requests via RabbitMQ.
    This class manages initialization of the messaging service, sending requests,
    and handling responses from RabbitMQ.
    """
    _LOG_PREFIX = "i-charging Runtime Request |"
    _RPC_QUEUE_NAME = "RPC"
    _TIMEOUT_SECONDS = 60

    _server: dict  # Server configuration dictionary containing environment-specific settings
    _publisher_connector: RabbitMQConnector  # RabbitMQ connector for publishing
    _consumer_connector: RabbitMQConnector   # RabbitMQ connector for consuming
    _logger: LoggingUtils  # Logger instance
    _message: dict  # Runtime request message, stored as dict before serialization
    _return_queue_name: str  # Name of the return queue created for responses

    def __init__(self, environments: list, configurations: dict, time_interval : int, logger: LoggingUtils) -> None:
        """
        Initialize the ICRuntimeRequest instance.

        Args:
            environments (dict): Dictionary of environment identifiers.
            configurations (dict): Configuration dictionary including server and frequency settings.
            logger (LoggingUtils): Logger instance for logging messages.
        """
        self._server = configurations.get("i-charging").get("receiver_server")
        self._logger = logger
        self._time_interval = time_interval

        self._scheduler = BackgroundScheduler()

        self._publisher_connector = RabbitMQConnector(self._server)
        self._consumer_connector = RabbitMQConnector(self._server)

        self._message = {
            "type": "runtime",
            "value": {
                "installations": environments,
                "frequency": self._time_interval,
            },
        }

        self._logger.info(f"{self._LOG_PREFIX} Request initialized for installations: {environments}")

        self._response_event: threading.Event = threading.Event()

    def _setup_publisher_service(self) -> None:
        """Initialize RabbitMQ connection for publishing."""
        self._publisher_connector.connect()
        self._publisher_connector.declare_queue(self._RPC_QUEUE_NAME)

        self._logger.info(f"{self._LOG_PREFIX} Publisher connection established.")

    def _setup_consumer_service(self) -> None:
        """Initialize RabbitMQ connection for consuming responses."""
        self._consumer_connector.connect()
        self._return_queue_name: str = self._consumer_connector.declare_queue(exclusive=True)
        self._consumer_connector.setup_consumer(
            queue_name=self._return_queue_name,
            callback=self._on_response
        )
        self._logger.info(f"{self._LOG_PREFIX} Consumer connection established (Queue: {self._return_queue_name}).")

    def _calculate_next_run_time(self) -> datetime.datetime:
        """
        Calculates the next execution time based on the time_interval.
        Aligns the schedule to the next 'round' multiple of the interval (e.g., top of the hour).
        """
        now = datetime.datetime.now()
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

        # Reset the event to allow for re-execution if needed (if i-charging stops sending observations)
        self._response_event.clear()

        final_state : bool = False

        try:
            # Initialize both publisher and consumer connectors with retry logic
            with_retries(self._setup_consumer_service, logger=self._logger)
            with_retries(self._setup_publisher_service, logger=self._logger)

            # Start the consumer thread as a daemon to handle incoming responses
            consumer_thread = threading.Thread(
                target=self._consumer_connector.start_listening,
                daemon=True,
            )
            consumer_thread.start()

            # Start the background scheduler, if not already started, for message dispatch
            if not self._scheduler.running:
                self._scheduler.start()
                self._logger.info(f"{self._LOG_PREFIX} Scheduler started.")
            else:
                self._logger.info(f"{self._LOG_PREFIX} Scheduler already running, skipping start.")

            # Call the new helper method
            next_run = self._calculate_next_run_time()
            seconds_to_wait = (next_run - datetime.datetime.now()).total_seconds()

            self._logger.info(f"{self._LOG_PREFIX} Message dispatch scheduled for: {next_run}")

            # Add the one-time job to the scheduler
            self._scheduler.add_job(
                self._send_message,
                trigger='date',
                run_date=next_run,
                id='send_runtime_request',
                misfire_grace_time=10
            )

            # Wait until the response event is set by the callback or a defined timeout occurs
            # The 'wait' method returns True if the event was set, and False if it timed out
            total_timeout = seconds_to_wait + self._TIMEOUT_SECONDS
            responded = self._response_event.wait(timeout=total_timeout)

            if not responded:
                # If no response is received within the timeframe, log a warning and proceed to cleanup
                self._logger.warning(
                    f"{self._LOG_PREFIX} Timeout: No response received after {self._TIMEOUT_SECONDS}s."
                )
            else:
                final_state = True

        except Exception as e:
            self._logger.error(f"{self._LOG_PREFIX} Error during service execution: {e}")

        finally:
            # Absolute cleanup: Ensure all resources are released even if errors occur
            self._logger.info(f"{self._LOG_PREFIX} Closing connections and cleaning up resources...")

            # Shut down the scheduler without waiting for pending jobs
            if self._scheduler.running:
                try:
                    self._scheduler.remove_job('send_runtime_request')
                except:
                    pass

            # Ensure the consumer thread is joined to prevent resource leaks
            if 'consumer_thread' in locals() and consumer_thread.is_alive():
                self._consumer_connector.stop_consuming_safely()

                consumer_thread.join()

            # Close RabbitMQ consumer and publisher connections
            self._consumer_connector.close()
            self._publisher_connector.close()

            return final_state


    def _send_message(self) -> None:
        """
        Send the prepared runtime request message to the RabbitMQ broker.

        - Serializes the message into JSON format.
        - Sets message properties including reply queue and unique message ID.
        - Waits briefly before publishing to ensure the consumer is ready.
        """
        """Sends the request message."""
        time.sleep(1)  # Small delay to ensure consumer is ready

        # Create a pretty-printed version of the message
        pretty_message = json.dumps(self._message, indent=4)

        self._publisher_connector.publish(
            self._RPC_QUEUE_NAME,
            self._message,
            properties={
                "reply_to": self._return_queue_name,
                "message_id": str(uuid.uuid4()),
                "expiration": "10000"
            },
        )

        # Log the message with the indented JSON
        self._logger.info(f"{self._LOG_PREFIX} Runtime request message published to RPC queue:\n{pretty_message}")

    def _on_response(self, ch: Any, method: Any, properties: Any, body: bytes) -> None:
        """
        Callback function to handle responses received from RabbitMQ.

        Args:
            ch (Any): The channel object.
            method (Any): Delivery method information.
            properties (Any): Message properties.
            body (bytes): The response payload received from RabbitMQ.
        """
        self._logger.info(f"{self._LOG_PREFIX} Response received: {body.decode()}")

        self._response_event.set()
