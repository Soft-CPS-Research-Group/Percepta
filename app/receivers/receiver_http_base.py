from abc import abstractmethod
from app.receivers.receiver_base import ReceiverBase
from apscheduler.schedulers.background import BlockingScheduler
from app.connectors.http_conector import HTTPConnector, HTTPErrorWrapper
from app.utils.logger import LoggingUtils
from app.utils.data import DataSet
from app.exceptions import general_exceptions
from app.utils.retry import with_retries

class ReceiverHTTPBase(ReceiverBase):
    """
    Base class for HTTP receivers.

    Manages HTTP connections, periodic data retrieval, and scheduling of jobs.
    """

    _server: dict # Server configuration dictionary containing environment-specific settings
    _http_connector: HTTPConnector # HTTP connector instance for performing GET/POST requests
    _time_interval: int # Interval in seconds for scheduling the periodic job
    _header: dict # Current HTTP headers used for requests, updated dynamically if needed
    _scheduler: BlockingScheduler

    def __init__(self, environment: str, environment_specs: dict, configurations: dict, logger: LoggingUtils):
        """
        Initialize the receiver with environment settings, HTTP connector, and scheduling interval.

        Args:
            environment (str): Current environment (e.g., production, staging).
            environment_specs (dict): Environment-specific configurations.
            configurations (dict): General configurations including provider info and frequency.
            logger (LoggingUtils): Logger instance for logging messages.
        """
        super().__init__(environment, environment_specs, configurations, logger)

        self._time_interval = DataSet.calculate_interval(configurations.get('frequency'))
        self._server = self._provider_configurations.get('receiver_server')
        self._header = None
        self._start_http_service()

    def _start_http_service(self):
        """
        Initialize the HTTP service by creating a connection to the server.
        Retries the connection up to `_max_reconnect_attempts` times in case of failure.

        Raises:
            Exception: If all connection attempts fail.
        """

        def start_http_service_auxiliar():

            # Attempt to create a new HTTPConnector instance
            self._http_connector = HTTPConnector(self._server.get('url'))

            # If successful, log the success message and exit the loop
            self._logger.info(f"Connection successfully established.")

        with_retries(func = start_http_service_auxiliar, logger = self._logger)

    @abstractmethod
    def _job(self):
        """
        Abstract method for the scheduled job.

        This method must be implemented by subclasses.
        """
        raise NotImplementedError

    def _run_job(self):
        """
        Wrapper to run the scheduled job and handle exceptions.
        """
        try:
            self._job()
        except Exception as e:
            self._logger.error(f"HTTP Receiver - {self._environment}: Scheduled job failed: {e}", exc_info=True)
            # Optionally propagate as a custom exception
            raise general_exceptions.SchedulerJobError(f"Scheduled job error: {e}") from e

    def retrieve_data(self, resource: str, timeout: int = 10):
        """
        Retrieve data from a given HTTP resource.

        Updates headers if needed and performs a GET request.

        Args:
            resource (str): API endpoint or resource path.
            timeout (int, optional): Timeout for the HTTP request in seconds. Defaults to 10.
        """

        # Checks if the session is still alive, if not it tries to restart it
        if self._http_connector.is_connected() is False:
            self._start_http_service()

        # Update headers if they have changed
        self._http_connector.update_headers(self._header)
        # Perform GET request
        response = self._http_connector.get(resource, timeout)

        # Check HTTP status
        if response.status_code != 200:
            raise HTTPErrorWrapper(
                f"Failed to retrieve data from {resource}: "
                f"HTTP {response.status_code}, response: {response.text[:500]}"
            )

            # Parse JSON and handle invalid JSON errors
        try:
            return response.json()
        except ValueError as e:
            raise ValueError(
                f"Failed to parse JSON from {resource}: {e}, response text: {response.text[:500]}"
            ) from e


    def stop(self):
        """
        Stop the scheduled job and close the HTTP session.

        This method shuts down the scheduler and closes the underlying HTTP connection.
        """
        self._http_connector.close()
        self._scheduler.shutdown()

    def run(self):
        """
        Start the scheduler and execute the job at defined intervals.

        Initializes the BlockingScheduler, schedules the job according to the configured time interval,
        runs the job immediately once, and then starts the scheduler loop.
        """
        self._scheduler = BlockingScheduler()
        self._scheduler.add_job(
            self._run_job,
            'interval',
            seconds=self._time_interval,
            misfire_grace_time=10,
            coalesce=True
        )

        # Execute the job once immediately
        self._run_job()

        # Start the scheduler loop
        self._scheduler.start()

