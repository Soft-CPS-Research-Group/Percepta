from abc import abstractmethod
from app.receivers.receiver_base import ReceiverBase
from apscheduler.schedulers.background import BlockingScheduler
from app.connectors.http_conector import HTTPConnector, HTTPErrorWrapper
from app.utils.logger import LoggingUtils
from app.utils.data import DataSet
from app.utils.retry import with_retries

class ReceiverHTTPBase(ReceiverBase):
    """
    Base class for HTTP receivers.

    Manages HTTP connections, periodic data retrieval, and scheduling of jobs.
    """

    _server: dict # Server configuration dictionary containing provider-specific settings
    _http_connector: HTTPConnector # HTTP connector instance for performing HTTP requests
    _time_interval: int # Interval in seconds for scheduling the periodic job
    _scheduler: BlockingScheduler # Scheduler that blocks the main thread while running

    def __init__(self, environment: str, environment_specs: dict, configurations: dict, logger: LoggingUtils):
        """
        Initialize the receiver with environment settings, HTTP connector, and scheduling interval.

        Args:
            environment (str): Name of the environment the receiver will operate in.
            environment_specs (dict): Specifications for the environment, including entities.
            configurations (dict): General configurations passed to the translator.
            logger (LoggingUtils): Logger instance for structured logging.
        """
        super().__init__(environment, environment_specs, configurations, logger)

        self._time_interval = DataSet.calculate_interval(configurations.get('frequency'))
        self._server = self._provider_configurations.get('receiver_server')

        with_retries(func = self._start_http_service, logger = self._logger)

    def _start_http_service(self):
        """
        Initializes the HTTP service by creating a connection to the server.
        """

        # Attempt to create a new HTTPConnector instance
        self._http_connector = HTTPConnector(self._server.get('url'))

        self._logger.info(f"Connection successfully established.")

    @abstractmethod
    def _job(self):
        """
        Abstract method for the scheduled job.

        This method must be implemented by subclasses.
        """
        raise NotImplementedError

    def retrieve_data(self, resource: str, timeout: int = 10, header: dict = None):
        """
        Retrieves data from a given HTTP resource.
        Updates headers if needed and performs a GET request.

        Args:
            resource (str): API endpoint or resource path.
            timeout (int, optional): Timeout for the HTTP request in seconds. Defaults to 10.
        """

        # Checks if the session is still alive, if not it tries to restart it
        if self._http_connector.is_connected() is False:
            self._start_http_service()

        # Update headers if they have changed
        if header is not None:
            self._http_connector.update_headers(header)

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
        Shuts down the scheduler and closes the underlying HTTP connection.
        """
        self._http_connector.close()
        self._scheduler.shutdown()

    def run(self):
        """
        Initializes the BlockingScheduler, schedules the job according to the configured time interval,
        runs the job immediately once, and then starts the scheduler loop.
        """
        self._scheduler = BlockingScheduler()
        total_seconds = self._time_interval

        # Convert the interval from seconds to hours and minutes
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60

        # Dynamically define cron arguments based on the interval
        cron_args = {}
        if hours > 0:
            cron_args['hour'] = f'*/{hours}'
        if minutes > 0:
            cron_args['minute'] = f'*/{minutes}'

        self._scheduler.add_job(
            self._job,
            'cron',
            misfire_grace_time=10,
            coalesce=True,
            **cron_args
        )

        # Run the job immediately once before scheduling
        self._job()

        # Start the scheduler loop
        self._scheduler.start()

    @classmethod
    def launch(cls, environments: dict, configurations: dict) -> list:
        raise NotImplementedError
