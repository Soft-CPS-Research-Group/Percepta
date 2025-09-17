from apscheduler.schedulers.background import BackgroundScheduler
from app.connectors.http_conector import HTTPConnector, HTTPErrorWrapper
from app.exceptions import general_exceptions
from app.utils.logger import LoggingUtils
from app.utils.retry import with_retries


class CWSession:
    """
       CWSession manages the authentication and session management for Cleanwatts API.
    """

    token : str                     # Current access token.
    refresh_token : str             # Token used to refresh the access token.
    _scheduler: BackgroundScheduler # Scheduler for periodically refreshing the token.
    _http_connector: HTTPConnector  # HTTP connector instance for performing GET/POST requests
    _configurations: dict           # General configurations for the session.
    _cw_configurations: dict        # Cleanwatts-specific configurations.
    _max_reconnect_attempts : int   # Maximum number of attempts to reconnect on failure.
    _logger: LoggingUtils           # Logger instance for logging session activity.
    _refresh_resource : str
    _login_resource : str

    @classmethod
    def start_token_refresher(cls, logger : LoggingUtils, configurations : dict):
        """
               Initializes the session, logs in to the Cleanwatts server, and starts the token refresher scheduler.

               Args:
                   logger (LoggingUtils): Logger instance used to log session events.
                   configurations (dict): Dictionary containing general and Cleanwatts-specific configurations.
        """

        cls._logger = logger
        cls._configurations = configurations
        cls._cw_configurations = configurations.get('cleanwatts')
        cls._max_reconnect_attempts = configurations.get('max_reconnect_attempts')

        cls._server = cls._cw_configurations.get('login_server')
        cls._auth = cls._server.get('auth')

        cls._credentials = {
                "Login": cls._auth.get('username'),
                "Password": cls._auth.get('password')
            }
        cls._refresh_resource = cls._server.get('resources').get("refresh")
        cls._login_resource = cls._server.get('resources').get("login")

        cls._start_http_service()

        cls._login()

        cls._scheduler = BackgroundScheduler()
        # Schedule token refresh every 3000 seconds. If a scheduled run is missed (e.g. due to system sleep), allow it to run within 10 seconds (misfire_grace_time).
        # coalesce=True ensures that if multiple runs were missed, only the latest one will be executed to avoid backlog.
        cls._scheduler.add_job(cls._run_job, 'interval', seconds=3000, misfire_grace_time=10, coalesce=True)
        cls._scheduler.start()

    @classmethod
    def _start_http_service(cls):
        """
        Initializes the HTTP service by creating a connection to the server.
        Retries the connection using the `with_retries` utility.

        Raises:
            Exception: If all connection attempts fail.
        """

        def _start_http_service_auxiliar():
            cls._http_connector = HTTPConnector(cls._server.get('url'))
            cls._logger.info(f"CWLogin: Connection successfully established.")

        with_retries(func=_start_http_service_auxiliar, logger=cls._logger)

    @classmethod
    def get_token(cls):
        """
        Returns the current access token.

        Returns:
            str: Current access token.
        """
        return cls.token

    @classmethod
    def _login(cls):
        """
        Logs in to the Cleanwatts server to obtain an access token and refresh token.

        Uses retry logic to handle transient errors and ensures that the HTTP service is connected.
        """
        if cls._http_connector.is_connected() is False:
            cls._start_http_service()

        def _login_auxiliar():
            response = cls._http_connector.post(endpoint=cls._login_resource, data=cls._credentials)

            if response.status_code != 201:
                raise HTTPErrorWrapper(
                    f"Failed to retrieve data from {cls._login_resource}: "
                    f"HTTP {response.status_code}, response: {response.text[:500]}"
                )

            cls.token = response.json().get('Token')
            cls.refresh_token = response.json().get('RefreshToken')

        with_retries(func=_login_auxiliar, logger=cls._logger)

    @classmethod
    def _refresh_tokens(cls):
        """
        Refreshes the access token using the refresh token.

        If the refresh fails, it attempts to log in again.
        Uses retry logic to handle transient HTTP errors.
        """
        if cls._http_connector.is_connected() is False:
            cls._start_http_service()

        refresh_resource_with_tokens = f"{cls._refresh_resource}token={cls.token}&refresh_token={cls.refresh_token}"

        try:
            response = cls._http_connector.put(endpoint=refresh_resource_with_tokens)

            if response.status_code != 201:
                raise HTTPErrorWrapper(
                    f"HTTP {response.status_code}, response: {response.text[:500]}"
                )

            cls.token = response.json().get('Token')
            cls.refresh_token = response.json().get('RefreshToken')

        except Exception as e:
            cls._logger.error(f"Failed to retrieve data from {refresh_resource_with_tokens}: {e}. Failed to refresh the token, the System will attempt to log in again.")
            cls._login()


    @classmethod
    def _run_job(self):
        """
        Wrapper to run the scheduled token refresh job and handle exceptions.

        Raises:
            general_exceptions.SchedulerJobError: If the scheduled job fails.
        """
        try:
            self._refresh_tokens()
        except Exception as e:
            self._logger.error(f"CWLogin: Scheduled job failed: {e}")
            raise general_exceptions.SchedulerJobError(f"Scheduled job error: {e}") from e

    @classmethod
    def stop_token_refresher(cls):
        """
        Stops the token refresher scheduler gracefully.
        """
        if cls._scheduler:
            cls._scheduler.shutdown()