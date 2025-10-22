import time
import datetime
from apscheduler.schedulers.background import BlockingScheduler
from app.connectors.http_conector import HTTPConnector, HTTPErrorWrapper
from app.exceptions import general_exceptions
from app.utils.logger import LoggingUtils
from app.utils.retry import with_retries, with_persistent_retries

class CWSession:
    """
       CWSession manages the authentication and session management for Cleanwatts API.
    """

    token : str                     # Current access token.
    refresh_token : str             # Token used to refresh the access token.
    token_valid : bool
    _scheduler: BlockingScheduler   # Scheduler for periodically refreshing the token.
    _http_connector: HTTPConnector  # HTTP connector instance for performing GET/POST requests
    _configurations: dict           # General configurations for the session.
    _cw_configurations: dict        # Cleanwatts-specific configurations.
    _max_reconnect_attempts : int   # Maximum number of attempts to reconnect on failure.
    _logger: LoggingUtils           # Logger instance for logging session activity.
    _refresh_resource : str
    _login_resource : str

    @classmethod
    def start_token_refresher_service(cls, logger : LoggingUtils, configurations : dict):
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
        cls.token_valid = False
        cls._start_http_service()

        cls._login()

        cls._scheduler = BlockingScheduler()
        # Schedule token refresh every 3500 seconds. If a scheduled run is missed (e.g. due to system sleep), allow it to run within 10 seconds (misfire_grace_time).
        # coalesce=True ensures that if multiple runs were missed, only the latest one will be executed to avoid backlog.
        cls._scheduler.add_job(cls._run_job, 'interval', seconds=3500, misfire_grace_time=10, coalesce=True)
        cls._logger.info("Starting blocking token refresher...")

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

        with_persistent_retries(func=_start_http_service_auxiliar, logger=cls._logger)

    @classmethod
    def get_token(cls) -> str:
        """
        Returns the current access token.

        Returns:
            str: Current access token.
        """
        return cls.token

    @classmethod
    def is_token_valid(cls) -> bool:
        """
        Returns the current token state.

        Returns:
            bool: Token state.
        """
        return cls.token_valid

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
                cls._logger.error(f"Failed to retrieve data from {cls._login_resource}: "
                                  f"HTTP {response.status_code}, response: {response.text[:500]}"
                                  )
                raise HTTPErrorWrapper(
                    f"Failed to retrieve data from {cls._login_resource}: "
                    f"HTTP {response.status_code}, response: {response.text[:500]}"
                )


            cls.token = response.json().get('Token')
            cls.refresh_token = response.json().get('RefreshToken')
            cls._logger.debug(f"Login access token: {cls.token}")

            cls.token_valid = True

        with_persistent_retries(func=_login_auxiliar, logger=cls._logger)


    @classmethod
    def _refresh_tokens(cls):
        """
        Refresh the access token using the refresh token, but wait if
        we are in the first 10 seconds of the minute to avoid invalidating
        the token while threads are using it.
        """

        cls._logger.debug("FUI CHAMADO\n")
        now = datetime.datetime.now()
        # Wait the first 10 seconds because a thread might be using the old (still valid) token
        if now.second < 10:
            sleep_time = 10 - now.second
            cls._logger.info(f"Token refresh waiting {sleep_time}s to avoid invalidating token in use")
            time.sleep(sleep_time)

        cls.token_valid = False

        if cls._http_connector.is_connected() is False:
            cls._start_http_service()

        def refresh_tokens_auxiliar():
            refresh_resource_with_tokens = f"{cls._refresh_resource}token={cls.token}&refresh_token={cls.refresh_token}"
            response = cls._http_connector.put(endpoint=refresh_resource_with_tokens)
            if response.status_code != 201:
                raise HTTPErrorWrapper(f"HTTP {response.status_code}, response: {response.text[:500]}")
            cls.token = response.json().get('Token')
            cls.refresh_token = response.json().get('RefreshToken')
            cls._logger.debug(f"Refreshed access token: {cls.token}")
            cls.token_valid = True

        try:
            with_retries(func=refresh_tokens_auxiliar, logger=cls._logger)
        except Exception as e:
            cls._logger.error(f"Failed to refresh token: {e}, attempting login again")
            cls._login()

    @classmethod
    def _run_job(cls):
        """
        Wrapper to run the scheduled token refresh job and handle exceptions.

        Raises:
            general_exceptions.SchedulerJobError: If the scheduled job fails.
        """
        try:
            cls._refresh_tokens()
        except Exception as e:
            cls._logger.error(f"CWLogin: Scheduled job failed: {e}")
            raise general_exceptions.SchedulerJobError(f"Scheduled job error: {e}") from e

    @classmethod
    def stop_token_refresher_service(cls):
        """
        Stops the token refresher scheduler gracefully.
        """
        if cls._scheduler and cls._scheduler.running:
            cls._logger.info("Stopping token refresher...")
            cls._scheduler.shutdown(wait=False)
        else:
            cls._logger.info("Token refresher already stopped.")
