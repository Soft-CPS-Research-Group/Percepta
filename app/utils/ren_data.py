import atexit
from app.connectors.http_conector import HTTPConnector, HTTPErrorWrapper
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
from app.utils.logger import LoggingUtils
from app.exceptions import general_exceptions
from app.utils.retry import with_persistent_retries


class ElectricityPriceFetcher:
    """
    Fetches daily electricity prices for Portugal from the REN DataHub API
    and stores them for hourly access.
    """

    _scheduler: BackgroundScheduler  # Scheduler for periodically retrieving data.
    _http_connector: HTTPConnector  # HTTP connector instance for performing GET/POST requests
    _prices : list
    _logger: LoggingUtils           # Logger instance for logging session activity.
    _configurations: dict           # General configurations for the session.


    @classmethod
    def start_electricity_price_fetcher_service(cls, logger : LoggingUtils, configurations : dict):
        """
            Args:
                   logger (LoggingUtils): Logger instance used to log session events.
                   configurations (dict): Dictionary containing general and REN-specific configurations.
        """
        cls._prices = [None] * 24  # Array from 0 to 23 to store hourly prices
        cls._logger = logger
        cls._configurations = configurations
        cls._ren_configurations = configurations.get('ren')
        cls._max_reconnect_attempts = configurations.get('max_reconnect_attempts')
        cls._server = cls._ren_configurations.get('receiver_server')
        cls._data_resource = cls._server.get('resources').get("data")
        cls._start_http_service()
        cls._fetch_prices(datetime.now())

        # Configure the scheduler
        cls._scheduler = BackgroundScheduler()
        # Schedule daily job at 23:30
        cls._scheduler.add_job(cls._run_job, 'cron', hour=23, minute=30)

        cls._scheduler.start()

        # Ensure the scheduler shuts down on exit
        # atexit.register(lambda: cls._scheduler.shutdown())

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
    def _fetch_next_day_prices(cls):
        tomorrow = datetime.now() + timedelta(days=1)
        cls._fetch_prices(tomorrow)

    @classmethod
    def _fetch_prices(cls, date):
        """
        Sends a request to the API for the next day's prices and updates self.prices_pt
        """

        try:

            date_str = date.strftime("%Y-%m-%d")

            response = cls._http_connector.get(endpoint=f"{cls._data_resource}?culture=pt-PT&date={date_str}",timeout=5)
            data = response.json()

            # Extract Portugal prices
            pt_series = next((s for s in data.get("series", []) if s.get("name") == "PT"), None)
            if pt_series and "data" in pt_series:
                cls._prices = pt_series["data"]
                print(f"[{datetime.now()}] Prices for {date_str} updated successfully. {cls._prices}\n")
            else:
                print(f"[{datetime.now()}] Could not find Portugal prices in the response.")

        except Exception as e:
            print(f"[{datetime.now()}] Error fetching prices: {e}")

    @classmethod
    def _run_job(cls):
        """
        Wrapper to run the scheduled token refresh job and handle exceptions.

        Raises:
            general_exceptions.SchedulerJobError: If the scheduled job fails.
        """
        try:
            cls._fetch_next_day_prices()
        except Exception as e:
            cls._logger.error(f"CWLogin: Scheduled job failed: {e}")
            raise general_exceptions.SchedulerJobError(f"Scheduled job error: {e}") from e

    @classmethod
    def get_price(cls, hour: int):
        """
        Returns the price for a specific hour (0-23)

        Args:
            hour (int): hour of the day (0 to 23)

        Returns:
            float | None: price for the hour or None if not available
        """
        if 0 <= hour <= 23:
            print(f"prices {cls._prices} for hour {hour}\n")
            if cls._prices[hour] is None:
                price = 0
            else:
                price = cls._prices[hour]
            return price
        else:
            raise ValueError("Hour must be between 0 and 23")


    @classmethod
    def stop_electricity_price_fetcher_service(cls):
        """
        Stops the electricity price fetcher scheduler gracefully.
        """
        if cls._scheduler and cls._scheduler.running:
            cls._logger.info("Stopping Electricity Price Fetcher...")
            cls._scheduler.shutdown()
        else:
            cls._logger.info("Electricity Price Fetcher already stopped.")