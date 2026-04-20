import threading
import json
import subprocess
from app.connectors.http_connector import HTTPConnector, HTTPErrorWrapper
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime, timedelta
from app.utils.logger import LoggingUtils
from app.exceptions import general_exceptions
from app.utils.retry import with_retries
from zoneinfo import ZoneInfo


class ElectricityPriceFetcher:
    """
    Fetches daily electricity prices for Portugal from the REN DataHub API
    and stores them for hourly access.
    """

    _scheduler: BlockingScheduler  # Scheduler for periodically retrieving data.
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
        cls._time_interval = 900 # seconds

        cls._send_event = threading.Event()
        cls._timer_ended = threading.Condition()

        cls._tz_cet = ZoneInfo("Europe/Madrid")
        cls._tz_utc = ZoneInfo("UTC")

        cls._prices_with_timestamps = {}

        cls._start_service()

    @classmethod
    def _start_service(cls):
        """
        Initializes the HTTP service by creating a connection to the server.
        Retries the connection using the `with_retries` utility.

        Raises:
            Exception: If all connection attempts fail.
        """

        def _start_http_service_auxiliar():
            cls._http_connector = HTTPConnector(cls._server.get('url'))
            cls._logger.info(f"Electricity Price Fetcher: Connection successfully established.")

            cls._scheduler = BlockingScheduler(timezone=cls._tz_cet)

            #cls._scheduler.add_job(cls._update_energy_price, next_run_time=datetime.now())

            cls._update_energy_price()

            now_cet = datetime.now(cls._tz_cet)
            cutoff_today = now_cet.replace(hour=13, minute=5, second=0, microsecond=0)
            run_immediately = now_cet if now_cet > cutoff_today else None

            cls._scheduler.add_job(
                cls._run_job,
                trigger='cron',
                hour=13,
                minute=5,
                misfire_grace_time=300,
                next_run_time=run_immediately,
                coalesce=True,
                id='daily_price_fetch'
            )

            # Publicação em Portugal: Como Portugal está no fuso horário WET (uma hora a menos que Espanha/CET), o leilão termina às 11:00 (hora de Lisboa).
            # Disponibilidade dos Resultados: Os preços horários finais são normalmente publicados no site por volta das 12:45 CET, o que equivale às 11:45 em Portugal continental.

            cls._scheduler.start()

        with_retries(func=_start_http_service_auxiliar, logger=cls._logger)

    @classmethod
    def _update_dates(cls, prices: list, date: datetime):
        """
        Converts the price list (received in CET/CEST) to UTC keys and updates the internal dictionary.
        Handles potential errors during timezone conversion or list iteration.

        Args:
            prices (list): A list of prices (e.g., 96 for 15-min intervals).
            date (datetime): The reference date for the received prices.
        """
        if not prices:
            raise Exception("Electricity Price Fetcher: Received empty price list. Skipping update.")

        try:

            # Start at midnight of the provided date in the Madrid timezone
            # Using .replace to ensure we have a clean start at 00:00:00
            current_time_madrid = datetime(
                date.year, date.month, date.day, 0, 0, tzinfo=cls._tz_cet
            )

            cls._logger.info(f"Electricity Price Fetcher: Processing {len(prices)} price points for {date.date()}...")

            for index, price in enumerate(prices):
                try:
                    # Convert the Madrid local time to UTC
                    time_utc = current_time_madrid.astimezone(cls._tz_utc)

                    # Store in the dictionary: { datetime(UTC) : price }
                    cls._prices_with_timestamps[time_utc] = price

                    # Increment by the defined interval (e.g., 15 minutes)
                    current_time_madrid += timedelta(seconds=cls._time_interval)

                except Exception as e:
                    cls._logger.error(f"Electricity Price Fetcher: Error processing price point at index {index}: {e}")
                    # We continue to the next price point instead of failing the whole batch
                    continue

            cls._logger.info(f"Electricity Price Fetcher: Successfully updated UTC price map.")

            try:
                # Define a safety margin: keep data from the last 2 hours to avoid "not found"
                # errors during slight clock skews or overlapping requests.
                expiry_limit = datetime.now(cls._tz_utc) - timedelta(hours=2)

                # Create a new dictionary filtering out old timestamps.
                # This operation is atomic-like and prevents "dict size changed" errors.
                cls._prices_with_timestamps = {
                    ts: p for ts, p in cls._prices_with_timestamps.items()
                    if ts >= expiry_limit
                }
                cls._logger.info(
                    f"Electricity Price Fetcher: Cleanup successful. Cache size: {len(cls._prices_with_timestamps)}")

            except Exception as e:
                raise Exception(f"Electricity Price Fetcher: Error during memory cleanup: {e}")

        except Exception as e:
            # Catch-all for initialization errors (e.g., invalid ZoneInfo or Date)
            raise Exception(f"Electricity Price Fetcher: Critical error during _update_dates: {e}")

    @classmethod
    def _update_energy_price(cls, date=None):
        if not date:
            date = datetime.now()

        date_str = date.strftime("%Y-%m-%d")  # Formato yyyy-mm-dd

        prices = []

        def _update_energy_price_aux():

            nonlocal prices

            cmd = ["pyomie", date_str]
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                prices = (json.loads(result.stdout)).get("pt_spot_price")
            except Exception as e:
                raise Exception(f"{e}") # TODO arranjar texto para meter aqui

        with_retries(_update_energy_price_aux, retry_config={"max_retries": 100, "timeout": 60*5}, logger=cls._logger)  # TODO Ver melhor estas partes dos retries

        if prices:
            cls._send_event.clear()
            with cls._timer_ended:

                cls._update_dates(prices, date)

                cls._send_event.set()

                cls._timer_ended.notify_all()


    @classmethod
    def _run_job(cls):
        """
        Wrapper to run the scheduled token refresh job and handle exceptions.

        Raises:
            general_exceptions.SchedulerJobError: If the scheduled job fails.
        """
        try:
            tomorrow = datetime.now() + timedelta(days=1)
            cls._update_energy_price(tomorrow)
        except Exception as e:
            cls._logger.error(f"CWLogin: Scheduled job failed: {e}")
            raise general_exceptions.SchedulerJobError(f"Scheduled job error: {e}") from e

    # TODO se for útil adicionar método que envia todos os dados daquele dia ao em vez de todos os dados a partir daquele momento
    @classmethod
    def get_future_prices(cls):
        """
        Returns a list of all available prices from the current 15-min block onwards.
        """
        # 1. Get current time in UTC, rounded down to the nearest 15-min interval

        now_utc = datetime.now(ZoneInfo("UTC"))

        time_interval_minutes = cls._time_interval // 60
        current_block = now_utc.replace(
            minute=(now_utc.minute // time_interval_minutes) * time_interval_minutes,
            second=0,
            microsecond=0
        )

        with cls._timer_ended:

            while not cls._send_event.is_set():
                cls._timer_ended.wait(timeout=10)

            # 2. Filter the dictionary for all keys >= current_block
            # We sort the keys to ensure the timeline is correct
            # If there is no values, the dictionary returned will be empty
            future_prices = {
                ts: cls._prices_with_timestamps[ts]
                for ts in sorted(cls._prices_with_timestamps.keys())
                if ts >= current_block
            }
        cls._logger.info(f"Future Prices: {future_prices}")

        return future_prices

    @classmethod
    def stop_electricity_price_fetcher_service(cls):
        """
        Stops the electricity price fetcher scheduler gracefully.
        """
        if hasattr(cls, "_scheduler") and cls._scheduler.running:
            cls._logger.info("Stopping Electricity Price Fetcher...")
            cls._scheduler.shutdown()
        else:
            cls._logger.info("Electricity Price Fetcher already stopped.")