import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from apscheduler.schedulers.background import BackgroundScheduler
from app.utils.retry import with_retries

class EnergyPrice:
    _energy_price = None
    _scheduler = None
    
    @classmethod
    def stop(cls):
        cls._scheduler.shutdown()

    @classmethod
    def _update_energy_price(cls, date = None):
        if not date:
            date = datetime.today()

        date_str = date.strftime("%Y-%m-%d") # Formato yyyy-mm-dd

        url_with_date = f"{cls._connection_params}{date_str}"

        def _update_energy_price_aux():
            try:
                response = requests.get(url_with_date)

                if response.status_code == 200:
                    # Successful response, update energy price and exit function
                    json_response = response.json()
                    cls._energy_price = json_response.get('series')[0].get('data')
                    print(cls._energy_price)
                    cls._logger.info(f"EnergyPrice: Updated energy price: {cls._energy_price}")
                    return

                elif response.status_code == 401:
                    # Unauthorized: refresh token and retry
                    raise Exception(f"EnergyPrice: Unauthorized access refreshing token.")

                else:
                    # Other HTTP errors: log warning and retry
                    raise Exception(f"EnergyPrice: Failed to get energy price, status code: {response.status_code}")

            except requests.exceptions.Timeout as e:
                # Timeout error: log and retry
                raise Exception(f"EnergyPrice: Connection timeout during price update: {e}")

            except requests.exceptions.ConnectionError as e:
                # Connection error: log and retry
                raise Exception(f"EnergyPrice: Connection error during price update: {e}")

            except requests.exceptions.RequestException as e:
                # Any other requests exception: log and retry
                raise Exception(f"EnergyPrice: Unexpected error during price update: {e}")

        with_retries(_update_energy_price_aux, logger=cls._logger)


    @classmethod
    def start_service(cls, logger, configurations):
        cls._logger = logger
        cls._connection_params = configurations.get('connection_params')

        cls._update_energy_price()
        tz_cet = ZoneInfo("Europe/Madrid")
        now_cet = datetime.now(tz_cet)

        if now_cet.hour >= 12:
            cls._update_energy_price(date=now_cet + timedelta(days=1))
        
        cls._scheduler = BackgroundScheduler()

        cls._scheduler.add_job(
            cls._update_energy_price,
            'cron',
            hour=0,
            minute=0,
            misfire_grace_time=300,
            coalesce=True
        )
        
        cls._scheduler.start()
        
    def get_energy_price(cls):
        """Return the energy price for the current hour."""
        if cls._energy_price is None:
            cls._logger.warning("Price data not loaded yet!")
            return None  # Price data not loaded yet

        # Get the current hour (0-23)
        current_hour = datetime.now().hour
        print(cls._energy_price)
        try:
            # Assuming self._energy_price is a list of 24 elements (one per hour)
            return cls._energy_price[current_hour]
        except IndexError:
            # In case the list does not have 24 elements
            cls._logger.error(f"EnergyPrice: No price data for current hour ({current_hour})")
            return None

EnergyPrice._update_energy_price()