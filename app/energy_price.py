import requests
from datetime import date, datetime
from apscheduler.schedulers.background import BackgroundScheduler

class EnergyPrice:
    _energy_price = None
    _scheduler = None
    
    @classmethod
    def stop(cls):
        cls._scheduler.shutdown()

    @classmethod
    def _update_energy_price(cls):
        today_str = date.today().strftime("%Y-%m-%d")  # Formato yyyy-mm-dd
        # Supondo que a API aceite a data como query parameter "date"
        url_with_date = f"{cls._connection_params}{today_str}"

        max_retries = 3
        for attempt in range(1, max_retries + 1):

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
                    cls._logger.warning(f"EnergyPrice: Unauthorized access on attempt {attempt}, refreshing token.")

                else:
                    # Other HTTP errors: log warning and retry
                    cls._logger.warning(f"EnergyPrice: Failed to get energy price, status code: {response.status_code}")

            except requests.exceptions.Timeout:
                # Timeout error: log and retry
                cls._logger.error(f"EnergyPrice: Connection timeout during price update on attempt {attempt}.")

            except requests.exceptions.ConnectionError as e:
                # Connection error: log and retry
                cls._logger.error(f"EnergyPrice: Connection error during price update on attempt {attempt}: {e}")

            except requests.exceptions.RequestException as e:
                # Any other requests exception: log and retry
                cls._logger.error(f"EnergyPrice: Unexpected error during price update on attempt {attempt}: {e}")

        # After max retries, log failure
        cls._logger.error("EnergyPrice: Failed to update energy price after 3 attempts.")

    @classmethod
    def start_service(cls, logger, configurations):
        cls._logger = logger
        cls._connection_params = configurations.get('connection_params')

        cls._update_energy_price()
        
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
            return None  # Price data not loaded yet

        # Get the current hour (0-23)
        current_hour = datetime.now().hour

        try:
            # Assuming self._energy_price is a list of 24 elements (one per hour)
            return cls._energy_price[current_hour]
        except IndexError:
            # In case the list does not have 24 elements
            cls._logger.error(f"EnergyPrice: No price data for current hour ({current_hour})")
            return None

