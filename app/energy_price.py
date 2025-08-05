import requests
from apscheduler.schedulers.background import BackgroundScheduler
from app.utils.cwlogin import CWSession


class EnergyPrice:
    def __init__(self, configurations, logger):
        self._energy_price = None
        self._configurations = configurations
        self._logger = logger

        CWSession.start_token_refresher()

        self._scheduler = BackgroundScheduler()
        self._scheduler.add_job(
            self._update_energy_price,
            'cron',
            minute=0,
            misfire_grace_time=300,
            coalesce=True
        )
        self._scheduler.start()

        self._update_energy_price()  # busca inicial


    def stop(self):
        CWSession.stop_token_refresher()
        self._scheduler.shutdown()

    def _update_energy_price(self):
        connection_params = self._configurations.get('electricity_pricing')

        max_retries = 3
        for attempt in range(1, max_retries + 1):
            token = CWSession.get_token()

            headers = {'Authorization': f"CW {token}"}

            try:
                response = requests.get(connection_params, headers=headers)

                if response.status_code == 200:
                    # Successful response, update energy price and exit function
                    json_response = response.json()
                    self._energy_price = json_response[0]['Read']
                    self._logger.info(f"EnergyPrice: Updated energy price: {self._energy_price}")
                    return

                elif response.status_code == 401:
                    # Unauthorized: refresh token and retry
                    self._logger.warning(f"EnergyPrice: Unauthorized access on attempt {attempt}, refreshing token.")
                    CWSession.refresh_token()

                else:
                    # Other HTTP errors: log warning and retry
                    self._logger.warning(f"EnergyPrice: Failed to get energy price, status code: {response.status_code}")

            except requests.exceptions.Timeout:
                # Timeout error: log and retry
                self._logger.error(f"EnergyPrice: Connection timeout during price update on attempt {attempt}.")

            except requests.exceptions.ConnectionError as e:
                # Connection error: log and retry
                self._logger.error(f"EnergyPrice: Connection error during price update on attempt {attempt}: {e}")

            except requests.exceptions.RequestException as e:
                # Any other requests exception: log and retry
                self._logger.error(f"EnergyPrice: Unexpected error during price update on attempt {attempt}: {e}")

        # After max retries, log failure
        self._logger.error("EnergyPrice: Failed to update energy price after 3 attempts.")

    def get_energy_price(self):
        return self._energy_price
