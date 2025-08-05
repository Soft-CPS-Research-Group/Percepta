import requests
from apscheduler.schedulers.background import BackgroundScheduler
from app.utils.data import DataSet
from app.utils.logger import LoggingUtils


configurations = DataSet.get_schema('./configs/runtimeConfigurations.json')
logger = LoggingUtils("cleanwatts", configurations)
cw_login = configurations['cw_login']


class CWSession:
    token = None
    refresh_token = None
    _scheduler = None

    @classmethod
    def get_token(cls):
        return cls.token

    @classmethod
    def login(cls):
        login_data = {
            "Login": cw_login.get('login'),
            "Password": cw_login.get('password')
        }

        session_url = cw_login.get('session_url')
        attempts = 0
        max_attempts = 3

        while attempts <= max_attempts:
            try:
                response = requests.post(session_url, json=login_data, timeout=60)

                if response.status_code == 201:
                    cls.token = response.json().get('Token') #atribuição em Python de objetos é atómica
                    cls.refresh_token = response.json().get('RefreshToken')
                    #logger.info(
                        #f"CWSession: Login Successful. \nToken: {cls.token}\nRefresh Token: {cls.refresh_token}")
                    break

                logger.warning(f"CWSession: Attempt {attempts + 1} failed. Status code: {response.status_code}")

            except requests.exceptions.Timeout:
                logger.warning(f"CWSession: Attempt {attempts + 1} failed: Connection timeout.")

            except requests.exceptions.ConnectionError:
                logger.warning(f"CWSession: Attempt {attempts + 1} failed: No internet connection.")

            except requests.exceptions.RequestException as e:
                logger.error(f"CWSession: Attempt {attempts + 1} failed: Unexpected error - {e}")

            attempts += 1

    @classmethod
    def refresh_tokens(cls):
        refresh_url = f"{cw_login.get('refresh_url')}token={cls.token}&refresh_token={cls.refresh_token}"

        attempts = 0
        max_attempts = 3

        while attempts <= max_attempts:
            try:
                response = requests.put(refresh_url, timeout=60)

                if response.status_code == 201:
                    cls.token = response.json().get('Token')
                    cls.refresh_token = response.json().get('RefreshToken')
                    #logger.info(
                        #f"CWSession: Successfully refreshed token. \nToken: {cls.token}\nRefresh Token: {cls.refresh_token}")
                    break
                else:
                    logger.warning(f"CWSession: Token expired. It will be necessary to login again.")
                    cls.login()

            except requests.exceptions.Timeout:
                logger.warning(f"CWSession: Attempt {attempts + 1} failed: Connection timeout.")

            except requests.exceptions.ConnectionError:
                logger.warning(f"CWSession: Attempt {attempts + 1} failed: No internet connection.")

            except requests.exceptions.RequestException as e:
                logger.error(f"CWSession: Attempt {attempts + 1} failed: Unexpected error - {e}")

            attempts += 1

    @classmethod
    def start_token_refresher(cls):
        cls.login()
        cls._scheduler = BackgroundScheduler()
        # Schedule token refresh every 3000 seconds. If a scheduled run is missed (e.g. due to system sleep), allow it to run within 10 seconds (misfire_grace_time).
        # coalesce=True ensures that if multiple runs were missed, only the latest one will be executed to avoid backlog.
        cls._scheduler.add_job(cls.refresh_tokens, 'interval', seconds=3000, misfire_grace_time=10, coalesce=True)
        cls._scheduler.start()

    @classmethod
    def stop_token_refresher(cls):
        if cls._scheduler:
            cls._scheduler.shutdown()
