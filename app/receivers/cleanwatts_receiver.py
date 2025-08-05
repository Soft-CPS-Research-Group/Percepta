from threading import Event
from apscheduler.schedulers.background import BlockingScheduler
import requests
import time
import datetime
from app.repositories.irepositories.environment_repository import EnvironmentRepository
from app.translators.cw_translator import CWTranslator
from app.utils.cwlogin import CWSession
from app.receivers.receiver_base import ReceiverBase

class CWReceiver(ReceiverBase):
    provider = "Cleanwatts"

    def __init__(self, environment, environment_specs, configurations, logger):
        super().__init__(environment, environment_specs, configurations, logger)

        self._translator = CWTranslator(environment, environment_specs, configurations, logger)

        self._connection_params = configurations.get('cw_server')
        self._session_time = 0
        self._stop_event = Event()
        self._count = 0

    def stop(self):
        CWSession.stop_token_refresher()
        self._stop_event.set()
        self._scheduler.shutdown()

    def _job(self):

        self._login()
        for entity_id, values in self._entities.items():
            all_entity_parameter_data = {}
            all_success = True

            for param_name, param_attr in values.get('parameters', {}).items():
                if param_attr:
                    tag_id = param_attr.get('id')
                    try:
                        # Build the API URL (TODO: replace this hardcoded URL with config)
                        url = f"https://ks.innov.cleanwatts.energy/api/2.0/data/lastvalue/Instant?from=2024-06-11&tags={tag_id}"

                        # Make the GET request with specified timeout (TODO: review timeout settings)
                        response = requests.get(url, headers=self._header, timeout=(3, 10))

                        # Check if the response status code is 200 (OK)
                        if response.status_code == 200:
                            data = response.json()
                            # Proceed only if the returned data is not empty (non-empty array or dict)
                            if data:
                                self._logger.info(f"CWReceiver: Tag {tag_id} successfully retrieved!")
                                all_entity_parameter_data.update({param_name: data})
                            else:
                                # Data is empty, treat as a failure
                                self._logger.warning(f"CWReceiver: Tag {tag_id} returned empty data.")
                                all_success = False
                                break
                        else:
                            # Status code is not 200, log a warning and mark failure
                            self._logger.warning(f"CWReceiver: Error getting data from tag {tag_id}: {response.status_code}")
                            all_success = False
                            break

                    except requests.exceptions.Timeout:
                        self._logger.error("CWReceiver: Connection timeout.")
                        all_success = False
                        break

                    except requests.exceptions.ConnectionError as e:
                        self._logger.error(f"CWReceiver: {e}")
                        all_success = False
                        break

                    except requests.exceptions.RequestException as e:
                        self._logger.error(f"CWReceiver: Unexpected error - {e}")
                        all_success = False
                        break
                else:
                    all_success = False
                    break

            if all_success:
                self._translator.translate(all_entity_parameter_data, values.get('label'), entity_id)

    def _login(self):
        try: 
            token = CWSession.get_token()
        except Exception as e:
            print(e)
            exit()
        self._header = {'Authorization': f"CW {token}"}
        if token is not None:
            self._session_time = datetime.datetime.now().timestamp()

    def run(self):
        self._scheduler = BlockingScheduler()
        self._scheduler.add_job(self._job, 'interval', seconds=self._time_interval, misfire_grace_time=10, coalesce=True)

        self._job()
        self._scheduler.start()

    @classmethod
    def pre_start(cls):
        CWSession.start_token_refresher()