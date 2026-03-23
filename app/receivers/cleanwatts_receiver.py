import datetime
import threading
from zoneinfo import ZoneInfo
from app.translators.cw_translator import CWTranslator
from app.utils.cwlogin import CWSession
from app.receivers.receiver_http_base import ReceiverHTTPBase
from app.utils.logger import LoggingUtils
from app.utils.providers import Provider
from concurrent.futures import ThreadPoolExecutor, as_completed


def round_timestamp_to_nearest(timestamp: datetime.datetime, interval_seconds: int) -> datetime.datetime:
    """
    Round a timezone-aware datetime object to the nearest time interval.

    Args:
        timestamp (datetime.datetime): The datetime object to round (must have tzinfo).
        interval_seconds (int): The time interval in seconds.

    Returns:
        datetime.datetime: A new datetime rounded to the nearest interval.
    """
    # Convert datetime to seconds since the Unix epoch
    timestamp_seconds = timestamp.timestamp()

    # Compute the nearest multiple of the interval
    rounded_seconds = round(timestamp_seconds / interval_seconds) * interval_seconds

    # Convert back to datetime, preserving the original timezone
    return datetime.datetime.fromtimestamp(rounded_seconds, tz=timestamp.tzinfo)

class CWReceiver(ReceiverHTTPBase):
    """
    CWReceiver is responsible for retrieving raw data from the Cleanwatts API
    for configured entities and parameters, and maintaining session management
    using CWSession.

    Note:
        Translation of provider-specific data into Percepta-specific format
        is handled by CWTranslator.
    """
    provider = Provider.CLEANWATTS.value     # Provider ID

    _translator: CWTranslator   # Translator which translates Cleanwatts-specific format into Percepta-specific format

    def __init__(self, environment_name: str, environment_specs: dict, configurations: dict, logger: LoggingUtils):
        """
        Initializes the CWReceiver instance.

        Args:
            environment_name (str): Name of the environment the receiver will operate in.
            environment_specs (dict): Specifications for the environment, including entities.
            configurations (dict): General configurations for the receiver, e.g., max reconnect attempts, frequency.
            logger (LoggingUtils): Logger instance for structured logging.
        """
        super().__init__(environment_name, environment_specs, configurations, logger)

        # The Cleanwatts data always comes in Lisbon Timezone
        self._tz = ZoneInfo("Europe/Lisbon")

        timestamp = datetime.datetime.now(self._tz)

        self._time_interval_timedelta = datetime.timedelta(seconds=self._time_interval)

        self._end = round_timestamp_to_nearest(timestamp, self._time_interval)
        self._start = self._end - self._time_interval_timedelta

        # Translator instance is created
        self._translator = CWTranslator(environment_name, environment_specs, configurations, logger)

    def stop(self):
        """
        Stops the receiver and gracefully stops the Cleanwatts token refresher.
        """
        self._logger.info(f"Stopping thread {self._environment_name}...")
        super().stop()

        #TODO não faz sentido isto estar aqui! Várias threads vão executar isto...
        CWSession.stop_token_refresher_service()


    def fetch_entity_parameter_data(self, entity_id, param_name):
        """
        Fetch data for a single parameter of a single entity.

        Args:
            entity_id (str): ID of the entity.
            param_name (str): Name of the parameter.

        Returns:
            tuple: (entity_id, {param_name: data})
        """
        all_entity_parameter_data = {}

        try:
            resource = self._resources_rules.get(param_name)
            if resource:

                # Perform the GET request with specified time range
                data = self.retrieve_data(
                    f"{self._server.get('resources').get('data')}{resource}&from={self._start.strftime('%Y-%m-%dT%H:%M:%S')}&to={self._end.strftime('%Y-%m-%dT%H:%M:%S')}", 5,
                    self._header_updater()
                )

                if not data:
                    # If data is empty, fallback to last value
                    fallback_from = (self._start - datetime.timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%S')

                    data = self.retrieve_data(
                        f"{self._server.get('resources').get('last_value')}{resource}&from={fallback_from}&",
                        5,
                        self._header_updater()
                    )

                all_entity_parameter_data.update({param_name: data})

        except Exception as e:
            self._logger.error(f"Error fetching {entity_id}-{param_name}: {e}")

        return entity_id, all_entity_parameter_data

    def _job(self):
        """
        Executes the main data retrieval job:
            - Ensures a valid session.
            - Retrieves raw data for all configured entities and parameters in parallel.
            - Passes collected data to CWTranslator after all requests complete.
        """
        results = {}  # Will store all data per entity

        # Parallelize requests using ThreadPoolExecutor (I/O-bound tasks)
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_entity_param = {}

            for entity_id, values in self._entities.items():
                for param_name in values.get('parameters', {}).keys():
                    future = executor.submit(self.fetch_entity_parameter_data, entity_id, param_name)
                    future_to_entity_param[future] = (entity_id, param_name)

            # Collect results as they complete
            for future in as_completed(future_to_entity_param):
                entity_id, param_data = future.result()
                if entity_id not in results:
                    results[entity_id] = {}
                results[entity_id].update(param_data)

        # Pass raw data to translator in a separate loop
        for entity_id, values in self._entities.items():
            self._translator.translate({
                'entity_id': entity_id,
                'label': values.get('label'),
                'parameters': results.get(entity_id, {})
            })

        # Update start/end timestamps for the next job
        self._start = self._end
        self._end = self._end + self._time_interval_timedelta

    def _header_updater(self) -> dict:
        """
        Sets the authorization header using the current CWSession token.

        """
        token = CWSession.get_token()
        if token is None:
            raise RuntimeError(f"Token is None.")

        return {'Authorization': f"CW {token}"}

    @classmethod
    def launch(cls, environments : dict, configurations : dict):
        threads = []

        logger_cw_session = LoggingUtils(f"{cls.provider}_login", configurations)

        cw_session = threading.Thread(
            target=CWSession.start_token_refresher_service,
            args=(logger_cw_session, configurations),
            daemon=True
        )
        cw_session.start()

        threads.append(cw_session)

        for environment, environment_specs in environments.items():
            logger_per_environment = LoggingUtils(f"{cls.provider}_receiver", configurations, environment)
            receiver = cls(environment, environment_specs, configurations, logger_per_environment)
            receiver.start()
            threads.append(receiver)

        return threads