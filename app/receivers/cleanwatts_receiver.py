import datetime
from zoneinfo import ZoneInfo
from app.translators.cw_translator import CWTranslator
from app.utils.cwlogin import CWSession
from app.receivers.receiver_http_base import ReceiverHTTPBase
from app.utils.logger import LoggingUtils
from app.utils.providers import Provider

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

    def __init__(self, environment: str, environment_specs: dict, configurations: dict, logger: LoggingUtils):
        """
        Initializes the CWReceiver instance.

        Args:
            environment (str): Name of the environment the receiver will operate in.
            environment_specs (dict): Specifications for the environment, including entities.
            configurations (dict): General configurations for the receiver, e.g., max reconnect attempts, frequency.
            logger (LoggingUtils): Logger instance for structured logging.
        """
        super().__init__(environment, environment_specs, configurations, logger)

        # The Cleanwatts data always comes in Lisbon Timezone
        self._tz = ZoneInfo("Europe/Lisbon")

        timestamp = datetime.datetime.now(self._tz)

        self._time_interval_timedelta = datetime.timedelta(seconds=self._time_interval)

        self._end = round_timestamp_to_nearest(timestamp, self._time_interval)
        self._start = self._end - self._time_interval_timedelta

        # Translator instance is created
        self._translator = CWTranslator(environment, environment_specs, configurations, logger)

    def stop(self):
        """
        Stops the receiver and gracefully stops the Cleanwatts token refresher.
        """
        self._logger.info(f"Stopping thread {self._environment}...")
        super().stop()

        #TODO não faz sentido isto estar aqui! Várias threads vão executar isto...
        CWSession.stop_token_refresher()

    def _job(self):
        """
        Executes the main data retrieval job:
            - Ensures a valid session.
            - Retrieves raw data for all configured entities and parameters.
            - Passes collected data to CWTranslator (does not perform translation itself).
        """

        print(f"Start = {self._start.strftime("%Y-%m-%dT%H:%M:%S")} End = {self._end.strftime("%Y-%m-%dT%H:%M:%S")}")
        self._header_updater()

        for entity_id, values in self._entities.items():
            all_entity_parameter_data = {}

            for param_name, param_attr in values.get('parameters', {}).items():
                try:

                    if param_attr:
                        tag_id = param_attr.get('id')

                        # TODO alterar lógica para request temporal e não last value
                        # Make the GET request with specified timeout
                        data = self.retrieve_data(f"{self._server.get('resources').get('data')}{tag_id}&from={self._start.strftime("%Y-%m-%dT%H:%M:%S")}&to={self._end.strftime("%Y-%m-%dT%H:%M:%S")}")

                        if data:
                            self._logger.info(f"Tag {tag_id} successfully retrieved!")
                        else:
                            # Data is empty, treat as a failure
                            self._logger.warning(f"Tag {tag_id} returned empty data. Last value will be used instead.")
                            data = self.retrieve_data(f"{self._server.get('resources').get('last_value')}{tag_id}")

                        all_entity_parameter_data.update({param_name: data})


                except Exception as e:
                    self._logger.error(e)
                    continue

            # Pass raw data to translator; translation is not performed here
            self._translator.translate({'entity_id' : entity_id, 'label' : values.get('label'), 'parameters' : all_entity_parameter_data})

            self._start = self._end
            self._end = self._end + self._time_interval_timedelta

    def _header_updater(self):
        """
        Sets the authorization header using the current CWSession token.

        """
        token = CWSession.get_token()

        if token is None:
            raise RuntimeError(f"Token is None.")

        self._header = {'Authorization': f"CW {token}"}

    # TODO: Verificar se isto é a melhor alternativa
    @classmethod
    def pre_start(cls, configurations : dict, logger : LoggingUtils):
        """
        Performs pre-start initialization for the receiver by starting the
        CWSession token refresher.

        Args:
            logger (LoggingUtils): Logger instance.
            configurations (dict): General configurations for CWSession.
        """
        CWSession.start_token_refresher(logger, configurations)
