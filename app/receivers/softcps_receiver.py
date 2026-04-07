import datetime
from app.translators.softcps_translator import SoftCPSTranslator
from app.receivers.receiver_http_base import ReceiverHTTPBase
from app.utils.logger import LoggingUtils
from app.utils.providers import Provider
from concurrent.futures import ThreadPoolExecutor, as_completed

class SoftCPSReceiver(ReceiverHTTPBase):
    """
    CWReceiver is responsible for retrieving raw data from the Cleanwatts API
    for configured entities and parameters, and maintaining session management
    using CWSession.

    Note:
        Translation of provider-specific data into Percepta-specific format
        is handled by CWTranslator.
    """
    provider = Provider.SOFTCPS.value     # Provider ID

    _translator: SoftCPSTranslator   # Translator which translates Cleanwatts-specific format into Percepta-specific format

    def __init__(self, environment_name: str, environment_specs: dict, configurations: dict, logger: LoggingUtils):
        """
        Initializes the SoftCPSReceiver instance.

        Args:
            environment_name (str): Name of the environment the receiver will operate in.
            environment_specs (dict): Specifications for the environment, including entities.
            configurations (dict): General configurations for the receiver, e.g., max reconnect attempts, frequency.
            logger (LoggingUtils): Logger instance for structured logging.
        """
        super().__init__(environment_name, environment_specs, configurations, logger)

        # Translator instance is created
        self._translator = SoftCPSTranslator(environment_name, environment_specs, configurations, logger)

    def stop(self):
        """
        Stops the receiver and gracefully stops the Cleanwatts token refresher.
        """
        self._logger.info(f"Stopping thread {self._environment_name}...")
        super().stop()

    def fetch_entity_parameter_data(self, entity_id) -> tuple[str, dict]:
        """
        Fetch data for a single parameter of a single entity.

        Args:
            entity_id (str): ID of the entity.

        Returns:
            tuple: (entity_id, {entity_name: data})
        """
        data = {}

        try:
            resource = self._resources_rules.get(entity_id)
            if resource:
                url_path = self._server.get('resources').get('data').format(entity_id=resource, time_interval=self._time_interval)
                #self._logger.info(f"URL {url_path}")
                # Perform the GET request with specified time range
                data = self.retrieve_data(url_path, 5)

        except Exception as e:
            self._logger.error(f"Error fetching {entity_id}: {e}")

        return entity_id, data

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
            future_to_entity = {}

            for entity_id in self._entities.keys():
                future = executor.submit(self.fetch_entity_parameter_data, entity_id)
                future_to_entity[future] = entity_id

            # Collect results as they complete
            for future in as_completed(future_to_entity):
                entity_id, param_data = future.result()
                if entity_id not in results:
                    results[entity_id] = {}
                results[entity_id].update(param_data)

        # Pass raw data to translator in a separate loop
        for entity_id, values in self._entities.items():
            result_values = results.get(entity_id, {})
            if result_values:
                self._translator.translate({
                    'entity_id': entity_id,
                    'label': values.get('label'),
                    'parameters': result_values
                })


    @classmethod
    def launch(cls, environments : dict, configurations : dict):
        threads = []

        for environment, environment_specs in environments.items():
            logger_per_environment = LoggingUtils(f"{cls.provider}_receiver", configurations, environment)
            receiver = cls(environment, environment_specs, configurations, logger_per_environment)
            receiver.start()
            threads.append(receiver)

        return threads