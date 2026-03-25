import copy

from app.connectors.http_connector import HTTPConnector, HTTPErrorWrapper
from app.repositories.irepositories.time_series_repository import TimeSeriesRepository
from app.utils.logger import LoggingUtils
from app.utils.retry import with_retries


class Predictor:
    _http_connector: HTTPConnector = {} # HTTP connector instance for performing HTTP requests

    def __init__(self, environment : str, environment_specs : dict, time_series_repository : TimeSeriesRepository, forwarders : dict, configurations : dict, logger : LoggingUtils):
        self._entities = environment_specs["entities"]
        self._group = environment_specs["group"]
        self._environment = environment
        self._forwarders = forwarders
        self._logger = logger
        self._time_series_repository = time_series_repository
        self._test_server = configurations.get('ai_model_test').get('inference_server')

        with_retries(func=self._start_http_service, logger=self._logger)

    def _start_http_service(self):
        """
        Initializes the HTTP service by creating a connection to the server.
        """
        if self._test_server.get('url'):
            self._http_connector = HTTPConnector(f"{self._test_server.get('url')}:{self._test_server.get('port')}")

        self._logger.info(f"Connection successfully established.")

    def predict(self, message):
        self._logger.info(f"\n\nReceived message: {message}\n\n")
        result = self._energaize(copy.deepcopy(message))
        if self._http_connector:
            self._forwarder(result)
        self._save_data(message, result)
        
    def _energaize(self, message) -> dict:
        message['timestamp'] = message.get('timestamp').strftime("%Y-%m-%dT%H:%M:%SZ")

        self._logger.info(f"EnergAIze message: {message}")

        to_ai : dict = {
            "features" : message
        }

        result = {}
        if self._http_connector:
            result = self._http_connector.post(self._test_server.get('resources').get('inference'),to_ai)

            if result.status_code != 200:
                self._logger.error(f"HTTP request failed with status code: {result.status_code}")
            else:
                self._logger.debug(f"HTTP request succeeded with result: {result}")
                result = result.json().get('actions').get('0')

        return result

    def _forwarder(self, result):
        #self._logger.debug(f"Entities: {self._entities}\nResult: {result}")
        for entity in result:
            if entity in self._entities:
                entity_provider = self._entities[entity].get('provider')
                if entity_provider:
                    self._forwarders[entity_provider].to_forward(entity, result.get(entity), self._entities[entity])

    def _save_data(self, message, result):
        message['decisions'] = result

        self._time_series_repository.write(message)

        return 0