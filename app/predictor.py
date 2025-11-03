from app.connectors.http_conector import HTTPConnector, HTTPErrorWrapper
from app.repositories.irepositories.time_series_repository import TimeSeriesRepository
from app.utils.logger import LoggingUtils
from app.utils.retry import with_retries


class Predictor:
    _http_connector: HTTPConnector # HTTP connector instance for performing HTTP requests

    def __init__(self, environment : str, environment_specs : dict, time_series_repository : TimeSeriesRepository, forwarders : dict, configurations : dict, logger : LoggingUtils):
        self._entities = environment_specs["entities"]
        self._group = environment_specs["group"]
        self._environment = environment
        self._forwarders = forwarders
        self._logger = logger
        self._time_series_repository = time_series_repository
        self._test_server = configurations.get('ai_model_test').get('inference_server')

        #with_retries(func=self._start_http_service, logger=self._logger)

    def _start_http_service(self):
        """
        Initializes the HTTP service by creating a connection to the server.
        """
        url = ''
        # Attempt to create a new HTTPConnector instance
        try:
            url = f"http://{self._environment}:8002"
            self._http_connector = HTTPConnector(url)
        except Exception as e:
            self._logger.warning(f"Failed to initialize HTTP service: {url}. Will try the default HTTP service: {self._test_server.get('url')}.")
            self._http_connector = HTTPConnector(f"{self._test_server.get('url')}:{self._test_server.get('port')}")

        self._logger.info(f"Connection successfully established.")

    def predict(self, message):
        self._logger.info(f"Received message: {message}")
        result = 0
        self._energaize_simulator(message)
        self._forwarder(result)
        self._save_data(message, result)
        
    def _energaize_simulator(self, message):
        pass

    def _forwarder(self,result):
        pass

    def _save_data(self,message,result):
        self._time_series_repository.write(message)

        return 0