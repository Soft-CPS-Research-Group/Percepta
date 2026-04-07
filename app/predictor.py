import copy
from app.connectors.http_connector import HTTPConnector, HTTPErrorWrapper
from app.repositories.irepositories.time_series_repository import TimeSeriesRepository
from app.utils.logger import LoggingUtils
from app.utils.retry import with_retries
from app.rabbitMQ_publisher import RabbitMQPublisher

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

        self._predictor_config = configurations.get('predictor', {})

        with_retries(func=self._start_http_service, logger=self._logger)
        with_retries(func=self._setup_mq_publisher, logger=self._logger)

    def _setup_mq_publisher(self):
        """
        Prepara o publisher com o ID do ambiente injetado no nome da exchange.
        """
        try:
            # Deep copy para não estragar a config original
            mq_config = copy.deepcopy(self._predictor_config.get("publisher_settings", {}))

            # Dinamizar o nome da exchange: predictor_SaoMamede por exemplo
            topology = mq_config.get("topology", {})
            raw_name = topology.get("exchange_name", "")
            topology["exchange_name"] = raw_name.format(environment_id=self._environment)

            self._mq_publisher = RabbitMQPublisher(mq_config, self._logger)
            self._logger.info(f"Predictor MQ Publisher initialized for exchange: {topology['exchange_name']}")
        except Exception as e:
            self._logger.error(f"Failed to initialize Predictor MQ Publisher: {e}")
            self._mq_publisher = None

    def _start_http_service(self):
        """
        Initializes the HTTP service by creating a connection to the server.
        """
        if self._test_server.get('url'):
            self._http_connector = HTTPConnector(f"{self._test_server.get('url')}:{self._test_server.get('port')}")

        self._logger.info(f"Connection successfully established.")

    def predict(self, message):
        self._logger.info(f"\n\nReceived message: {message}\n\n")
        #result = self._energaize(copy.deepcopy(message))
        result = {
            "B01" : -5
        }
        if self._http_connector:
            self._forwarder(result)

        message['decisions'] = result

        self._save_data(message)

        if self._mq_publisher:
            message['timestamp'] = message.get('timestamp').strftime("%Y-%m-%dT%H:%M:%SZ")

            self._mq_publisher.send_async(message)
        
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
        for entity in result:
            if entity in self._entities:
                entity_provider = self._entities[entity].get('provider')
                if entity_provider:
                    try:
                        self._forwarders[entity_provider].to_forward(
                            entity,
                            result.get(entity),
                            self._entities[entity]
                        )
                    except Exception as e:
                        self._logger.error(f"Error forwarding entity {entity} for provider {entity_provider}: {e}")


    def _save_data(self, message):
        message = copy.deepcopy(message)

        self._time_series_repository.write(message)

        return 0