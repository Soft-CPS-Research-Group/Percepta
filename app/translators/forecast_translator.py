import datetime
import json
from zoneinfo import ZoneInfo
from app.translators.translator_rabbitmq_base import TranslatorRabbitMQBase
from app.utils.logger import LoggingUtils
from app.utils.labels import Label

class ForecastTranslator(TranslatorRabbitMQBase):
    """
    Concrete implementation of a translator for IC entities.

    Handles translation of entity messages into a standardized format,
    applying special rules for specific labels such as EV chargers.
    """

    _entities: dict  # Stores entities defined in the environment specifications
    _labels_functions_mapper: dict  # Maps labels to corresponding processing functions

    def __init__(self, environment: str, environment_specs: dict, configurations: dict, logger: LoggingUtils) -> None:
        """
        Initializes the ICTranslator.

        Args:
            environment (str): String to identify the environment which the data belongs.
            environment_specs (dict): Environment specifications including entities.
            configurations (dict): General configurations passed to the translator.
            logger (LoggingUtils): Logger instance for structured logging.
        """
        super().__init__(environment, configurations, logger)

        self._entities = environment_specs.get('entities')

        self._tz = self._set_time_zone()

        self._labels_functions_mapper = {
            "consumption": self._consumption_forecast_service,
            "production": self._production_forecast_service
        }

        # TODO meter isto num ficheiro para reutilizar pois também é usado por pelo menos um tradutor

    def _set_time_zone(self) -> ZoneInfo:
        # Get the current timestamp in UTC without microseconds
        tz_name = self._configurations.get("timezone", "UTC")
        try:
            return ZoneInfo(tz_name)
        except Exception:
            self._logger.warning(f"Invalid timezone '{tz_name}', falling back to UTC")
            return ZoneInfo("UTC")


    def _consumption_forecast_service(self, timestamp: str, data) -> list:

        for entity_id, entity_data in self._entities.items():
            if entity_data.get("label") == Label.CONSUMPTION_FORECAST_SERVICE.value:
                _entity_parameters: dict = entity_data.get('parameters')

                # Create and return a message using pv_production data and timestamp
                value: dict = {
                    "consumption_total":  data
                }

                self._parameters_validation(value, _entity_parameters)

                return [ForecastTranslator._message_creator(value, entity_id, timestamp)]

    def _production_forecast_service(self, timestamp: str, data) -> list:

        for entity_id, entity_data in self._entities.items():
            if entity_data.get("label") == Label.PRODUCTION_FORECAST_SERVICE.value:
                _entity_parameters: dict = entity_data.get('parameters')

                # Create and return a message using pv_production data and timestamp
                value: dict = {
                    "production_total": data
                }

                self._parameters_validation(value, _entity_parameters)

                return [ForecastTranslator._message_creator(value, entity_id, timestamp)]


    def prepare_data_ignore_micros(self, start_time, values_list, delta_minutes=15):

        formatted_dict = {}

        for i, value in enumerate(values_list):
            current_dt = start_time + datetime.timedelta(minutes=i * delta_minutes)
            formatted_dict[current_dt] = value

        return formatted_dict


    def translate(self, message: bytes) -> None:
        """
        Translates incoming entity messages into a standardized format.
        Handles both generic translation and label-specific logic.

        Args:
            message (bytes): Dictionary containing i-charging-format environment data, encoded as bytes.
        """
        print("TRANSLATE CHAMADO\n")
        # Decode the incoming bytes message to a UTF-8 string, then parse it as JSON.
        # Extract the 'observation' key which contains the relevant data.
        message_dict: dict = json.loads(message.decode('utf-8'))

        # Generate a timestamp for when the message is being processed.
        timestamp: datetime.datetime = datetime.datetime.fromisoformat(message_dict.pop("target_time")).astimezone(ZoneInfo("UTC")).replace(microsecond=0)

        message_dict.pop("house_id")

        # Initialize an empty list that will hold the translated messages.
        message_list: list = []
        self._logger.info(f"MESSAGES DICT {message_dict}")
        # Iterate through each attribute in the parsed message dictionary.

        for key, value in message_dict.items():
            formatted_list: list = ForecastTranslator._build_values_array(self.prepare_data_ignore_micros(timestamp, value))
            handler = self._labels_functions_mapper[key]
            value = handler(timestamp.strftime("%Y-%m-%d %H:%M:%S %z"), formatted_list)

            message_list.extend(value)

        self._logger.info(f"MESSAGES LIST FORECAST {message_list}")
        # Send the final standardized message list to the environment queue.
        self.send_message_to_environment_queue(message_list)

    @staticmethod
    def _message_creator(value: dict, entity_id: str, timestamp: str) -> dict:
        """
        Creates a standardized message format for an entity reading.

        Args:
            value (dict): Dictionary containing entity-specific values.
            entity_id (str): Unique entity identifier.
            timestamp (str): Current timestamp for the message.

        Returns:
            dict: Standardized message dictionary.
        """
        new_message: dict = {
            "id": entity_id,
            "value": value,
            "timestamp": timestamp
        }

        return new_message

    @staticmethod
    def _build_values_array(prices_with_timestamp: dict) -> list:
        returned_dict = []

        for date in prices_with_timestamp.keys():
            returned_dict.append({"timestamp": date.strftime("%Y-%m-%d %H:%M:%S %z"),
                                  "value": prices_with_timestamp[date]})

        return returned_dict

# Em caso de erro corro o risco da mensagem ser enviada duas vezes, mas não é um problema
