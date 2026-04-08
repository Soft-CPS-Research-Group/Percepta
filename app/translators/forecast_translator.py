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

        # TODO meter isto num ficheiro para reutilizar pois também é usado por pelo menos um tradutor

    def _set_time_zone(self) -> ZoneInfo:
        # Get the current timestamp in UTC without microseconds
        tz_name = self._configurations.get("timezone", "UTC")
        try:
            return ZoneInfo(tz_name)
        except Exception:
            self._logger.warning(f"Invalid timezone '{tz_name}', falling back to UTC")
            return ZoneInfo("UTC")

    def prepare_data_ignore_micros(self, start_iso_time, values_list, delta_minutes=15):
        dt = datetime.datetime.fromisoformat(start_iso_time)

        start_dt_utc = dt.astimezone(ZoneInfo("UTC")).replace(microsecond=0)

        formatted_dict = {}

        for i, value in enumerate(values_list):
            current_dt = start_dt_utc + datetime.timedelta(minutes=i * delta_minutes)
            formatted_dict[current_dt] = value

        return formatted_dict

    def translate(self, message: bytes) -> None:
        """
        Translates incoming entity messages into a standardized format.
        Handles both generic translation and label-specific logic.

        Args:
            message (bytes): Dictionary containing i-charging-format environment data, encoded as bytes.
        """
        #print("TRANSLATE CHAMADO\n")
        # Decode the incoming bytes message to a UTF-8 string, then parse it as JSON.
        # Extract the 'observation' key which contains the relevant data.
        message_dict: dict = json.loads(message.decode('utf-8'))

        # Generate a timestamp for when the message is being processed.
        timestamp: datetime.datetime = datetime.datetime.fromisoformat(message_dict.pop("target_time")).astimezone(ZoneInfo("UTC")).replace(microsecond=0)

        # Initialize an empty list that will hold the translated messages.
        message_list: list = []
        self._logger.info(f"MESSAGES DICT {message_dict}")
        # Iterate through each attribute in the parsed message dictionary.



        #self._logger.info(f"MESSAGES LIST {message_list}")

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

# Em caso de erro corro o risco da mensagem ser enviada duas vezes, mas não é um problema
