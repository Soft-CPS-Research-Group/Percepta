import json
import datetime
from app.translators.translator_rabbitmq_base import TranslatorRabbitMQBase
from app.utils.logger import LoggingUtils

class PCTranslator(TranslatorRabbitMQBase):
    """
    Concrete implementation of a translator for IC entities.

    Handles translation of entity messages into a standardized format,
    applying special rules for specific labels such as EV chargers.
    """

    _entities: dict  # Stores entities defined in the environment specifications
    _soc_cache: dict

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

        self._soc_cache = {}

    def translate(self, message: bytes) -> None:
        """
        Translates incoming entity messages into a standardized format.
        Handles both generic translation and label-specific logic.

        Args:
            message (bytes): Dictionary containing i-charging-format environment data, encoded as bytes.
        """

        # Generate a timestamp for when the message is being processed.
        timestamp: str = datetime.datetime.now(self._tz).strftime("%Y-%m-%d %H:%M:%S %z")

        try:
            message_dict: dict = json.loads(message.decode('utf-8'))
        except Exception as e:
            self._logger.error(f"Failed to decode JSON message: {e}")
            return

        self._logger.info(f"PulseCharge message {message_dict}\n")

        user_id = message_dict.get('user_id')
        if not user_id:
            self._logger.warning("Message received without user_id. Skipping translation.")
            return

        current_soc = message_dict.get('current_soc')

        if current_soc is not None:
            # Update cache with the new SoC value
            self._soc_cache[user_id] = current_soc
        else:
            # Attempt to retrieve the last known SoC from cache
            current_soc = self._soc_cache.get(user_id)
            self._logger.debug(f"Missing SoC in message. Using cached value for user {user_id}: {current_soc}")

        message = {}
        print(message_dict)
        for key, entity in self._entities.items():
            if entity.get('user_id') == user_id:
                value = {
                    "SoC" : current_soc,
                    "flexibility" : {
                        "estimated_soc_at_arrival" : message_dict.get('estimated_soc_at_arrival',None),
                        "estimated_soc_at_departure" : message_dict.get('estimated_soc_at_departure',None),
                        "estimated_time_at_arrival" : message_dict.get('estimated_time_at_arrival',""),
                        "estimated_time_at_departure" : message_dict.get('estimated_time_at_departure',""),
                        "charger" : message_dict.get('charger',''),
                        "mode" : message_dict.get('mode','')
                    }
                }
                message = PCTranslator._message_creator(value, key, timestamp)
                # Send the final standardized message list to the environment queue.
                self.send_message_to_environment_queue([message])
                break



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