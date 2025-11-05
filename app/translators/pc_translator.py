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


    def translate(self, message: bytes) -> None:
        """
        Translates incoming entity messages into a standardized format.
        Handles both generic translation and label-specific logic.

        Args:
            message (bytes): Dictionary containing i-charging-format environment data, encoded as bytes.
        """

        # Generate a timestamp for when the message is being processed.
        timestamp: str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        message_dict: dict = json.loads(message.decode('utf-8'))
        print(f"PulseCharge message {message_dict}\n")
        vin = message_dict.get('vin')
        message = {}
        print(message_dict)
        for key, entity in self._entities.items():
            if entity.get('vin') == vin:
                value = {
                    "SoC" : message_dict.get('current_soc',None),
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