import datetime
from zoneinfo import ZoneInfo
from app.translators.translator_rabbitmq_base import TranslatorRabbitMQBase
from app.utils.logger import LoggingUtils

class EnergyPriceTranslator(TranslatorRabbitMQBase):
    """
    Concrete implementation of a translator for CW entities.
    Handles translation of entity messages into a standardized format,
    applying special rules for specific labels such as EV chargers.
    """

    _entities: dict  # Stores entities defined in the environment specifications
    _tz : ZoneInfo

    def __init__(self, environment: str,  environment_specs: dict, configurations: dict, logger: LoggingUtils):
        """
        Initializes the CWTranslator.

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

    def translate(self, messages : dict) -> None:
        """
        Translates incoming entity messages into a standardized format.
        Handles both generic translation and label-specific logic.

        Args:
        messages (dict): A dictionary containing the entity identifier,
                        the data type identifier, and the corresponding readings,
                        where keys represent parameters and values are lists of readings.
        """
        # Format timestamp using the configured timezone
        timestamp = datetime.datetime.now(self._tz).strftime("%Y-%m-%d %H:%M:%S %z")

        if not isinstance(messages, dict):
            raise TypeError(f"Translator | translate expected dict, got {type(messages)}")

        entity_id = messages.get("entity_id")

        if messages.get("value"): # TODO verificar se este é o melhor sítio para meter mas basicamente não existem dados
            value = self._build_values_array(messages.get("value"))

            # Construct standardized message with ID, values, and timestamp
            new_message = [{
                "id": entity_id,
                "value": {
                    "energy_price" : value
                },
                "timestamp": timestamp
            }]

            # Send the message to the environment queue
            self.send_message_to_environment_queue(new_message)


    @staticmethod
    def _build_values_array(prices_with_timestamp: dict) -> list:
        returned_dict = []

        for date in prices_with_timestamp.keys():
            returned_dict.append({"timestamp": date.strftime("%Y-%m-%d %H:%M:%S %z"), "value": round(prices_with_timestamp[date]/1000,8)})

        return returned_dict