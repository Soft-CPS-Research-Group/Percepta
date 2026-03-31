import datetime
from app.translators.translator_rabbitmq_base import TranslatorRabbitMQBase
from app.utils.logger import LoggingUtils
from app.utils.labels import Label

# Strategy handlers: links JSON strategy names to their respective logic methods
_LABEL_STRATEGIES = {}

def register_label_strategy(name):
    """Decorator to register a label strategy method into the class mapping."""

    def decorator(func):
        _LABEL_STRATEGIES[name] = func
        return func

    return decorator


class SoftCPSTranslator(TranslatorRabbitMQBase):

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

    @register_label_strategy(Label.GRID_METER.value)
    def _grid_meter(self, entity_id: str, messages: dict) -> dict:
        if not isinstance(messages, dict):
            raise TypeError(f"Translator | grid_meter expected dict, got {type(messages)}")

        # Extract data nested under the entity ID (e.g., "GR01")
        entity_data = messages.get(entity_id, {})

        # Fallback to current time if timestamp is missing in the message
        raw_ts = messages.get('timestamp') or datetime.datetime.now().isoformat()
        timestamp = datetime.datetime.fromisoformat(raw_ts.replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M:%S %z")

        values = {}

        # List of expected fields to translate
        fields = [
            "energy_in_total", "energy_in_l1", "energy_in_l2", "energy_in_l3",
            "energy_out_total", "energy_out_l1", "energy_out_l2", "energy_out_l3"
        ]

        for field in fields:
            val = entity_data.get(field)
            # Only add to the dictionary if the value is not None (null)
            if val is not None:
                values[field] = [{"timestamp": timestamp, "value": val}]

        return values


    @register_label_strategy(Label.BATTERY.value)
    def _battery(self, entity_id : str, messages: dict) -> dict:
        # Ensure the input is a dictionary; raise an exception otherwise
        if not isinstance(messages, dict):
            raise TypeError(f"Translator | battery expected dict, got {type(messages)}")

        print(f"\n\nTESTE {messages}\n\n")
        timestamp = datetime.datetime.fromisoformat(messages.get('timestamp').replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M:%S %z")
        energy_in = messages.get('energy_charged_kwh_window')
        energy_out = messages.get('energy_discharged_kwh_window')
        soc = round(messages.get('soc_pct')/100,4)

        values = {"energy_in" : [{"timestamp": timestamp, "value": energy_in}], "energy_out" : [{"timestamp": timestamp, "value": energy_out}], "SoC" : [{"timestamp": timestamp, "value": soc}]}

        return values


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
        timestamp = datetime.datetime.now(self._tz).strftime("%Y-%m-%d %H:%M:%S")

        if not isinstance(messages, dict):
            raise TypeError(f"Translator | translate expected dict, got {type(messages)}")

        entity_id = messages.get("entity_id")
        label = messages.get("label")
        parameters_readings = messages.get("parameters")

        handler = _LABEL_STRATEGIES.get(label)

        value = {}

        if handler and parameters_readings:
            # Use label-specific translation method
            value = handler(self, entity_id, parameters_readings)

        # Construct standardized message with ID, values, and timestamp
        new_message = [{
            "id": entity_id,
            "value": value,
            "timestamp": timestamp
        }]

        # Send the message to the environment queue
        self.send_message_to_environment_queue(new_message)