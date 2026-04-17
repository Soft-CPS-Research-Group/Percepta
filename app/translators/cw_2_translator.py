import datetime
import json
from zoneinfo import ZoneInfo
from app.translators.translator_rabbitmq_base import TranslatorRabbitMQBase
from app.utils.logger import LoggingUtils


grid_meter_mapper = {
    "a_total_act_energy": "energy_in_l1",
    "a_total_act_ret_energy": "energy_out_l1",
    "b_total_act_energy": "energy_in_l2",
    "b_total_act_ret_energy": "energy_out_l2",
    "c_total_act_energy": "energy_in_l3",
    "c_total_act_ret_energy": "energy_out_l3",
    "total_act": "energy_in_total",
    "total_act_ret": "energy_out_total",
    "total_act_energy": "energy_in_total",
    "total_act_ret_energy": "energy_out_total"
}

# Strategy handlers: links JSON strategy names to their respective logic methods
_LABEL_STRATEGIES = {}

def register_label_strategy(name):
    """Decorator to register a label strategy method into the class mapping."""

    def decorator(func):
        _LABEL_STRATEGIES[name] = func
        return func

    return decorator

class CW2Translator(TranslatorRabbitMQBase):
    """
    Concrete implementation of a translator for CW entities.
    Handles translation of entity messages into a standardized format,
    applying special rules for specific labels such as EV chargers.
    """

    _entities: dict  # Stores entities defined in the environment specifications
    _labels_functions_mapper : dict # Maps labels to corresponding processing functions
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

    @register_label_strategy("grid_meter")
    def _grid_meter(self, entity_id, message, timestamp):

        topic = message.get('Topic')
        if "emdata" not in topic and "em1data" not in topic:
            return []

        self._logger.warning(f"TESTE {message}")

        payload = message.get('Message')
        current_data = json.loads(payload) if isinstance(payload, str) else payload
        if current_data.pop('id', None) == 1:
            return []

        # Initialize persistent storage for previous readings if not present
        # We use this to calculate the delta (difference) between cumulative readings
        if not hasattr(self, '_last_readings'):
            self._last_readings = {}  # Structure: {entity_id: {"data": dict, "ts": datetime}}

        # If no previous reading exists for this entity, store current and return
        # We need at least two points in time to calculate a delta
        if entity_id not in self._last_readings:
            self._last_readings[entity_id] = {"data": current_data, "ts": timestamp}
            self._logger.info(f"Last reading updated with the following data: {self._last_readings[entity_id]['data']}")
            return []

        last_entry = self._last_readings[entity_id]
        last_data = last_entry["data"]
        last_ts = last_entry["ts"]

        translated_output = {}

        # 2. Process each field based on the mapper
        for raw_key, current_val in current_data.items():
            if raw_key not in grid_meter_mapper:
                continue

            param_name = grid_meter_mapper[raw_key]

            # Retrieve entity specs and periodicity from environment configuration
            entity_specs = self._entities.get(entity_id, {}).get("parameters", {}).get(param_name, {})
            periodicity_cfg = entity_specs.get("temporal_behavior", {}).get("periodicity", {})

            if not periodicity_cfg:
                continue

            # Calculate how many periods have passed (e.g., how many minutes)
            delta_seconds = (timestamp - last_ts).total_seconds()
            period_minutes = periodicity_cfg.get("value", 1)
            num_periods = int(delta_seconds // (period_minutes * 60))

            # Only process if at least one full period has elapsed
            if num_periods < 1 and delta_seconds <= period_minutes * 60 - 10:
                continue
            else:
                num_periods = 1

                # 3. Calculate value difference and distribute across periods
            # If the key is missing in last_data, default to current_val (delta = 0)
            last_val = last_data.get(raw_key, current_val)
            total_delta = current_val - last_val
            value_per_period = total_delta / num_periods

            readings_list = []
            for i in range(1, num_periods + 1):
                # Calculate the timestamp for each intermediate interval
                reading_ts = last_ts + datetime.timedelta(minutes=i * period_minutes)

                readings_list.append({
                    "timestamp": reading_ts.strftime("%Y-%m-%d %H:%M:%S %z"),
                    "value": round(value_per_period/1000, 4) # os dados vêm em watts
                })

            translated_output[param_name] = readings_list

        # Update global state for the next iteration
        self._last_readings[entity_id] = {"data": current_data, "ts": timestamp}

        # Debug print of the final structure

        return [CW2Translator._message_creator(translated_output, entity_id, timestamp.strftime("%Y-%m-%d %H:%M:%S"))]

    def translate(self, messages : dict) -> None:
        """
        Translates incoming entity messages into a standardized format.
        Handles both generic translation and label-specific logic.

        Args:
        messages (dict): A dictionary containing the entity identifier,
                        the data type identifier, and the corresponding readings,
                        where keys represent parameters and values are lists of readings.
        """

        timestamp = datetime.datetime.now(self._tz)


        label = messages.get('label')

        handler = _LABEL_STRATEGIES.get(label)

        if not handler:
            self._logger.error(f"Unsupported strategy: '{label}'")
            raise ValueError(f"Mapping strategy '{label}' is not supported.")

        entity_id = messages.get('entity_id')
        message = messages.get('message')
        message = json.loads(message)

        # Use label-specific translation method
        message_list : list = handler(self, entity_id, message, timestamp)

        if message_list:
            print(json.dumps(message_list, indent=2))
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

        if "energy_in_l1" not in value.keys():
            value["energy_in_l1"] = value.get("energy_in_total", 0)

        if "energy_out_l1" not in value.keys():
            value["energy_out_l1"] = value.get("energy_out_total", 0)

        print(f"TESTE {value}")
        new_message: dict = {
            "id": entity_id,
            "value": value,
            "timestamp": timestamp
        }

        return new_message
