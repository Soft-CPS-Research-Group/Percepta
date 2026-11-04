import datetime
import json
from app.core.adapters.telemetry_adapter_base import TelemetryAdapterBase
from app.core.output_handlers.output_handler_base import OutputHandlerBase
from app.core.utils.logger import LoggingUtils


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

# Translation strategies registered by entity label.
_LABEL_STRATEGIES = {}

def register_label_strategy(name):
    """Register a translation strategy for an entity label.

    Args:
        name: Entity label associated with the strategy.

    Returns:
        A decorator that registers the strategy function.
    """

    def decorator(func):
        _LABEL_STRATEGIES[name] = func
        return func

    return decorator

class CW2Translator(TelemetryAdapterBase):
    """
    Translate Cleanwatts v2 telemetry into canonical entity messages.
    """

    _entities: dict

    _LOG_PREFIX: str = "Cleanwatts 2 Translator |"

    def __init__(self, environment: str,  environment_specs: dict, configurations: dict, output_handler : OutputHandlerBase, logger: LoggingUtils):
        """
        Initialize the translator.

        Args:
            environment: Name of the target environment.
            environment_specs: Entity definitions for the environment.
            configurations: Application configuration values.
            output_handler: Handler used to dispatch translated messages.
            logger: Logger used for operational messages.
        """
        super().__init__(environment, configurations, output_handler, logger)

        self._entities = environment_specs.get('entities')

    @register_label_strategy("grid_meter")
    def _grid_meter(self, entity_id, message, current_datetime):

        topic = message.get('Topic')
        if "emdata" not in topic and "em1data" not in topic:
            return []

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
            self._last_readings[entity_id] = {"data": current_data, "ts": current_datetime}
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
            delta_seconds = (current_datetime - last_ts).total_seconds()
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
                    "value": round(value_per_period/1000, 4)  # Provider values are in W.
                })

            translated_output[param_name] = readings_list

        # Update global state for the next iteration
        self._last_readings[entity_id] = {"data": current_data, "ts": current_datetime}

        if "energy_in_l1" not in translated_output.keys():
            translated_output["energy_in_l1"] = translated_output.get("energy_in_total", 0)

        if "energy_out_l1" not in translated_output.keys():
            translated_output["energy_out_l1"] = translated_output.get("energy_out_total", 0)

        return translated_output

    async def translate(self, messages : dict) -> None:
        """
        Translates incoming entity messages into a standardized format.
        Handles label-specific translation strategies.

        Expected Input Data Format:
        ----------------------------------------
        A dictionary containing the entity metadata and the raw stringified MQTT payload:
        {
            "entity_id": "grid_meter_01",              # (str) Target entity unique identifier
            "label": "grid_meter",                     # (str) Strategy handler tag ('grid_meter')
            "message": "{\"Topic\":\"shellypro3em-0cb815fd4324/status/emdata:0\", \"Message\":\"{\\\"id\\\":0,\\\"a_total_act_energy\\\":16887505.30,\\\"a_total_act_ret_energy\\\":17083079.59,\\\"b_total_act_energy\\\":8373395.70,\\\"b_total_act_ret_energy\\\":0.00,\\\"c_total_act_energy\\\":7424821.62,\\\"c_total_act_ret_energy\\\":0.00,\\\"total_act\\\":32685722.61,\\\"total_act_ret\\\":17083079.59}\"}"
        }
        ------------------------------
        1. Cumulative Energy Topic (`.../status/emdata:0` or `.../status/em1data:0`) -> [PROCESSED]:
           {
               "Topic": "shellypro3em-0cb815fd4324/status/emdata:0",
               "Message": {
                   "id": 0,
                   "a_total_act_energy": 16887505.30,      # Phase A active energy (Wh) -> mapped to 'energy_in_l1'
                   "a_total_act_ret_energy": 17083079.59,  # Phase A returned energy (Wh) -> mapped to 'energy_out_l1'
                   "b_total_act_energy": 8373395.70,       # Phase B active energy (Wh) -> mapped to 'energy_in_l2'
                   "b_total_act_ret_energy": 0.00,         # Phase B returned energy (Wh) -> mapped to 'energy_out_l2'
                   "c_total_act_energy": 7424821.62,       # Phase C active energy (Wh) -> mapped to 'energy_in_l3'
                   "c_total_act_ret_energy": 0.00,         # Phase C returned energy (Wh) -> mapped to 'energy_out_l3'
                   "total_act": 32685722.61,               # Total active energy (Wh) -> mapped to 'energy_in_total'
                   "total_act_ret": 17083079.59            # Total returned energy (Wh) -> mapped to 'energy_out_total'
               }
           }

        2. Instantaneous Measurements Topic (`.../status/em:0`) -> [FILTERED OUT / IGNORED]:
           {
               "Topic": "shellypro3em-0cb815fd4324/status/em:0",
               "Message": {
                   "a_current": 2.141, "a_voltage": 238.4, "a_act_power": 121.7,
                   "b_current": 1.007, "b_voltage": 240.2, "b_act_power": 106.3,
                   "c_current": 0.937, "c_voltage": 237.5, "c_act_power": 45.5,
                   "total_act_power": 273.519
               }
           }

        Standardized Output Emitted to Output Handler:
        ----------------------------------------------
        [
            {
                "id": "grid_meter_01",
                "timestamp": "2026-08-17 12:05:43 +0100",  # Translation execution timestamp
                "value": {
                    "energy_in_total": [{"timestamp": "2026-08-17 12:05:43 +0100", "value": 0.0452}],  # Delta Wh converted to kWh
                    "energy_out_total": [{"timestamp": "2026-08-17 12:05:43 +0100", "value": 0.0}],
                    "energy_in_l1": [{"timestamp": "2026-08-17 12:05:43 +0100", "value": 0.0210}],
                    "energy_out_l1": [{"timestamp": "2026-08-17 12:05:43 +0100", "value": 0.0}],
                    "energy_in_l2": [{"timestamp": "2026-08-17 12:05:43 +0100", "value": 0.0142}],
                    "energy_in_l3": [{"timestamp": "2026-08-17 12:05:43 +0100", "value": 0.0100}]
                }
            }
        ]
        """

        # Generate translation timestamp localized to the configured timezone
        timestamp = datetime.datetime.now(self._tz).strftime("%Y-%m-%d %H:%M:%S %z")

        # --- 1. Type Safety Gate: Ensure incoming payload is a valid dictionary ---
        if not isinstance(messages, dict):
            raise TypeError(f"{self._LOG_PREFIX} | translate expected dict, got {type(messages)}")

        # --- 2. Ingress Unpacking: Extract target identifiers, label strategies, and readings ---
        entity_id = messages.get("entity_id")
        label = messages.get("label")

        # --- 3. Translation Strategy Dispatching ---
        if label in _LABEL_STRATEGIES:
            # Delegate to specialized registered label strategy (e.g., 'grid_meter')
            strategy_func = _LABEL_STRATEGIES[label]
            message = messages.get('message')
            message = json.loads(message)

            value: list = strategy_func(self, entity_id, message, timestamp)

            # --- 4. Canonical Outbound Framing ---
            # Wrap translated parameters into standard Percepta Manager ingress format
            new_message = [{
                "id": entity_id,
                "value": value,
                "timestamp": timestamp
            }]

            await self._output_handler.send(new_message) # TODO FAZER VERIFICAÇÃO PARA VER SE AINDA ESTÁ ATIVO ANTES DE USAR
