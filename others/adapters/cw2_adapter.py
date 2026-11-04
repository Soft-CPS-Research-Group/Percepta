# --- Standard Library Imports ---
import datetime
import json
from typing import Any, Dict, List, Optional, Union

# --- Internal Framework Imports ---
from app.core.adapters.telemetry_adapter_base import TelemetryAdapterBase
from app.core.utils.logger import LoggingUtils

# Mapping of raw Shelly meter keys to canonical entity parameter identifiers.
GRID_METER_MAPPER = {
    "a_total_act_energy": "energy_in_l1",
    "a_total_act_ret_energy": "energy_out_l1",
    "b_total_act_energy": "energy_in_l2",
    "b_total_act_ret_energy": "energy_out_l2",
    "c_total_act_energy": "energy_in_l3",
    "c_total_act_ret_energy": "energy_out_l3",
    "total_act": "energy_in_total",
    "total_act_ret": "energy_out_total",
    "total_act_energy": "energy_in_total",
    "total_act_ret_energy": "energy_out_total",
    "act_power": "energy_in_total",
    "voltage": "energy_in_l1"
}

_LABEL_STRATEGIES = {}


def register_label_strategy(name: str):
    """Register an adaptation strategy for an entity classification label."""
    def decorator(func):
        _LABEL_STRATEGIES[name] = func
        return func

    return decorator


class CW2TelemetryAdapter(TelemetryAdapterBase):
    """Pure domain adapter for Cleanwatts v2 (Shelly MQTT/AMQP) telemetry normalization.

    Converts raw Shelly energy meter telemetry into standardized canonical entity
    messages and calculates cumulative energy delta steps across sampling periods.
    """

    _entities: dict
    _LOG_PREFIX: str = "CW2Adapter |"

    def __init__(
        self,
        environment: str,
        environment_specs: dict,
        source_mapping_rules: dict,
        logger: LoggingUtils,
        timezone: str = "UTC"
    ) -> None:
        """Initialize the Cleanwatts v2 domain adapter.

        Expected `environment_specs` Data Structure:
        -------------------------------------------
        {
            "group": "living lab",                  # (str, optional) Environmental cluster tag
            "entities": {                           # (dict) Map of entity unique identifiers
                "GR02": {                           # (str) Entity ID
                    "label": "grid_meter",          # (str) Domain classification label
                    "parameters": {
                        "energy_in_l1": {
                            "measurementUnit": "kWh",
                            "temporal_behavior": {
                                "cumulative": true,
                                "periodicity": {"value": 1, "unit": "min"},
                                "aggregation_operation": "sum",
                                "fill_operation": "linear"
                            },
                            "label": "energy_in_l1"
                        },
                        "energy_in_total": {
                            "measurementUnit": "kWh",
                            "temporal_behavior": {
                                "cumulative": true,
                                "periodicity": {"value": 1, "unit": "min"},
                                "aggregation_operation": "sum",
                                "fill_operation": "linear"
                            },
                            "label": "energy_in_total"
                        }
                    }
                }
            }
        }

        Expected `source_mapping_rules` Data Structure:
        --------------------------------
        {
            # Maps internal entity ID directly to remote Shelly AMQP Exchange
            "GR02": "Shelly-shellyproem50-08f9e0e6a5a4"
        }

        Args:
            environment (str): Name of the target environment (e.g., "R-H-02").
            environment_specs (dict): Entity schema specifications for the environment.
            source_mapping_rules (dict): Mapping rules between entity ID and remote exchange name.
            timezone (str): Target execution timezone. Defaults to "UTC".
            logger (LoggingUtils): Structured telemetry logging interface.

        Raises:
            ValueError: If mandatory parameters are missing or empty.
            TypeError: If input arguments violate expected types.
        """
        super().__init__(environment, timezone, logger)

        # --- Guard Clause 1: Validate environment_specs ---
        if not isinstance(environment_specs, dict) or "entities" not in environment_specs:
            error_msg = (
                f"{self._LOG_PREFIX} Initialization error: 'environment_specs' must be a dictionary "
                f"containing an 'entities' key for environment '{environment}'."
            )
            self._logger.error(error_msg)
            raise ValueError(error_msg)

        entities_dict = environment_specs.get("entities")
        if not isinstance(entities_dict, dict) or not entities_dict:
            error_msg = (
                f"{self._LOG_PREFIX} Initialization error: 'entities' specification cannot be empty "
                f"for environment '{environment}'."
            )
            self._logger.error(error_msg)
            raise ValueError(error_msg)

        # --- Guard Clause 2: Validate rules dictionary ---
        if not isinstance(source_mapping_rules, dict):
            error_msg = (
                f"{self._LOG_PREFIX} Initialization error: 'rules' must be a dictionary, "
                f"got {type(source_mapping_rules).__name__}."
            )
            self._logger.error(error_msg)
            raise TypeError(error_msg)

        self._entities = entities_dict
        self._rules = source_mapping_rules
        self._last_readings: Dict[str, Dict[str, Any]] = {}

        # Reverse lookup mapping: remote exchange/device descriptor -> entity_id
        self._source_to_entity: Dict[str, str] = {}
        for entity_id, target_exchange in source_mapping_rules.items():
            clean_name = str(target_exchange).strip("/").strip()
            self._source_to_entity[clean_name.lower()] = entity_id
            if clean_name.startswith("Shelly-"):
                self._source_to_entity[clean_name.replace("Shelly-", "").lower()] = entity_id

    def adapt(self, message: Union[dict, bytes, str]) -> List[Dict[str, Any]]:
        """Transforms incoming Cleanwatts v2 telemetry envelope into canonical entity messages.

        Expected Input Data Structure (`message`):
        ------------------------------------------
        {
            "protocol": "AMQP",
            "source": "Shelly-shellyproem50-08f9e0e6a5a4/",
            "status": null,
            "content_type": "application/octet-stream",
            "payload": b'{"Topic":"shellyproem50-08f9e0e6a5a4/status/em1:1","Message":"{\\"id\\":1,\\"voltage\\":239.6,\\"current\\":0.020,\\"act_power\\":0.0,\\"aprt_power\\":4.9,\\"pf\\":0.00,\\"freq\\":50.0,\\"calibration\\":\\"factory\\"}"}',
            "properties": {}
        }

        Expected Output Structure:
        --------------------------
        [
            {
                "id": "GR02",
                "value": {
                    "energy_in_total": [{"timestamp": "2026-08-19 16:15:00 +0100", "value": 0.0452}],
                    "energy_out_total": [{"timestamp": "2026-08-19 16:15:00 +0100", "value": 0.0}],
                    "energy_in_l1": [{"timestamp": "2026-08-19 16:15:00 +0100", "value": 0.0210}],
                    "energy_out_l1": [{"timestamp": "2026-08-19 16:15:00 +0100", "value": 0.0}],
                    "energy_in_l2": [{"timestamp": "2026-08-19 16:15:00 +0100", "value": 0.0142}],
                    "energy_in_l3": [{"timestamp": "2026-08-19 16:15:00 +0100", "value": 0.0100}]
                },
                "timestamp": "2026-08-19 16:15:31 +0100"
            }
        ]

        Args:
            message (Union[dict, bytes, str]): Raw inbound AMQP transport envelope.

        Returns:
            List[Dict[str, Any]]: Standardized canonical entity message dictionaries.
        """
        now_dt = datetime.datetime.now(self._tz)
        formatted_ts = now_dt.strftime("%Y-%m-%d %H:%M:%S %z")

        if not message:
            self._logger.debug(f"{self._LOG_PREFIX} Skipping empty message payload.")
            return []

        # 1. Extract entity, label, and raw Shelly payload from envelope
        normalized = self._normalize_ingress_payload(message)
        if not normalized:
            return []

        entity_id = normalized["entity_id"]
        label = normalized["label"]
        inner_message = normalized["message"]

        entity_spec = self._entities.get(entity_id, {})
        entity_parameters = entity_spec.get("parameters", {})

        # 2. Strategy dispatch based on entity classification label
        if label in _LABEL_STRATEGIES:
            strategy_func = _LABEL_STRATEGIES[label]
            value = strategy_func(self, entity_id, inner_message, now_dt)
            if not value:
                return []

            TelemetryAdapterBase.parameters_validation(value, entity_parameters, [])

            canonical_message = self._message_creator(value=value, entity_id=entity_id, timestamp=formatted_ts)
            return [canonical_message]

        self._logger.warning(f"{self._LOG_PREFIX} No adaptation strategy registered for label '{label}'.")
        return []

    def _normalize_ingress_payload(self, raw_message: Union[dict, bytes, str]) -> Optional[Dict[str, Any]]:
        """Extracts entity identifier, label, and decoded Shelly dictionary from the AMQP transport envelope."""
        message: Any = raw_message
        if isinstance(raw_message, (bytes, bytearray, str)):
            try:
                message = json.loads(raw_message if isinstance(raw_message, str) else raw_message.decode("utf-8"))
            except Exception as err:
                self._logger.error(f"{self._LOG_PREFIX} Failed to decode raw JSON envelope: {err}")
                return None

        if not isinstance(message, dict):
            return None

        # 1. Resolve source exchange identifier from envelope
        raw_source = message.get("source")
        clean_source = str(raw_source).strip("/").strip().lower()

        # 2. Decode inner payload bytes / string into dict
        raw_payload = message.get("payload")
        if isinstance(raw_payload, (bytes, bytearray, str)):
            try:
                raw_payload = json.loads(raw_payload if isinstance(raw_payload, str) else raw_payload.decode("utf-8"))
            except Exception as err:
                self._logger.error(f"{self._LOG_PREFIX} Failed to decode envelope payload: {err}")
                return None

        if not isinstance(raw_payload, dict):
            return None

        # 3. Resolve Entity ID: Check source match -> Topic prefix -> Single entity fallback
        entity_id = self._source_to_entity.get(clean_source)

        if not entity_id:
            self._logger.debug(f"{self._LOG_PREFIX} Could not map source '{raw_source}' to any known entity.")
            return None

        entity_label = self._entities.get(entity_id, {}).get("label", "grid_meter")
        return {
            "entity_id": entity_id,
            "label": entity_label,
            "message": raw_payload
        }

    @register_label_strategy("grid_meter")
    def _grid_meter(self, entity_id: str, message: dict, current_datetime: datetime.datetime) -> dict:
        topic = message.get('Topic')
        if "emdata" not in topic and "em1data" not in topic:
            return {}

        payload = message.get('Message')

        current_data = json.loads(payload) if isinstance(payload, str) else payload
        if current_data.pop('id', None) == 1:
            return {}

        # Initialize persistent storage for previous readings if not present
        # We use this to calculate the delta (difference) between cumulative readings
        if not hasattr(self, '_last_readings'):
            self._last_readings = {}  # Structure: {entity_id: {"data": dict, "ts": datetime}}

        # If no previous reading exists for this entity, store current and return
        # We need at least two points in time to calculate a delta
        if entity_id not in self._last_readings:
            self._last_readings[entity_id] = {"data": current_data, "ts": current_datetime}
            self._logger.info(f"Last reading updated with the following data: {self._last_readings[entity_id]['data']}")
            return {}

        last_entry = self._last_readings[entity_id]
        last_data = last_entry["data"]
        last_ts = last_entry["ts"]

        translated_output = {}

        # 2. Process each field based on the mapper
        for raw_key, current_val in current_data.items():
            if raw_key not in GRID_METER_MAPPER:
                continue

            param_name = GRID_METER_MAPPER[raw_key]

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
