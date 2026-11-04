# --- Standard Library Imports ---
import datetime
import json
from typing import Any, Dict, List, Optional

# --- Internal Framework Imports ---
from app.core.adapters.telemetry_adapter_base import TelemetryAdapterBase
from app.core.utils.logger import LoggingUtils
from app.extension.utils.labels import PARAMETER_TYPE_EXPECTED_TYPES, Label

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S %z"

# How many kWh one unit of each energy measurement unit is worth. Readings are
# always reported in kWh, so each parameter's 'measurement_unit' must be one of these.
ENERGY_UNIT_TO_KWH_FACTOR: Dict[str, float] = {
    "wh": 0.001,
    "kwh": 1.0,
    "mwh": 1000.0,
}

# Number of decimal places kept after converting to kWh, to avoid floating point noise.
KWH_DECIMAL_PLACES = 6

# A lifetime reading older than this (in seconds) is considered too old to
# compute a meaningful delta against.
MAX_LIFETIME_READING_AGE_SECONDS = 120

_LABEL_STRATEGIES = {}


def register_label_strategy(name: str):
    """Registers an adaptation strategy for an entity type id.

    Args:
        name (str): Entity type id the strategy handles.

    Returns:
        Callable: A decorator that registers the strategy function.
    """
    def decorator(func):
        _LABEL_STRATEGIES[name] = func
        return func

    return decorator


class CW2TelemetryAdapter(TelemetryAdapterBase):
    """Adapter for Cleanwatts v2 (Shelly MQTT/AMQP) telemetry.

    Converts raw Shelly energy meter data into standardized canonical entity
    messages, with energy always reported in kWh. For parameters that report a
    running lifetime total instead of a per-interval reading, it computes the
    per-period delta.
    """

    _entities: dict
    _LOG_PREFIX: str = "CW2 Telemetry Adapter |"

    def __init__(
        self,
        environment: str,
        environment_specs: dict,
        source_mapping_rules: dict,
        logger: LoggingUtils,
        timezone: str = "UTC"
    ) -> None:
        """Initializes the Cleanwatts v2 adapter.

        Expected `environment_specs` Data Structure:
        -------------------------------------------
        {
            "group": "living lab",
            "entities": {
                "GR02": {
                    "entity_type_id": "grid_meter",
                    "parameters": {
                        "32df": {
                            "measurement_unit": "kWh",
                            "temporal_behavior": {
                                "cumulative": true,
                                "periodicity": {"value": 1, "unit": "min"},
                                "aggregation_operation": "sum",
                                "fill_operation": "linear"
                            },
                            "parameter_type_id": "energy_in_l1"
                        },
                        "67fgh": {
                            "measurement_unit": "Wh",
                            "temporal_behavior": {
                                "cumulative": true,
                                "periodicity": {"value": 1, "unit": "min"},
                                "aggregation_operation": "sum",
                                "fill_operation": "linear"
                            },
                            "parameter_type_id": "energy_in_total"
                        }
                    }
                }
            }
        }

        Expected `source_mapping_rules` Data Structure:
        ------------------------------------------------
        {
            "32df": {
                "fetch_key": "Shelly-shellyproem50-08f9e0e6a5a4",   # AMQP exchange the entity's messages come from
                "match_key": "a_total_act_energy",                  # Key of the value in the Shelly message
                "topic": "/status/emdata:0",                        # Topic the Shelly message is published on
                "shelly_accumulation": "lifetime"                   # "lifetime" (running total) or "interval"
            },
            "67fgh": {
                "fetch_key": "Shelly-shellyproem50-08f9e0e6a5a4",
                "match_key": "total_act_energy",
                "topic": "/status/emdata:0",
                "shelly_accumulation": "interval"
            }
        }

        Args:
            environment (str): Name of the target environment (e.g., "R-H-02").
            environment_specs (dict): Entity schema for the environment.
            source_mapping_rules (dict): For each parameter id, its keys and Shelly settings.
            logger (LoggingUtils): Logging interface.
            timezone (str): Timezone used to timestamp readings. Defaults to "UTC".

        Raises:
            ValueError: If mandatory parameters are missing or empty.
            TypeError: If input arguments have the wrong type.
        """
        super().__init__(environment, timezone, logger)

        # --- Guard Clause 1: environment_specs must have an 'entities' key ---
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

        # --- Guard Clause 2: source_mapping_rules must be a dict ---
        if not isinstance(source_mapping_rules, dict):
            error_msg = (
                f"{self._LOG_PREFIX} Initialization error: 'source_mapping_rules' must be a dictionary, "
                f"got {type(source_mapping_rules).__name__}."
            )
            self._logger.error(error_msg)
            raise TypeError(error_msg)

        self._entities = entities_dict
        self._source_mapping_rules = source_mapping_rules

        # For each parameter, the last raw lifetime value reported by the
        # Shelly device (in the unit it reports in) and when it was recorded.
        # Used to compute the per-period delta for parameters whose
        # 'shelly_accumulation' (in the source mapping) is "lifetime".
        # Assumes parameter ids are unique across all entities.
        # Expected `_last_parameter_values` Data Structure:
        # --------------------------------------------------
        # {
        #     "32df": {"value": 4961.5, "ts": datetime(...)}
        # }
        self._last_parameter_values: Dict[str, dict] = {}

        # Reverse lookup: remote exchange name -> entity_id
        # Expected `_source_to_entity` Data Structure:
        # --------------------------------------------
        # {
        #     "shelly-shellyproem50-08f9e0e6a5a4": "GR02"
        # }
        self._source_to_entity: Dict[str, str] = {}
        for param_id, rule in source_mapping_rules.items():
            for entity_id, entity_spec in self._entities.items():
                if param_id in entity_spec.get("parameters", {}):
                    exchange = str(rule["fetch_key"]).strip("/").strip().lower()
                    self._source_to_entity[exchange] = entity_id
                    break

    def adapt(self, message: dict) -> List[Dict[str, Any]]:
        """Converts an incoming Cleanwatts v2 message into canonical entity messages.

        Energy readings are always reported in kWh, converted from each
        parameter's configured 'measurement_unit'.

        Expected Input Data Structure (`message`):
        ------------------------------------------
        {
            "protocol": "AMQP",
            "source": "Shelly-shellyproem50-08f9e0e6a5a4/",
            "status": null,
            "content_type": "application/octet-stream",
            "metadata": {},
            "payload": b'{"Topic":"shellyproem50-08f9e0e6a5a4/status/emdata:0","Message":"{\\"id\\":0,\\"a_total_act_energy\\":4961.521,\\"total_act_energy\\":45.2}"}'
        }

        Expected Output Structure (using the example configuration, with timezone "UTC",
        and a previous stored value of 4961.5 for parameter "32df"):
        ------------------------------------------------------------
        [
            {
                "id": "GR02",
                "entity_type_id": "grid_meter",
                "parameters": {
                    "67fgh": {
                        "parameter_type_id": "energy_in_total",
                        "values": [{"timestamp": "2026-08-19 16:15:31 +0000", "value": 0.0452}]
                    },
                    "32df": {
                        "parameter_type_id": "energy_in_l1",
                        "values": [{"timestamp": "2026-08-19 16:15:31 +0000", "value": 0.021}]
                    }
                },
                "timestamp": "2026-08-19 16:15:31 +0000"
            }
        ]

        Args:
            message (dict): Raw inbound AMQP message.

        Returns:
            List[Dict[str, Any]]: Standardized canonical entity messages.
        """
        if not message:
            self._logger.info(f"{self._LOG_PREFIX} Skipping empty message payload.")
            return []

        now_dt = datetime.datetime.now(self._tz)
        timestamp = now_dt.strftime(TIMESTAMP_FORMAT)

        # 1. Extract entity and parameter info from the raw message
        canonical_message = self._normalize_ingress_payload(message)
        if not canonical_message:
            return []

        entity_id = canonical_message["id"]
        entity_type_id = canonical_message["entity_type_id"]
        parameters_ids = canonical_message["parameters_ids"]
        raw_payload = canonical_message["raw_payload"]

        if not entity_type_id:
            self._logger.warning(
                f"{self._LOG_PREFIX} Entity '{entity_id}' has no 'entity_type_id' configured."
            )
            return []

        # 2. Pick the strategy for this entity type
        if entity_type_id not in _LABEL_STRATEGIES:
            self._logger.warning(
                f"{self._LOG_PREFIX} No adaptation strategy registered for entity type '{entity_type_id}'."
            )
            return []

        strategy_func = _LABEL_STRATEGIES[entity_type_id]

        entity_parameters = self._entities.get(entity_id, {}).get("parameters", {})

        adapted_parameters = strategy_func(
            self, entity_id, entity_parameters, parameters_ids, raw_payload, now_dt, timestamp
        )
        if not adapted_parameters:
            self._logger.debug(f"{self._LOG_PREFIX} No usable readings for entity '{entity_id}' in this message.")
            return []

        # 3. Validate the received parameters' readings
        adapted_parameters = self.parameters_validation(
            adapted_parameters, entity_parameters, PARAMETER_TYPE_EXPECTED_TYPES
        )

        return [self._message_creator(adapted_parameters, entity_id, entity_type_id, timestamp)]

    def _normalize_ingress_payload(self, message: dict) -> Optional[Dict[str, Any]]:
        """Finds the entity and its matching parameters for an incoming message, and
        decodes the Shelly message inside it.

        Expected Input Structure (`message`):
        -------------------------------------
        {
            "protocol": "AMQP",
            "source": "Shelly-shellyproem50-08f9e0e6a5a4/",
            "status": null,
            "content_type": "application/octet-stream",
            "metadata": {},
            "payload": b'{"Topic":"shellyproem50-08f9e0e6a5a4/status/emdata:0","Message":"{\\"id\\":0,\\"total_act_energy\\":4961488.92,\\"total_act_ret_energy\\":0.0}"}'
        }

        Expected Output Structure:
        --------------------------
        {
            "id": "GR02",
            "entity_type_id": "grid_meter",
            "parameters_ids": ["32df", "67fgh"],
            "raw_payload": {"id": 0, "total_act_energy": 4961488.92, "total_act_ret_energy": 0.0}
        }

        Args:
            message (dict): Inbound AMQP message.

        Returns:
            Optional[Dict[str, Any]]: The entity id and type (None if the entity has no type
                configured), the ids of the parameters that match this message's topic, and
                the decoded Shelly message. None if the message cannot be matched to a known
                entity or parameter.
        """
        if not isinstance(message, dict):
            self._logger.warning(f"{self._LOG_PREFIX} Skipping message that is not a dictionary.")
            return None

        # 1. Get the exchange this message came from
        source = str(message.get("source")).strip("/").strip().lower()

        # 2. Find the entity this message belongs to
        entity_id = self._source_to_entity.get(source)
        if not entity_id:
            self._logger.debug(f"{self._LOG_PREFIX} Could not map source '{source}' to any known entity.")
            return None

        # 3. Decode the payload if it's still bytes/string
        envelope_payload = message.get("payload")
        if isinstance(envelope_payload, (bytes, bytearray, str)):
            try:
                envelope_payload = json.loads(
                    envelope_payload if isinstance(envelope_payload, str) else envelope_payload.decode("utf-8"))
            except ValueError as err:
                self._logger.error(f"{self._LOG_PREFIX} Failed to decode payload: {err}")
                return None

        if not isinstance(envelope_payload, dict):
            self._logger.warning(
                f"{self._LOG_PREFIX} Skipping message of entity '{entity_id}': its payload is not a dictionary."
            )
            return None

        entity_spec = self._entities.get(entity_id, {})
        entity_type_id = entity_spec.get("entity_type_id")

        # 4. Find which of the entity's parameters match this message's topic
        message_topic = envelope_payload.get("Topic", "")
        entity_parameters = entity_spec.get("parameters", {})
        parameters_ids = [
            param_id for param_id in entity_parameters
            if self._source_mapping_rules.get(param_id, {}).get("topic")
            and message_topic.endswith(self._source_mapping_rules[param_id]["topic"])
        ]

        if not parameters_ids:
            self._logger.debug(
                f"{self._LOG_PREFIX} No parameters configured for entity '{entity_id}' match topic '{message_topic}'."
            )
            return None

        # 5. Decode the actual Shelly message
        raw_payload = envelope_payload.get("Message")
        if isinstance(raw_payload, (bytes, bytearray, str)):
            try:
                raw_payload = json.loads(raw_payload if isinstance(raw_payload, str) else raw_payload.decode("utf-8"))
            except ValueError as err:
                self._logger.error(f"{self._LOG_PREFIX} Failed to decode Shelly message: {err}")
                return None

        if not isinstance(raw_payload, dict):
            return None

        return {
            "id": entity_id,
            "entity_type_id": entity_type_id,
            "parameters_ids": parameters_ids,
            "raw_payload": raw_payload
        }

    @register_label_strategy(Label.GRID_METER.value)
    def _grid_meter(
            self,
            entity_id: str,
            entity_parameters: dict,
            parameters_ids: List[str],
            raw_payload: dict,
            now_dt: datetime.datetime,
            timestamp: str
    ) -> Dict[str, Dict[str, Any]]:
        """Builds the canonical readings for a grid meter entity from the raw Shelly values.

        Every reading is converted to kWh, based on the parameter's 'measurement_unit'.
        A parameter whose unit is missing or unknown is skipped, as its reading could
        not be converted safely. Readings that are not numbers are skipped too.

        For parameters whose 'shelly_accumulation' (in the source mapping) is "lifetime", the Shelly device
        reports a running total since its own reset, not a per-interval value. In
        that case, the reading for this period is the delta between the current
        value and the last one stored, as long as that last value is no older than
        MAX_LIFETIME_READING_AGE_SECONDS. The reading is skipped if the last value is
        older, if there is no previous value yet, or if the delta is negative (the
        counter went backwards, e.g. after the device restarted). In all these cases,
        the current value and its timestamp are stored as the new baseline for next time.

        Last known values are tracked per parameter id, not per entity, so
        parameter ids are assumed to be unique across all entities.

        Expected Input Data Format (`raw_payload`):
        ------------------------------------------
        {
            "id": 0,
            "a_total_act_energy": 4961.521,
            "total_act_energy": 45.2
        }

        Expected Output Structure (with the example configuration, and a previous
        stored value of 4961.5 for parameter "32df"):
        ----------------------------------------------
        {
            "67fgh": {
                "parameter_type_id": "energy_in_total",
                "values": [{"timestamp": "2026-08-19 16:15:31 +0000", "value": 0.0452}]
            },
            "32df": {
                "parameter_type_id": "energy_in_l1",
                "values": [{"timestamp": "2026-08-19 16:15:31 +0000", "value": 0.021}]
            }
        }

        Args:
            entity_id (str): Id of the grid meter entity.
            entity_parameters (dict): The entity's parameter definitions.
            parameters_ids (List[str]): Ids of the entity's parameters that match this message's topic.
            raw_payload (dict): Decoded Shelly message.
            now_dt (datetime.datetime): Current time, timezone-aware.
            timestamp (str): Current time, formatted as a string, used for the resulting readings.

        Returns:
            Dict[str, Dict[str, Any]]: The resulting parameters, with values in kWh.
        """
        adapted_parameters: Dict[str, Dict[str, Any]] = {}

        for param_id in parameters_ids:
            param_spec = entity_parameters.get(param_id, {})
            parameter_type_id = param_spec.get("parameter_type_id")
            if not parameter_type_id:
                continue

            # The output is always in kWh, so the parameter's unit must be known to convert it
            measurement_unit = param_spec.get("measurement_unit")
            kwh_factor = ENERGY_UNIT_TO_KWH_FACTOR.get(str(measurement_unit).strip().lower())
            if kwh_factor is None:
                self._logger.warning(
                    f"{self._LOG_PREFIX} Skipping parameter '{param_id}' of entity '{entity_id}': "
                    f"measurement unit {measurement_unit!r} is missing or not a known energy unit."
                )
                continue

            rule = self._source_mapping_rules[param_id]
            raw_value = raw_payload.get(rule["match_key"])

            if raw_value is None:
                continue

            # Only numbers can be subtracted or converted. A bool is excluded because
            # Python treats True/False as the numbers 1/0, but they are not readings.
            if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float)):
                self._logger.warning(
                    f"{self._LOG_PREFIX} Skipping non-numeric reading for parameter '{param_id}' "
                    f"of entity '{entity_id}': {raw_value!r}"
                )
                continue

            if rule.get("shelly_accumulation") == "lifetime":
                last_entry = self._last_parameter_values.get(param_id)

                # Store the current value as the new baseline either way
                self._last_parameter_values[param_id] = {"value": raw_value, "ts": now_dt}

                if last_entry is None:
                    self._logger.debug(
                        f"{self._LOG_PREFIX} No previous lifetime reading for parameter '{param_id}' "
                        f"of entity '{entity_id}'. Skipping until a baseline is established."
                    )
                    continue

                elapsed_seconds = (now_dt - last_entry["ts"]).total_seconds()
                if elapsed_seconds > MAX_LIFETIME_READING_AGE_SECONDS:
                    self._logger.debug(
                        f"{self._LOG_PREFIX} Skipping reading for parameter '{param_id}' of entity '{entity_id}': "
                        f"last known value is {elapsed_seconds:.0f}s old, older than the "
                        f"{MAX_LIFETIME_READING_AGE_SECONDS}s limit."
                    )
                    continue

                value = raw_value - last_entry["value"]

                if value < 0:
                    self._logger.warning(
                        f"{self._LOG_PREFIX} Lifetime counter of parameter '{param_id}' of entity "
                        f"'{entity_id}' went backwards ({last_entry['value']} -> {raw_value}). "
                        f"Skipping, and using the new value as the baseline."
                    )
                    continue
            else:
                value = raw_value

            adapted_parameters[param_id] = {
                "parameter_type_id": parameter_type_id,
                "values": [{
                    "timestamp": timestamp,
                    "value": round(value * kwh_factor, KWH_DECIMAL_PLACES)
                }]
            }

        return adapted_parameters