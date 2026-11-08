# --- Standard Library Imports ---
import datetime
import json
import re
from typing import Any, Dict, List, Optional, Union

# --- Internal Framework Imports ---
from app.core.adapters.telemetry_adapter_base import TelemetryAdapterBase
from app.core.utils.logger import LoggingUtils
from app.extension.utils.labels import PARAMETER_TYPE_EXPECTED_TYPES, Label

# Adaptation strategies registered by entity type id.
_ENTITY_TYPES_STRATEGIES = {}


def register_entity_type_strategy(name: str):
    """Registers an adaptation strategy for an entity type id.

    Args:
        name (str): Entity type id the strategy handles.

    Returns:
        Callable: A decorator that registers the strategy function.
    """
    def decorator(func):
        _ENTITY_TYPES_STRATEGIES[name] = func
        return func

    return decorator


class SoftCPSTelemetryAdapter(TelemetryAdapterBase):
    """Adapter for SoftCPS telemetry.

    Each call to `adapt` handles one envelope, which carries the readings of a
    single entity (a battery or a grid meter). The entity is identified by the
    last part of the envelope's `source`, and the source mapping says which
    key of the payload holds the value of each of its parameters.
    """

    _entities: dict
    _LOG_PREFIX: str = "SoftCPS Telemetry Adapter |"

    def __init__(
        self,
        environment: str,
        environment_specs: dict,
        source_mapping_rules: dict,
        logger: LoggingUtils,
        timezone: str = "UTC"
    ) -> None:
        """Initializes the SoftCPS adapter.

        Expected `environment_specs` Data Structure:
        -------------------------------------------
        {
            "group": "living lab",
            "entities": {
                "B01": {
                    "entity_type_id": "battery",
                    "parameters": {
                        "b01_energy_in": {"parameter_type_id": "energy_in", "measurement_unit": "kWh"},
                        "b01_energy_out": {"parameter_type_id": "energy_out", "measurement_unit": "kWh"},
                        "b01_soc": {"parameter_type_id": "state_of_charge", "measurement_unit": "ratio"}
                    }
                },
                "GR01": {
                    "entity_type_id": "grid_meter",
                    "parameters": {
                        "gr01_in_total": {"parameter_type_id": "energy_in_total", "measurement_unit": "kWh"}
                    }
                }
            }
        }

        Expected `source_mapping_rules` Data Structure:
        ------------------------------------------------
        {
            # Parameter id -> where the data comes from and which key of the payload holds it.
            # Only the last part of the 'fetch_key' is compared with the last part of the
            # envelope's `source` (e.g. "softcps/batteries/battery_id_1" -> "battery_id_1").
            # The 'source_measurement_unit' is mandatory when the parameter has a 'measurement_unit' in the schema. It is
            # the unit in which the source gives the readings. They are converted to the unit of the schema.
            "b01_energy_in": {"fetch_key": "softcps/batteries/battery_id_1", "match_key": "energy_charged_kwh_window",
                              "source_measurement_unit": "kWh"},
            "b01_energy_out": {"fetch_key": "softcps/batteries/battery_id_1", "match_key": "energy_discharged_kwh_window",
                               "source_measurement_unit": "kWh"},
            "b01_soc": {"fetch_key": "softcps/batteries/battery_id_1", "match_key": "soc_pct",
                        "source_measurement_unit": "%"},
            "gr01_in_total": {"fetch_key": "softcps/grid_meters/hq-grid-meter", "match_key": "energy_in_total",
                              "source_measurement_unit": "kWh"}
        }

        Args:
            environment (str): Name of the target environment (e.g., "R-H-03").
            environment_specs (dict): Entity schema for the environment.
            source_mapping_rules (dict): Rules of each parameter, as detailed above.
            logger (LoggingUtils): Logging interface.
            timezone (str): Timezone used to timestamp readings. Defaults to "UTC".

        Raises:
            ValueError: If mandatory parameters are missing or empty.
            TypeError: If input arguments have the wrong type.
        """
        super().__init__(environment, timezone, logger)

        # --- Guard Clause 1: environment_specs must be a dictionary ---
        if not isinstance(environment_specs, dict):
            error_msg = (
                f"{self._LOG_PREFIX} Initialization error: 'environment_specs' must be a dictionary "
                f"for environment '{environment}'."
            )
            self._logger.error(error_msg)
            raise ValueError(error_msg)

        # --- Guard Clause 2: environment_specs must have a non-empty 'entities' dictionary ---
        entities_dict = environment_specs.get("entities")
        if not isinstance(entities_dict, dict) or not entities_dict:
            error_msg = (
                f"{self._LOG_PREFIX} Initialization error: 'entities' specification cannot be empty "
                f"for environment '{environment}'."
            )
            self._logger.error(error_msg)
            raise ValueError(error_msg)

        # --- Guard Clause 3: source_mapping_rules must be a dict ---
        if not isinstance(source_mapping_rules, dict):
            error_msg = (
                f"{self._LOG_PREFIX} Initialization error: 'source_mapping_rules' must be a dictionary, "
                f"got {type(source_mapping_rules).__name__} for environment '{environment}'."
            )
            self._logger.error(error_msg)
            raise TypeError(error_msg)

        self._entities = entities_dict
        self._source_mapping_rules = source_mapping_rules
        # Checks the units of the parameters and prepares the conversion of the readings to the units of the schema
        self._prepare_unit_conversion(entities_dict, source_mapping_rules)

        # Reverse lookup: id used in the envelope's source -> entity_id
        # Expected `_source_to_entity` Data Structure:
        # --------------------------------------------
        # {
        #     "battery_id_1": "B01",
        #     "hq-grid-meter": "GR01"
        # }
        self._source_to_entity: Dict[str, str] = {}

        # Key of the payload that holds the value of each parameter
        # Expected `_payload_keys` Data Structure:
        # ----------------------------------------
        # {
        #     "b01_energy_in": "energy_charged_kwh_window",
        #     "b01_soc": "soc_pct"
        # }
        self._payload_keys: Dict[str, str] = {}

        for entity_id, entity_data in self._entities.items():
            for param_id in entity_data.get("parameters", {}):
                rule = source_mapping_rules.get(param_id)
                if rule is None:
                    continue

                # str() accepts numeric values, and strip() removes spaces at the ends, so the keys match the ones in the messages
                fetch_key = str(rule.get("fetch_key", "")).strip() if isinstance(rule, dict) else ""
                match_key = str(rule.get("match_key", "")).strip() if isinstance(rule, dict) else ""
                if not fetch_key or not match_key:
                    self._logger.warning(
                        f"{self._LOG_PREFIX} Parameter '{param_id}' of entity '{entity_id}' (parameter type: {self._parameter_type(entity_id, param_id)}): "
                        f"it has no 'fetch_key' or 'match_key' in 'source_mapping_rules'."
                    )
                    continue

                source_id = self._last_part_of_source(fetch_key)
                known_entity = self._source_to_entity.setdefault(source_id, entity_id)
                if known_entity != entity_id:
                    self._logger.warning(
                        f"{self._LOG_PREFIX} Source id '{source_id}' is mapped to more than one entity. "
                        f"Keeping '{known_entity}'."
                    )
                    continue

                self._payload_keys[param_id] = match_key

    def adapt(self, message: Union[dict, bytes, str]) -> List[Dict[str, Any]]:
        """Converts a SoftCPS envelope into a canonical entity message.

        Expected Input Format (`message`), battery:
        --------------------------------------------
        {
            "protocol": "HTTP",
            "source": "softcps/batteries/battery_id_1",
            "status": 200,
            "content_type": "application/json",
            "metadata": {},
            "payload": {
                "timestamp": "2026-08-19T10:00:00Z",
                "energy_charged_kwh_window": 0.4,
                "energy_discharged_kwh_window": 0.1,
                "soc_pct": 85.0
            }
        }

        Expected Output Structure (battery):
        -------------------------------------
        [
            {
                "id": "B01",
                "entity_type_id": "battery",
                "parameters": {
                    "b01_energy_in": {
                        "parameter_type_id": "energy_in",
                        "values": [{"timestamp": "2026-08-19 10:00:00 +0000", "value": 0.4}]
                    },
                    "b01_energy_out": {
                        "parameter_type_id": "energy_out",
                        "values": [{"timestamp": "2026-08-19 10:00:00 +0000", "value": 0.1}]
                    },
                    "b01_soc": {
                        "parameter_type_id": "state_of_charge",
                        "values": [{"timestamp": "2026-08-19 10:00:00 +0000", "value": 0.85}]
                    }
                },
                "timestamp": "2026-08-19 10:00:03 +0000"
            }
        ]

        Args:
            message (Union[dict, bytes, str]): Raw inbound envelope.

        Returns:
            List[Dict[str, Any]]: A single standardized canonical entity message, or an
                empty list if the message could not be processed.
        """
        if not message:
            self._logger.debug(f"{self._LOG_PREFIX} Skipping empty message payload.")
            return []

        now_dt = datetime.datetime.now(self._tz)
        timestamp = now_dt.strftime("%Y-%m-%d %H:%M:%S %z")

        # 1. Find the entity and decode the payload
        resolved = self._normalize_ingress_payload(message)
        if not resolved:
            return []

        entity_id = resolved["entity_id"]
        entity_type_id = resolved["entity_type_id"]
        payload = resolved["payload"]

        if not entity_type_id:
            self._logger.warning(
                f"{self._LOG_PREFIX} Entity '{entity_id}' has no 'entity_type_id' configured."
            )
            return []

        # 2. Pick the strategy for this entity type
        strategy_func = _ENTITY_TYPES_STRATEGIES.get(entity_type_id)
        if strategy_func is None:
            self._logger.warning(
                f"{self._LOG_PREFIX} No adaptation strategy registered for entity type '{entity_type_id}'."
            )
            return []

        # 3. Decide when the readings were taken
        reading_timestamp = self._resolve_reading_timestamp(payload.get("timestamp"), now_dt)
        if reading_timestamp is None:
            return []

        # 4. Extract the values, by parameter type id
        values_by_type = strategy_func(self, entity_id, payload)
        if not values_by_type:
            self._logger.debug(f"{self._LOG_PREFIX} No readings found for entity '{entity_id}'.")
            return []

        # 5. Assign them to the entity's parameters, and validate
        entity_parameters = self._entities.get(entity_id, {}).get("parameters", {})
        parameters = self._build_parameters(entity_id, entity_parameters, values_by_type, reading_timestamp)
        if not parameters:
            return []

        # Converts the readings from the unit of the source to the unit of the schema (see 'source_measurement_unit')
        parameters = self.convert_to_schema_units(parameters)
        parameters = self.parameters_validation(
            parameters, entity_parameters, PARAMETER_TYPE_EXPECTED_TYPES
        )

        return [self._message_creator(parameters, entity_id, entity_type_id, timestamp)]

    def _normalize_ingress_payload(self, message: Union[dict, bytes, str]) -> Optional[Dict[str, Any]]:
        """Finds the entity an envelope belongs to and decodes its payload.

        The entity is identified by the last part of the envelope's `source`
        (for example "softcps/batteries/battery_id_1" gives "battery_id_1"),
        which is compared with the 'fetch_key' of the rules in `source_mapping_rules`.

        Envelopes with an error status (not 2xx), or with a payload that is not
        a valid JSON object, are discarded.

        Expected Input Structure (`message`):
        ------------------------------------
        {
            "protocol": "HTTP",
            "source": "softcps/batteries/battery_id_1",
            "status": 200,
            "content_type": "application/json",
            "metadata": {},
            "payload": b'{"timestamp": "2026-08-19T10:00:00Z", "soc_pct": 85.0}'
        }

        Expected Output Structure:
        --------------------------
        {
            "entity_id": "B01",
            "entity_type_id": "battery",
            "payload": {"timestamp": "2026-08-19T10:00:00Z", "soc_pct": 85.0}
        }

        Args:
            message (Union[dict, bytes, str]): Inbound raw envelope.

        Returns:
            Optional[Dict[str, Any]]: The entity id and type, and the decoded payload.
                None if the envelope cannot be used.
        """
        if isinstance(message, (bytes, bytearray, str)):
            try:
                message = json.loads(message if isinstance(message, str) else message.decode("utf-8"))
            except ValueError as err:
                self._logger.error(f"{self._LOG_PREFIX} Failed to decode envelope: {err}")
                return None

        if not isinstance(message, dict):
            return None

        # 1. Discard envelopes with an error status
        status = message.get("status")
        if isinstance(status, int) and not isinstance(status, bool) and not 200 <= status < 300:
            self._logger.warning(
                f"{self._LOG_PREFIX} Skipping envelope with error status {status}: {message.get('source')}"
            )
            return None

        # 2. Find the entity from the last part of the source
        raw_source = message.get("source")
        source_id = self._last_part_of_source(raw_source)
        entity_id = self._source_to_entity.get(source_id)

        if not entity_id:
            self._logger.debug(
                f"{self._LOG_PREFIX} Could not map source '{raw_source}' (id '{source_id}') to any known entity."
            )
            return None

        entity_type_id = self._entities.get(entity_id, {}).get("entity_type_id")

        # 3. Decode the payload
        payload = message.get("payload")
        if isinstance(payload, (bytes, bytearray, str)):
            try:
                payload = json.loads(payload if isinstance(payload, str) else payload.decode("utf-8"))
            except ValueError as err:
                self._logger.error(f"{self._LOG_PREFIX} Failed to decode payload: {err}")
                return None

        if not isinstance(payload, dict):
            self._logger.warning(
                f"{self._LOG_PREFIX} Skipping envelope whose payload is not a JSON object: {raw_source}"
            )
            return None

        return {
            "entity_id": entity_id,
            "entity_type_id": entity_type_id,
            "payload": payload
        }

    def _build_parameters(
            self,
            entity_id: str,
            entity_parameters: dict,
            values_by_type: Dict[str, Any],
            reading_timestamp: str
    ) -> Dict[str, Dict[str, Any]]:
        """Assigns each value to the entity's parameter of the matching type.

        Note: in the context of this Telemetry System, an entity is expected to have
        only one parameter of each type, as the SoftCPS payload carries a single
        value per type. If an entity has more than one parameter of the same type,
        only the first one (in the order of the entity's parameters) receives the
        value, and the others are ignored.

        Expected Input (`values_by_type`):
        ----------------------------------
        {"energy_in": 0.4, "state_of_charge": 85.0}

        Expected Output Structure:
        --------------------------
        {
            "b01_energy_in": {
                "parameter_type_id": "energy_in",
                "values": [{"timestamp": "2026-08-19 10:00:00 +0000", "value": 0.4}]
            },
            "b01_soc": {
                "parameter_type_id": "state_of_charge",
                "values": [{"timestamp": "2026-08-19 10:00:00 +0000", "value": 85.0}]
            }
        }

        Args:
            entity_id (str): Id of the entity.
            entity_parameters (dict): The entity's parameter definitions.
            values_by_type (Dict[str, Any]): Value read for each parameter type id.
            reading_timestamp (str): Formatted time the readings were taken.

        Returns:
            Dict[str, Dict[str, Any]]: The entity's parameters that received a value.
        """
        parameters: Dict[str, Dict[str, Any]] = {}

        for parameter_type_id, value in values_by_type.items():
            param_id = next(
                (p_id for p_id, p_spec in entity_parameters.items()
                 if p_spec.get("parameter_type_id") == parameter_type_id),
                None
            )
            if param_id is None:
                self._logger.debug(
                    f"{self._LOG_PREFIX} Entity '{entity_id}' has no parameter of type "
                    f"'{parameter_type_id}'. Skipping its reading."
                )
                continue

            parameters[param_id] = {
                "parameter_type_id": parameter_type_id,
                "values": [{"timestamp": reading_timestamp, "value": value}]
            }

        return parameters

    def _read_values(self, entity_id: str, payload: dict) -> Dict[str, Any]:
        """Reads, for each parameter type id of the entity, the value under the key of its rule.

        The key of the payload that holds each value comes from the parameter's rule. A key that
        is missing from the payload, or null, is skipped. The values are not type-checked here;
        that is done by parameters_validation.

        Expected Input (`payload`):
        ---------------------------
        {"timestamp": "2026-08-19T10:00:00Z", "energy_charged_kwh_window": 0.4, "soc_pct": 85.0}

        Expected Output:
        ----------------
        {"energy_in": 0.4, "state_of_charge": 85.0}

        Args:
            entity_id (str): Id of the entity.
            payload (dict): Decoded payload.

        Returns:
            Dict[str, Any]: The value found for each parameter type id that has one.
        """
        found: Dict[str, Any] = {}

        for param_id, param_spec in self._entities.get(entity_id, {}).get("parameters", {}).items():
            payload_key = self._payload_keys.get(param_id)
            if payload_key is None or payload.get(payload_key) is None:
                continue

            parameter_type_id = param_spec.get("parameter_type_id")
            if not parameter_type_id:
                self._logger.warning(
                    f"{self._LOG_PREFIX} Parameter '{param_id}' of entity '{entity_id}' (parameter type: {self._parameter_type(entity_id, param_id)}): it has no "
                    f"'parameter_type_id' configured. Skipping its reading."
                )
                continue

            # If the entity has two parameters of the same type, the first one wins (see _build_parameters)
            found.setdefault(parameter_type_id, payload[payload_key])

        return found

    def _resolve_reading_timestamp(self, raw_timestamp: Any, now_dt: datetime.datetime) -> Optional[str]:
        """Gets the time the readings were taken, formatted in the adapter's timezone.

        If the payload has no timestamp, the current time is used. If it has one
        that is not a valid date, the readings cannot be placed in time and are
        discarded.

        Args:
            raw_timestamp (Any): The payload's timestamp, as received.
            now_dt (datetime.datetime): Current time, timezone-aware.

        Returns:
            Optional[str]: The formatted timestamp, or None if the date is invalid.
        """
        if raw_timestamp is None or raw_timestamp == "":
            return now_dt.strftime("%Y-%m-%d %H:%M:%S %z")

        dt = self._parse_date(raw_timestamp)
        if dt is None:
            self._logger.warning(
                f"{self._LOG_PREFIX} Skipping readings with invalid timestamp '{raw_timestamp}'."
            )
            return None

        return dt.astimezone(self._tz).strftime("%Y-%m-%d %H:%M:%S %z")

    @register_entity_type_strategy(Label.GRID_METER.value)
    def _grid_meter(self, entity_id: str, payload: dict) -> Dict[str, Any]:
        """Reads a grid meter's values from the payload.

        Expected Input (`payload`):
        ---------------------------
        {
            "timestamp": "2026-08-19T10:00:00Z",
            "energy_in_total": 1.2,
            "energy_out_total": 0.0
        }

        Expected Output:
        ----------------
        {"energy_in_total": 1.2, "energy_out_total": 0.0}

        Args:
            entity_id (str): Id of the grid meter entity.
            payload (dict): Decoded payload.

        Returns:
            Dict[str, Any]: The value found for each parameter type id.
        """
        return self._read_values(entity_id, payload)

    @register_entity_type_strategy(Label.BATTERY.value)
    def _battery(self, entity_id: str, payload: dict) -> Dict[str, Any]:
        """Reads a battery's values from the payload.

        The values stay in the unit of SoftCPS (e.g., the state of charge as a percentage, 0-100). The conversion to
        the unit of the schema (e.g., a proportion from 0 to 1) is done afterwards for all the adapters, with the
        'source_measurement_unit' of the source mapping.

        Expected Input (`payload`):
        ---------------------------
        {
            "timestamp": "2026-08-19T10:00:00Z",
            "energy_charged_kwh_window": 0.4,
            "energy_discharged_kwh_window": 0.1,
            "soc_pct": 85.0
        }

        Expected Output:
        ----------------
        {"energy_in": 0.4, "energy_out": 0.1, "state_of_charge": 85.0}

        Args:
            entity_id (str): Id of the battery entity.
            payload (dict): Decoded payload.

        Returns:
            Dict[str, Any]: The value found for each parameter type id.
        """
        return self._read_values(entity_id, payload)

    @staticmethod
    def _clean_source(source: Any) -> str:
        """Normalizes a source, or a source id, for comparison.

        Converts it to text, removes slashes at the start and end, removes spaces
        at the start and end, and converts it to lower case. Slashes in the middle
        are kept. Both the 'fetch_key' of the rules and the envelope's source
        go through this, so they are compared in the same form.

        Examples:
        ---------
        "softcps/batteries/battery_id_1"   -> "softcps/batteries/battery_id_1"
        "/softcps/batteries/Battery_ID_1/" -> "softcps/batteries/battery_id_1"
        "Battery_ID_1"                     -> "battery_id_1"
        "battery_id_1 "                    -> "battery_id_1"
        None                               -> "none"

        Args:
            source (Any): The source, as received or configured.

        Returns:
            str: The normalized text.
        """
        return str(source).strip("/").strip().lower()

    @classmethod
    def _last_part_of_source(cls, source: Any) -> str:
        """Returns the id of the asset that a source identifies, which says where the data comes from.

        If the source has the path of the SoftCPS API ("/assets/<asset_id>/..."), the id is the part after "/assets/",
        and whatever comes after it (e.g., "/observations?window_seconds=15", or the same path repeated) is ignored.
        Otherwise, the id is the last part of the source.

        Examples:
        ---------
        "http://193.136.62.78:8003/assets/Sao-Mamede-Battery/observations?window_seconds=15"
                                                          -> "sao-mamede-battery"
        "softcps/batteries/battery_id_1"                  -> "battery_id_1"
        "hq-grid-meter"                                   -> "hq-grid-meter"

        Args:
            source (Any): The envelope's source, or the 'fetch_key' of a rule.

        Returns:
            str: The id of the asset, normalized for comparison.
        """
        cleaned = cls._clean_source(source)
        match = re.search(r"/assets/([^/?#]+)", cleaned)
        if match:
            return match.group(1)
        return cleaned.split("/")[-1]

    @staticmethod
    def _parse_date(raw_date: Any) -> Optional[datetime.datetime]:  # TODO passar isto para um utils
        """Parses a date string into a timezone-aware datetime.

        Accepts ISO 8601 strings, with or without a timezone. A date with no
        timezone is assumed to be UTC.

        Args:
            raw_date (Any): The date, as received.

        Returns:
            Optional[datetime.datetime]: The parsed datetime, or None if it is not a valid date.
        """
        try:
            dt = datetime.datetime.fromisoformat(str(raw_date).replace("Z", "+00:00"))
        except ValueError:
            return None

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt