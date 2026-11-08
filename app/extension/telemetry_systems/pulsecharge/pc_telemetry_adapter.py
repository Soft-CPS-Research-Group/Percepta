# --- Standard Library Imports ---
import datetime
import json
from typing import Any, Dict, List, Optional, Union

# --- Internal Framework Imports ---
from app.core.adapters.telemetry_adapter_base import TelemetryAdapterBase
from app.core.utils.logger import LoggingUtils
from app.extension.utils.labels import PARAMETER_TYPE_EXPECTED_TYPES, Label, ParamLabel

# A cached state of charge older than this is considered too old to be reused.
SOC_CACHE_MAX_AGE = datetime.timedelta(hours=1)

# PulseCharge reports the state of charge as a percentage (0-100). It is stored as a fraction (0-1).
SOC_DECIMAL_PLACES = 4

# Adaptation strategies registered by entity type id.
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


class PCTelemetryAdapter(TelemetryAdapterBase):
    """Adapter for PulseCharge (PC) telemetry.

    Each call to `adapt` handles one envelope, which carries the data of one
    user. The user is mapped to an entity through `source_mapping_rules`.

    When a message omits the state of charge, the last one received for that
    entity is reused, as long as it is not older than SOC_CACHE_MAX_AGE.
    """

    _entities: dict
    _LOG_PREFIX: str = "PC Telemetry Adapter |"

    def __init__(
        self,
        environment: str,
        environment_specs: dict,
        source_mapping_rules: dict,
        logger: LoggingUtils,
        timezone: str = "UTC"
    ) -> None:
        """Initializes the PulseCharge adapter.

        Expected `environment_specs` Data Structure:
        -------------------------------------------
        {
            "group": "living lab",
            "entities": {
                "EV01": {
                    "entity_type_id": "electric_vehicle",
                    "parameters": {
                        "ev01_soc": {"parameter_type_id": "state_of_charge", "measurement_unit": "%"},
                        "ev01_flexibility": {"parameter_type_id": "flexibility"}
                    }
                }
            }
        }

        Expected `source_mapping_rules` Data Structure:
        ------------------------------------------------
        {
            # Parameter id -> keys. The 'match_key' is the PulseCharge user id of the entity
            # the parameter belongs to, so all the parameters of an entity share it.
            # The 'source_measurement_unit' (mandatory when the parameter has a 'measurement_unit' in the schema) is the
            # unit in which the source gives the readings. They are converted to the unit of the schema.
            "ev01_soc": {"match_key": "user-123", "source_measurement_unit": "%"},
            "ev01_flexibility": {"match_key": "user-123"}
        }

        Args:
            environment (str): Name of the target environment (e.g., "R-H-03").
            environment_specs (dict): Entity schema for the environment.
            source_mapping_rules (dict): For each parameter id, its keys (the user id is the 'match_key').
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

        # Last state of charge received for each entity, and when it was received.
        # Expected `_soc_cache` Data Structure:
        # -------------------------------------
        # {
        #     "EV01": {"value": 72.5, "ts": datetime(...)}
        # }
        self._soc_cache: Dict[str, Dict[str, Any]] = {}

        # Reverse lookup: PulseCharge user id -> entity_id
        # Expected `_user_to_entity` Data Structure:
        # ------------------------------------------
        # {
        #     "user-123": "EV01"
        # }
        self._user_to_entity: Dict[str, str] = {}
        for entity_id, entity_data in self._entities.items():
            for param_id in entity_data.get("parameters", {}):
                rule = source_mapping_rules.get(param_id)
                if rule is None:
                    continue

                # str() accepts numeric ids, and strip() removes spaces at the ends, so the id matches the one in the messages
                user_id = str(rule.get("match_key", "")).strip() if isinstance(rule, dict) else ""
                if not user_id:
                    self._logger.warning(
                        f"{self._LOG_PREFIX} Parameter '{param_id}' of entity '{entity_id}' (parameter type: {self._parameter_type(entity_id, param_id)}): "
                        f"it has no 'match_key' (user id) in 'source_mapping_rules'."
                    )
                    continue

                known_entity = self._user_to_entity.setdefault(user_id, entity_id)
                if known_entity != entity_id:
                    self._logger.warning(
                        f"{self._LOG_PREFIX} User id '{user_id}' is mapped to more than one entity. "
                        f"Keeping '{known_entity}'."
                    )

    def adapt(self, message: Union[dict, bytes, str]) -> List[Dict[str, Any]]:
        """Converts a PulseCharge envelope into a canonical entity message.

        Expected Input Format (`message`):
        -----------------------------------
        {
            "protocol": "RabbitMQ",
            "source": "pulsecharge",
            "status": null,
            "content_type": "application/json",
            "metadata": {},
            "payload": {
                "user_id": "user-123",
                "current_soc": 72.5,
                "estimated_soc_at_arrival": 40.0,
                "estimated_soc_at_departure": 90.0,
                "estimated_time_at_arrival": "2026-08-19T18:00:00Z",
                "estimated_time_at_departure": "2026-08-20T07:00:00Z",
                "charger": "EVC01",
                "mode": "smart"
            }
        }

        Expected Output Structure:
        --------------------------
        [
            {
                "id": "EV01",
                "entity_type_id": "electric_vehicle",
                "parameters": {
                    "ev01_soc": {
                        "parameter_type_id": "state_of_charge",
                        "values": [{"timestamp": "2026-08-19 10:00:03 +0000", "value": 0.725}]
                    },
                    "ev01_flexibility": {
                        "parameter_type_id": "flexibility",
                        "values": [{
                            "timestamp": "2026-08-19 10:00:03 +0000",
                            "value": {
                                "estimated_soc_at_arrival": 0.4,
                                "estimated_soc_at_departure": 0.9,
                                "estimated_time_at_arrival": "2026-08-19T18:00:00Z",
                                "estimated_time_at_departure": "2026-08-20T07:00:00Z",
                                "charger": "EVC01",
                                "mode": "smart"
                            }
                        }]
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

        # 1. Find the user and entity, and decode the payload
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
        strategy_func = _LABEL_STRATEGIES.get(entity_type_id)
        if strategy_func is None:
            self._logger.warning(
                f"{self._LOG_PREFIX} No adaptation strategy registered for entity type '{entity_type_id}'."
            )
            return []

        # 3. Extract the values, by parameter type id
        values_by_type = strategy_func(self, entity_id, payload, now_dt)
        if not values_by_type:
            self._logger.debug(f"{self._LOG_PREFIX} No readings found for entity '{entity_id}'.")
            return []

        # 4. Assign them to the entity's parameters, and validate
        entity_parameters = self._entities.get(entity_id, {}).get("parameters", {})
        parameters = self._build_parameters(entity_id, entity_parameters, values_by_type, timestamp)
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

        The entity is found from the payload's `user_id`, which is compared with
        the user ids in `source_mapping_rules`.

        Envelopes with a payload that is not a valid JSON object, or with a
        missing or unknown user id, are discarded.

        Expected Input Structure (`message`):
        ------------------------------------
        {
            "protocol": "RabbitMQ",
            "source": "pulsecharge",
            "status": null,
            "content_type": "application/json",
            "metadata": {},
            "payload": b'{"user_id": "user-123", "current_soc": 72.5}'
        }

        Expected Output Structure:
        --------------------------
        {
            "entity_id": "EV01",
            "entity_type_id": "electric_vehicle",
            "payload": {"user_id": "user-123", "current_soc": 72.5}
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

        # 1. Decode the payload
        payload = message.get("payload")
        if isinstance(payload, (bytes, bytearray, str)):
            try:
                payload = json.loads(payload if isinstance(payload, str) else payload.decode("utf-8"))
            except ValueError as err:
                self._logger.error(f"{self._LOG_PREFIX} Failed to decode payload: {err}")
                return None

        if not isinstance(payload, dict):
            self._logger.warning(
                f"{self._LOG_PREFIX} Skipping envelope whose payload is not a JSON object."
            )
            return None

        # 2. Find the entity from the user id
        raw_user_id = payload.get("user_id")

        # None is checked first, because str(None) would give the text "None", which is not a missing id.
        # str() accepts numeric ids, and strip() catches ids made only of spaces.
        if raw_user_id is None or str(raw_user_id).strip() == "":
            self._logger.warning(f"{self._LOG_PREFIX} Skipping message with no 'user_id'.")
            return None

        # Same cleaning as the 'match_key' in __init__, so both sides can be compared
        user_id = str(raw_user_id).strip()
        entity_id = self._user_to_entity.get(user_id)
        if not entity_id:
            self._logger.debug(f"{self._LOG_PREFIX} Could not map user '{user_id}' to any known entity.")
            return None

        return {
            "entity_id": entity_id,
            "entity_type_id": self._entities.get(entity_id, {}).get("entity_type_id"),
            "payload": payload
        }

    @register_label_strategy(Label.ELECTRIC_VEHICLE.value)
    def _electric_vehicle(self, entity_id: str, payload: dict, now_dt: datetime.datetime) -> Dict[str, Any]:
        """Reads an electric vehicle's values from the payload.

        Reports the state of charge (the last known one, if the message omits it)
        and the flexibility data. All states of charge arrive as percentages
        (0-100) and are converted to fractions (0-1).

        Expected Input (`payload`):
        ---------------------------
        {
            "user_id": "user-123",
            "current_soc": 72.5,
            "estimated_soc_at_arrival": 40.0,
            "estimated_soc_at_departure": 90.0,
            "estimated_time_at_arrival": "2026-08-19T18:00:00Z",
            "estimated_time_at_departure": "2026-08-20T07:00:00Z",
            "charger": "EVC01",
            "mode": "smart"
        }

        Expected Output:
        ----------------
        {
            "state_of_charge": 0.725,
            "flexibility": {
                "estimated_soc_at_arrival": 0.4,
                "estimated_soc_at_departure": 0.9,
                "estimated_time_at_arrival": "2026-08-19T18:00:00Z",
                "estimated_time_at_departure": "2026-08-20T07:00:00Z",
                "charger": "EVC01",
                "mode": "smart"
            }
        }

        Args:
            entity_id (str): Id of the electric vehicle entity.
            payload (dict): Decoded payload.
            now_dt (datetime.datetime): Current time, timezone-aware.

        Returns:
            Dict[str, Any]: The value found for each parameter type id.
        """
        values: Dict[str, Any] = {}

        soc = self._resolve_soc(entity_id, payload.get("current_soc"), now_dt)
        if soc is not None:
            values[ParamLabel.STATE_OF_CHARGE.value] = self._soc_to_fraction(soc)

        values[ParamLabel.FLEXIBILITY.value] = self._build_flexibility(payload)

        return values

    def _resolve_soc(self, entity_id: str, current_soc: Any, now_dt: datetime.datetime) -> Any:
        """Returns the state of charge to report for an entity.

        If the message has one, it is stored and used. If not, the last one stored
        for that entity is used, unless it is older than SOC_CACHE_MAX_AGE (in which
        case it is dropped). If there is none, nothing is reported.

        Expected Input / Output:
        ------------------------
        _resolve_soc("EV01", 72.5, 10:00)  -> 72.5   (and stored at 10:00)
        _resolve_soc("EV01", None, 10:20)  -> 72.5   (stored value is 20 minutes old)
        _resolve_soc("EV01", None, 11:30)  -> None   (stored value is 90 minutes old, so it is dropped)

        Args:
            entity_id (str): Id of the entity.
            current_soc (Any): The state of charge in the message, or None.
            now_dt (datetime.datetime): Current time, timezone-aware.

        Returns:
            Any: The state of charge to report, or None if there is none.
        """
        if current_soc is not None:
            self._soc_cache[entity_id] = {"value": current_soc, "ts": now_dt}
            return current_soc

        cached = self._soc_cache.get(entity_id)
        if cached is None:
            self._logger.debug(f"{self._LOG_PREFIX} No 'current_soc' for entity '{entity_id}' and none cached.")
            return None

        if now_dt - cached["ts"] > SOC_CACHE_MAX_AGE:
            self._logger.debug(
                f"{self._LOG_PREFIX} Cached 'current_soc' for entity '{entity_id}' is too old. Dropping it."
            )
            del self._soc_cache[entity_id]
            return None

        self._logger.debug(
            f"{self._LOG_PREFIX} Missing 'current_soc'. Using cached value for entity '{entity_id}': {cached['value']}"
        )
        return cached["value"]

    @staticmethod
    def _soc_to_fraction(soc: Any) -> Any:
        """Converts a state of charge from a percentage (0-100) to a fraction (0-1).

        Only numbers are converted. None, and anything that is not a number, is
        returned as is; invalid values are flagged later by parameters_validation.

        Examples:
        ---------
        72.5   -> 0.725
        None   -> None
        "abc"  -> "abc"

        Args:
            soc (Any): The state of charge, as received.

        Returns:
            Any: The state of charge as a fraction, or the original value if it is not a number.
        """
        if isinstance(soc, (int, float)) and not isinstance(soc, bool):
            return round(soc / 100, SOC_DECIMAL_PLACES)
        return soc

    @staticmethod
    def _build_flexibility(payload: dict) -> Dict[str, Any]:
        """Collects the flexibility fields of a message into one dictionary.

        Fields missing from the message get a default: None for the estimated
        states of charge, and an empty string for the rest. The estimated states
        of charge arrive as percentages (0-100) and are converted to fractions (0-1).

        Expected Output:
        ----------------
        {
            "estimated_soc_at_arrival": 0.4,
            "estimated_soc_at_departure": 0.9,
            "estimated_time_at_arrival": "2026-08-19T18:00:00Z",
            "estimated_time_at_departure": "2026-08-20T07:00:00Z",
            "charger": "EVC01",
            "mode": "smart"
        }

        Args:
            payload (dict): Decoded payload.

        Returns:
            Dict[str, Any]: The flexibility fields.
        """
        return {
            "estimated_soc_at_arrival": PCTelemetryAdapter._soc_to_fraction(
                payload.get("estimated_soc_at_arrival", None)
            ),
            "estimated_soc_at_departure": PCTelemetryAdapter._soc_to_fraction(
                payload.get("estimated_soc_at_departure", None)
            ),
            "estimated_time_at_arrival": payload.get("estimated_time_at_arrival", ""),
            "estimated_time_at_departure": payload.get("estimated_time_at_departure", ""),
            "charger": payload.get("charger", ""),
            "mode": payload.get("mode", "")
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
        only one parameter of each type. If an entity has more than one parameter of
        the same type, only the first one (in the order of the entity's parameters)
        receives the value, and the others are ignored.

        Expected Input (`values_by_type`):
        ----------------------------------
        {"state_of_charge": 0.725, "flexibility": {"charger": "EVC01", "mode": "smart"}}

        Expected Output Structure:
        --------------------------
        {
            "ev01_soc": {
                "parameter_type_id": "state_of_charge",
                "values": [{"timestamp": "2026-08-19 10:00:03 +0000", "value": 0.725}]
            },
            "ev01_flexibility": {
                "parameter_type_id": "flexibility",
                "values": [{"timestamp": "2026-08-19 10:00:03 +0000", "value": {"charger": "EVC01", "mode": "smart"}}]
            }
        }

        Args:
            entity_id (str): Id of the entity.
            entity_parameters (dict): The entity's parameter definitions.
            values_by_type (Dict[str, Any]): Value to report for each parameter type id.
            reading_timestamp (str): Formatted time of the readings.

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