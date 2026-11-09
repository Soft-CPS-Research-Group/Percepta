# --- Standard Library Imports ---
import datetime
import json
from typing import Any, Dict, List, Optional, Union

# --- Internal Framework Imports ---
from app.core.data_ingestion.telemetry_adapter_base import TelemetryAdapterBase
from app.core.utils.logger import LoggingUtils
from app.extension.utils.labels import PARAMETER_TYPE_EXPECTED_TYPES, Label

# Time between two consecutive values of a forecast, in minutes.
# Example: target_time 10:00 -> values at 10:00, 10:15, 10:30, ...
FORECAST_STEP_MINUTES = 15

# Adaptation strategies registered by entity type id.
_ENTITY_TYPES_STRATEGIES = {}


def register_entity_type_strategy(name: str):
    """Registers an adaptation strategy for an entity type id.

    Expected Input Structure:
    -------------------------
    name = "forecast_service"

    Expected Output Structure:
    --------------------------
    A decorator. After `@register_entity_type_strategy("forecast_service")` is applied to a function:
    _ENTITY_TYPES_STRATEGIES = {"forecast_service": <function _forecast_service>}

    Args:
        name (str): Entity type id the strategy handles.

    Returns:
        Callable: A decorator that registers the strategy function.
    """
    def decorator(func):
        _ENTITY_TYPES_STRATEGIES[name] = func
        return func

    return decorator


class PredictorTelemetryAdapter(TelemetryAdapterBase):
    """Translates the messages of the Predictor Telemetry System into the standard format required by Percepta.

    The Predictor is a Telemetry System that sends forecasts: at each moment it sends the expected energy
    consumption and the expected solar production for the next hours. It sends predicted values, not measurements.
    A forecast is a list of values, one for each future time step, that starts at `target_time` and has
    FORECAST_STEP_MINUTES between values. Each call to `adapt` handles one message of the Predictor.

    The entity is the Predictor itself and each of its parameters is one forecast. The payload can hold several
    forecasts (for example 'consumption' and 'production'), and the source mapping says which one (the 'match_key')
    each parameter wants. Every entity of the environment that has such a rule receives its message, with all its
    forecasts together.

    This adapter assumes that there is only one forecast service in each environment, because the
    Predictor has a different channel for each installation. The messages do not say which entity they are for, so
    if an environment had two forecast service entities, both would receive the same forecasts.
    """

    _entities: dict
    _LOG_PREFIX: str = "Predictor Telemetry Adapter |"

    def __init__(
        self,
        environment: str,
        environment_specs: dict,
        source_mapping_rules: dict,
        logger: LoggingUtils,
        timezone: str = "UTC"
    ) -> None:
        """Initializes the Predictor adapter.

        Expected `environment_specs` Data Structure:
        -------------------------------------------
        {
            "group": "living lab",
            "entities": {
                "PR01": {
                    "entity_type_id": "forecast_service",
                    "parameters": {
                        "pr01_consumption": {"parameter_type_id": "energy_consumption_prediction", "measurement_unit": "kWh"},
                        "pr01_production": {"parameter_type_id": "solar_energy_prediction", "measurement_unit": "kWh"}
                    }
                }
            }
        }

        Expected `source_mapping_rules` Data Structure:
        ------------------------------------------------
        {
            # Parameter id -> forecast of the payload that holds its values ('consumption' or 'production').
            # There is no 'fetch_key': the forecasts are not requested, they are sent by the Predictor.
            # The 'source_measurement_unit' is mandatory when the parameter has a 'measurement_unit' in the schema. It is
            # the unit in which the Predictor gives the values. They are converted to the unit of the schema.
            "pr01_consumption": {"match_key": "consumption", "source_measurement_unit": "kWh"},
            "pr01_production": {"match_key": "production", "source_measurement_unit": "kWh"}
        }

        Args:
            environment (str): Name of the target environment (e.g., "R-H-03").
            environment_specs (dict): Entity schema for the environment.
            source_mapping_rules (dict): Rules of each parameter, as detailed above.
            logger (LoggingUtils): Logging interface.
            timezone (str): Timezone used to format the timestamps. Defaults to "UTC".

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

        # Forecast of the payload that holds the values of each parameter
        # Expected `_forecast_keys` Data Structure:
        # -----------------------------------------
        # {
        #     "pr01_consumption": "consumption",
        #     "pr01_production": "production"
        # }
        self._forecast_keys: Dict[str, str] = {}

        # Parameters already warned about because their forecast is not in the payload, so the warning is logged
        # once and not at every message (the Predictor keeps sending messages).
        # Example: {"pr01_consumption"}
        self._missing_forecast_warned: set = set()

        for entity_id, entity_data in self._entities.items():
            for param_id in entity_data.get("parameters", {}):
                # Rule of this parameter. Example: {"match_key": "consumption"}
                # A parameter with no rule is not fed by the Predictor (it may belong to another Telemetry System)
                rule = source_mapping_rules.get(param_id)
                if rule is None:
                    continue

                # str() accepts numeric values, and strip() removes spaces at the ends, so the key matches the payload's
                # Example: {"match_key": " consumption "} -> "consumption"
                match_key = str(rule.get("match_key", "")).strip() if isinstance(rule, dict) else ""
                if not match_key:
                    self._logger.warning(
                        f"{self._LOG_PREFIX} Parameter '{param_id}' of entity '{entity_id}' (parameter type: {self._parameter_type(entity_id, param_id)}): "
                        f"it has no 'match_key' in 'source_mapping_rules'."
                    )
                    continue

                # Example: self._forecast_keys["pr01_consumption"] = "consumption"
                self._forecast_keys[param_id] = match_key

    def adapt(self, message: Union[dict, bytes, str]) -> Dict[str, Dict[str, Any]]:
        """Converts a Predictor envelope into canonical entity messages.

        Expected Input Format (`message`):
        ----------------------------------
        {
            "protocol": "RabbitMQ",
            "source": "predictor",
            "status": null,
            "content_type": "application/json",
            "metadata": {},
            "payload": {
                "target_time": "2026-10-22T09:00:00Z",          # Time of the first value of every forecast (with its
                                                                # offset; without one, it is taken as UTC)
                "house_id": "house-1",                          # Ignored: the entities come from the source mapping
                "consumption": [1.2, 1.4, 1.1],                 # One value for each time step
                "production": [0.0, 0.3, 0.8]                   # A forecast that no rule names is ignored
            }
        }

        Expected Output Structure (timezone "Europe/Lisbon"):
        -----------------------------------------------------
        {
            "PR01": {
                "entity_type_id": "forecast_service",
                "parameters": {
                    "pr01_consumption": {
                        "parameter_type_id": "energy_consumption_prediction",
                        "values": [
                            {"timestamp": "2026-10-22 10:00:00 +0100", "value": 1.2},
                            {"timestamp": "2026-10-22 10:15:00 +0100", "value": 1.4},
                            {"timestamp": "2026-10-22 10:30:00 +0100", "value": 1.1}
                        ]
                    },
                    "pr01_production": {
                        "parameter_type_id": "solar_energy_prediction",
                        "values": [
                            {"timestamp": "2026-10-22 10:00:00 +0100", "value": 0.0},
                            {"timestamp": "2026-10-22 10:15:00 +0100", "value": 0.3},
                            {"timestamp": "2026-10-22 10:30:00 +0100", "value": 0.8}
                        ]
                    }
                },
                "timestamp": "2026-10-22 10:00:00 +0100"
            }
        }

        Args:
            message (Union[dict, bytes, str]): Raw inbound envelope.

        Returns:
            Dict[str, Dict[str, Any]]: One canonical message, by entity id, for each entity that received a forecast, or an
                empty dictionary if the message could not be processed.
        """
        if not message:
            self._logger.debug(f"{self._LOG_PREFIX} Skipping empty message payload.")
            return {}

        # 1. Decode the envelope and keep its payload
        payload = self._normalize_ingress_payload(message)
        if payload is None:
            return {}

        # 2. Time of the first value of the forecasts. Example: "2026-10-22T09:00:00Z" -> 2026-10-22 09:00:00 UTC
        # (a time with no offset is taken as UTC)
        # Without it the values cannot be placed in time, so the whole message is discarded
        start_time = self._parse_target_time(payload.get("target_time"))
        if start_time is None:
            return {}

        # The message is stamped with the time of the first value, in the adapter timezone.
        # Example (timezone "Europe/Lisbon"): 2026-10-22 10:00:00 +0100
        timestamp = start_time.astimezone(self._tz).strftime("%Y-%m-%d %H:%M:%S %z")

        messages: Dict[str, Dict[str, Any]] = {}

        # 3. Every entity of the environment that has a forecast to receive gets its own message
        # (there is expected to be only one forecast service entity in the environment, see the class docstring)
        # Example: entities = {"PR01": {...}} -> one message for "PR01" (if its parameters have forecasts)
        for entity_id, entity_data in self._entities.items():
            entity_type_id = entity_data.get("entity_type_id")

            if not entity_type_id:
                self._logger.warning(
                    f"{self._LOG_PREFIX} Entity '{entity_id}' has no 'entity_type_id' configured."
                )
                continue

            # 4. Pick the strategy for this entity type. Example: "forecast_service" -> self._forecast_service
            strategy_func = _ENTITY_TYPES_STRATEGIES.get(entity_type_id)
            if strategy_func is None:
                self._logger.warning(
                    f"{self._LOG_PREFIX} No adaptation strategy registered for entity type '{entity_type_id}'."
                )
                continue

            # 5. Read the forecast of each parameter of the entity
            # Example: {"pr01_consumption": {...}, "pr01_production": {...}}, each with "parameter_type_id" and "values"
            # An empty result means the payload had no forecast for this entity: no message is created
            parameters = strategy_func(self, entity_id, payload, start_time)
            if not parameters:
                self._logger.debug(f"{self._LOG_PREFIX} No forecasts found for entity '{entity_id}'.")
                continue

            # 6. Validate the parameters against the schema, and create the message
            # Example: a parameter whose values have the wrong type is dropped by 'parameters_validation'
            entity_parameters = entity_data.get("parameters", {})
            # Converts the readings from the unit of the source to the unit of the schema (see 'source_measurement_unit')
            parameters = self.convert_to_schema_units(parameters)
            parameters = self.parameters_validation(
                parameters, entity_parameters, PARAMETER_TYPE_EXPECTED_TYPES, entity_id
            )

            # Only correct data is sent: an entity without any correct parameter has no message
            if not parameters:
                self._logger.debug(f"{self._LOG_PREFIX} No correct data for entity '{entity_id}' in this message.")
                continue

            messages.update(self._message_creator(parameters, entity_id, entity_type_id, timestamp))

        return messages

    def _normalize_ingress_payload(self, message: Union[dict, bytes, str]) -> Optional[Dict[str, Any]]:
        """Decodes an envelope and returns its payload.

        Envelopes with an error status (not 2xx), or with a payload that is not a valid
        JSON object, are discarded.

        Expected Input Structure (`message`):
        ------------------------------------
        {
            "protocol": "RabbitMQ",
            "source": "predictor",
            "status": null,
            "content_type": "application/json",
            "metadata": {},
            "payload": b'{"target_time": "2026-10-22T09:00:00Z", "consumption": [1.2, 1.4]}'
        }

        Expected Output Structure:
        --------------------------
        {"target_time": "2026-10-22T09:00:00Z", "consumption": [1.2, 1.4]}

        Args:
            message (Union[dict, bytes, str]): Inbound raw envelope.

        Returns:
            Optional[Dict[str, Any]]: The decoded payload. None if the envelope cannot be used.
        """
        if isinstance(message, (bytes, bytearray, str)):
            try:
                message = json.loads(message if isinstance(message, str) else message.decode("utf-8"))
            except ValueError as err:
                self._logger.error(f"{self._LOG_PREFIX} Failed to decode envelope: {err}")
                return None

        if not isinstance(message, dict):
            return None

        # 1. Discard envelopes with an error status. Example: status 503 -> discarded; 200 -> kept;
        # no status at all -> kept (the check only applies when a number is given)
        status = message.get("status")
        if isinstance(status, int) and not isinstance(status, bool) and not 200 <= status < 300:
            self._logger.warning(
                f"{self._LOG_PREFIX} Skipping envelope with error status {status}: {message.get('source')}"
            )
            return None

        # 2. Decode the payload. Example: b'{"consumption": [...]}' or '{"consumption": [...]}' -> a dict
        payload = message.get("payload")
        if isinstance(payload, (bytes, bytearray, str)):
            try:
                payload = json.loads(payload if isinstance(payload, str) else payload.decode("utf-8"))
            except ValueError as err:
                self._logger.error(f"{self._LOG_PREFIX} Failed to decode payload: {err}")
                return None

        if not isinstance(payload, dict):
            self._logger.warning(
                f"{self._LOG_PREFIX} Skipping envelope whose payload is not a JSON object: {message.get('source')}"
            )
            return None

        return payload

    def _parse_target_time(self, raw_target_time: Any) -> Optional[datetime.datetime]:
        """Reads the time of the first value of the forecasts.

        The Predictor is expected to send the time with its offset ("Z" or "+01:00"), which is used as it is.
        A time without an offset is taken as UTC. A time that is missing or is not a valid date
        cannot place the values in time, so None is returned and the message is discarded.

        Expected Input / Output:
        ------------------------
        "2026-10-22T09:00:00Z"       -> 2026-10-22 09:00:00 UTC
        "2026-10-22T09:00:00+01:00"  -> 2026-10-22 08:00:00 UTC
        "2026-10-22T09:00:00"        -> 2026-10-22 09:00:00 UTC   (no offset: taken as UTC)
        None / "" / "abc"            -> None

        Args:
            raw_target_time (Any): The 'target_time' of the payload, as received.

        Returns:
            Optional[datetime.datetime]: The time in UTC, without microseconds. None if it is missing or invalid.
        """
        # --- Guard Clause 1: target_time must be present ---
        if raw_target_time is None or raw_target_time == "":
            self._logger.warning(f"{self._LOG_PREFIX} Forecast payload is missing 'target_time'.")
            return None

        # --- Guard Clause 2: target_time must be a valid ISO date ---
        try:
            # fromisoformat does not accept the "Z" suffix before Python 3.11, so it is replaced by "+00:00"
            parsed = datetime.datetime.fromisoformat(str(raw_target_time).replace("Z", "+00:00"))
        except ValueError:
            self._logger.warning(f"{self._LOG_PREFIX} Invalid target_time '{raw_target_time}'.")
            return None

        # No offset in the time: it is taken as UTC. Example: 09:00:00 -> 09:00:00 +00:00
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=datetime.timezone.utc)

        return parsed.astimezone(datetime.timezone.utc).replace(microsecond=0)

    def _build_values_array(self, start_time: datetime.datetime, values: list) -> List[Dict[str, Any]]:
        """Gives each value of a forecast its own time, one time step after the previous one.

        The steps are added in UTC and only then shown in the adapter timezone. Adding them in a local
        timezone would be wrong when the forecast crosses a clock change.

        Expected Input Structure (timezone "Europe/Lisbon", FORECAST_STEP_MINUTES = 15):
        -------------------------------------------------------------------------------
        start_time = 2026-10-22 09:00:00 UTC
        values = [1.2, 1.4, 1.1]

        Expected Output Structure:
        --------------------------
        [
            {"timestamp": "2026-10-22 10:00:00 +0100", "value": 1.2},
            {"timestamp": "2026-10-22 10:15:00 +0100", "value": 1.4},
            {"timestamp": "2026-10-22 10:30:00 +0100", "value": 1.1}
        ]

        Args:
            start_time (datetime.datetime): Time of the first value, in UTC.
            values (list): Forecast values in chronological order.

        Returns:
            List[Dict[str, Any]]: Canonical timestamp-value records.
        """
        return [
            {
                "timestamp": (start_time + datetime.timedelta(minutes=index * FORECAST_STEP_MINUTES))
                .astimezone(self._tz)
                .strftime("%Y-%m-%d %H:%M:%S %z"),
                "value": value
            }
            for index, value in enumerate(values)
        ]

    @register_entity_type_strategy(Label.FORECAST_SERVICE.value)
    def _forecast_service(
        self,
        entity_id: str,
        payload: dict,
        start_time: datetime.datetime
    ) -> Dict[str, Dict[str, Any]]:
        """Reads, for each parameter of the entity, the forecast under the key of its rule.

        The forecast of each parameter is chosen by its rule (the 'match_key'). A forecast that is
        missing from the payload, or is not a non-empty list, is skipped. The values are not type-checked here;
        that is done by parameters_validation.

        Expected Input (`payload`, with the example configuration, for entity "PR01"):
        -----------------------------------------------------------------------------
        {"target_time": "2026-10-22T09:00:00Z", "consumption": [1.2, 1.4], "production": [0.0, 0.3]}

        Expected Output (timezone "Europe/Lisbon"):
        -------------------------------------------
        {
            "pr01_consumption": {
                "parameter_type_id": "energy_consumption_prediction",
                "values": [
                    {"timestamp": "2026-10-22 10:00:00 +0100", "value": 1.2},
                    {"timestamp": "2026-10-22 10:15:00 +0100", "value": 1.4}
                ]
            },
            "pr01_production": {
                "parameter_type_id": "solar_energy_prediction",
                "values": [
                    {"timestamp": "2026-10-22 10:00:00 +0100", "value": 0.0},
                    {"timestamp": "2026-10-22 10:15:00 +0100", "value": 0.3}
                ]
            }
        }

        Args:
            entity_id (str): Id of the entity.
            payload (dict): Decoded payload.
            start_time (datetime.datetime): Time of the first value of the forecasts, in UTC.

        Returns:
            Dict[str, Dict[str, Any]]: The forecast of each parameter that has one, by parameter id.
        """
        found: Dict[str, Dict[str, Any]] = {}

        for param_id, param_spec in self._entities.get(entity_id, {}).get("parameters", {}).items():
            # Forecast this parameter wants. Example: "pr01_consumption" -> "consumption"
            forecast_key = self._forecast_keys.get(param_id)
            if forecast_key is None:
                continue

            parameter_type_id = param_spec.get("parameter_type_id")
            if not parameter_type_id:
                self._logger.warning(
                    f"{self._LOG_PREFIX} Parameter '{param_id}' of entity '{entity_id}' (parameter type: {self._parameter_type(entity_id, param_id)}): it has no "
                    f"'parameter_type_id' configured. Skipping its forecast."
                )
                continue

            values = payload.get(forecast_key)

            # --- Guard Clause: the forecast must be a non-empty list ---
            # Example: "consumption" missing, null, [] or "abc" -> skipped
            if not isinstance(values, list) or not values:
                # Warned once per parameter: the Predictor keeps sending messages, and the same warning
                # would otherwise be logged again in each one
                if param_id not in self._missing_forecast_warned:
                    self._missing_forecast_warned.add(param_id)
                    self._logger.warning(
                        f"{self._LOG_PREFIX} Parameter '{param_id}' of entity '{entity_id}' (parameter type: {self._parameter_type(entity_id, param_id)}): "
                        f"forecast '{forecast_key}' is missing, empty or not a list in the payload."
                    )
                continue

            # The forecast is back, so a new disappearance is warned about again
            self._missing_forecast_warned.discard(param_id)

            found[param_id] = {
                "parameter_type_id": parameter_type_id,
                "values": self._build_values_array(start_time, values)
            }

        return found