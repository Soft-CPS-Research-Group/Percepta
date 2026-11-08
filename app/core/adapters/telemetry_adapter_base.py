from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Iterable, List, Optional, Union
from app.core.utils.type_compatibility import is_type_compatible
from app.core.utils.logger import LoggingUtils
from app.core.utils.unit_utils import build_converter
from zoneinfo import ZoneInfo


class TelemetryAdapterBase(ABC):
    """Base domain adapter."""

    _LOG_PREFIX: str = "TelemetryAdapterBase |"

    def __init__(
        self,
        environment: str,
        timezone: str,
        logger: LoggingUtils
    ) -> None:
        """
        Initializes the Telemetry Adapter with its environment context and dependencies.

        Args:
            environment (str): Identifier of the environment this Adapter translates data for.
            timezone (str): IANA timezone name used to resolve timestamps (e.g., "Europe/Lisbon").
            logger (LoggingUtils): Structured telemetry logging interface.

        Raises:
            ValueError: If 'logger' or 'environment' dependencies are missing or invalid.
        """
        if not logger:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'logger' dependency is missing.")

        self._logger = logger

        if not environment or not isinstance(environment, str) or not environment.strip():
            error_msg = f"{self._LOG_PREFIX} Initialization error: 'environment' must be a valid non-empty string."
            self._logger.error(error_msg)
            raise ValueError(error_msg)

        self._environment = environment
        self._tz = ZoneInfo(timezone)

        # For each parameter, the function that converts its readings from the unit of the source to the unit of the
        # schema. They are filled by `_prepare_unit_conversion`, which each adapter calls when it starts.
        self._unit_converters: Dict[str, Callable[[Any], Any]] = {}
        self._parameter_units: Dict[str, tuple] = {}
        self._unit_errors_logged: set = set()

    @abstractmethod
    def adapt(self, data: Any) -> None:
        """
           Translate incoming device messages into a standardized format.

           Args:
               data (Any): Raw data from the source system.
                   Each subclass is responsible for handling its expected type:
                   - bytes (JSON encoded string with "observation")
                   - dict with "messages", "label", "entity_id"
           """
        raise NotImplementedError()

    def _prepare_unit_conversion(self, entities: dict, source_mapping_rules: dict) -> None:
        """Reads the units of the parameters, to convert their readings to the unit of the schema.

        Every parameter that has a rule in the source mapping and a 'measurement_unit' in the schema must say, in its
        rule, the unit in which the source gives its readings: 'source_measurement_unit'. The adapter converts the
        readings from that unit to the 'measurement_unit' of the schema (see `convert_to_schema_units`).

        This method only checks that the 'source_measurement_unit' is there. It does NOT check that the two units can be
        converted into each other: that is only checked when a reading is converted. This way, a parameter with units of
        another kind (e.g., "A" and "kW"), which an adapter translates in its own way, does not stop the adapter from
        starting.

        Expected Input Structure (`entities`):
        --------------------------------------
        {
            "GR01": {
                "parameters": {
                    "energy_in_l1": {"parameter_type_id": "energy_in_l1", "measurement_unit": "kWh"}
                }
            }
        }

        Expected Input Structure (`source_mapping_rules`):
        --------------------------------------------------
        {
            "energy_in_l1": {"match_key": "a_total_act_energy", "source_measurement_unit": "Wh"},
            "connected_vehicle": None
        }

        A rule that is None, a parameter with no rule, and a parameter with no 'measurement_unit' in the schema (e.g., a
        flag) are not converted and need no 'source_measurement_unit'.

        Args:
            entities (dict): The entities of the environment, with their parameters (the schema).
            source_mapping_rules (dict): The rules of the parameters of this Telemetry System, by parameter id.

        Raises:
            ValueError: If a parameter has a unit in the schema but no 'source_measurement_unit' in its rule.
        """
        # For each parameter that is converted: (unit of the source, unit of the schema).
        # Example: {"energy_in_l1": ("Wh", "kWh")}
        self._parameter_units: Dict[str, tuple] = {}
        self._unit_converters = {}
        self._unit_errors_logged: set = set()

        for entity_id, entity in entities.items():
            for parameter_id, parameter in (entity.get("parameters") or {}).items():
                schema_unit = parameter.get("measurement_unit")
                rule = source_mapping_rules.get(parameter_id)

                # A parameter with no rule, or with no unit in the schema, is not converted
                if not isinstance(rule, dict) or not schema_unit:
                    continue

                # --- Guard Clause: the unit of the source is mandatory ---
                source_unit = rule.get("source_measurement_unit")
                if not isinstance(source_unit, str) or not source_unit.strip():
                    error_msg = (
                        f"{self._LOG_PREFIX} Initialization error: parameter '{parameter_id}' of entity "
                        f"'{entity_id}' has no 'source_measurement_unit' in its source mapping."
                    )
                    self._logger.error(error_msg)
                    raise ValueError(error_msg)

                self._parameter_units[parameter_id] = (source_unit, schema_unit)

    def convert_to_schema_units(self, parameters: dict, skip: Iterable[str] = ()) -> dict:
        """Converts the readings of the parameters from the unit of the source to the unit of the schema.

        It must be called with the readings as the source gave them, before `parameters_validation`. The readings are
        changed in place. A reading whose value is not a number is left as it is, and so is a parameter with no
        conversion to do (the same unit, or no unit in the schema).

        If the two units of a parameter cannot be converted into each other (e.g., "A" and "kW"), the conversion of
        that parameter fails: the error is logged (once for the parameter) and its readings are removed from the message,
        because they would reach the next container in the wrong unit. The other parameters are not affected.
        A parameter that the adapter translated in its own way must be given in `skip`, so that it is left as it is.

        Expected Input Structure:
        -------------------------
        {"energy_in_l1": {"parameter_type_id": "energy_in_l1",
                          "values": [{"timestamp": "2026-08-19 16:15:31 +0000", "value": 45200.0}]}}

        Expected Output Structure (with 'source_measurement_unit' "Wh" and 'measurement_unit' "kWh"):
        ---------------------------------------------------------------------------------------------
        {"energy_in_l1": {"parameter_type_id": "energy_in_l1",
                          "values": [{"timestamp": "2026-08-19 16:15:31 +0000", "value": 45.2}]}}

        Args:
            parameters (dict): The parameters to send, by parameter id, with their lists of readings.
            skip (Iterable[str]): Ids of the parameters that the adapter already translated and that must not be
                converted.

        Returns:
            dict: The same dictionary, with the values converted.
        """
        skip = set(skip)

        for parameter_id in list(parameters):
            parameter = parameters[parameter_id]
            units = self._parameter_units.get(parameter_id)
            if units is None or parameter_id in skip or not isinstance(parameter, dict):
                continue

            # The function is built the first time the parameter is converted, and used again after that.
            converter = self._unit_converters.get(parameter_id)
            if converter is None:
                try:
                    converter = self._unit_converters[parameter_id] = build_converter(*units)
                except ValueError as err:
                    # The error is logged only once per parameter, so that a wrong configuration does not fill the log
                    if parameter_id not in self._unit_errors_logged:
                        self._unit_errors_logged.add(parameter_id)
                        self._logger.error(
                            f"{self._LOG_PREFIX} Parameter '{parameter_id}': {err} Its readings are not sent. "
                            f"Translate it in the adapter and pass it in 'skip'."
                        )
                    del parameters[parameter_id]
                    continue

            for reading in parameter.get("values") or []:
                if isinstance(reading, dict) and "value" in reading:
                    reading["value"] = converter(reading["value"])

        return parameters

    def parameters_validation(
            self,
            parameters_to_send: dict,
            entity_parameters: dict,
            parameter_type_expected_types: Optional[Dict[str, type]] = None,
    ) -> dict:
        """
        Validates the parameters present in a message. It does not check whether
        other parameters of the entity are missing, as a message may carry only
        some of them.
        Parameters with an empty list of readings are set to None, with metadata.
        Parameters already flagged upstream (with 'values' set to None and a 'metadata'
        message) are left untouched, so their own message is preserved.
        When `parameter_type_expected_types` is provided, also validates each reading's
        value against the expected native type for its parameter type id, and adds
        metadata if invalid. If a parameter's type id is not in that mapping, its type
        is not validated, and an info message is logged.

        Expected Input Structure (`parameters_to_send`):
        ------------------------------------------------
        {
            "35927": {
                "parameter_type_id": "energy_in",
                "values": [
                    {"timestamp": "2026-08-19 11:00:00 +0100", "value": 12.5}
                ]
            },
            "33690": {
                "parameter_type_id": "state_of_charge",
                "values": []  # Empty reading list -> will be flagged
            },
            "33692": {
                "parameter_type_id": "power",
                "values": None,
                "metadata": "Parameter readings malformed"  # Already flagged upstream -> kept as is
            }
        }

        Expected Input Structure (`entity_parameters`):
        -----------------------------------------------
        {
            "35927": {"parameter_type_id": "energy_in"},
            "33690": {"parameter_type_id": "state_of_charge"},
            "33692": {"parameter_type_id": "power"}
        }

        Expected Input Structure (`parameter_type_expected_types`):
        -------------------------------------------------------------
        {
            "energy_in": float,
            "state_of_charge": float,
            "power": float
        }

        Expected Output Structure:
        --------------------------
        {
            "35927": {
                "parameter_type_id": "energy_in",
                "values": [
                    {"timestamp": "2026-08-19 11:00:00 +0100", "value": 12.5}
                ]
            },
            "33690": {
                "parameter_type_id": "state_of_charge",
                "values": None,
                "metadata": "Parameter sent empty by receiver"
            },
            "33692": {
                "parameter_type_id": "power",
                "values": None,
                "metadata": "Parameter readings malformed"
            }
        }

        Args:
            parameters_to_send (dict): Dictionary of parameter values to validate (keyed by parameter ID).
            entity_parameters (dict): Dictionary of entity parameter definitions (keyed by parameter ID).
            parameter_type_expected_types (Dict[str, type], optional): Mapping from
                parameter type id to its expected native type (e.g., float, str).
                Specific to the use case; if omitted, value-type validation is skipped.

        Returns:
            dict: The updated dictionary with empty/invalid parameters handled.
        """
        for param_id, param_entry in list(parameters_to_send.items()):
            parameter_type_id = entity_parameters.get(param_id, {}).get("parameter_type_id", param_id)

            # Already flagged upstream: keep its own metadata message as is
            if (
                    isinstance(param_entry, dict)
                    and "values" in param_entry
                    and param_entry["values"] is None
                    and "metadata" in param_entry
            ):
                continue

            # Extracts readings list whether structure is a raw list or {"parameter_type_id": ..., "values": [...]}
            if isinstance(param_entry, dict) and "values" in param_entry:
                param_values = param_entry.get("values")
            else:
                param_values = param_entry

            # Treat empty list as None with metadata
            if isinstance(param_values, list) and len(param_values) == 0:
                parameters_to_send[param_id] = {
                    "parameter_type_id": parameter_type_id,
                    "values": None,
                    "metadata": "Parameter sent empty by receiver"
                }
                continue

            # Validate each reading's value, if a type mapping was provided
            if parameter_type_expected_types is not None:
                expected_type = parameter_type_expected_types.get(parameter_type_id)

                if expected_type is None:
                    self._logger.debug(
                        f"{self._LOG_PREFIX} Parameter type '{parameter_type_id}' of parameter '{param_id}' "
                        f"is not registered in parameter_type_expected_types. Skipping type validation."
                    )
                    continue

                if not isinstance(param_values, list) or any(
                        not isinstance(entry, dict) or not is_type_compatible(entry.get("value"), expected_type)
                        for entry in param_values
                ):
                    parameters_to_send[param_id] = {
                        "parameter_type_id": parameter_type_id,
                        "values": None,
                        "metadata": "Parameter value invalid"
                    }

        return parameters_to_send

    def _parameter_type(self, entity_id: str, param_id: str) -> str:
        """Returns the parameter type of a parameter, as the environment says, to be shown in the logs.

        Args:
            entity_id (str): Id of the entity.
            param_id (str): Id of the parameter.

        Returns:
            str: The 'parameter_type_id' of the parameter, or "unknown" if the environment does not have it.
        """
        entities = getattr(self, "_entities", None) or {}
        parameter = (entities.get(entity_id) or {}).get("parameters", {}).get(param_id) or {}
        return str(parameter.get("parameter_type_id", "unknown"))

    @staticmethod
    def _message_creator(
            parameters: Dict[str, Dict[str, Any]],
            entity_id: str,
            entity_type_id: str,
            timestamp: str
    ) -> Dict[str, Any]:
        """Creates a standardized canonical entity reading envelope.

        Expected Input Arguments:
        -------------------------
        - parameters:
            {
                "energy_in": {
                    "parameter_type_id": "energy_in",
                    "values": [
                        {"timestamp": "2026-08-19 11:00:00 +0100", "value": 12.5}
                    ]
                },
                "state_of_charge": {
                    "parameter_type_id": "state_of_charge",
                    "values": [
                        {"timestamp": "2026-08-19 11:00:00 +0100", "value": 85.0}
                    ]
                }
            }
        - entity_id: "B01"
        - entity_type_id: "battery"
        - timestamp: "2026-08-19 11:00:05 +0100"

        Expected Output Structure:
        --------------------------
        {
            "id": "B01",
            "entity_type_id": "battery",
            "parameters": {
                "energy_in": {
                    "parameter_type_id": "energy_in",
                    "values": [
                        {"timestamp": "2026-08-19 11:00:00 +0100", "value": 12.5}
                    ]
                },
                "state_of_charge": {
                    "parameter_type_id": "state_of_charge",
                    "values": [
                        {"timestamp": "2026-08-19 11:00:00 +0100", "value": 85.0}
                    ]
                }
            },
            "timestamp": "2026-08-19 11:00:05 +0100"
        }

        Args:
            parameters (Dict[str, Dict[str, Any]]): Structured parameter reading values with their types.
            entity_id (str): Target entity unique identifier.
            entity_type_id (str): Target entity type id.
            timestamp (str): ISO/timezone-aware reading timestamp string.

        Returns:
            Dict[str, Any]: Standardized canonical message dictionary.
        """
        return {
            "id": entity_id,
            "entity_type_id": entity_type_id,
            "parameters": parameters,
            "timestamp": timestamp
        }