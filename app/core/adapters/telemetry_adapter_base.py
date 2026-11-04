from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional, Union
from app.core.utils.type_compatibility import is_type_compatible
from app.core.utils.logger import LoggingUtils
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
                    self._logger.info(
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