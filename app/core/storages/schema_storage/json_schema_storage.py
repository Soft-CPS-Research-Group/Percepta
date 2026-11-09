# --- Standard Library Imports ---
import copy
import os
from typing import Any, Dict

# --- Internal Framework Imports ---
from app.core.storages.schema_storage.schema_storage_base import SchemaStorageBase
from app.core.utils.json_reader import read_json_object
from app.core.utils.logger import LoggingUtils


class JSONSchemaStorage(SchemaStorageBase):
    """Schema Storage that gets the data of the environments from a JSON file.

    The file is read once, when the storage is created, and its path is given in the 'connection_info'
    (see __init__). It is checked and then returned as it is, because it is already written in the format
    that every Schema Storage returns (see SchemaStorageBase.get_environments).

    Expected File Structure:
    ------------------------
    {
        "environment_1_id": {
            "group": "group_1",
            "entities": {
                "entity_1_id": {
                    "entity_type_id": "entity_type_1",
                    "parameters": {"parameter_1_id": {"parameter_type_id": "parameter_type_1"}}
                },
                "entity_2_id": {
                    "entity_type_id": "entity_type_2",
                    "actuators": {"actuator_1_id": {"actuator_type_id": "actuator_type_1"}}
                }
            },
            "telemetry_systems": {
                "telemetry_system_1_id": {
                    "source_mapping": {"parameter_1_id": {"fetch_key": "external_id_1", "match_key": "external_id_1"}}
                },
                "telemetry_system_2_id": {
                    "source_mapping": {"parameter_2_id": {"match_key": "external_id_2"}}
                }
            },
            "target_systems": {
                "target_system_1_id": {
                    "actuator_mapping": {
                        "actuator_1_id": {
                            "destination": "external_destination_1",
                            "destination_measurement_unit": "kW"    # Mandatory: unit of the real device
                        },
                        "actuator_2_id": {
                            "destination": "external_destination_2",
                            "destination_measurement_unit": "A",
                            "voltage": 230                      # (optional) Volts, to convert kW into amperes
                        }
                    }
                }
            },
            "ml_model_id": "ml_model_1"
        },
        "environment_2_id": {
            ...
        }
    }
    """

    _LOG_PREFIX: str = "JSON Schema Storage |"

    def __init__(self, connection_info: dict, logger: LoggingUtils) -> None:
        """Validates the connection info, reads the JSON file and builds the environments.

        Expected `connection_info` Data Structure:
        ------------------------------------------
        {
            "file_path": "environment_files/environments.json"     # Path of the JSON file
        }

        Args:
            connection_info (dict): Settings to access the storage, as detailed above.
            logger (LoggingUtils): Structured telemetry logging interface.

        Raises:
            ValueError: If 'file_path' is missing, the file is not valid JSON, or its structure is invalid.
            FileNotFoundError: If the file does not exist.
        """
        super().__init__(connection_info, logger)

        # --- Guard Clause 1: connection_info must be a dictionary with the path of the file ---
        file_path = connection_info.get("file_path") if isinstance(connection_info, dict) else None
        if not isinstance(file_path, str) or not file_path.strip():
            raise ValueError(
                f"{self._LOG_PREFIX} Initialization error: 'connection_info.file_path' must be a non-empty string."
            )

        # --- Guard Clause 2: the file must exist ---
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"{self._LOG_PREFIX} Initialization error: file '{file_path}' does not exist.")

        try:
            content = read_json_object(file_path)
        except ValueError as err:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: {err}") from err

        self._environments = self._build_environments(content, file_path)

        self._logger.info(
            f"{self._LOG_PREFIX} Loaded {len(self._environments)} environment(s) from '{file_path}': "
            f"{list(self._environments)}."
        )

    def get_environments(self) -> Dict[str, Dict[str, Any]]:
        """Returns all the environments, in the format of the Schema Storage.

        Returns:
            Dict[str, Dict[str, Any]]: The environments, by id. It is a copy, so changing it does not
                change the storage.
        """
        return copy.deepcopy(self._environments)

    def _build_environments(self, content: dict, file_path: str) -> Dict[str, Dict[str, Any]]:
        """Checks the environments of the file and returns a copy of them.

        Everything an environment defines (e.g., 'group') is kept as it is.

        Expected Input / Output Structure:
        ----------------------------------
        {
            "environment_1_id": {
                "group": "group_1",
                "entities": {},
                "telemetry_systems": {"telemetry_system_1_id": {"source_mapping": {}}},
                "target_systems": {"target_system_1_id": {"actuator_mapping": {}}},
                "ml_model_id": "ml_model_1"
            }
        }

        'entities' is mandatory. 'telemetry_systems', 'target_systems' and 'ml_model_id' are optional, but
        when they are given they must have the structure above. An environment with no 'telemetry_systems' is
        accepted (the Application Launcher refuses to launch it).

        Args:
            content (dict): Content of the file.
            file_path (str): Path of the file, for the error messages.

        Returns:
            Dict[str, Dict[str, Any]]: The environments, by id.

        Raises:
            ValueError: If there are no environments, or an environment, a Telemetry System, a Target System
                or the Machine Learning model id is invalid.
        """
        if not content:
            raise ValueError(f"{self._LOG_PREFIX} File '{file_path}' has no environments.")

        for name, specs in content.items():
            if not isinstance(specs, dict) or not isinstance(specs.get("entities"), dict):
                raise ValueError(
                    f"{self._LOG_PREFIX} Environment '{name}' of '{file_path}' must be a dictionary with 'entities'."
                )

            systems = specs.get("telemetry_systems", {})
            if not isinstance(systems, dict):
                raise ValueError(
                    f"{self._LOG_PREFIX} 'telemetry_systems' of environment '{name}' must be a dictionary, "
                    f"keyed by the id of the Telemetry System."
                )
            for system_id, system in systems.items():
                if not isinstance(system, dict) or not isinstance(system.get("source_mapping"), dict):
                    raise ValueError(
                        f"{self._LOG_PREFIX} Telemetry System '{system_id}' of environment '{name}' must be a "
                        f"dictionary with 'source_mapping'."
                    )

            targets = specs.get("target_systems", {})
            if not isinstance(targets, dict):
                raise ValueError(
                    f"{self._LOG_PREFIX} 'target_systems' of environment '{name}' must be a dictionary, "
                    f"keyed by the id of the Target System."
                )
            for target_id, target in targets.items():
                if not isinstance(target, dict) or not isinstance(target.get("actuator_mapping"), dict):
                    raise ValueError(
                        f"{self._LOG_PREFIX} Target System '{target_id}' of environment '{name}' must be a "
                        f"dictionary with 'actuator_mapping'."
                    )
                for actuator_id, mapping in target["actuator_mapping"].items():
                    self._validate_actuator_mapping(mapping, f"environment '{name}', Target System '{target_id}', "
                                                             f"actuator '{actuator_id}'")

            ml_model_id = specs.get("ml_model_id")
            if "ml_model_id" in specs and (not isinstance(ml_model_id, str) or not ml_model_id.strip()):
                raise ValueError(
                    f"{self._LOG_PREFIX} 'ml_model_id' of environment '{name}' must be a non-empty string."
                )

        return copy.deepcopy(content)

    def _validate_actuator_mapping(self, mapping: Any, location: str) -> None:
        """Checks what the 'actuator_mapping' of a Target System says about one actuator.

        It is a dictionary with the destination and the unit of the real device.

        Expected Input Structure:
        -------------------------
        {
            "destination": "39052",                     # Mandatory: where the decisions of the actuator go
            "destination_measurement_unit": "A",        # Mandatory: unit of the device. The decisions are in kW, and the
                                                        # Target System converts them. Which units are accepted depends
                                                        # on the Target System
            "voltage": 230                              # (optional) Volts, positive. To convert kW into amperes
        }

        Args:
            mapping (Any): The value of the actuator in the 'actuator_mapping'.
            location (str): Where the value is, for the error message.

        Raises:
            ValueError: If the value is not a dictionary as above, has a key that is not known, or lacks the destination or
                the unit.
        """
        def is_destination(value: Any) -> bool:
            return (isinstance(value, str) and bool(value.strip())) or (
                isinstance(value, int) and not isinstance(value, bool))

        # The destination alone is no longer accepted: the unit of the device is mandatory
        if not isinstance(mapping, dict):
            raise ValueError(
                f"{self._LOG_PREFIX} The mapping of {location} must be a dictionary with 'destination' and "
                f"'destination_measurement_unit', got {mapping!r}."
            )

        unknown = sorted(set(mapping) - {"destination", "destination_measurement_unit", "voltage"})
        if unknown:
            raise ValueError(
                f"{self._LOG_PREFIX} The mapping of {location} has unknown keys {unknown}. Known keys: "
                f"['destination', 'destination_measurement_unit', 'voltage']."
            )
        if not is_destination(mapping.get("destination")):
            raise ValueError(f"{self._LOG_PREFIX} The mapping of {location} needs a 'destination' (text or integer).")

        unit = mapping.get("destination_measurement_unit")
        if not isinstance(unit, str) or not unit.strip():
            raise ValueError(
                f"{self._LOG_PREFIX} The mapping of {location} needs a 'destination_measurement_unit' (non-empty text)."
            )

        voltage = mapping.get("voltage")
        if "voltage" in mapping and (
                isinstance(voltage, bool) or not isinstance(voltage, (int, float)) or voltage <= 0):
            raise ValueError(f"{self._LOG_PREFIX} The 'voltage' of {location} must be a positive number.")
