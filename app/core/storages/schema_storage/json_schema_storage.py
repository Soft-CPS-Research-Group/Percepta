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
    (see __init__). It is returned as it is, because it is already written in the format that every Schema
    Storage returns (see SchemaStorageBase.get_environments). The storage does not check what is inside the
    environments: that is done by `EnvironmentsConfiguration`, for every type of storage.

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
        """Validates the connection info and reads the JSON file.

        Expected `connection_info` Data Structure:
        ------------------------------------------
        {
            "file_path": "environment_files/environments.json"     # Path of the JSON file
        }

        Args:
            connection_info (dict): Settings to access the storage, as detailed above.
            logger (LoggingUtils): Structured telemetry logging interface.

        Raises:
            ValueError: If 'file_path' is missing, or the file is not valid JSON or has no JSON object.
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

        self._environments = content

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
