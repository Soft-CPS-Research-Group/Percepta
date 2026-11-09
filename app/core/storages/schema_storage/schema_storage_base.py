# --- Standard Library Imports ---
from abc import ABC, abstractmethod
from typing import Any, Dict

# --- Internal Framework Imports ---
from app.core.utils.logger import LoggingUtils


class SchemaStorageBase(ABC):
    """Abstract base class of all Schema Storages.

    A Schema Storage keeps the structure of the environments of a Percepta instance: their entities
    (with their parameters), the Telemetry Systems that feed them (with the source mapping of each one),
    the Target Systems that receive the decisions (with the actuator mapping of each one) and the
    Machine Learning model used by each environment. How that information is kept (a JSON file, a
    relational database, a NoSQL database...) is up to each implementation, but all of them return it
    in the same format, so the rest of Percepta does not depend on the technology used.

    A Schema Storage does not check that the ids it returns exist in the Percepta Instance Configuration
    (the ids of the Telemetry Systems, of the Target Systems and of the Machine Learning models). It only
    returns what it keeps. Each container checks the ids it uses when it launches an environment (e.g.,
    the Data Ingestion Launcher checks the Telemetry Systems).

    To add a new type of Schema Storage, create a class that inherits from this one, and register it
    in the Schema Storage Launcher with an id (see the README of this folder).
    """

    _LOG_PREFIX: str = "Schema Storage Base |"

    def __init__(self, connection_info: dict, logger: LoggingUtils) -> None:
        """Initializes the Schema Storage.

        The implementation validates its own 'connection_info' before it tries to connect or read anything,
        so that an invalid configuration is reported clearly.

        Args:
            connection_info (dict): Settings needed to access the storage. Their content depends on the type
                of storage (e.g., a JSON storage needs the path of the file).
            logger (LoggingUtils): Structured telemetry logging interface.
        """
        self._connection_info = connection_info
        self._logger = logger

    @abstractmethod
    def get_environments(self) -> Dict[str, Dict[str, Any]]:
        """Returns all the environments of the storage.

        Expected Output Structure:
        --------------------------
        {
            "R-H-01": {
                "group": "living lab",
                "entities": {
                    "PV01": {
                        "entity_type_id": "pv_panel",
                        "parameters": {
                            "tefhr": {
                                "parameter_type_id": "energy",
                                "measurement_unit": "kWh",
                                "temporal_behavior": {
                                    "cumulative": True,
                                    "periodicity": {"value": 5, "unit": "min"},
                                    "aggregation_operation": "sum",
                                    "fill_operation": "linear"
                                },
                                "necessary": True
                            }
                        }
                    }
                },
                "telemetry_systems": {
                    "cleanwatts": {
                        "source_mapping": {
                            "tefhr": {"fetch_key": "33711", "match_key": "33711"}
                        }
                    }
                },
                "target_systems": {
                    "softcps": {
                        "actuator_mapping": {
                            "b01_setpoint": {"destination": "battery_power_setpoint",
                                             "destination_measurement_unit": "kW"},
                            "ev1_setpoint": {"destination": "ev_1", "destination_measurement_unit": "A", "voltage": 230}
                        }
                    }
                },
                "ml_model_id": "model_1"
            }
        }

        Returns:
            Dict[str, Dict[str, Any]]: The environments, by id. The caller may change the result without
                changing the storage.
        """
        raise NotImplementedError()