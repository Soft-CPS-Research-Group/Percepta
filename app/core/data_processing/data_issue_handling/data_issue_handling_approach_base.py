# --- Standard Library Imports ---
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List

# --- Internal Framework Imports ---
from app.core.utils.logger import LoggingUtils


class DataIssueHandlingApproachBase(ABC):
    """Abstract base class of all Data-Issue Handling Approaches.

    A Data-Issue Handling Approach detects and corrects ONE type of issue in the data that the Data Processing
    container received from Data Ingestion during a period (e.g., missing data, duplicated data, out-of-order data,
    different reporting frequencies). Each approach does only its own job, and the Data Processing Service applies
    several of them one after the other, in the order configured in 'data_issue_handling_approaches' of the Percepta
    Instance Configuration. The output of one approach is the input of the next, so the order matters.

    One instance of an approach is created for each environment, and the same instance is used in every period of that
    environment. So an approach may keep state between periods (e.g., the Missing Data approach remembers the most
    recent values of each entity, to rebuild an entity that stops reporting). Nothing is shared between environments.

    The structure of the message must be kept: at most, an approach may ADD fields to it (e.g., a field that flags a
    reading as estimated). It must not remove or rename any field, or change its type, because the next
    approaches, the combination logic and the data-derivation logic all expect the structure described in
    'handle'. Inside that structure, an approach is free to change the data itself:
    - the readings in the lists 'values': it can remove readings (e.g., the repeated ones), add readings (e.g., the
      missing ones), change their order or change their values;
    - the entities in the message: it can add an entity that did not report any data in the period.

    To add a new Data-Issue Handling Approach, create a class that inherits from this one and register it in the
    Data-Issue Handling Approaches Launcher with an id (see the README of this folder).
    """

    _LOG_PREFIX: str = "Data-Issue Handling Approach |"

    def __init__(self, environment: Dict[str, Any], frequency_seconds: float, logger: LoggingUtils) -> None:
        """Initializes the approach.

        This base class only keeps what it receives. The Data-Issue Handling Approaches Launcher has already checked
        that they have the right type. An approach that needs more than that (e.g., a parameter in the schema) checks
        it in its own constructor.

        Expected Input Structure:
        -------------------------
        environment = {
            "group": "group_1",
            "entities": {
                "entity_1_id": {
                    "entity_type_id": "entity_type_1",
                    "parameters": {
                        "parameter_1_id": {
                            "parameter_type_id": "parameter_type_1",
                            "measurement_unit": "unit_1",
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
            ...                                 # The rest of the environment is not used by the approaches
        }
        frequency_seconds = 15                  # Already in seconds (e.g., 300 for a frequency of 5 minutes)

        Args:
            environment (Dict[str, Any]): The environment this approach works for, as returned by the Schema Storage.
                Its entities and the temporal behaviour of their parameters tell the approach what data to expect.
            frequency_seconds (float): How often the data is processed, in seconds. It is the length of a period.
                It arrives already converted (the InstanceConfiguration does it), so the approaches do not have to
                read the unit.
            logger (LoggingUtils): Logger of the environment.
        """
        self._environment = environment
        self._frequency_seconds = frequency_seconds
        self._logger = logger

    @abstractmethod
    def handle(self, data: Dict[str, Dict[str, Any]], period_end: datetime) -> Dict[str, Dict[str, Any]]:
        """Corrects one type of issue in the data of a period.

        Expected Input Structure:
        -------------------------
        data = {
            "entity_1_id": {
                "entity_type_id": "entity_type_1",
                "parameters": {
                    "parameter_1_id": {
                        "parameter_type_id": "parameter_type_1",
                        "values": [
                            {"timestamp": "2026-08-19 10:55:00 +0000", "value": 10.2},
                            {"timestamp": "2026-08-19 11:00:00 +0000", "value": 12.5}
                        ]
                    }
                },
                "timestamp": "2026-08-19 11:00:00 +0000"       # Arrival time of the most recent message
            }
        }
        period_end = datetime(2026, 8, 19, 11, 0, 0, tzinfo=timezone.utc)

        Expected Output Structure:
        --------------------------
        The same structure as the input, with the issue corrected. For example, the Duplicated Data approach returns
        the lists 'values' without the repeated timestamps. At most, the approach may add fields to the entities and
        parameters (e.g., a flag on a reading). It must not remove or rename any field of the structure above.

        Args:
            data (Dict[str, Dict[str, Any]]): The data received during the period (and during the grace period, if any),
                one dictionary for each entity that reported data.
            period_end (datetime): The end of the period being processed, with a timezone (e.g., 11:00:00 for the
                period that started at 10:45:00, when the frequency is 15 minutes). The approaches that create data
                use it to give it the timestamps of the current period.

        Returns:
            Dict[str, Dict[str, Any]]: The data with the issue corrected, in the same structure. It may be the same dictionary
                that was received, changed.
        """
        raise NotImplementedError()
