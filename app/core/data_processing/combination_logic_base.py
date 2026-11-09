# --- Standard Library Imports ---
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List

# --- Internal Framework Imports ---
from app.core.utils.logger import LoggingUtils


class CombinationLogicBase(ABC):
    """Abstract base class of the Combination Logic of an environment.

    The Combination Logic is applied by the Data Processing Service after all the Data-Issue Handling Approaches.
    It takes the corrected data that the environment's parameters reported during a period, and combines it into ONE
    message, which is the message that goes on to the next step (the Data-Derivation Logic, if there is one, or
    the Inference container).

    The input is always in the same format (the one described in 'combine'). The output is NOT fixed: each Percepta
    instance produces its own structure.

    One instance of the logic is created for each environment, and the same instance is used in every period of that
    environment. Nothing is shared between environments.

    To integrate a Combination Logic, create a class that inherits from this one, implement 'combine', and put the
    path of the class in 'integrated_logic.combination_logic' of the Percepta Instance Configuration.
    """

    _LOG_PREFIX: str = "Combination Logic |"

    def __init__(self, environment: Dict[str, Any], settings: Dict[str, Any], logger: LoggingUtils) -> None:
        """Initializes the logic.

        This base class only keeps what it receives. A logic that needs more than that checks it in its own
        constructor.

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
            ...                                 # The rest of the environment, as returned by the Schema Storage
        }
        settings = {
            "timezone": "UTC",                  # The timezone of the periods
            "frequency_seconds": 600,           # The length of a period, in seconds
            "grace_period_seconds": 2           # Wait after the end of a period before processing it
        }

        Args:
            environment (Dict[str, Any]): The environment this logic works for, as returned by the Schema Storage. It
                tells the logic which entities and parameters exist, their types, and how each parameter behaves in
                time (e.g., if it is cumulative).
            settings (Dict[str, Any]): The settings of the Data Processing container (the same ones the Data
                Processing Service uses). The logic may need them, e.g., 'frequency_seconds' is the length of
                the period that it combines.
            logger (LoggingUtils): Logger of the environment.
        """
        self._environment = environment
        self._settings = settings
        self._logger = logger

    @abstractmethod
    def combine(self, data: Dict[str, Dict[str, Any]], period_end: datetime) -> Dict[str, Any]:
        """Combines the data of a period into one message.

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
        Any JSON-compatible dictionary: the structure that the next step and the ML Model expect.

        Args:
            data (Dict[str, Dict[str, Any]]): The data of the period, already treated by the Data-Issue Handling
                Approaches, by the id of the entity, one entry for each entity that reported data.
            period_end (datetime): The end of the period being processed, with a timezone. The logic may use it as the
                'period_end' of the message (and 'period_end' minus the period as its 'period_start').

        Returns:
            Dict[str, Any]: The combined message of the period.
        """
        raise NotImplementedError()
