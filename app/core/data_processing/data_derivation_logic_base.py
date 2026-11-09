# --- Standard Library Imports ---
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict

# --- Internal Framework Imports ---
from app.core.utils.logger import LoggingUtils


class DataDerivationLogicBase(ABC):
    """Abstract base class of the Data-Derivation Logic of an environment.

    The Data-Derivation Logic is integrated logic, and it is optional: a Percepta instance only has one when it
    needs parameters that no Telemetry System reports, but that can be worked out from the ones that exist.
    It is applied by the Data Processing Service right after the Combination Logic. It takes the message produced by
    the Combination Logic and adds to it the new parameters, which are worked out from the values of one or more of
    the parameters that the message already has. The result is the message that goes on to the Inference container.

    The structure of the message is the one produced by the Combination Logic of the same Percepta instance, so it is
    not fixed by Percepta. The logic must return the whole message, not only the new parameters, because Percepta
    does not know the structure and cannot merge them.

    One instance of the logic is created for each environment, and the same instance is used in every period of that
    environment. Nothing is shared between environments.

    To integrate a Data-Derivation Logic, create a class that inherits from this one, implement 'derive', and put
    the path of the class in 'integrated_logic.data_derivation_logic' of the Percepta Instance Configuration.
    """

    _LOG_PREFIX: str = "Data-Derivation Logic |"

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
                    "parameters": {"parameter_1_id": {"parameter_type_id": "parameter_type_1", ...}}
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
                tells the logic which entities and parameters exist and their types.
            settings (Dict[str, Any]): The settings of the Data Processing container (the same ones the Data
                Processing Service uses). The logic may need them, e.g., 'frequency_seconds' is the length of
                the period that it combines.
            logger (LoggingUtils): Logger of the environment.
        """
        self._environment = environment
        self._settings = settings
        self._logger = logger

    @abstractmethod
    def derive(self, message: Dict[str, Any], period_end: datetime) -> Dict[str, Any]:
        """Adds the derived data to the combined message of a period.

        Expected Input Structure:
        -------------------------
        The message produced by the Combination Logic of the same Percepta instance.

        Expected Output Structure:
        --------------------------
        The whole message, with the derived data added.

        Args:
            message (Dict[str, Any]): The combined message of the period.
            period_end (datetime): The end of the period being processed, with a timezone.

        Returns:
            Dict[str, Any]: The message with the derived data. It may be the same dictionary that was received,
                changed.
        """
        raise NotImplementedError()
