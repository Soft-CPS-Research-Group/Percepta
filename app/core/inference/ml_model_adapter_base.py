# --- Standard Library Imports ---
from abc import ABC, abstractmethod
from typing import Any, Dict, List

# --- Internal Framework Imports ---
from app.core.utils.logger import LoggingUtils


class MLModelAdapterBase(ABC):
    """Abstract base class of the Adapter of an ML Model.

    Each ML Model expects its own input format and returns its decisions in its own output format, and the next
    container (Decision Dispatching) expects the decisions in the normalised format of Percepta. The Adapter is the
    component that translates between them. It has two jobs:

        1. Translates the unified message, received from Data Processing, into the format that the ML Model requires.
        2. Translates the decisions of the ML Model into the normalised format.

    One instance of the Adapter is created for each environment, and the same instance is used in every period of that
    environment. Nothing is shared between environments.

    To integrate an Adapter, create a class that inherits from this one and implement 'adapt_model_input' and
    'adapt_model_decisions'. The Adapter is built by the launcher of the ML Model (see 'MLModelLauncherBase').
    """

    _LOG_PREFIX: str = "ML Model Adapter |"

    def __init__(self, environment_name: str, environment: Dict[str, Any], logger: LoggingUtils) -> None:
        """Initializes the Adapter.

        Expected Input Structure:
        -------------------------
        environment = {
            "group": "group_1",
            "entities": {
                "AC000001_1": {
                    "entity_type_id": "ev_charger",
                    "parameters": {...},
                    "actuators": {
                        "actuator_1": {"actuator_type_id": "power", ...}
                    }
                }
            },
            ...                                 # The rest of the environment, as returned by the Schema Storage
        }

        Args:
            environment_name (str): Name of the environment this Adapter translates data for.
            environment (Dict[str, Any]): The environment, as returned by the Schema Storage. It tells the Adapter
                which entities, parameters and actuators exist, so it can, for example, find the actuator that
                receives the decision of an entity.
            logger (LoggingUtils): Logger of the environment.

        Raises:
            ValueError: If 'environment_name' is not a non-empty string, if 'environment' is not a dictionary, or if
                'logger' is missing.
        """
        # Guard Clause 1: Checks that the logger exists, because it is needed to report the other errors.
        if not logger:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'logger' dependency is missing.")

        # Guard Clause 2: Checks that the name of the environment is a non-empty string.
        if not isinstance(environment_name, str) or not environment_name.strip():
            error_msg = f"{self._LOG_PREFIX} Initialization error: 'environment_name' must be a non-empty string."
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Guard Clause 3: Checks that the environment is a dictionary.
        if not isinstance(environment, dict):
            error_msg = f"{self._LOG_PREFIX} Initialization error: 'environment' must be a dictionary."
            logger.error(error_msg)
            raise ValueError(error_msg)

        self._environment_name = environment_name
        self._environment = environment
        self._logger = logger

    @abstractmethod
    def adapt_model_input(self, message: Dict[str, Any]) -> Any:
        """Translates the unified message into the format that the ML Model requires.

        The unified message is the output of the Data Processing container, so its structure is the one produced by
        the Combination Logic and the Data-Derivation Logic of the instance. The structure that this method
        returns is whatever the ML Model expects (it can be a dictionary, a list, text...). If the ML Model already
        expects the unified message, the method returns it as it is.

        Expected Input Structure:
        -------------------------
        message = {
            "period_start": "2026-08-19 10:45:00 +0000",
            "period_end": "2026-08-19 11:00:00 +0000",
            "entities": {...},                  # The structure depends on the Combination Logic of the instance
            ...
        }

        Expected Output Structure:
        --------------------------
        Specific to the ML Model.

        Args:
            message (Dict[str, Any]): The unified message of a period.

        Returns:
            Any: The input of the ML Model for that period.
        """
        raise NotImplementedError()

    @abstractmethod
    def adapt_model_decisions(self, decisions: Any) -> List[Dict[str, Any]]:
        """Translates the decisions of the ML Model into the normalised format.

        The decisions arrive in the format that the ML Model produces (or, if the instance has a Decision Validation
        and Correction Logic, in the same format after the correction, because that logic preserves it). The method
        returns them in the normalised format: a list with one dictionary for each entity that has decisions. Each
        decision is directed at an actuator of the entity, so the Adapter has to find which actuator of the entity
        receives it. The timestamp of each decision is the moment when the Adapter
        maps it.

        Expected Input Structure:
        -------------------------
        Specific to the ML Model. For example, for a model that returns one power setpoint for each entity:
        decisions = {"AC000001_1": 3}

        Expected Output Structure:
        --------------------------
        [
            {
                "id": "AC000001_1",                         # The entity that the decision is directed at
                "entity_type_id": "ev_charger",
                "actuators": {
                    "actuator_1": {                         # The id of the actuator
                        "actuator_type_id": "power",
                        "value": 3                          # The value to apply to the actuator
                    }
                },
                "timestamp": "2026-08-19 11:15:00 +0000"
            }
        ]

        Args:
            decisions (Any): The decisions of the ML Model, in its own format.

        Returns:
            List[Dict[str, Any]]: The decisions in the normalised format. Empty if there are none.
        """
        raise NotImplementedError()
