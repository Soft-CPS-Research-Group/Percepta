# --- Standard Library Imports ---
from abc import ABC, abstractmethod
from typing import Any, Dict

# --- Internal Framework Imports ---
from app.core.utils.logger import LoggingUtils


class DecisionValidationCorrectionLogicBase(ABC):
    """Abstract base class of the Decision Validation and Correction Logic of an ML Model.

    It is optional. When it is integrated, the Inference Service invokes it with the decisions produced by the ML
    Model, before they are mapped into the normalised format. The logic validates the decisions against rules that are
    specific to the use case (e.g., the limits of a device) and, if necessary, corrects them.

    The logic accepts the decisions in whatever format the ML Model produces and returns them in that same format.
    Preserving the structure means the Adapter processes the decisions in the same way, whether they were corrected
    or not.

    One instance of the logic is created for each environment, and the same instance is used in every period of that
    environment.

    To integrate a Decision Validation and Correction Logic, create a class that inherits from this one and implement
    'validate_and_correct'. The logic is built by the launcher of the ML Model (see 'MLModelLauncherBase').
    """

    _LOG_PREFIX: str = "Decision Validation and Correction Logic |"

    def __init__(self, environment: Dict[str, Any], logger: LoggingUtils) -> None:
        """Initializes the logic.

        Expected Input Structure:
        -------------------------
        environment = {
            "group": "group_1",
            "entities": {...},
            ...                                 # The rest of the environment, as returned by the Schema Storage
        }

        Args:
            environment (Dict[str, Any]): The environment this logic works for, as returned by the Schema Storage.
            logger (LoggingUtils): Logger of the environment.

        Raises:
            ValueError: If 'environment' is not a dictionary or 'logger' is missing.
        """
        # Guard Clause 1: Checks that the logger exists.
        if not logger:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'logger' dependency is missing.")

        # Guard Clause 2: Checks that the environment is a dictionary.
        if not isinstance(environment, dict):
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'environment' must be a dictionary.")

        self._environment = environment
        self._logger = logger

    @abstractmethod
    def validate_and_correct(self, decisions: Any) -> Any:
        """Validates the decisions of the ML Model and corrects them if necessary.

        Expected Input Structure:
        -------------------------
        Specific to the ML Model. For example, for a model that returns one power setpoint for each entity:
        decisions = {"AC000001_1": 3}

        Expected Output Structure:
        --------------------------
        The same structure as the input, with the values corrected.

        Args:
            decisions (Any): The decisions, in the format that the ML Model produces.

        Returns:
            Any: The decisions, in the same format, corrected if necessary.
        """
        raise NotImplementedError()
