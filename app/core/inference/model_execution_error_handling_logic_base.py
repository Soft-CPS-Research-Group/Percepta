# --- Standard Library Imports ---
from abc import ABC, abstractmethod
from typing import Any, Dict

# --- Internal Framework Imports ---
from app.core.utils.logger import LoggingUtils


class ModelExecutionErrorHandlingLogicBase(ABC):
    """Abstract base class of the Model Execution Error Handling Logic of an ML Model.

    The Inference Service invokes this logic when an error is returned during inference, instead of the expected
    decisions. The logic receives the error and says whether the inference should be tried again (True) or the process
    should be terminated (False). When it returns False, the rest of the flow is not performed for that period: the
    Decision Validation and Correction Logic is not invoked, the Adapter does not convert the decisions into the
    normalised format, and no decisions are sent to the next container.

    Each ML Model has its own logic, because the errors, and what to do with them, depend on the use case.

    One instance of the logic is created for each environment, and the same instance is used in every period of that
    environment.

    To integrate a Model Execution Error Handling Logic, create a class that inherits from this one and implement
    'should_retry'. The logic is built by the launcher of the ML Model (see 'MLModelLauncherBase').
    """

    _LOG_PREFIX: str = "Model Execution Error Handling Logic |"

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
    async def should_retry(self, error: Any, period_id: str) -> bool:
        """Handles an error of the inference and decides if the inference should be tried again.

        The logic handles the error internally, according to the requirements of the use case (e.g., it can keep count
        of the failures, or wait before saying that the inference should be tried again). It is a coroutine so the
        logic can wait without blocking the other environments.

        The same instance of the logic is used in every period, so the 'period_id' tells it which period the error
        belongs to. All the errors of the inference of a period have the same 'period_id', and each period has a
        different one. A logic that keeps state (e.g., a count of failures) can use it to know when a new period
        starts.

        Expected Input Structure:
        -------------------------
        error = Any type of error that can occur during the inference (e.g., an exception, or the error returned by
                the ML Model)
        period_id = "2026-08-19 11:15:00 +0000"     # Identifies the period of the message being inferred

        Args:
            error (Any): The error that occurred.
            period_id (str): Identifies the period of the inference that failed.

        Returns:
            bool: True if the inference should be tried again, False if the process should be terminated.
        """
        raise NotImplementedError()
