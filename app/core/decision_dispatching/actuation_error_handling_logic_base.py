# --- Standard Library Imports ---
from abc import ABC, abstractmethod
from typing import Any, Dict

# --- Internal Framework Imports ---
from app.core.utils.logger import LoggingUtils


class ActuationErrorHandlingLogicBase(ABC):
    """Abstract base class of the Actuation Error Handling Logic of a Target System.

    The Decision Dispatching Service invokes this logic when an error is returned while a decision is being sent to the
    Target System. The logic receives the error and says whether the sending should be tried again (True) or the
    processing of that decision should be terminated (False).

    Each Target System has its own logic, because the errors, and what to do with them, depend on the Target System.

    One instance of the logic is created for each Target System of each environment, and the same instance is used in
    every period.

    To integrate an Actuation Error Handling Logic, create a class that inherits from this one and implement
    'should_retry'. The logic is built by the launcher of the Target System (see 'TargetSystemLauncherBase').
    """

    _LOG_PREFIX: str = "Actuation Error Handling Logic |"

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
    async def should_retry(self, error: Any, message_id: str) -> bool:
        """Handles an error of the actuation and decides if the sending should be tried again.

        The logic handles the error internally, according to the requirements of the Target System (e.g., it can keep
        count of the failures, or wait before saying that the sending should be tried again). It is a coroutine so the
        logic can wait without blocking the other environments.

        The same instance of the logic is used for every message, so the 'message_id' tells it which message the error
        belongs to. All the errors of the sending of a message have the same 'message_id', and each message has a
        different one. A logic that keeps state (e.g., a count of failures) can use it to know when a new message
        starts.

        Expected Input Structure:
        -------------------------
        error = Any type of error that can occur while sending (e.g., an exception, or the error returned by the
                Output Handler)
        message_id = "2026-08-19 11:15:00 +0000 | 0"      # Identifies the message being sent

        Args:
            error (Any): The error that occurred.
            message_id (str): Identifies the message that failed.

        Returns:
            bool: True if the sending should be tried again, False if its processing should be terminated.
        """
        raise NotImplementedError()
