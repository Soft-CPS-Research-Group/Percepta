# --- Standard Library Imports ---
import asyncio
from typing import Any, Dict, Optional

# --- Internal Framework Imports ---
from app.core.inference.model_execution_error_handling_logic_base import ModelExecutionErrorHandlingLogicBase
from app.core.utils.logger import LoggingUtils


class EnergAIzeModelExecutionErrorHandlingLogic(ModelExecutionErrorHandlingLogicBase):
    """Model Execution Error Handling Logic of EnergAIze.

    The strategy is the same for any error that occurs during the execution of the model: the inference is tried again
    up to three times, with a pause of one second between the attempts. If the three attempts fail, no further
    inference is performed in that period, so its data is not processed by the model.

    The strategy is intentionally simple, because EnergAIze is still in the testing phase and a more elaborate
    error-handling strategy is not yet required.
    """

    # The number of attempts that can be made in a period, including the first one
    _MAX_ATTEMPTS: int = 3
    # The pause between two attempts, in seconds
    _RETRY_INTERVAL_SECONDS: float = 1.0

    def __init__(self, environment: Dict[str, Any], logger: LoggingUtils) -> None:
        """Initializes the logic.

        Args:
            environment (Dict[str, Any]): The environment this logic works for, as returned by the Schema Storage.
            logger (LoggingUtils): Logger of the environment.

        Raises:
            ValueError: If 'environment' is not a dictionary or 'logger' is missing.
        """
        super().__init__(environment, logger)

        # The period of the failures that are being counted, and how many attempts have failed in it. The same instance
        # is used in every period, so the count starts again when the 'period_id' changes.
        self._period_id: Optional[str] = None
        self._failed_attempts: int = 0

    async def should_retry(self, error: Any, period_id: str) -> bool:
        """Counts the failure and decides if the inference should be tried again, waiting one second before saying yes.

        Expected Input Structure:
        -------------------------
        error = ConnectionError("Connection refused")
        period_id = "2026-08-19 11:15:00 +0000"

        Args:
            error (Any): The error that occurred.
            period_id (str): Identifies the period of the inference that failed. If it is not the one of the previous
                failure, this is the first failure of a new period.

        Returns:
            bool: True if fewer than three attempts have failed, so the inference should be tried again. False
                otherwise.
        """
        # Guard Clause 1: Checks if this failure belongs to a new period, which starts the count again.
        if period_id != self._period_id:
            self._period_id = period_id
            self._failed_attempts = 0

        self._failed_attempts += 1
        attempt: int = self._failed_attempts

        # Guard Clause 2: Checks that there are attempts left.
        if attempt >= self._MAX_ATTEMPTS:
            self._logger.error(
                f"{self._LOG_PREFIX} Attempt {attempt} of {self._MAX_ATTEMPTS} failed ({type(error).__name__}: "
                f"{error}). No attempts left, so there is no inference in this period."
            )
            return False

        self._logger.warning(
            f"{self._LOG_PREFIX} Attempt {attempt} of {self._MAX_ATTEMPTS} failed ({type(error).__name__}: {error}). "
            f"Trying again in {self._RETRY_INTERVAL_SECONDS} second(s)."
        )
        await asyncio.sleep(self._RETRY_INTERVAL_SECONDS)
        return True
