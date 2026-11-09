# --- Standard Library Imports ---
import asyncio
from typing import Any, Dict

# --- Internal Framework Imports ---
from app.core.decision_dispatching.actuation_error_handling_logic_base import ActuationErrorHandlingLogicBase
from app.core.utils.logger import LoggingUtils


class DefaultActuationErrorHandlingLogic(ActuationErrorHandlingLogicBase):
    """Actuation Error Handling Logic used by the Target Systems that do not configure their own.

    The strategy is the same for any error that occurs while a message is sent: the sending is tried again up to three
    times, with a pause of one second between the attempts. If the three attempts fail, the message is given up, and the
    other messages are not affected.

    Several messages of the same Target System can be in the middle of their attempts at the same time, so the failed
    attempts are counted for each message.
    """

    # The number of attempts that can be made for a message, including the first one
    _MAX_ATTEMPTS: int = 3
    # The pause between two attempts, in seconds
    _RETRY_INTERVAL_SECONDS: float = 1.0
    # The most messages whose count is kept. A message that fails and then succeeds is not reported to this logic, so
    # its count is never closed; the oldest ones are discarded to not grow forever.
    _MAX_TRACKED_MESSAGES: int = 1000

    def __init__(self, environment: Dict[str, Any], logger: LoggingUtils) -> None:
        """Initializes the logic.

        Args:
            environment (Dict[str, Any]): The environment this logic works for, as returned by the Schema Storage.
            logger (LoggingUtils): Logger of the environment.

        Raises:
            ValueError: If 'environment' is not a dictionary or 'logger' is missing.
        """
        super().__init__(environment, logger)

        # Message id -> how many attempts have failed
        self._failed_attempts: Dict[str, int] = {}

    async def should_retry(self, error: Any, message_id: str) -> bool:
        """Counts the failure and decides if the sending should be tried again, waiting one second before saying yes.

        Expected Input Structure:
        -------------------------
        error = "Timeout after 5000 ms"
        message_id = "2026-08-19 11:15:00 +0000 | cleanwatts | 0"

        Args:
            error (Any): The error that occurred.
            message_id (str): Identifies the message that failed.

        Returns:
            bool: True if fewer than three attempts have failed, so the sending should be tried again. False otherwise.
        """
        # Discards the oldest counts if there are too many (dictionaries keep the order of insertion)
        if message_id not in self._failed_attempts and len(self._failed_attempts) >= self._MAX_TRACKED_MESSAGES:
            self._failed_attempts.pop(next(iter(self._failed_attempts)))

        attempt: int = self._failed_attempts.get(message_id, 0) + 1

        # Guard Clause 1: Checks that there are attempts left.
        if attempt >= self._MAX_ATTEMPTS:
            self._failed_attempts.pop(message_id, None)
            self._logger.error(
                f"{self._LOG_PREFIX} Attempt {attempt} of {self._MAX_ATTEMPTS} failed for '{message_id}' "
                f"({type(error).__name__}: {error}). No attempts left, so the message is given up."
            )
            return False

        self._failed_attempts[message_id] = attempt
        self._logger.warning(
            f"{self._LOG_PREFIX} Attempt {attempt} of {self._MAX_ATTEMPTS} failed for '{message_id}' "
            f"({type(error).__name__}: {error}). Trying again in {self._RETRY_INTERVAL_SECONDS} second(s)."
        )
        await asyncio.sleep(self._RETRY_INTERVAL_SECONDS)
        return True
