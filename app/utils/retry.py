import time
from app.utils.logger import LoggingUtils

def with_retries(func, retry_config: dict = None, error_msg : str ="Operation failed", logger : LoggingUtils = None):
    """
    Executes a function with retries and logging.

    Args:
        func (callable): Function to execute.
        retry_config (dict, optional): Dictionary with keys:
            - "max_retries" (int): Max retry attempts.
            - "timeout" (int): Delay in seconds between retries.
        logger (logging.Logger | None): Logger to use (optional).
        error_msg (str): Message for logging and final exception.

    Returns:
        Any: The return value of func if successful.

    Raises:
        Exception: The last exception if retries are exhausted.
    """

    # --- Retry configuration ---
    if retry_config is None:
        """If retry_config is not provided, use default values"""
        retry_config = {"max_retries": 3, "timeout": 5}

    max_retries = retry_config.get("max_retries", 3)
    timeout = retry_config.get("timeout", 5)

    # --- Initialize variables ---
    attempts = max_retries
    last_error = None  # Stores the last caught exception

    # --- Retry loop ---
    while attempts > 0:
        try:
            """Try to execute the function"""
            return func()
        except Exception as e:
            """Catch exception and decrement attempts"""
            attempts -= 1
            last_error = e

            # --- Logging ---
            if logger:
                if attempts > 0:
                    """Log a warning if there are remaining retries"""
                    logger.warning(f"{error_msg} ({e}). Retries left: {attempts}")
                else:
                    """Log an error if max_retries is exceeded"""
                    logger.error(f"{error_msg} - Max retries exceeded. Last error: {e}")

            # --- Delay before next attempt ---
            if attempts > 0:
                """Wait 'timeout' seconds before retrying"""
                time.sleep(timeout)

    # --- Raise the last error if all attempts fail ---
    raise last_error


def with_persistent_retries(func, retry_config: dict = None, error_msg: str = "Operation failed", logger: LoggingUtils = None):
    """
    Executes a function persistently with retry cycles and waiting periods between them.

    This version never stops retrying (useful for critical operations like authentication),
    but it does pause between cycles to avoid busy loops.

    Args:
        func (callable): Function to execute.
        retry_config (dict, optional): Dictionary with keys:
            - "max_retries" (int): Retries per cycle.
            - "timeout" (int): Delay between retries inside a cycle.
            - "wait_between_cycles" (int): Delay before restarting a new retry cycle.
            - "max_backoff" (int, optional): Maximum time to wait between cycles (for exponential backoff).
        error_msg (str): Message for logging.
        logger (LoggingUtils | None): Logger instance.

    Returns:
        Any: Return value of func when successful.
    """

    if retry_config is None:
        retry_config = {"max_retries": 3, "timeout": 5, "wait_between_cycles": 60}

    max_retries = retry_config.get("max_retries", 3)
    timeout = retry_config.get("timeout", 5)
    wait_between_cycles = retry_config.get("wait_between_cycles", 60)
    max_backoff = retry_config.get("max_backoff", 600)

    backoff = wait_between_cycles

    while True:
        attempts = max_retries
        last_error = None

        while attempts > 0:
            try:
                return func()
            except Exception as e:
                attempts -= 1
                last_error = e

                if logger:
                    if attempts > 0:
                        logger.warning(f"{error_msg} ({e}). Retries left: {attempts}")
                    else:
                        logger.error(f"{error_msg} - Retry cycle failed. Last error: {e}")

                if attempts > 0:
                    time.sleep(timeout)

        # All retries in this cycle failed
        if logger:
            logger.warning(f"All retries failed. Waiting {backoff}s before next cycle...")

        time.sleep(backoff)

        # Optional exponential backoff (to avoid hammering a dead server)
        backoff = min(backoff * 2, max_backoff)
