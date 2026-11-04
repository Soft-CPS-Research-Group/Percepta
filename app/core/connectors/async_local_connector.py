# --- Standard Library Imports ---
import asyncio
import inspect
from typing import Any, Callable, Optional

# --- Internal Framework Imports ---
from app.core.utils.logger import LoggingUtils


class AsyncLocalConnector:
    """Unified in-memory transport connector.

    Supports both buffered in-memory queue pipelines (for Input Handlers)
    and direct callable execution dispatching (for Output Handlers).
    """

    _LOG_PREFIX: str = "Async Local Connector |"

    def __init__(
        self,
        server_configurations: dict,
        logger: LoggingUtils
    ) -> None:
        """Initializes the unified in-memory connector.

        Expected `server_configurations` Data Structure:
        -----------------------------------------------
        # For Output Handlers (Direct Execution Dispatch):
        {
            "target_callable": <Callable or Coroutine>  # Mandatory for outbound dispatch
        }

        # For Input Handlers (Buffered Queue Ingestion):
        {
            "max_queue_size": 10000                     # (int, optional) Maximum buffer capacity (default: 10000)
        }

        Args:
            server_configurations (dict): Configuration mapping for callable sinks or queue limits.
            logger (LoggingUtils): Shared telemetry logging interface.

        Raises:
            ValueError: If 'logger' is missing, 'target_callable' is not callable,
                or 'max_queue_size' is invalid.
            TypeError: If 'server_configurations' is not a dictionary.
        """
        # --- Guard Clause 1: Validate logger dependency ---
        if not logger:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'logger' dependency is missing.")

        self._logger = logger
        self._is_connected = True

        # --- Guard Clause 2: Validate server_configurations ---
        if not isinstance(server_configurations, dict):
            error_msg = (
                f"{self._LOG_PREFIX} Initialization error: 'server_configurations' must be a dictionary, "
                f"got {type(server_configurations).__name__}."
            )
            self._logger.error(error_msg)
            raise TypeError(error_msg)

        # 1. Setup target callable sink (used by Output Handlers)
        target = server_configurations.get("target_callable")
        if target is not None:
            if not callable(target):
                error_msg = (
                    f"{self._LOG_PREFIX} Initialization error: 'target_callable' must be an executable "
                    f"function or coroutine, got {type(target).__name__}."
                )
                self._logger.error(error_msg)
                raise ValueError(error_msg)
            self._target_callable: Optional[Callable] = target
        else:
            self._target_callable = None

        # 2. Setup buffered queue (used by Input Handlers)
        queue_capacity = server_configurations.get("max_queue_size", 10000)
        if not isinstance(queue_capacity, int) or queue_capacity < 0:
            error_msg = (
                f"{self._LOG_PREFIX} Initialization error: 'max_queue_size' must be a non-negative integer, "
                f"got {queue_capacity}."
            )
            self._logger.error(error_msg)
            raise ValueError(error_msg)

        self._queue: asyncio.Queue = asyncio.Queue(maxsize=queue_capacity)

    def is_connected(self) -> bool:
        """Indicates whether the local connector is active and open."""
        return self._is_connected

    # =========================================================================
    # Ingress / Queue Methods (Used by AsyncLocalInputHandler)
    # =========================================================================

    def push(self, payload: Any) -> None:
        """Pushes an incoming payload into the internal queue without blocking.

        Args:
            payload (Any): The telemetry payload to buffer in memory.
        """
        if not self._is_connected:
            self._logger.warning(f"{self._LOG_PREFIX} Dropping payload; connector is closed.")
            return

        try:
            self._queue.put_nowait(payload)
        except asyncio.QueueFull:
            self._logger.error(f"{self._LOG_PREFIX} Buffer overflow: in-memory queue reached capacity limit.")

    async def get(self) -> Any:
        """Pulls the next payload from the in-memory queue buffer.

        Returns:
            Any: The dequeued payload object.
        """
        return await self._queue.get()

    def task_done(self) -> None:
        """Signals to the underlying queue that a previously enqueued task is complete."""
        self._queue.task_done()

    # =========================================================================
    # Egress / Dispatch Methods (Used by AsyncLocalOutputHandler)
    # =========================================================================

    async def dispatch(self, payload: Any) -> Any:
        """Dispatches payload directly to the registered callable sink target.

        Args:
            payload (Any): The payload to forward to the target callable.

        Returns:
            Any: The direct execution result returned by the target callable.

        Raises:
            RuntimeError: If dispatch is attempted when disconnected or without a target_callable.
        """
        if not self._is_connected or self._target_callable is None:
            error_msg = f"{self._LOG_PREFIX} Dispatch failed: connector is closed or no target_callable is configured."
            self._logger.error(error_msg)
            raise RuntimeError(error_msg)

        if inspect.iscoroutinefunction(self._target_callable):
            return await self._target_callable(payload)
        else:
            return await asyncio.to_thread(self._target_callable, payload)

    # =========================================================================
    # Lifecycle Teardown
    # =========================================================================

    async def close(self) -> None:
        """Releases internal resources and closes the connector."""
        self._logger.info(f"{self._LOG_PREFIX} Closing local in-memory connector transport.")
        self._is_connected = False
        self._target_callable = None