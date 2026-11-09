# --- Standard Library Imports ---
import asyncio  # Used for running asynchronous tasks concurrently
import inspect  # Inspects callable attributes and coroutine signatures
import threading  # Used for thread-safe status flag synchronization
from typing import Any, Callable, Optional  # Standard PEP 484 static typing annotations

# --- Internal Framework Imports ---
from app.core.connectors.async_local_connector import AsyncLocalConnector
from app.core.output_handlers.output_handler_base import OutputHandlerBase
from app.core.utils.logger import LoggingUtils


class AsyncLocalOutputHandler(OutputHandlerBase):
    """
    Handles outbound data transmission directly to local in-memory consumer functions
    without passing through external broker network layers (e.g., RabbitMQ).
    Supports both synchronized (RPC Enabled) and Fire-and-Forget (RPC Disabled) execution modes.
    """

    # --- Class Attribute & Type Declarations ---
    _LOG_PREFIX: str
    _topology_setup: dict
    _connector: AsyncLocalConnector
    _stop_event: threading.Event

    _LOG_PREFIX = "Async Local Output Handler |"

    def __init__(
        self,
        topology_setup: Optional[dict],
        connector: AsyncLocalConnector,
        logger: LoggingUtils
    ) -> None:
        """
        Initializes the logical local output handler with connection dependencies and schema validation.

        Expected `topology_setup` Data Structure:
        -----------------------------------------
        # Example 1: Fire-and-Forget (RPC Disabled - Background Task)
        {
            "rpc_properties": {
                "enabled": False
            }
        }

        # Example 2: RPC Synchronous (Awaited Target Callable Result)
        {
            "rpc_properties": {
                "enabled": True
            }
        }

        Args:
            topology_setup (dict, optional): Topology & RPC routing configuration mapping.
            connector (AsyncLocalConnector): In-memory active connector instance.
            logger (LoggingUtils): Structured telemetry logger instance.

        Raises:
            ValueError: If mandatory 'connector' dependency is missing or topology is invalid.
        """
        super().__init__(logger)

        if topology_setup:
            # --- Guard Clause: Validate topology schema structure ---
            try:
                self._validate_topology_schema(topology_setup)
            except ValueError as val_err:
                self._logger.error(f"{self._LOG_PREFIX} {val_err}")
                raise

            self._topology_setup = topology_setup
        else:
            self._topology_setup = {"rpc_properties": {"enabled": False}}

        # --- Guard Clause: Validate connector dependency ---
        if not connector:
            error_msg = "Initialization error: 'connector' dependency is missing."
            self._logger.error(f"{self._LOG_PREFIX} {error_msg}")
            raise ValueError(error_msg)

        self._connector = connector
        self._stop_event = threading.Event()

    async def send(self, message: Any) -> Optional[Any]:
        """
        Dispatches incoming standardized payloads to the configured local memory receiver.
        Executes synchronously (awaited) if RPC is enabled, or asynchronously via background task
        if RPC is disabled (Fire-and-Forget).

        Args:
            message (Any): Transformed message payload to deliver locally.

        Returns:
            Optional[Any]: Result returned by the receiver if RPC is enabled, or None for Fire-and-Forget.
        """
        if self._stop_event.is_set():
            self._logger.warning(
                f"{self._LOG_PREFIX} Rejecting transmission: Handler is running under shutdown constraints."
            )
            return None

        if not message:
            self._logger.debug(f"{self._LOG_PREFIX} Skipping dispatch for empty message payload.")
            return None

        # Extract RPC configuration flags from topology setup
        rpc_properties = self._topology_setup.get("rpc_properties", {})
        is_rpc_enabled = rpc_properties.get("enabled", False)

        if is_rpc_enabled:
            # --- CASE A: RPC ENABLED (Awaited Sync Execution) ---
            try:
                self._logger.debug(f"{self._LOG_PREFIX} Dispatching payload locally (RPC Sync): {message}")
                result = await self._connector.dispatch(message)
                return result
            except Exception as e:
                self._logger.error(
                    f"{self._LOG_PREFIX} Failed to execute local memory payload dispatch: {e}"
                )
                return None
        else:
            # --- CASE B: RPC DISABLED (Optimized Fire-and-Forget) ---
            # Dispatch to event loop background context and exit immediately
            self._logger.debug(f"{self._LOG_PREFIX} Dispatching payload locally (Fire-and-Forget background task)")
            asyncio.create_task(self._safe_background_dispatch(message))
            return None

    async def _safe_background_dispatch(self, message: Any) -> None:
        """
        Safely executes background local dispatching without crashing the event loop on errors.
        """
        try:
            await self._connector.dispatch(message)
        except Exception as e:
            self._logger.error(
                f"{self._LOG_PREFIX} Background local dispatch failed: {e}"
            )

    async def stop(self) -> None:
        """
        Graceful shutdown pipeline hook. Halts in-memory transmission pipelines
        and safely cleans up the underlying local connector bindings.
        """
        self._logger.debug(f"{self._LOG_PREFIX} Stopping local in-memory message dispatcher.")

        # 1. Signal handler to reject new outbound payloads
        self._stop_event.set()

        # 2. Close local in-memory connector and clear references
        try:
            if self._connector and hasattr(self._connector, "close"):
                await self._connector.close()
        except Exception as e:
            self._logger.error(f"{self._LOG_PREFIX} Failed to close AsyncLocal connector cleanly: {e}")

    @staticmethod
    def _validate_topology_schema(topology_setup: dict) -> None:
        """
        Validates the architectural schema structure of the local output topology configuration dictionary.

        Args:
            topology_setup (dict): Egress topology configurations to validate.

        Raises:
            ValueError: If topology is not a dictionary or contains invalid property types.
        """
        if not isinstance(topology_setup, dict):
            raise ValueError("Topology validation error: 'topology_setup' must be a dictionary.")

        rpc_properties = topology_setup.get("rpc_properties")
        if rpc_properties is not None:
            if not isinstance(rpc_properties, dict):
                raise ValueError("Topology validation error: 'rpc_properties' must be a dictionary.")

            if "enabled" in rpc_properties and not isinstance(rpc_properties["enabled"], bool):
                raise ValueError("Topology validation error: 'enabled' in 'rpc_properties' must be a boolean.")