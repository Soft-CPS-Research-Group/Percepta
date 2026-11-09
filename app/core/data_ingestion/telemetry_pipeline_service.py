# --- Standard Library Imports ---
import asyncio
import inspect
from typing import Any, List, Union

# --- Internal Framework Imports ---
from app.core.data_ingestion.telemetry_adapter_base import TelemetryAdapterBase
from app.core.output_handlers.output_handler_base import OutputHandlerBase
from app.core.utils.logger import LoggingUtils


class TelemetryPipelineService:
    """Generic Application Service orchestrating the Ingress -> Adapt -> Egress pipeline.

    Decouples transport ingestion from domain adaptation rules and outbound dispatching.
    Acts as the entrypoint callback for InputHandlers across all provider telemetry_systems.
    """

    _LOG_PREFIX: str = "Telemetry Pipeline Service |"

    def __init__(
        self,
        adapter: TelemetryAdapterBase,
        output_handler: OutputHandlerBase,
        logger: LoggingUtils
    ) -> None:
        """Initialize the reusable telemetry pipeline service.

        Args:
            adapter (TelemetryAdapterBase): Pure domain adapter instance implementing `adapt()`.
            output_handler (OutputHandlerBase): Injected egress handler instance implementing `send()`.
            logger (LoggingUtils): Structured telemetry logging interface.

        Raises:
            ValueError: If mandatory dependencies are missing or invalid.
            TypeError: If dependencies violate expected base types.
        """
        # --- Guard Clause 1: Validate logger dependency ---
        if not logger:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'logger' dependency is missing.")

        self._logger = logger

        # --- Guard Clause 2: Validate adapter dependency ---
        if not adapter:
            error_msg = f"{self._LOG_PREFIX} Initialization error: 'adapter' dependency is missing."
            self._logger.error(error_msg)
            raise ValueError(error_msg)

        if not isinstance(adapter, TelemetryAdapterBase) or not hasattr(adapter, "adapt"):
            error_msg = (
                f"{self._LOG_PREFIX} Initialization error: 'adapter' must inherit from AdapterBase "
                f"and implement the 'adapt' method, got {type(adapter).__name__}."
            )
            self._logger.error(error_msg)
            raise TypeError(error_msg)

        self._adapter = adapter

        # --- Guard Clause 3: Validate output_handler dependency ---
        if not output_handler:
            error_msg = f"{self._LOG_PREFIX} Initialization error: 'output_handler' dependency is missing."
            self._logger.error(error_msg)
            raise ValueError(error_msg)

        if not isinstance(output_handler, OutputHandlerBase) or not hasattr(output_handler, "send"):
            error_msg = (
                f"{self._LOG_PREFIX} Initialization error: 'output_handler' must inherit from OutputHandlerBase "
                f"and implement the 'send' method, got {type(output_handler).__name__}."
            )
            self._logger.error(error_msg)
            raise TypeError(error_msg)

        self._output_handler = output_handler

        # Evaluate and cache coroutine introspection once during initialization
        self._is_async_adapt: bool = inspect.iscoroutinefunction(self._adapter.adapt)

    async def process_message(self, message: Union[List[dict], dict, bytes, str]) -> None:
        """Entrypoint callback executed synchronously or asynchronously by the InputHandler.

        Schedules the async execution of the adaptation and outbound dispatch pipeline
        within the active event loop.

        Args:
            message (Union[List[dict], dict, bytes, str]): Raw inbound payload from transport ingress.
        """
        if not message:
            self._logger.debug(f"{self._LOG_PREFIX} Skipping empty message payload.")
            return
        try:
            if self._is_async_adapt:
                canonical_messages = await self._adapter.adapt(message)
            else:
                canonical_messages = self._adapter.adapt(message)

            if canonical_messages:
                self._logger.debug(
                    f"{self._LOG_PREFIX} Dispatching {len(canonical_messages)} canonical messages to OutputHandler."
                )
                await self._output_handler.send(canonical_messages)
            else:
                self._logger.debug(f"{self._LOG_PREFIX} No canonical messages produced by adapter for current payload.")
        except Exception as err:
            self._logger.error(f"{self._LOG_PREFIX} Unhandled exception during pipeline execution: {err}")

