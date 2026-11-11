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
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'adapter' dependency is missing.")

        if not isinstance(adapter, TelemetryAdapterBase) or not hasattr(adapter, "adapt"):
            raise TypeError(f"{self._LOG_PREFIX} Initialization error: 'adapter' must inherit from AdapterBase and implement the 'adapt' method, got {type(adapter).__name__}.")

        self._adapter = adapter

        # --- Guard Clause 3: Validate output_handler dependency ---
        if not output_handler:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'output_handler' dependency is missing.")

        if not isinstance(output_handler, OutputHandlerBase) or not hasattr(output_handler, "send"):
            raise TypeError(f"{self._LOG_PREFIX} Initialization error: 'output_handler' must inherit from OutputHandlerBase and implement the 'send' method, got {type(output_handler).__name__}.")

        self._output_handler = output_handler

        # Evaluate and cache coroutine introspection once during initialization
        self._is_async_adapt: bool = inspect.iscoroutinefunction(self._adapter.adapt)

    async def process_message(self, message: Union[List[dict], dict, bytes, str]) -> None:
        """Runs the pipeline for one message: the Adapter translates it and the Output Handler sends the result.

        This is the callback that the Input Handlers call for each message that arrives.

        It does not catch errors. If the Adapter or the Output Handler fail, the exception goes up to the Input
        Handler, which is the one that knows what to do with a message that failed (for example, the RabbitMQ one
        rejects it, which applies the 'acknowledgement_properties' of the queue) and which logs the error. If it
        were caught here, the Input Handler would think that the message was processed and would acknowledge it,
        losing the message.

        Args:
            message (Union[List[dict], dict, bytes, str]): Raw inbound payload from transport ingress.

        Raises:
            Exception: Any error raised by the Adapter or by the Output Handler.
        """
        # --- Guard Clause: an empty message has nothing to translate. It is not an error, so it is skipped and the
        # Input Handler considers it processed. ---
        if not message:
            self._logger.debug(f"{self._LOG_PREFIX} Skipping empty message payload.")
            return

        # Translates the message into canonical messages. Several messages can be translated at the same time, so an
        # Adapter that keeps data between messages has to protect it itself, with the locks that it needs (for
        # example, a `threading.Lock` for each piece of data).
        if self._is_async_adapt:
            # An 'async' Adapter (found once, in the constructor) is awaited in the event loop, like the Output
            # Handlers. Today none of the Adapters is 'async'.
            canonical_messages = await self._adapter.adapt(message)
        else:
            # A normal Adapter is run in another thread, so that the event loop is not blocked while it works (it
            # would be, if the Adapter did something slow). 'asyncio.to_thread' waits for the result without
            # blocking. This is the same as the Input Handlers and the Output Handlers do with normal functions.
            canonical_messages = await asyncio.to_thread(self._adapter.adapt, message)

        # --- Guard Clause: the Adapter may produce nothing for a message (for example, it has no parameter of this
        # environment). It is not an error, so there is nothing to send and the message is considered processed. ---
        if not canonical_messages:
            self._logger.debug(f"{self._LOG_PREFIX} No canonical messages produced by adapter for current payload.")
            return

        # Sends the canonical messages to the next container (or to the log, if there is no link configured)
        self._logger.debug(f"{self._LOG_PREFIX} Dispatching {len(canonical_messages)} canonical messages to OutputHandler.")
        await self._output_handler.send(canonical_messages)
