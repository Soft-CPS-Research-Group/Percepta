# --- Standard Library Imports ---
import asyncio
import inspect
from typing import Any, Callable, Optional

# --- Internal Framework Imports ---
from app.core.connectors.async_local_connector import AsyncLocalConnector
from app.core.input_handlers.input_handler_base import InputHandlerBase
from app.core.utils.logger import LoggingUtils
from app.core.input_handlers.payload_normalizer import PayloadNormalizer



class AsyncLocalInputHandler(InputHandlerBase):
    """Local in-memory asynchronous message consumer.

    Buffers incoming payloads pushed to its dedicated `AsyncLocalConnector`
    and executes background worker loops to dispatch messages to the registered
    downstream callback (e.g., ManagerCore or Service Layer).
    """

    _LOG_PREFIX: str = "Async Local Input Handler |"

    def __init__(
        self,
        connector: AsyncLocalConnector,
        on_message_callback: Callable[[Any], Any],
        source: str = "",
        logger: LoggingUtils = None,
    ) -> None:
        """Initializes the local in-memory input handler.

        Expected Payload Data Structure Received via `on_message`:
        ---------------------------------------------------------
        [
            {
                "id": "PV01",
                "value": {
                    "energy": [
                        {
                            "timestamp": "2026-08-19 10:00:00 +0100",
                            "value": 45.2
                        }
                    ]
                },
                "timestamp": "2026-08-19 10:00:00 +0100"
            }
        ]

        Args:
            connector (AsyncLocalConnector): Dedicated in-memory connector managing the buffer queue.
            on_message_callback (Callable): Downstream handler function or coroutine executed upon message consumption.
            source(str):
            logger (LoggingUtils): Structured telemetry logging interface.

        Raises:
            ValueError: If 'connector', 'callback', or 'logger' dependencies are missing or invalid.
            TypeError: If 'connector' is not an instance of AsyncLocalConnector.
        """

        super().__init__(logger)

        self._source = source

        # --- Guard Clause: Validate callback exists and is executable ---
        if not on_message_callback or not callable(on_message_callback):
            error_msg = "Initialization error: 'on_message_callback' is missing or not callable."
            self._logger.error(f"{self._LOG_PREFIX} {error_msg}")
            raise ValueError(error_msg)

        self._on_message_callback = on_message_callback

        # --- Guard Clause: Validate connector dependency ---
        if not connector:
            error_msg = "Initialization error: 'connector' dependency is missing."
            self._logger.error(f"{self._LOG_PREFIX} {error_msg}")
            raise ValueError(error_msg)

        self._connector = connector
        self._consumer_task: Optional[asyncio.Task] = None
        self._is_running: bool = False

        # Cache reflection inspection once during initialization
        self._is_async_callback: bool = inspect.iscoroutinefunction(self._on_message_callback) # TODO meter isto em todos os que fazem esta validação

    def on_message(self, payload: Any) -> None:
        """Public ingestion entrypoint passed as `target_callable` to local output connectors.

        Pushes the incoming payload directly into the connector's in-memory queue without blocking.

        Args:
            payload (Any): Inbound payload received from upstream components.
        """
        self._connector.push(payload)

    async def start(self) -> None:
        """Starts the background worker consuming from the local queue."""
        if self._is_running:
            self._logger.warning(f"{self._LOG_PREFIX} Consumer loop is already running.")
            return

        self._is_running = True
        self._consumer_task = asyncio.create_task(self._consume_loop())
        self._logger.debug(f"{self._LOG_PREFIX} In-memory message consumer loop started.")

    async def _consume_loop(self) -> None:
        while self._is_running:
            try:
                payload = await self._connector.get()
            except asyncio.CancelledError:
                break

            try:
                envelope = await PayloadNormalizer.from_local(
                    payload, source=self._source
                )
                if self._is_async_callback:
                    await self._on_message_callback(envelope)
                else:
                    await asyncio.to_thread(self._on_message_callback, envelope)
            except Exception as err:
                self._logger.error(f"{self._LOG_PREFIX} Error dispatching message to callback: {err}")
            finally:
                self._connector.task_done()

    async def stop(self) -> None:
        """Gracefully terminates the consumer task and closes the physical connector."""
        self._logger.debug(f"{self._LOG_PREFIX} Stopping in-memory consumer loop.")
        self._is_running = False

        if self._consumer_task and not self._consumer_task.done():
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass

        await self._connector.close()