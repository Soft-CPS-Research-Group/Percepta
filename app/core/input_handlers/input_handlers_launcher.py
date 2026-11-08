# --- Standard Library Imports ---
from typing import Any, Dict, Callable, Coroutine, Optional  # Standard PEP 484 static typing annotations

# --- Internal Framework Imports ---
from app.core.connectors.async_rabbitmq_connector import AsyncRabbitMQConnector  # Async AMQP channel manager
from app.core.input_handlers.async_rabbitmq_input_handler import AsyncRabbitMQInputHandler  # Async AMQP subscriber
from app.core.endpoint_builders.endpoint_builder_base import EndpointBuilderBase
from app.core.connectors.async_http_connector import AsyncHTTPConnector  # Low-level asynchronous HTTP session manager
from app.core.input_handlers.async_http_input_handler import HTTPInputHandler as AsyncHTTPInputHandler  # Polling HTTP handler
from app.core.connectors.async_local_connector import AsyncLocalConnector
from app.core.input_handlers.async_local_input_handler import AsyncLocalInputHandler
from app.core.utils.async_periodic_scheduler import AsyncPeriodicScheduler  # Non-blocking async scheduler
from app.core.utils.labels import InputHandlerLabel  # Ids of the types of Input Handler
from app.core.utils.logger import LoggingUtils  # Central telemetry logger instance
from app.core.utils.timezone import TimeZoneUtils

# Global dictionary map designed to escrow unbound async factory function pointers indexed by string identifiers.
_INPUT_BUILDERS: Dict[str, Callable[..., Coroutine[Any, Any, Any]]] = {}


def register_input_handler(name: str) -> Callable:
    """
    Global decorator utility. Maps and registers raw, unbound async factory handler
    functions directly into the central _INPUT_BUILDERS registry map using text-based keys.
    """

    def decorator(func: Callable) -> Callable:
        _INPUT_BUILDERS[name] = func
        return func

    return decorator


class InputHandlersLauncher:
    """
    The master orchestrator for all inbound (ingest) handlers within the system.
    Dynamically resolves, configures, and instantiates asynchronous input handlers across supported
    protocols. Centralizes infrastructure connection pooling, scheduling, and stateful teardown routines.
    """

    # --- Class Attribute & Type Declarations ---
    _LOG_PREFIX: str  # Standardized logging identifier prefix
    _logger: LoggingUtils  # Injected central logging facility

    _LOG_PREFIX = "Input Handlers Launcher |"

    def __init__(self, logger: LoggingUtils) -> None:
        """
        Initializes the Launcher boundary container with an empty resource allocation stack.
        """
        self._logger = logger

    async def build_input_handler(
            self,
            handler_type: str,
            topology_setup: dict,
            callback: Callable,
            server_configurations: dict,
            logger: Optional[LoggingUtils] = None,
            **kwargs: Any
    ) -> Any:
        """
        Unified public asynchronous entrypoint. Fetches the unbound async builder from the global
        registry and executes it by passing 'self' as the first parameter context.

        Args:
            handler_type (str): Key of the registered handler ("AsyncRabbitMQ", "AsyncHTTP", "AsyncLocal").
            topology_setup (dict): Route/exchange/endpoint configuration.
            callback (Callable): Function the handler calls with each message it receives.
            server_configurations (dict): Connection parameters.
            logger (LoggingUtils, optional): Logger given to the handler, its connector and its scheduler, so
                their logs show the environment they were built for. Defaults to the logger of the launcher.
            **kwargs: Extra arguments for the builder of the handler type (for example, the HTTP polling interval).
        """
        # --- Variable Declarations & Type Hints ---
        unbound_builder: Callable[..., Coroutine[Any, Any, Any]]

        # The factory is shared, so the logger of whoever asks for the handler is used instead of its own
        logger = logger or self._logger

        logger.debug(f"{self._LOG_PREFIX} Resolving global factory provider for identifier: '{handler_type}'...")

        # Guard clause protecting execution pipelines from unregistered interface formats
        if handler_type not in _INPUT_BUILDERS:
            error_msg = f"{self._LOG_PREFIX} Execution fail: '{handler_type}' is not registered in the global registry."
            logger.error(error_msg)
            raise ValueError(error_msg)

        # 1. Retrieve the pure unbound function blueprint out of the global registry
        unbound_builder = _INPUT_BUILDERS[handler_type]

        # 2. Manual Context Binding: Pass the live class instance ('self') as the first positional argument
        return await unbound_builder(self, topology_setup, callback, server_configurations, logger=logger, **kwargs)

    @register_input_handler(InputHandlerLabel.ASYNC_RABBITMQ.value)
    async def _build_async_rabbitmq_input_handler(
            self,
            topology_setup: dict,
            callback: Callable,
            server_configurations: dict,
            logger: LoggingUtils
    ) -> AsyncRabbitMQInputHandler:
        """
        Internal async factory provider responsible for configuring and instantiating RabbitMQ data consumers.
        """
        # --- Variable Declarations & Type Hints ---
        consumer_connector: AsyncRabbitMQConnector
        input_handler: AsyncRabbitMQInputHandler

        # Establish the non-blocking physical network link dedicated strictly to inbound data processing streams
        consumer_connector = AsyncRabbitMQConnector(server_configurations, logger)
        await consumer_connector.connect()

        logger.debug(f"{self._LOG_PREFIX} RabbitMQ physical network connection established.")

        # Materialize the logical boundary injecting its standalone transport connection link
        input_handler = AsyncRabbitMQInputHandler(
            topology_setup=topology_setup,
            on_message_callback=callback,
            connector=consumer_connector,
            logger=logger
        )

        return input_handler

    @register_input_handler(InputHandlerLabel.ASYNC_HTTP.value)
    async def _build_async_http_input_handler(
            self,
            topology_setup: dict,
            callback: Callable,
            server_configurations: dict,
            logger: LoggingUtils,
            time_interval_seconds: int = 15,
            endpoint_builder: Optional[EndpointBuilderBase] = None
    ) -> AsyncHTTPInputHandler:
        """
        Internal async factory provider responsible for configuring and instantiating HTTP Input Handlers.
        Symmetrically registers async physical teardown hooks to manage persistent session lifecycles.
        """
        # --- Variable Declarations & Type Hints ---
        provider_connector: AsyncHTTPConnector
        scheduler: AsyncPeriodicScheduler
        input_handler: AsyncHTTPInputHandler
        timezone_str: Optional[str]

        # Establish the physical persistent async HTTP connection pool
        provider_connector = AsyncHTTPConnector(server_configurations, logger)
        await provider_connector.connect()

        # Configure the infrastructure async scheduler boundary
        timezone_str = server_configurations.get("timezone")
        target_tz = TimeZoneUtils.resolve_timezone(timezone_str, logger)

        scheduler = AsyncPeriodicScheduler(
            timezone=target_tz,
            logger=logger
        )

        logger.debug(f"{self._LOG_PREFIX} HTTP physical connector and async scheduler engine prepared.")

        # Instantiate the input handler with complete encapsulated boundaries
        input_handler = AsyncHTTPInputHandler(
            topology_setup=topology_setup,
            on_message_callback=callback,
            connector=provider_connector,
            scheduler=scheduler,
            time_interval_seconds=time_interval_seconds,
            logger=logger,
            endpoint_builder=endpoint_builder
        )

        return input_handler

    @register_input_handler(InputHandlerLabel.ASYNC_LOCAL.value)
    async def _build_async_local_input_handler(
            self,
            topology_setup: dict,  # TODO VER O QUE FAZER COM ISTO
            callback: Callable,
            server_configurations: dict,
            logger: LoggingUtils
    ) -> AsyncLocalInputHandler:
        """
        Internal async factory provider responsible for configuring and instantiating
        the in-process (IPC) input handler, used when the previous container runs
        within the same process.
        """
        # --- Variable Declarations & Type Hints ---
        local_connector: AsyncLocalConnector
        input_handler: AsyncLocalInputHandler

        # The connector is shared with the previous container's Output Handler,
        # which pushes payloads directly into it; no network connection is required
        local_connector = AsyncLocalConnector(server_configurations, logger)

        input_handler = AsyncLocalInputHandler(
            connector=local_connector,
            on_message_callback=callback,
            source = "", # TODO VER O QUE FAZER COM ISTO
            logger=logger
        )

        return input_handler