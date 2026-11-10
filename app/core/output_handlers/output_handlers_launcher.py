# --- Standard Library Imports ---
from typing import Any, Callable, Coroutine, Dict, Optional, Tuple  # PEP 484 static typing rules

# --- Internal Framework Imports ---
from app.core.connectors.async_http_connector import AsyncHTTPConnector  # Async HTTP client session pool
from app.core.connectors.async_local_connector import AsyncLocalConnector  # In-memory callable connector
from app.core.connectors.async_rabbitmq_connector import AsyncRabbitMQConnector  # Async AMQP channel manager
from app.core.input_handlers.async_rabbitmq_input_handler import AsyncRabbitMQInputHandler  # Async AMQP subscriber for RPC
from app.core.output_handlers.async_http_output_handler import AsyncHTTPOutputHandler  # Async HTTP egress handler
from app.core.output_handlers.async_local_output_handler import AsyncLocalOutputHandler  # In-memory local handler
from app.core.output_handlers.async_rabbitmq_output_handler import AsyncRabbitMQOutputHandler  # Async AMQP publisher
from app.core.utils.labels import OutputHandlerLabel  # Ids of the types of Output Handler
from app.core.utils.logger import LoggingUtils  # Central telemetry logger instance

# Global dictionary map designed to escrow unbound async factory function pointers indexed by string identifiers.
_OUTPUT_BUILDERS: Dict[str, Callable[..., Coroutine[Any, Any, Tuple[Any, Optional[Any]]]]] = {}


def register_output_handler(name: str) -> Callable:
    """
    Global decorator utility. Maps and registers raw, unbound async factory handler
    functions directly into the central _OUTPUT_BUILDERS registry map using text-based keys.
    """

    def decorator(func: Callable) -> Callable:
        _OUTPUT_BUILDERS[name] = func
        return func

    return decorator


class OutputHandlersLauncher:
    """
    The master orchestrator for all outbound (egress) handlers within the system.
    Dynamically resolves, configures, and instantiates asynchronous output handlers across supported
    protocols. Operates as a stateless factory, transferring ownership of connectors to instantiated handlers.
    """

    # --- Class Attribute & Type Declarations ---
    _LOG_PREFIX: str  # Standardized logging identifier prefix
    _logger: LoggingUtils  # Injected central logging facility

    _LOG_PREFIX = "Output Handlers Launcher |"

    def __init__(self, logger: LoggingUtils) -> None:
        """
        Initializes the stateless Output Launcher factory.
        """
        self._logger = logger

    async def build_output_handler(
        self,
        handler_type: str,
        topology_setup: dict,
        server_configurations: dict,
        logger: Optional[LoggingUtils] = None
    ) -> Tuple[Any, Optional[Any]]:
        """
        Unified public asynchronous entrypoint. Fetches the unbound async builder from the global
        registry and executes it by passing 'self' as the first parameter context.

        Args:
            handler_type (str): Key of registered handler ("AsyncLocal", "AsyncHTTP", "AsyncRabbitMQ").
            topology_setup (dict): Route/exchange configuration mappings.
            server_configurations (dict): Connection parameters or callable references.
            logger (LoggingUtils, optional): Logger given to the handler and its connector, so their logs
                show the environment they were built for. Defaults to the logger of the launcher.

        Returns:
            Tuple[Any, Optional[Any]]: Primary OutputHandler instance and optional RPC listener InputHandler.
        """
        # The factory is shared, so the logger of whoever asks for the handler is used instead of its own
        logger = logger or self._logger

        logger.debug(f"{self._LOG_PREFIX} Resolving global factory provider for identifier: '{handler_type}'...")

        if handler_type not in _OUTPUT_BUILDERS:
            error_msg = f"{self._LOG_PREFIX} Execution fail: '{handler_type}' is not registered in the global registry."
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Retrieve the unbound factory function and execute with self context
        unbound_builder = _OUTPUT_BUILDERS[handler_type]
        return await unbound_builder(self, topology_setup, server_configurations, logger)

    @register_output_handler(OutputHandlerLabel.ASYNC_LOCAL.value)
    async def _build_async_local_output_handler(
        self,
        topology_setup: dict,
        server_configurations: dict,
        logger: LoggingUtils
    ) -> Tuple[AsyncLocalOutputHandler, Optional[Any]]:
        """
        Internal async factory provider responsible for configuring and instantiating Local In-Memory Output Handlers.
        """
        # Materialize the physical in-memory connector using target callable
        local_connector = AsyncLocalConnector(server_configurations)

        logger.debug(f"{self._LOG_PREFIX} Local memory connector bound successfully.")

        # Materialize the output handler and transfer ownership of the connector
        output_handler = AsyncLocalOutputHandler(
            topology_setup=topology_setup,
            connector=local_connector,
            logger=logger
        )

        return output_handler, None

    @register_output_handler(OutputHandlerLabel.ASYNC_HTTP.value)
    async def _build_async_http_output_handler(
        self,
        topology_setup: dict,
        server_configurations: dict,
        logger: LoggingUtils
    ) -> Tuple[AsyncHTTPOutputHandler, Optional[Any]]:
        """
        Internal async factory provider responsible for configuring and instantiating HTTP Output Handlers.
        """
        # Establish the physical persistent async HTTP connection pool
        publisher_connector = AsyncHTTPConnector(server_configurations)
        await publisher_connector.connect()

        logger.debug(f"{self._LOG_PREFIX} HTTP physical persistent network connection session established.")

        # Materialize the output handler and transfer ownership of the connector
        output_handler = AsyncHTTPOutputHandler(
            topology_setup=topology_setup,
            connector=publisher_connector,
            logger=logger
        )

        return output_handler, None

    @register_output_handler(OutputHandlerLabel.ASYNC_RABBITMQ.value)
    async def _build_async_rabbitmq_output_handler(
        self,
        topology_setup: dict,
        server_configurations: dict,
        logger: LoggingUtils
    ) -> Tuple[AsyncRabbitMQOutputHandler, Optional[AsyncRabbitMQInputHandler]]:
        """
        Internal async factory provider responsible for configuring and instantiating RabbitMQ data handlers.
        Schedules optional standalone RPC listener when reply_to configurations are detected.
        """
        # 1. Establish publisher physical connection
        publisher_connector = AsyncRabbitMQConnector(server_configurations)
        await publisher_connector.connect()

        logger.debug(f"{self._LOG_PREFIX} RabbitMQ physical publisher network connection established.")

        # 2. Extract potential RPC reply_to topology requirements
        rpc_topology_aggregated: dict = {"exchanges": []}
        rpc_input_handler: Optional[AsyncRabbitMQInputHandler] = None

        exchanges_list: list = topology_setup.get("exchanges", [])
        for exchange_block in exchanges_list:
            rpc_props: dict = exchange_block.get("rpc_properties", {})

            if rpc_props.get("enabled", False):
                reply_to_config: dict = rpc_props.get("reply_to", {})
                rpc_exchanges_list: list = reply_to_config.get("exchanges", [])

                for rpc_exchange_block in rpc_exchanges_list:
                    rpc_topology_aggregated["exchanges"].append(rpc_exchange_block)

        # 3. Build standalone RPC listener if reply_to queues were mapped
        if rpc_topology_aggregated["exchanges"]:
            logger.debug(f"{self._LOG_PREFIX} Valid RPC structures parsed. Assembling standalone AsyncRabbitMQInputHandler...")

            receiver_connector = AsyncRabbitMQConnector(server_configurations)
            await receiver_connector.connect()

            logger.debug(f"{self._LOG_PREFIX} RabbitMQ physical receiver RPC network connection established.")

            rpc_input_handler = AsyncRabbitMQInputHandler(
                topology_setup=rpc_topology_aggregated,
                on_message_callback=None,  # Delayed bind below
                connector=receiver_connector,
                logger=logger
            )

        # 4. Instantiate Output Handler
        output_handler = AsyncRabbitMQOutputHandler(
            topology_setup=topology_setup,
            connector=publisher_connector,
            logger=logger
        )

        # 5. Bind RPC response callback to Output Handler
        if rpc_input_handler:
            rpc_input_handler._on_message_callback = output_handler.on_rpc_response

        return output_handler, rpc_input_handler