# --- Standard Library Imports ---
from typing import Any, List, Dict, Optional, Tuple, \
    Callable  # Standard PEP 484 type annotations for static analysis references

# --- Internal Framework Imports ---
from others.save.sync_rabbitmq_connector import SyncRabbitMQConnector  # Low-level network AMQP channel manager
from others.save.sync_rabbitMQ_output_handler import RabbitMQOutputHandler
from others.save.sync_rabbitMQ_input_handler import RabbitMQInputHandler
from others.save.sync_http_connector import HTTPConnector
from others.save.sync_http_output_handler import HTTPOutputHandler
from app.core.utils.logger import LoggingUtils

# Global dictionary map designed to escrow unbound factory function pointers indexed by string identifiers.
_OUTPUT_BUILDERS: Dict[str, Callable] = {}


def register_output_handler(name: str) -> Callable:
    """
    Global decorator utility. Maps and registers raw, unbound factory handler
    functions directly into the central _OUTPUT_BUILDERS registry map using text-based keys.
    """

    def decorator(func: Callable) -> Callable:
        _OUTPUT_BUILDERS[name] = func
        return func

    return decorator


class OutputHandlersLauncher:
    """
    The master orchestrator for all output handlers within the system.
    It maintains a unified registry of every available handler across all supported
    protocols. Passing a specific identifier string allows the launcher to dynamically
    resolve, configure, and return the correct output handler to publish data.
    Additionally, it tracks these sessions to close all physical connections and sockets
    centralized during system shutdown.
    """

    # --- Class Attribute & Type Declarations ---
    _logger: LoggingUtils  # Injected central logging facility
    _teardown_hooks: List[Callable[[], None]]  # Registry of anonymous technology-specific shutdown functions

    def __init__(self, logger: LoggingUtils) -> None:
        """
        Initializes the Launcher boundary container with an empty resource allocation stack.
        """
        self._logger = logger

        # Symmetrical infrastructure allocation pool tracking encapsulated closure blocks
        self._teardown_hooks = []

    def build_output_handler(self, handler_type: str, topology_setup: dict, server_configurations: dict) -> Tuple[
        Any, Optional[Any]]:
        """
        Unified public entrypoint. Fetches the raw unbound function pointer from the global
        registry and manually binds the execution context by passing 'self' as the first parameter.
        """
        # --- Variable Declarations & Type Hints ---
        unbound_builder: Callable

        self._logger.info(f"Launcher Core | Resolving global factory provider for identifier: '{handler_type}'...")

        # Guard clause protecting execution pipelines from unregistered interface formats
        if handler_type not in _OUTPUT_BUILDERS:
            error_msg = f"Launcher Core | Execution fail: '{handler_type}' is not registered in the global registry."
            self._logger.error(error_msg)
            raise ValueError(error_msg)

        # 1. Retrieve the pure unbound function blueprint out of the global registry
        unbound_builder = _OUTPUT_BUILDERS[handler_type]

        # 2. Manual Context Binding: Pass the live class instance ('self') as the first positional argument
        return unbound_builder(self, topology_setup, server_configurations)

    @register_output_handler("RabbitMQ")
    def _build_rabbitmq_output_handler(self, topology_setup: dict, server_configurations: dict) -> Tuple[
        RabbitMQOutputHandler, Optional[RabbitMQInputHandler]]:
        """
        Internal factory provider responsible for configuring and instantiating RabbitMQ data handlers.
        """
        # --- Variable Declarations & Type Hints ---
        publisher_connector: SyncRabbitMQConnector
        receiver_connector: SyncRabbitMQConnector
        exchanges_list: list
        exchange_block: dict
        rpc_props: dict
        reply_to_config: dict
        rpc_exchanges_list: list
        rpc_exchange_block: dict
        rpc_topology_aggregated: dict
        rpc_input_handler: Optional[RabbitMQInputHandler]
        output_handler: RabbitMQOutputHandler

        # Initialize data structures matching the standardized subscriber footprint
        rpc_topology_aggregated = {"exchanges": []}
        rpc_input_handler = None

        # Establish the physical network link dedicated strictly to outbound data processing streams
        publisher_connector = SyncRabbitMQConnector(server_configurations)
        publisher_connector.connect()

        self._logger.info("Launcher | RabbitMQ physical publisher network connection established.")

        # Traverse the configuration tree to evaluate active RPC channel allocations
        exchanges_list = topology_setup.get("exchanges", [])
        for exchange_block in exchanges_list:
            rpc_props = exchange_block.get("rpc_properties", {})

            if rpc_props.get("enabled", False):
                reply_to_config = rpc_props.get("reply_to", {})
                rpc_exchanges_list = reply_to_config.get("exchanges", [])

                for rpc_exchange_block in rpc_exchanges_list:
                    rpc_topology_aggregated["exchanges"].append(rpc_exchange_block)

        # Map and build the explicit standalone Input Handler if an active RPC lane was mapped
        if rpc_topology_aggregated["exchanges"]:
            self._logger.info("Launcher | Valid RPC structures parsed. Assembling standalone RabbitMQInputHandler...")

            # Dedicated physical link isolation allocated strictly for the background listening thread context
            receiver_connector = SyncRabbitMQConnector(server_configurations)
            receiver_connector.connect()

            self._logger.info("Launcher | RabbitMQ physical receiver RPC network connection established.")

            # --- WHY WE INSTANTIATE AND RETURN THE rpc_input_handler HERE ---
            # AMQP (RabbitMQ) is an inherently asynchronous protocol. To expose a clean, synchronous
            # request-response API to the domain core, we build a "synchronous facade".
            # This requires a dedicated, concurrent background thread running the Input Handler to constantly
            # consume response frames from the broker and release the thread blocking on '_wait_for_responses'.
            rpc_input_handler = RabbitMQInputHandler(
                topology_setup=rpc_topology_aggregated,
                on_message_callback=None,  # Delayed bind to guarantee safe instance creation order stability
                connector=receiver_connector,
                logger=self._logger
            )

        # Materialize the pure business Output Handler layer
        output_handler = RabbitMQOutputHandler(
            topology_setup=topology_setup,
            publisher_connector=publisher_connector,
            logger=self._logger
        )

        # Securely map the raw response bridge callback targeting the newly instantiated object reference
        if rpc_input_handler:
            # Resolves the signature hook pointing strictly to the underlying response processing mechanism
            rpc_input_handler._on_message_callback = output_handler.on_raw_rpc_response

        def _shutdown_rabbitmq_output_handler() -> None:
            """Symmetrical teardown routine attached strictly to the isolated RabbitMQ link assets."""
            self._logger.info("Launcher | Executing specific teardown for RabbitMQ Output/RPC Connectors...")

            # Safely dismantle the primary publishing channel interface
            try:
                if hasattr(publisher_connector, "is_connected") and publisher_connector.is_connected():
                    publisher_connector.close()
            except Exception as e:
                self._logger.error(f"Launcher | Failed to close concrete RabbitMQ publisher connector link safely: {e}")

            # Safely dismantle the asynchronous reply queue interface if instantiated
            if rpc_topology_aggregated["exchanges"]:
                try:
                    if hasattr(receiver_connector, "is_connected") and receiver_connector.is_connected():
                        receiver_connector.close()
                except Exception as e:
                    self._logger.error(
                        f"Launcher | Failed to close concrete RabbitMQ RPC receiver connector link safely: {e}")

        # Escrow the localized destruction block straight into the stateful instance array stack
        self._teardown_hooks.append(_shutdown_rabbitmq_output_handler)

        return output_handler, rpc_input_handler

    @register_output_handler("HTTP")
    def _build_http_output_handler(self, topology_setup: dict, server_configurations: dict) -> Tuple[
        HTTPOutputHandler, Optional[Any]]:
        """
        Internal factory provider responsible for configuring and instantiating HTTP data handlers.
        Symmetrically registers physical teardown hooks to manage connection lifecycles.
        """
        # --- Variable Declarations & Type Hints ---
        publisher_connector: HTTPConnector
        output_handler: HTTPOutputHandler

        # Establish the physical network persistent session dedicated to outbound HTTP delivery
        publisher_connector = HTTPConnector(server_configurations)
        publisher_connector.connect()

        self._logger.info("Launcher | HTTP physical persistent network connection session established.")

        # Materialize the pure business Output Handler layer, injecting the active connector
        output_handler = HTTPOutputHandler(
            topology_setup=topology_setup,
            publisher_connector=publisher_connector,
            logger=self._logger
        )

        def _shutdown_http_output_handler() -> None:
            """Symmetrical teardown routine attached strictly to the isolated HTTP session assets."""
            self._logger.info("Launcher | Executing specific teardown for HTTP Output Connector...")

            # Safely dismantle the primary publishing persistent session pool
            try:
                if hasattr(publisher_connector, "is_connected") and publisher_connector.is_connected():
                    publisher_connector.close()
            except Exception as e:
                self._logger.error(f"Launcher | Failed to close concrete HTTP publisher connector session safely: {e}")

        # Escrow the localized destruction block straight into the stateful instance array stack
        self._teardown_hooks.append(_shutdown_http_output_handler)

        # --- WHY THE SECOND TUPLE ELEMENT (INPUT HANDLER) IS None ---
        # To maintain a unified and loose-coupled signature across all dynamic protocol builders,
        # we return a Tuple[OutputHandlerBase, Optional[InputHandlerBase]].
        # Because HTTP is a naturally synchronous protocol, the network socket blocks and collects the response
        # inline. Thus, no asynchronous background listening loop is needed, and we return 'None' safely.
        return output_handler, None # TODO secalhar faz sentido mudar para algo assincrono na parte dos requests

    def shutdown_infrastructure(self) -> None:
        """
        Centralized infrastructure teardown. It iterates through the stateful collection
        of registered closures, triggering multi-protocol connection drops from the root.
        """
        # --- Variable Declarations & Type Hints ---
        teardown_hook: Callable[[], None]

        self._logger.info("Launcher Core | Initiating multi-protocol infrastructure teardown sequence...")

        # Sequentially fire all encapsulated hooks without requiring direct technology type awareness
        for teardown_hook in self._teardown_hooks:
            try:
                teardown_hook()
            except Exception as e:
                self._logger.error(f"Launcher Core | Critical failure running generalized teardown hook: {e}")

        # Clear pointer tracking buffers to assist the runtime Garbage Collector
        self._teardown_hooks.clear()
        self._logger.info("Launcher Core | Structural multi-protocol infrastructure teardown completed.")