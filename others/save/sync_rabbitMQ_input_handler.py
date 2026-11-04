# --- Standard Library Imports ---
import threading  # Used for thread-safe condition checking and signaling flags
from typing import Any, Callable, Optional  # Structural method signature type hints for code strategy injection

# --- Internal Framework Imports ---
from others.save.sync_rabbitmq_connector import SyncRabbitMQConnector  # Low-level network AMQP channel manager
from app.core.input_handlers import InputHandlerBase  # Interface base contract abstraction
from app.core.utils.logger import LoggingUtils  # Telemetry framework logger instance
from app.core.utils import with_retries  # Resilient exponential connection recovery loop wrapper


class RabbitMQInputHandler(InputHandlerBase):
    """
    Acts as an ingress boundary component responsible for binding network message lifecycles
    to an AMQP topology layout and driving core domain pipelines upon message frames arrival.

    Operates strictly as a pure logical consumer interface adapter, completely decoupled
    from connector socket setup and connection allocations.
    """

    # --- Class Attribute & Type Declarations ---
    _rabbitmq_connector: SyncRabbitMQConnector  # Injected network connector link managing AMQP streams
    _on_message_callback: Optional[Callable[[bytes], bool]]  # Injected processing strategy hook
    _logger: LoggingUtils  # Telemetry tracker context
    _stop_event: threading.Event  # Thread-safe indicator to block or drop processing states
    _topology_setup: dict  # Centralized list-based ingress topology configuration tree

    _LOG_PREFIX = "RabbitMQ Input Handler |"

    def __init__(
            self,
            topology_setup: dict,
            on_message_callback: Optional[Callable],
            connector: SyncRabbitMQConnector,
            logger: LoggingUtils
    ) -> None:
        """
        Initializes the dynamic list-based RabbitMQ Input Handler boundary.
        """
        super().__init__(logger)

        self._topology_setup = topology_setup
        if not self._topology_setup:
            self._logger.error(f"{self._LOG_PREFIX} Initialization error: 'topology_setup' list tree missing.")
            raise KeyError("The topology configuration footprint is required for RabbitMQInputHandler.")

        self._on_message_callback = on_message_callback
        self._rabbitmq_connector = connector
        self._stop_event = threading.Event()

    def start(self) -> None:
        """
        Enters the operational listener phase by declaring the ingress topology layout
        and launching the blocking consumption I/O engine.
        """

        def _initialize_and_listen() -> None:
            """
            Atomic core execution closure step. Ensures configuration layers
            and listening constraints match broker allocations cleanly.
            """
            # 1. Map and declare the nested dynamic infrastructure boundaries
            self._setup_consumer_service()

            # 2. Engage the active low-level channel driver consumption blocking loops
            self._rabbitmq_connector.start_listening()

        # Execute loop context under structural resiliency framework rules
        with_retries(
            _initialize_and_listen,
            error_msg="RabbitMQ layout binding or network background consumption loop failed",
            logger=self._logger
        )

    def _setup_consumer_service(self) -> None:
        """
        Parses the unified list layout matrix to declare exchanges, queues,
        and bindings, then registers message handlers.
        """
        # --- Variable Declarations & Type Hints ---
        exchanges_list: list
        exchange_block: dict
        exchange_name: str
        exchange_props: dict
        queues_list: list
        queue_block: dict
        queue_name: str
        routing_key: str
        queue_props: dict
        consume_props: dict
        auto_ack_setting: bool
        setup_queues: set

        exchanges_list = self._topology_setup.get("exchanges", [])
        setup_queues = set()

        # Loop 1: Traverse the primary exchange nodes array list
        for exchange_block in exchanges_list:
            exchange_name = exchange_block.get("exchange_name", "")
            if not exchange_name:
                continue

            exchange_props = exchange_block.get("exchange_properties", {})
            self._rabbitmq_connector.declare_exchange(
                exchange_name=exchange_name,
                **exchange_props
            )

            # Loop 2: Traverse the independent queues bound to this exchange channel node
            queues_list = exchange_block.get("queues", [])
            for queue_block in queues_list:
                queue_name = queue_block.get("queue_name", "")
                routing_key = queue_block.get("routing_key", "")

                if not queue_name:
                    continue

                queue_props = queue_block.get("queue_properties", {})

                # Symmetrically deploy physical queue layouts and establish bindings
                queue_name = self._rabbitmq_connector.declare_queue(
                    queue_name=queue_name,
                    exchange_name=exchange_name,
                    routing_key=routing_key,
                    **queue_props
                )

                # Avoid establishing duplicate concurrent consumer setups on overlapping channels
                if queue_name not in setup_queues:
                    consume_props = queue_block.get("consume_properties", {})
                    auto_ack_setting = consume_props.get("auto_ack", False)  # TODO eventualmente poderão existir mais configurações de consumo, explorar isso depois

                    self._rabbitmq_connector.setup_consumer(
                        queue_name=queue_name,
                        callback=self._callback,
                        auto_ack=auto_ack_setting
                    )
                    setup_queues.add(queue_name)

        self._logger.info(f"{self._LOG_PREFIX} Network ingress streams and layout bindings deployed successfully.")

    def _callback(self, ch: Any, method: Any, properties: Any, body: bytes) -> None:
        """
        Driver-level callback triggered by low-level network frame arrival events.
        Proxies raw binary blocks downstream into core processing strategy contexts.
        """
        if self._stop_event.is_set():
            self._logger.warning(
                f"{self._LOG_PREFIX} Dropping incoming frame: Handler running under shutdown constraints.")
            return

        if not self._on_message_callback:
            self._logger.error(
                f"{self._LOG_PREFIX} Execution fail: Injected business callback hook processing strategy is missing.")
            return

        # Forward the raw payload to the downstream processing function
        if self._on_message_callback(body):
            # Domain successfully ingested frame, issue basic acknowledgement removal
            self._rabbitmq_connector.ack(method.delivery_tag)
        else:
            # Domain processing failure occurred, flag safe negative acknowledgement reject
            self._rabbitmq_connector.nack(method.delivery_tag)
            self._logger.warning(f"{self._LOG_PREFIX} Core processing lane failure. AMQP frame rejected.")

    def stop(self) -> None:
        """
        Polymorphic shutdown hook. Signals logical execution flags to drop.
        Physical low-level connector severing is managed entirely inside the Launcher root.
        """
        self._logger.info(f"{self._LOG_PREFIX} Stopping logical consumer message ingestion processing pipelines.")
        self._stop_event.set()