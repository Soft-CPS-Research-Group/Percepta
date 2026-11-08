# --- Standard Library Imports ---
import asyncio
import functools  # Used to give each queue consumer its own queue name (see _setup_consumer_service)
from typing import Callable, Optional, Set

# --- Third-Party Imports ---
import aio_pika

# --- Internal Framework Imports ---
from app.core.connectors.async_rabbitmq_connector import AsyncRabbitMQConnector
from app.core.input_handlers.input_handler_base import InputHandlerBase
from app.core.utils.logger import LoggingUtils
from app.core.input_handlers.payload_normalizer import PayloadNormalizer


class AsyncRabbitMQInputHandler(InputHandlerBase):
    """
    Asynchronous RabbitMQ message consumer.

    Declares the configured exchange and queue topology, listens for incoming
    messages in the background, and forwards their payloads to a domain callback.

    Does not manage the underlying connection socket or reconnect logic,
    delegating those connection lifecycles entirely to the AsyncRabbitMQConnector.
    """

    # Define a standard text prefix to be used for all log messages within this class
    _LOG_PREFIX = "Async RabbitMQ Input Handler |"

    def __init__(
            self,
            topology_setup: dict,
            on_message_callback: Optional[Callable],
            connector: AsyncRabbitMQConnector,
            logger: LoggingUtils
    ) -> None:
        """
        Initializes the RabbitMQ consumer with its configuration and dependencies.

        Args:
            topology_setup (dict): Exchange and queue mapping topology configurations.
                Example:
                    {
                      "exchanges": [
                        {
                          "exchange_name": "community_porto_telemetry",
                          "exchange_properties": { "exchange_type": "fanout", "durable": true },
                          "queues": [
                            {
                              "queue_name": "percepta_db_persistence_porto",
                              "routing_key": "",
                              "consume_properties": {
                                "no_ack": false,
                                "requeue": false
                              }
                            }
                          ]
                        }
                      ]
                    }
            on_message_callback (Optional[Callable]): Callback function to process received message.
            connector (AsyncRabbitMQConnector): Connector to manage the RabbitMQ network stream.
            logger (LoggingUtils): Utility to handle system logs and telemetry.
        """
        super().__init__(logger)

        # --- Guard Clause: Validate topology_setup exists ---
        # Check if the configuration dictionary is empty, null, or missing entirely
        if not topology_setup:
            error_msg = "Initialization error: 'topology_setup' configuration is missing."
            self._logger.error(f"{self._LOG_PREFIX} {error_msg}")
            raise KeyError(f"The topology configuration footprint is required for {self.__class__.__name__}.")

        # --- Guard Clause: Validate topology schema structure ---
        try:
            self._validate_topology_schema(topology_setup)
        except ValueError as val_err:
            self._logger.error(f"{self._LOG_PREFIX} {val_err}")
            raise

        self._topology_setup = topology_setup

        # Save the message processing function into a private attribute for subsequent triggering
        self._on_message_callback = on_message_callback

        # --- Guard Clause: Validate connector dependency ---
        if not connector:
            error_msg = "Initialization error: 'connector' dependency is missing."
            self._logger.error(f"{self._LOG_PREFIX} {error_msg}")
            raise ValueError(error_msg)

        # Save the asynchronous RabbitMQ connector to manage connections, channels, and active listening
        self._rabbitmq_connector = connector

        # Extract the list of configured routing exchanges; falls back to an empty list if missing
        self._exchanges = self._topology_setup.get("exchanges", [])

        # Initialize an empty dictionary to map and hold operational properties for each queue consumer
        self._queue_consumers_props = {}

        # Create a native asyncio Event flag for asynchronous shutdown coordination inside the event loop
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        """
        Starts the listener by declaring the topology and registering consumers.
        """
        # --- Lifecycle Gate ---
        # Abort startup immediately if the internal stop event flag has already been signaled
        if self._stop_event.is_set():
            self._logger.warning(
                f"{self._LOG_PREFIX} Start aborted: Handler is running under shutdown constraints."
            )
            return

        # --- Connection Safety Gate ---
        # Verify whether the underlying RabbitMQ network connector is active and online
        if not self._rabbitmq_connector.is_connected():
            self._logger.info(
                f"{self._LOG_PREFIX} Injected connector is offline. Establishing broker connection..."
            )
            await self._rabbitmq_connector.connect()

        # Await the downstream configuration routine to bind exchanges, declare queues, and start consuming
        await self._setup_consumer_service()

    async def _setup_consumer_service(self) -> None:
        """
        Parses topology layout to declare exchanges, queues, and bindings,
        then registers the asynchronous message handlers.
        """
        # --- Lifecycle Gate ---
        # Abort startup immediately if the internal stop event flag has already been signaled
        if self._stop_event.is_set():
            self._logger.warning(
                f"{self._LOG_PREFIX} Start aborted: Handler is running under shutdown constraints."
            )
            return

        setup_queues: Set[str] = set()

        # Process exchange definitions
        for exchange_block in self._exchanges:
            exchange_name: str = exchange_block.get("exchange_name", "")
            if not exchange_name:
                continue

            exchange_props: dict = exchange_block.get("exchange_properties", {})
            await self._rabbitmq_connector.declare_exchange(
                exchange_name=exchange_name,
                **exchange_props
            )

            # Process queues bound to this exchange
            queues_list: list = exchange_block.get("queues", [])
            if not queues_list:
                continue

            for queue_block in queues_list:
                queue_name: str = queue_block.get("queue_name", "")
                routing_key: str = queue_block.get("routing_key", "")
                queue_props: dict = queue_block.get("queue_properties", {})

                # Declare queue and establish routing bindings
                real_queue_name = await self._rabbitmq_connector.declare_queue(
                    queue_name=queue_name,
                    exchange_name=exchange_name,
                    routing_key=routing_key,
                    **queue_props
                )

                # Avoid duplicate concurrent consumer setups on overlapping channels
                if real_queue_name not in setup_queues:
                    consume_props: dict = queue_block.get("consume_properties", {})

                    # Track dynamic consumption settings bound to this specific queue name
                    self._queue_consumers_props[real_queue_name] = consume_props

                    # aio-pika calls the callback with the message only, and the message does not say which queue it
                    # came from. functools.partial fixes the queue name in advance, so _callback always knows it.
                    # Example: for queue "percepta_local_forecast_R-H-01", aio-pika calls
                    # _callback(message, queue_name="percepta_local_forecast_R-H-01")
                    await self._rabbitmq_connector.setup_consumer(
                        queue_name=real_queue_name,
                        callback=functools.partial(self._callback, queue_name=real_queue_name),
                        **consume_props
                    )
                    setup_queues.add(real_queue_name)

    async def _callback(self, message: aio_pika.abc.AbstractIncomingMessage, queue_name: str = "") -> None:
        """
        Asynchronous driver callback triggered by aio-pika dispatcher.
        Handles message parsing and coordinates safe acknowledgement via connector.

        Args:
            message (aio_pika.abc.AbstractIncomingMessage): Incoming AMQP message frame instance.
            queue_name (str): Name of the queue the message was consumed from. It is given by the
                functools.partial created in _setup_consumer_service.
        """
        # The consume properties of this queue (e.g., {"no_ack": False, "requeue": False}) decide if the message
        # has to be acknowledged. They are found with the queue name given when the consumer was registered.
        # (An aio-pika message has no attribute with its queue, so it cannot be read from the message itself.
        # With an empty name no properties were found, 'no_ack' defaulted to True and no ack was ever sent.)
        consume_properties = self._queue_consumers_props.get(queue_name, {})

        # --- Lifecycle Gate ---
        # Abort startup immediately if the internal stop event flag has already been signaled
        if self._stop_event.is_set():
            self._logger.warning(
                f"{self._LOG_PREFIX} Dropping incoming frame: Handler is running under shutdown constraints."
            )
            # Enforce data protection by requeueing transit payloads during shutdown phases
            await self._settle_message(message, False, consume_properties)
            return

        if not self._on_message_callback:
            self._logger.error(
                f"{self._LOG_PREFIX} Execution fail: Injected business callback hook strategy is missing."
            )
            await self._settle_message(message, False, consume_properties)
            return

        try:
            # 1. Normalize the message into the Canonical Integration Envelope (with the real queue name)
            envelope = await PayloadNormalizer.from_rabbitmq(message, queue_name=queue_name)
            # --- Guard Clause: Validate callback exists and is executable ---
            # Check if the callback function is missing or if the provided object cannot be executed

            if self._on_message_callback and callable(self._on_message_callback):
                # 2. Execute the business callback using the normalized envelope
                if asyncio.iscoroutinefunction(self._on_message_callback):
                    result = await self._on_message_callback(envelope)
                else:
                    result = await asyncio.to_thread(self._on_message_callback, envelope)

                success = result is not False
            else:
                success = False

            # 3. Handle AMQP transaction states based on execution outcome
            await self._settle_message(message, success, consume_properties)

        except Exception as e:
            self._logger.error(f"{self._LOG_PREFIX} Unhandled error inside ingestion callback task: {e}")
            # Structural fallback to prevent message loss on unhandled code domain crashes
            await self._settle_message(message, False, consume_properties)

    async def _settle_message(
            self,
            message: aio_pika.abc.AbstractIncomingMessage,
            accepted: bool,
            consume_properties: dict
    ) -> None:
        """
        Acknowledges an accepted message or rejects a refused one.

        Args:
            message (aio_pika.abc.AbstractIncomingMessage): Incoming AMQP message frame instance.
            accepted (bool): True to acknowledge the message, False to reject it.
            consume_properties (dict): Consumption settings of the queue the message came from.
        """
        # --- Guard Clause: Nothing to settle with 'no_ack' ---
        # The broker considers the message delivered as soon as it is sent, so it cannot be acknowledged or rejected.
        # The default matches the one the connector applies when consuming.
        if consume_properties.get("no_ack", True):
            return

        if accepted:
            await self._rabbitmq_connector.acknowledge_message(message)
        else:
            await self._rabbitmq_connector.reject_message(message, **consume_properties)

    async def stop(self) -> None:
        """
        Graceful shutdown pipeline hook. Halts incoming consumer processing loops
        and safely closes underlying RabbitMQ channels and physical connections.
        """
        self._logger.debug(f"{self._LOG_PREFIX} Stopping logical consumer message ingestion processing pipelines.")

        # 1. Signal background consumption loops to reject/stop processing new frames
        self._stop_event.set()

        # 2. Close physical channels and active AMQP transport connection
        try:
            if hasattr(self._rabbitmq_connector, "is_connected") and self._rabbitmq_connector.is_connected():
                await self._rabbitmq_connector.close()
        except Exception as e:
            self._logger.error(f"{self._LOG_PREFIX} Failed to close AsyncRabbitMQ connector safely: {e}")

    @staticmethod
    def _validate_topology_schema(topology_setup: dict) -> None:
        """
        Validates the architectural schema structure of the topology configuration dictionary.

        Ensures that exchanges and queues contain mandatory string names and correct data types,
        while intentionally ignoring the contents of *_properties dictionaries to retain native
        compatibility with pika/aio-pika configurations.

        Args:
            topology_setup (dict): Topology configurations to validate.

        Raises:
            ValueError: If mandatory keys are missing, names are empty, or data types are invalid.
        """
        exchanges = topology_setup.get("exchanges")
        if not isinstance(exchanges, list) or not exchanges:
            raise ValueError("Topology validation error: 'exchanges' must be a non-empty list.")

        for ex_idx, exchange_block in enumerate(exchanges):
            if not isinstance(exchange_block, dict):
                raise ValueError(f"Topology validation error: Exchange item at index {ex_idx} must be a dictionary.")

            exchange_name = exchange_block.get("exchange_name")
            if not exchange_name or not isinstance(exchange_name, str) or not exchange_name.strip():
                raise ValueError(
                    f"Topology validation error: Mandatory string 'exchange_name' is missing or empty at index {ex_idx}."
                )

            # Validate optional exchange_properties type if present
            if "exchange_properties" in exchange_block and not isinstance(exchange_block["exchange_properties"], dict):
                raise ValueError(
                    f"Topology validation error: 'exchange_properties' for exchange '{exchange_name}' must be a dictionary."
                )

            queues = exchange_block.get("queues")
            if not isinstance(queues, list) or not queues:
                raise ValueError(
                    f"Topology validation error: 'queues' for exchange '{exchange_name}' must be a non-empty list."
                )

            for q_idx, queue_block in enumerate(queues):
                if not isinstance(queue_block, dict):
                    raise ValueError(
                        f"Topology validation error: Queue item at index {q_idx} under exchange '{exchange_name}' must be a dictionary."
                    )

                queue_name = queue_block.get("queue_name")
                if not isinstance(queue_name, str):
                    raise ValueError(
                        f"Topology validation error: 'queue_name' under exchange '{exchange_name}' must be a string."
                    )

                # Validate optional routing_key type if present
                if "routing_key" in queue_block and not isinstance(queue_block["routing_key"], str):
                    raise ValueError(
                        f"Topology validation error: 'routing_key' for queue '{queue_name}' must be a string."
                    )

                # Validate optional queue_properties type if present
                if "queue_properties" in queue_block and not isinstance(queue_block["queue_properties"], dict):
                    raise ValueError(
                        f"Topology validation error: 'queue_properties' for queue '{queue_name}' must be a dictionary."
                    )

                # Validate optional consume_properties type if present
                if "consume_properties" in queue_block and not isinstance(queue_block["consume_properties"], dict):
                    raise ValueError(
                        f"Topology validation error: 'consume_properties' for queue '{queue_name}' must be a dictionary."
                    )