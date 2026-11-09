# --- Standard Library Imports ---
import asyncio  # Asynchronous I/O event loop primitives for non-blocking execution
from enum import Enum  # Used to define structured execution routing modes
import json  # Used for data serialization and pretty logging formats
import uuid  # Used for creating unique message and correlation identifiers
from typing import Any, Dict, List, Optional, Set  # Type annotations for static analysis boundaries

# --- Internal Framework Imports ---
from app.core.connectors.async_rabbitmq_connector import \
    AsyncRabbitMQConnector  # Low-level network async AMQP channel manager
from app.core.output_handlers.output_handler_base import OutputHandlerBase  # Abstract baseline contract
from app.core.utils.logger import LoggingUtils  # Standardized telemetry logger instance


class MqPublishMode(Enum):
    """
    Specifies how dynamic exchange routing parameters are combined with the static
    topology setup during outbound transmission.
    """
    STATIC = "static"  # Ignores dynamic inputs; targets only pre-configured exchanges
    OVERRIDE = "override"  # Bypasses static exchanges; targets ONLY dynamic inputs
    ADDITIVE = "additive"  # Targets both static exchanges and dynamic inputs merged


class AsyncRabbitMQOutputHandler(OutputHandlerBase):
    """
    Handles outbound message publishing across multiple modular exchanges with
    dynamic asynchronous RPC synchronization capabilities.

    Operates strictly as a pure logical business layer component, decoupled
    from low-level infrastructure thread or lifecycle allocation.
    """

    # --- Class Attribute & Type Declarations ---
    _LOG_PREFIX: str  # Standardized logging identifier prefix
    _topology_setup: dict  # Centralized list-based exchange routing architecture
    _publisher_connector: AsyncRabbitMQConnector  # Active async connection pipeline dedicated strictly to egress
    _requests_lock: asyncio.Lock  # Mutex guard protecting the pending requests table
    _pending_requests: Dict[str, dict]  # Shared lookup table tracking active asynchronous RPC events
    _stop_event: asyncio.Event  # Async flag indicating system shutdown sequence
    _declared_exchanges: Set[str]  # Tracking cache containing successfully verified/declared exchanges

    _LOG_PREFIX = "RabbitMQ Output Handler |"

    def __init__(
            self,
            topology_setup: dict,
            connector: AsyncRabbitMQConnector,
            logger: LoggingUtils
    ) -> None:
        """
        Initializes the resilient Publisher logical boundary with pre-built async connection assets.
        Schedules the static exchange setup task directly onto the active Event Loop.
        """
        super().__init__(logger)

        # --- Guard Clause: Validate connector dependency ---
        if not connector:
            error_msg = "Initialization error: 'connector' dependency is missing."
            self._logger.error(f"{self._LOG_PREFIX} {error_msg}")
            raise ValueError(error_msg)

        self._publisher_connector = connector
        self._requests_lock = asyncio.Lock()
        self._pending_requests = {}
        self._stop_event = asyncio.Event()
        self._declared_exchanges = set()

        if topology_setup:
            # --- Guard Clause: Validate topology schema structure ---
            try:
                self._validate_topology_schema(topology_setup)
            except ValueError as val_err:
                self._logger.error(f"{self._LOG_PREFIX} {val_err}")
                raise

            self._topology_setup = topology_setup
        else:
            self._topology_setup = {"exchanges": []}


    async def _setup_publisher_service(self, exchanges_list: List[dict]) -> None:
        """
        Iterates over a specified list of exchange configurations to declare and verify
        network routing boundaries. Maintains an in-memory verification cache to reduce I/O.
        """
        # --- Variable Declarations ---
        exchange_block: dict
        exchange_name: str
        exchange_properties: dict

        # Safety Gate: Ensure the underlying connector is connected before executing network calls
        if hasattr(self._publisher_connector, "is_connected") and not self._publisher_connector.is_connected():
            await self._publisher_connector.connect()

        for exchange_block in exchanges_list:
            exchange_name = exchange_block.get("exchange_name", "")
            if not exchange_name:
                self._logger.warning(
                    f"{self._LOG_PREFIX} Missing 'exchange_name' key inside target configuration block."
                )
                continue

            # Event Loop safe check and network verification of the exchange
            if exchange_name not in self._declared_exchanges:
                exchange_properties = exchange_block.get("exchange_properties", {})

                await self._publisher_connector.declare_exchange(
                    exchange_name=exchange_name,
                    **exchange_properties
                )
                self._declared_exchanges.add(exchange_name)
                self._logger.debug(f"{self._LOG_PREFIX} Egress Exchange '{exchange_name}' verified/declared.")

    async def send(
            self,
            message: dict,
            dynamic_exchanges: Optional[List[dict]] = None,
            mode: MqPublishMode = MqPublishMode.STATIC
    ) -> Optional[dict]:
        """
        Publishes a payload frame asynchronously. Resolves target exchanges dynamically based on the
        chosen execution mode (STATIC, OVERRIDE, or ADDITIVE) and registers any unverified exchange.
        Saves correlation contexts to wait for asynchronous RPC responses if flagged.
        """
        # --- Variable Declarations & Type Hints ---
        rpc_wait_list: List[Dict[str, Any]]
        static_exchanges: list
        resolved_exchanges: List[dict]
        exchange_block: dict
        exchange_name: str
        routing_key: str
        properties: dict
        message_properties: dict
        rpc_properties: dict
        reply_to_config: dict
        rpc_exchanges_list: list
        first_rpc_exchange: dict
        rpc_queues_list: list
        first_rpc_queue: dict
        return_routing_key: str
        corr_id: str
        event: asyncio.Event
        pretty_msg: str

        rpc_wait_list = []

        if self._stop_event.is_set():
            self._logger.warning(
                f"{self._LOG_PREFIX} Rejecting transmission: Handler is running under shutdown constraints."
            )
            return None

        # 1. Resolve configuration boundaries by extracting static topology mappings
        static_exchanges = self._topology_setup.get("exchanges", [])
        dynamic_exchanges = dynamic_exchanges or []
        # TODO VALIDAR EXCHANGES
        # 2. Compute the targeting matrix based on the specified publication mode
        if mode == MqPublishMode.OVERRIDE:
            if not dynamic_exchanges:
                self._logger.warning(
                    f"{self._LOG_PREFIX} Override mode requested but no dynamic exchanges were provided. Falling back to static."
                )
                resolved_exchanges = static_exchanges
            else:
                resolved_exchanges = dynamic_exchanges
        elif mode == MqPublishMode.ADDITIVE:
            resolved_exchanges = static_exchanges + dynamic_exchanges
        else:
            resolved_exchanges = static_exchanges

        # 3. Symmetrically register and declare any unresolved target exchanges before publishing
        await self._setup_publisher_service(resolved_exchanges)

        ## 4. Dispatch parallel broadcast tasks across all resolved exchange nodes
        broadcast_tasks = [
            self._process_single_exchange_publication(
                exchange_block=exchange_block,
                message=message,
                rpc_wait_list=rpc_wait_list
            )
            for exchange_block in resolved_exchanges
            if exchange_block.get("exchange_name")
        ]

        if broadcast_tasks:
            # Executes all target exchange dispatch operations concurrently in parallel
            await asyncio.gather(*broadcast_tasks)

        # Step 5: Once the parallel broadcast sequence finishes, enter non-blocking wait routines if necessary
        if rpc_wait_list:
            return await self._wait_for_responses(rpc_wait_list)

        return None

    async def _process_single_exchange_publication(
            self,
            exchange_block: dict,
            message: Any,
            rpc_wait_list: list
    ) -> None:
        """
        Processes metadata preparation, RPC context registration, and asynchronous dispatch
        for a single exchange target configuration.
        """
        # --- Variable Declarations & Type Hints ---
        exchange_name: str
        routing_key: str
        properties: dict
        message_properties: dict
        rpc_properties: dict
        reply_to_config: dict
        rpc_exchanges_list: list
        first_rpc_exchange: dict
        rpc_queues_list: list
        first_rpc_queue: dict
        return_routing_key: str
        corr_id: str
        event: asyncio.Event
        pretty_msg: str
        is_rpc_enabled: bool

        exchange_name = exchange_block.get("exchange_name", "")
        routing_key = exchange_block.get("routing_key", "")

        # Base envelope metadata packet
        properties = {"message_id": str(uuid.uuid4())}

        # Extract and inject standard publishing metadata parameters
        message_properties = exchange_block.get("message_properties", {})
        properties.update(message_properties)

        # Evaluate conditional RPC synchronization properties
        rpc_properties = exchange_block.get("rpc_properties", {})
        is_rpc_enabled = rpc_properties.get("enabled", False)

        if is_rpc_enabled:
            reply_to_config = rpc_properties.get("reply_to", {})
            rpc_exchanges_list = reply_to_config.get("exchanges", [])

            if rpc_exchanges_list:
                first_rpc_exchange = rpc_exchanges_list[0]
                rpc_queues_list = first_rpc_exchange.get("queues", [])

                if rpc_queues_list:
                    first_rpc_queue = rpc_queues_list[0]
                    return_routing_key = first_rpc_queue.get("routing_key", "")

                    corr_id = str(uuid.uuid4())
                    event = asyncio.Event()

                    # Inbound the routing key to the worker via native AMQP properties
                    properties.update({
                        "reply_to": return_routing_key,
                        "correlation_id": corr_id,
                        "headers": {
                            "reply_exchange": first_rpc_exchange.get("exchange_name", "")
                        }
                    })

                    # Escrow tracking identifiers into the shared concurrent lookups map (Thread-safe lock)
                    async with self._requests_lock:
                        self._pending_requests[corr_id] = {"event": event, "payload": None}

                    rpc_wait_list.append({
                        "source_identifier": f"{first_rpc_exchange.get('exchange_name', '')}/{first_rpc_queue.get('queue_name')}",
                        "correlation_id": corr_id,
                        "timeout_ms": rpc_properties.get("timeout_ms", 10000)
                    })

        # Routing Execution Strategy (Awaited Sync for RPC vs. Async Fire-and-Forget)
        if is_rpc_enabled:
            # --- CASE A: RPC ENABLED ---
            # Must await publish to guarantee the message is strictly in-flight
            await self._publisher_connector.publish(
                exchange_name=exchange_name,
                routing_key=routing_key,
                message=message,
                properties=properties
            )
        else:
            # --- CASE B: FIRE-AND-FORGET ---
            # Dispatch publication as a background task to unblock execution immediately.
            asyncio.create_task(
                self._safe_background_publish(
                    exchange_name=exchange_name,
                    routing_key=routing_key,
                    message=message,
                    properties=properties
                )
            )

        pretty_msg = json.dumps(message, indent=4) if isinstance(message, (dict, list)) else str(message)
        self._logger.debug(
            f"{self._LOG_PREFIX} Message published to [{exchange_name}]:\n{pretty_msg}"
        )

    async def _safe_background_publish(
            self,
            exchange_name: str,
            routing_key: str,
            message: dict,
            properties: dict
    ) -> None:
        """
        Helper wrapper to execute background publishing safely without crashing the event loop on errors.
        """
        try:
            await self._publisher_connector.publish(
                exchange_name=exchange_name,
                routing_key=routing_key,
                message=message,
                properties=properties
            )
        except Exception as e:
            self._logger.error(
                f"{self._LOG_PREFIX} Background publish failed for exchange [{exchange_name}]: {e}"
            )

    async def _wait_for_responses(self, rpc_wait_list: List[dict]) -> dict:
        """
        Asynchronously waits to collect response payloads released from background consumption tasks
        in parallel across all RPC contexts without blocking the Event Loop.
        """
        # --- Variable Declarations & Type Hints ---
        tasks: List[Any]
        results: List[tuple]
        corr_id: str
        res_data: dict
        collected_responses: dict

        collected_responses = {}

        async def _wait_single(request_metadata: dict) -> Optional[tuple[str, dict[str, str]]]:
            """
            Internal helper coroutine to wait for a single RPC response event asynchronously.
            """
            # --- Variable Declarations & Type Hints ---
            corr_id: str
            timeout_sec: float
            request_info: Optional[dict]
            event: asyncio.Event
            payload: Any
            source_identifier: str = request_metadata.get("source_identifier", "")
            corr_id = request_metadata["correlation_id"]
            timeout_sec = request_metadata["timeout_ms"] / 1000

            async with self._requests_lock:
                request_info = self._pending_requests.get(corr_id)

            if not request_info:
                return corr_id, {"error": "Internal synchronization mapping data loss"}

            event = request_info["event"]

            # Cooperatively yield control to the event loop until payload materializes or timeout expires
            try:
                await asyncio.wait_for(event.wait(), timeout=timeout_sec)

                async with self._requests_lock:
                    payload = self._pending_requests.get(corr_id, {}).get("payload")
                    return corr_id, payload

            except asyncio.TimeoutError:
                self._logger.warning(f"{self._LOG_PREFIX} RPC Response timeout expired for ID: {corr_id}")
                return corr_id, {"protocol": "AMQP", "source": source_identifier, "status": 408, "content_type": "text/plain", "payload": "timeout"} # TODO AQUI TERÁ DE SER O PAYLOADNORMALIZER PARA ERROS

            finally:
                # Clear heap allocations immediately to protect memory buffers
                async with self._requests_lock:
                    if corr_id in self._pending_requests:
                        del self._pending_requests[corr_id]

        # 1. Dispatch all individual RPC wait routines concurrently to the Event Loop
        tasks = [_wait_single(req) for req in rpc_wait_list]
        results = await asyncio.gather(*tasks)

        # 2. Map all resolved coroutine tuples into the final responses collection dictionary
        for corr_id, res_data in results:
            collected_responses[corr_id] = res_data

        return collected_responses

    async def on_rpc_response(self, response: dict) -> bool:
        """
        Direct hook invoked by transport/launcher bridges upon RPC response envelope arrival.
        Extracts correlation metadata and signals waiting asynchronous event conditions.
        """
        # --- Variable Declarations & Type Hints ---
        properties: Optional[dict]
        corr_id: Optional[str]

        try:
            # Extract correlation ID safely from envelope properties dictionary
            properties = response.get("metadata") if isinstance(response, dict) else None

            if properties and isinstance(properties, dict):
                corr_id = properties.get("correlation_id")

                async with self._requests_lock:
                    if corr_id and corr_id in self._pending_requests:
                        self._pending_requests[corr_id]["payload"] = response
                        self._pending_requests[corr_id]["event"].set()
                        self._logger.debug(
                            f"{self._LOG_PREFIX} Inbound RPC response frame matched securely for ID: {corr_id}"
                        )
                        return True

        except Exception as e:
            self._logger.error(f"{self._LOG_PREFIX} Error processing RPC response payload envelope: {e}")
            return False

        self._logger.warning(
            f"{self._LOG_PREFIX} Received unmatched, corrupted or expired RPC response envelope context."
        )
        return False

    async def stop(self) -> None:
        """
        Graceful shutdown pipeline hook. Halts logical transmission pipelines,
        cancels pending RPC contexts, and safely closes the underlying RabbitMQ publisher connector.
        """
        self._logger.debug(
            f"{self._LOG_PREFIX} Stopping outbound message dispatcher and closing AMQP transport pipelines.")

        # 1. Signal transmission routines to reject new outbound publishing tasks
        self._stop_event.set()

        # 2. Release any pending RPC response wait events to prevent hanging coroutines
        async with self._requests_lock:
            for corr_id, req_data in self._pending_requests.items():
                event = req_data.get("event")
                if event and not event.is_set():
                    req_data["payload"] = {
                        "protocol": "AMQP",
                        "status": 503,
                        "content_type": "text/plain",
                        "payload": "Handler stopped during execution"
                    }
                    event.set()
            self._pending_requests.clear()

        # 3. Close physical channels and active AMQP transport connection
        try:
            if hasattr(self._publisher_connector, "is_connected") and self._publisher_connector.is_connected():
                await self._publisher_connector.close()
        except Exception as e:
            self._logger.error(f"{self._LOG_PREFIX} Failed to close AsyncRabbitMQ publisher connector safely: {e}")

    @staticmethod
    def _validate_topology_schema(topology_setup: dict) -> None:
        """
        Validates the architectural schema structure of the RabbitMQ output topology configuration dictionary.

        Ensures that exchanges contain mandatory string names and that optional RPC configurations adhere
        to the strict reply-to topology hierarchy, while intentionally ignoring the contents of *_properties
        dictionaries to retain native driver compatibility (pika/aio-pika).

        Args:
            topology_setup (dict): Egress topology configurations to validate.

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

            # Validate optional routing_key type
            if "routing_key" in exchange_block and not isinstance(exchange_block["routing_key"], str):
                raise ValueError(
                    f"Topology validation error: 'routing_key' for exchange '{exchange_name}' must be a string."
                )

            # Validate optional properties dictionaries
            for prop_key in ("exchange_properties", "message_properties"):
                if prop_key in exchange_block and not isinstance(exchange_block[prop_key], dict):
                    raise ValueError(
                        f"Topology validation error: '{prop_key}' for exchange '{exchange_name}' must be a dictionary."
                    )

            # Validate optional rpc_properties structure
            rpc_properties = exchange_block.get("rpc_properties")
            if rpc_properties is not None:
                if not isinstance(rpc_properties, dict):
                    raise ValueError(
                        f"Topology validation error: 'rpc_properties' for exchange '{exchange_name}' must be a dictionary."
                    )

                if "enabled" in rpc_properties and not isinstance(rpc_properties["enabled"], bool):
                    raise ValueError(
                        f"Topology validation error: 'enabled' in 'rpc_properties' for exchange '{exchange_name}' must be a boolean."
                    )

                # Validate RPC reply_to hierarchy when RPC is enabled
                if rpc_properties.get("enabled", False):
                    if "timeout_ms" in rpc_properties:
                        timeout_ms = rpc_properties["timeout_ms"]
                        if not isinstance(timeout_ms, (int, float)) or timeout_ms <= 0:
                            raise ValueError(
                                f"Topology validation error: 'timeout_ms' in 'rpc_properties' for exchange '{exchange_name}' must be a positive number."
                            )

                    reply_to = rpc_properties.get("reply_to")
                    if not isinstance(reply_to, dict):
                        raise ValueError(
                            f"Topology validation error: 'reply_to' in 'rpc_properties' for exchange '{exchange_name}' must be a dictionary when RPC is enabled."
                        )

                    reply_exchanges = reply_to.get("exchanges")
                    if not isinstance(reply_exchanges, list) or not reply_exchanges:
                        raise ValueError(
                            f"Topology validation error: 'exchanges' inside 'reply_to' for exchange '{exchange_name}' must be a non-empty list."
                        )

                    for r_idx, reply_ex in enumerate(reply_exchanges):
                        if not isinstance(reply_ex, dict):
                            raise ValueError(
                                f"Topology validation error: Reply exchange item at index {r_idx} under '{exchange_name}' must be a dictionary."
                            )

                        r_name = reply_ex.get("exchange_name")
                        if not r_name or not isinstance(r_name, str) or not r_name.strip():
                            raise ValueError(
                                f"Topology validation error: Mandatory 'exchange_name' missing or empty in RPC reply_to at index {r_idx}."
                            )

                        reply_queues = reply_ex.get("queues")
                        if not isinstance(reply_queues, list) or not reply_queues:
                            raise ValueError(
                                f"Topology validation error: 'queues' inside RPC reply exchange '{r_name}' must be a non-empty list."
                            )

                        for q_idx, reply_q in enumerate(reply_queues):
                            if not isinstance(reply_q, dict):
                                raise ValueError(
                                    f"Topology validation error: Reply queue item at index {q_idx} under exchange '{r_name}' must be a dictionary."
                                )

                            q_name = reply_q.get("queue_name")
                            if not isinstance(q_name, str):
                                raise ValueError(
                                    f"Topology validation error: 'queue_name' in RPC reply queue at index {q_idx} must be a string."
                                )


    # TODO SECALHAR TB FAZ SENTIDO USAR O PAYLOADNORMALIZER AQUI NAS RESPOSTAS