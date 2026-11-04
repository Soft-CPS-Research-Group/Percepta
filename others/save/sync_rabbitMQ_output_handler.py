# --- Standard Library Imports ---
from enum import Enum  # Used to define structured execution routing modes
import json  # Used for data serialization and pretty logging formats
import threading  # Used for thread-safe synchronization guards and mutex locking
import uuid  # Used for creating unique message and correlation identifiers
from typing import Any, Dict, List, Optional, Set  # Type annotations for static analysis boundaries

# --- Internal Framework Imports ---
from others.save.sync_rabbitmq_connector import SyncRabbitMQConnector  # Low-level network AMQP channel manager
from app.core.output_handlers import OutputHandlerBase  # Abstract baseline contract
from app.core.utils.logger import LoggingUtils  # Standardized telemetry logger instance


class MqPublishMode(Enum):
    """
    Specifies how dynamic exchange routing parameters are combined with the static
    topology setup during outbound transmission.
    """
    STATIC = "static"  # Ignores dynamic inputs; targets only pre-configured exchanges
    OVERRIDE = "override"  # Bypasses static exchanges; targets ONLY dynamic inputs
    ADDITIVE = "additive"  # Targets both static exchanges and dynamic inputs merged


class RabbitMQOutputHandler(OutputHandlerBase):
    """
    Handles outbound message publishing across multiple modular exchanges with
    dynamic asynchronous RPC synchronization capabilities.

    Operates strictly as a pure logical business layer component, decoupled
    from low-level infrastructure thread or lifecycle allocation.
    """

    # --- Class Attribute & Type Declarations ---
    _LOG_PREFIX: str  # Standardized logging identifier prefix
    _topology_setup: dict  # Centralized list-based exchange routing architecture
    _publisher_connector: SyncRabbitMQConnector  # Active connection pipeline dedicated strictly to egress
    _requests_lock: threading.Lock  # Mutex guard protecting the pending requests table
    _pending_requests: Dict[str, dict]  # Shared lookup table tracking active synchronous RPC events
    _stop_event: threading.Event  # Thread-safe flag indicating system shutdown sequence
    _declared_exchanges: Set[str]  # Tracking cache containing successfully verified/declared exchanges

    _LOG_PREFIX = "RabbitMQ Output Handler |"

    def __init__(
            self,
            topology_setup: dict,
            publisher_connector: SyncRabbitMQConnector,
            logger: LoggingUtils
    ) -> None:
        """
        Initializes the resilient Publisher logical boundary with pre-built connection assets.
        """
        super().__init__(logger)
        self._topology_setup = topology_setup
        self._publisher_connector = publisher_connector

        # Thread-safe synchronization primitives
        self._requests_lock = threading.Lock()
        self._pending_requests = {}
        self._stop_event = threading.Event()
        self._declared_exchanges = set()

        # Bootstrap and verify default static publishing exchange network layout
        static_exchanges = self._topology_setup.get("exchanges", [])
        self._setup_publisher_service(static_exchanges)

    def _setup_publisher_service(self, exchanges_list: List[dict]) -> None:
        """
        Iterates over a specified list of exchange configurations to declare and verify
        network routing boundaries. Maintains an in-memory verification cache to reduce I/O.
        """
        # --- Variable Declarations ---
        exchange_block: dict
        exchange_name: str
        exchange_properties: dict

        for exchange_block in exchanges_list:
            exchange_name = exchange_block.get("exchange_name", "")
            if not exchange_name:
                self._logger.warning(
                    f"{self._LOG_PREFIX} Missing 'exchange_name' key inside target configuration block."
                )
                continue

            # Thread-safe check and network verification of the exchange
            if exchange_name not in self._declared_exchanges:
                exchange_properties = exchange_block.get("exchange_properties", {})

                self._publisher_connector.declare_exchange(
                    exchange_name=exchange_name,
                    **exchange_properties
                )
                self._declared_exchanges.add(exchange_name)
                self._logger.info(f"{self._LOG_PREFIX} Egress Exchange '{exchange_name}' verified/declared.")

    def send(
            self,
            message: dict,
            dynamic_exchanges: Optional[List[dict]] = None,
            mode: MqPublishMode = MqPublishMode.STATIC
    ) -> Optional[dict]:
        """
        Publishes a payload frame. Resolves target exchanges dynamically based on the
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
        event: threading.Event
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
        self._setup_publisher_service(resolved_exchanges)

        # 4. Sequential broadcast loop firing messages across resolved channel nodes
        for exchange_block in resolved_exchanges:
            exchange_name = exchange_block.get("exchange_name", "")
            if not exchange_name:
                continue

            # Base envelope metadata packet
            properties = {"message_id": str(uuid.uuid4())}

            # Extract and inject standard publishing metadata parameters
            message_properties = exchange_block.get("message_properties", {})
            properties.update(message_properties)

            # Evaluate conditional RPC synchronization properties
            rpc_properties = exchange_block.get("rpc_properties", {})

            if rpc_properties.get("enabled", False):
                reply_to_config = rpc_properties.get("reply_to", {})
                rpc_exchanges_list = reply_to_config.get("exchanges", [])

                if rpc_exchanges_list:
                    # Safely extract the first matching array node reflecting the strict RPC return contract
                    first_rpc_exchange = rpc_exchanges_list[0]
                    rpc_queues_list = first_rpc_exchange.get("queues", [])

                    if rpc_queues_list:
                        first_rpc_queue = rpc_queues_list[0]
                        return_routing_key = first_rpc_queue.get("routing_key", "")

                        corr_id = str(uuid.uuid4())
                        event = threading.Event()

                        # Inbound the routing key to the worker via native AMQP properties
                        properties.update({
                            "reply_to": return_routing_key,
                            "correlation_id": corr_id,
                            "headers": {
                                "reply_exchange": first_rpc_exchange.get("exchange_name", "")
                            }
                        })

                        # Escrow tracking identifiers into the shared concurrent lookups map
                        with self._requests_lock:
                            self._pending_requests[corr_id] = {"event": event, "payload": None}

                        rpc_wait_list.append({
                            "correlation_id": corr_id,
                            "timeout_ms": rpc_properties.get("timeout_ms", 10000)
                        })

            # Fire data down to the underlying transport connector link
            routing_key = exchange_block.get("routing_key", "")
            self._publisher_connector.publish(
                exchange_name=exchange_name,
                routing_key=routing_key,
                message=message,
                properties=properties
            )

            pretty_msg = json.dumps(message, indent=4)
            self._logger.info(
                f"{self._LOG_PREFIX} Message published to [{exchange_name}] via route [{routing_key}]:\n{pretty_msg}"
            )

        # Step 5: Once the broadcast sequence finishes, enter synchronous wait block loops if necessary
        if rpc_wait_list:
            return self._wait_for_responses(rpc_wait_list)

        return None

    def _wait_for_responses(self, rpc_wait_list: List[dict]) -> dict:
        """
        Blocks the active calling thread sequentially to collect response payloads
        released from background consumption threads.
        """
        # --- Variable Declarations & Type Hints ---
        collected_responses: dict
        request_metadata: dict
        corr_id: str
        timeout_sec: float
        request_info: Optional[dict]
        event: threading.Event
        is_set: bool
        payload: Any

        collected_responses = {}

        for request_metadata in rpc_wait_list:
            corr_id = request_metadata["correlation_id"]
            timeout_sec = request_metadata["timeout_ms"] / 1000

            with self._requests_lock:
                request_info = self._pending_requests.get(corr_id)

            if not request_info:
                collected_responses[corr_id] = {"error": "Internal synchronization mapping data loss"}
                continue

            event = request_info["event"]

            # Force the thread context to block until payload materializes or timeout expires
            is_set = event.wait(timeout=timeout_sec)

            with self._requests_lock:
                if is_set:
                    payload = self._pending_requests.get(corr_id, {}).get("payload")
                    collected_responses[corr_id] = {"status": "success", "data": payload}
                else:
                    self._logger.warning(f"{self._LOG_PREFIX} RPC Response timeout expired for ID: {corr_id}")
                    collected_responses[corr_id] = {"error": "timeout", "status": 408}

                # Clear heap allocations immediately to protect memory buffers
                if corr_id in self._pending_requests:
                    del self._pending_requests[corr_id]

        return collected_responses

    def on_raw_rpc_response(self, body_bytes: bytes) -> bool:
        """
        Direct raw signature hook invoked by late-bind launcher bridges upon frame arrival.
        Maps bytes, unpacks structures, and releases waiting blocking conditions.
        """
        # --- Variable Declarations & Type Hints ---
        raw_body: str
        payload_unpacked: dict
        corr_id: Optional[str]
        message_properties: Any

        try:
            # We fetch correlation ID straight out of internal parsing logic (assumes bytes unpacking or wrapper pass)
            # Adjust this lookup logic if your connector passes pika properties down independently
            payload_unpacked = json.loads(body_bytes.decode("utf-8"))
            corr_id = payload_unpacked.get("correlation_id")  # O teu trabalhador devolve isto no payload ou no wrapper

            with self._requests_lock:
                if corr_id and corr_id in self._pending_requests:
                    self._pending_requests[corr_id]["payload"] = payload_unpacked
                    self._pending_requests[corr_id]["event"].set()
                    self._logger.debug(
                        f"{self._LOG_PREFIX} Inbound RPC response frame matched securely for ID: {corr_id}")
                    return True

        except Exception as e:
            self._logger.error(f"{self._LOG_PREFIX} Error decoding raw asynchronous RPC payload: {e}")
            return False

        self._logger.warning(f"{self._LOG_PREFIX} Received unmatched, corrupted or expired RPC response frame context.")
        return False

    def stop(self) -> None:
        """
        Polymorphic interface shutdown hook. Transitions internal logical flags.
        Infrastructure termination (sockets closing) is handles exclusively by the root Launcher.
        """
        self._logger.info(f"{self._LOG_PREFIX} Rejection constraints triggered. Flushing logical boundaries.")
        self._stop_event.set()