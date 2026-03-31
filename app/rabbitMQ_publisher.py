import threading
import uuid
import json
import copy
from concurrent.futures import ThreadPoolExecutor
from app.utils.logger import LoggingUtils
from app.utils.retry import with_retries
from typing import Any
from app.connectors.rabbitmq_connector import RabbitMQConnector

class RabbitMQPublisher:
    """
    Handles asynchronous and synchronous message publishing with optional RPC support.
    """

    _LOG_PREFIX = "[RabbitMQ Publisher]"

    def __init__(self, server_configurations : dict, logger : LoggingUtils):
        """
        Initializes the Publisher based on the configuration dictionary.
        """
        self._logger = logger
        # Thread pool to handle async publishing without blocking the main flow
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="MQPublisher")

        self._connection = server_configurations.get("connection", {})

        if not self._connection:
            self._logger.error(f"{self._LOG_PREFIX} Configuration error: 'connection' section is missing.")
            raise KeyError("The 'connection' section is required for RabbitMQPublisher.")

        self._topology = server_configurations.get("topology", {})

        if not self._topology:
            self._logger.error(f"{self._LOG_PREFIX} Configuration error: 'topology' section is missing.")
            raise KeyError("The 'topology' section is required for RabbitMQPublisher.")

        self._exchange_props = self._topology.get("exchange_properties", {})
        self._base_properties = self._topology.get("message_properties", {})
        self._rpc_config = server_configurations.get("rpc_config", {})

        # Setup RPC consumer if enabled in configurations
        if self._rpc_config and self._rpc_config.get("enabled", False):
            self._pending_requests = {}
            with_retries(self._setup_consumer_service, logger=self._logger)

        # Setup the main publisher service
        with_retries(self._setup_publisher_service, logger=self._logger)

    def _setup_publisher_service(self) -> None:
        """Initialize RabbitMQ connection and declare exchange for publishing."""
        self._publisher_connector = RabbitMQConnector(self._connection)
        self._publisher_connector.connect()

        exchange_name = self._topology.get("exchange_name")

        if exchange_name:

            self._publisher_connector.declare_exchange(
                exchange_name=exchange_name,
                **self._exchange_props
            )

            self._logger.info(f"{self._LOG_PREFIX} Exchange '{exchange_name}' declared successfully.")
        else:
            self._logger.warning(f"{self._LOG_PREFIX} No exchange_name found in topology. Using default exchange.")

    def send_async(self, message: dict):
        """
        Submits a message to be sent asynchronously via the thread pool.
        """
        future = self._executor.submit(self.send_message, message)

        def _callback(f):
            try:
                f.result()
            except Exception as e:
                self._logger.error(f"{self._LOG_PREFIX} Async Publish Error: {e}")

        future.add_done_callback(_callback)

    def _setup_consumer_service(self) -> None:
        """Initialize RabbitMQ connection for consuming RPC responses."""
        self._consumer_connector = RabbitMQConnector(self._connection)

        self._consumer_connector.connect()

        reply_queue_configs = copy.deepcopy(self._rpc_config.get("reply_queue", {}))

        if not reply_queue_configs:
            self._logger.error(f"{self._LOG_PREFIX} Configuration error: 'reply_queue' section is missing.")
            raise KeyError("The 'reply_queue' section is required for RPC.")

        queue_name = reply_queue_configs.pop("name", "")

        # Declare the queue and store the assigned name (useful for exclusive/random names)
        self._return_queue_name: str = self._consumer_connector.declare_queue(queue_name=queue_name, **reply_queue_configs)

        self._logger.info(f"{self._LOG_PREFIX} RPC Reply queue established: '{self._return_queue_name}'")

        self._consumer_connector.setup_consumer(
            queue_name=self._return_queue_name,
            callback=self._on_response
        )

        # Start the background thread to listen for RPC responses
        self._consumer_thread = threading.Thread(
            target=self._consumer_connector.start_listening,
            daemon=False,
            name="RPC-Consumer-Thread"
        )
        self._consumer_thread.start()

    def send_message(self, message: dict):
        """
        Publishes a message and handles RPC correlation if enabled.
        """
        properties = copy.deepcopy(self._base_properties)
        properties["message_id"] = str(uuid.uuid4())
        corr_id = None

        use_rpc = self._rpc_config.get("enabled", False)

        if use_rpc:
            corr_id = str(uuid.uuid4())
            properties.update({
                "reply_to": self._return_queue_name,
                "correlation_id": corr_id
            })
            # Event to block the thread until response arrives
            event = threading.Event()
            self._pending_requests[corr_id] = {"event": event, "payload": None}

        # Publish the message to RabbitMQ
        self._publisher_connector.publish(
            exchange_name=self._topology.get("exchange_name", ""),
            routing_key=self._topology.get("routing_key", ""),
            message=message,
            properties=properties if properties else None
        )

        # Log the outgoing message with pretty JSON format
        pretty_msg = json.dumps(message, indent=4)
        self._logger.info(f"{self._LOG_PREFIX} Message published successfully:\n{pretty_msg}")

        if use_rpc:
            return self._wait_for_response(corr_id)

        return None

    def _wait_for_response(self, corr_id: str):
        """
        Blocks the current thread until an RPC response matches the correlation ID or times out.
        """

        request_info = self._pending_requests.get(corr_id)
        if not request_info:
            self._logger.error(
                f"{self._LOG_PREFIX} Internal Error: Request ID '{corr_id}' not found in pending requests.")
            return {"error": "Internal Error: request ID not found"}

        event = request_info["event"]
        timeout_sec = self._rpc_config.get("timeout_ms", 10000) / 1000

        # Block until 'event.set()' is called in _on_response or timeout is reached
        is_set = event.wait(timeout=timeout_sec)

        if is_set:
            response_data = self._pending_requests[corr_id]["payload"]
            del self._pending_requests[corr_id]
            return response_data
        else:
            self._logger.warning(f"{self._LOG_PREFIX} RPC Timeout reached for Correlation ID: {corr_id}")
            del self._pending_requests[corr_id]
            return {"error": "timeout", "status": 408}


    def stop(self) -> None:
        """
        Gracefully stops connectors and shuts down the background threads.
        """
        self._logger.info(f"{self._LOG_PREFIX} Shutting down publisher services...")

        # Shutdown the async executor pool
        self._executor.shutdown(wait=True)

        if hasattr(self, '_consumer_connector') and self._consumer_connector:
            self._consumer_connector.stop_consuming_safely()
            self._consumer_thread.join(timeout=5)
            self._consumer_connector.close()

        if hasattr(self, '_publisher_connector') and self._publisher_connector:
            self._publisher_connector.close()

        self._logger.info(f"{self._LOG_PREFIX} Publisher service stopped.")


    def _on_response(self, ch: Any, method: Any, properties: Any, body: bytes) -> None:
        """
        Callback triggered when a message arrives at the RPC reply queue.
        """
        try:
            raw_body = body.decode()
        except Exception:
            raw_body = str(body)

        # Extract correlation ID to match it with the waiting thread
        corr_id = properties.correlation_id

        if corr_id and corr_id in self._pending_requests:
            try:
                # Decode body and signal the blocked thread
                self._pending_requests[corr_id]["payload"] = json.loads(body.decode())
                self._pending_requests[corr_id]["event"].set()
                self._logger.debug(f"{self._LOG_PREFIX} Response matched for Correlation ID: {corr_id}")
            except Exception as e:
                self._logger.error(f"{self._LOG_PREFIX} Error decoding RPC response body: {e}")
        else:
            self._logger.warning(
                f"{self._LOG_PREFIX} Received unmatched or invalid RPC response. "
                f"ID: {corr_id}. Content: {raw_body}"
            )