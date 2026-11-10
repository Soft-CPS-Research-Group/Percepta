# --- Standard Library Imports ---
import asyncio
from typing import Any, Callable, Dict, List, Optional, Union

# --- Internal Framework Imports ---
from app.core.input_handlers.input_handler_base import InputHandlerBase
from app.core.connectors.async_http_connector import AsyncHTTPConnector
from app.core.utils.logger import LoggingUtils
from app.core.utils.async_periodic_scheduler import AsyncPeriodicScheduler
from app.core.input_handlers.payload_normalizer import PayloadNormalizer
from app.core.endpoint_builders.endpoint_builder_base import EndpointBuilderBase


class HTTPInputHandler(InputHandlerBase):
    """
    Asynchronous HTTP polling client.

    Periodically requests data from configured endpoints in parallel, parses
    the responses, and forwards the payloads to a domain callback. Supports
    entity-grouped endpoint maps for independent, concurrent group dispatch.

    Does not manage low-level client sessions or connection pool lifecycles,
    delegating those network operations entirely to the AsyncHTTPConnector.
    """

    # Define a standard text prefix to be used for all log messages within this class
    _LOG_PREFIX: str = "Async HTTP Input Handler |"
    # Define an immutable set containing the allowed HTTP methods (only GET in this case)
    _ALLOWED_METHODS = {"GET"}

    def __init__(
            self,
            topology_setup: dict,
            on_message_callback: Callable[[Any], Any],
            connector: AsyncHTTPConnector,
            scheduler: AsyncPeriodicScheduler,
            time_interval_seconds: int = 15,
            logger: LoggingUtils = None,
            endpoint_builder: Optional[EndpointBuilderBase] = None
    ) -> None:
        """
        Initializes the HTTP polling client with its configuration and dependencies.

        Args:
            topology_setup (dict): Target endpoints configuration.
                Example:
                    {
                      "endpoints": [
                        {
                          "path": "/v1/telemetry/meter_01",
                          "method": "GET",
                          "timeout_ms": 4000,
                          "headers": { "Authorization": "Bearer token_hash_abc" }
                        },
                        {
                          "path": "/v1/telemetry/meter_02",
                          "method": "GET",
                          "timeout_ms": 4000,
                          "headers": { "Authorization": "Bearer token_hash_abc" }
                        }
                      ]
                    }
            on_message_callback (Callable): Callback function to process received message.
            connector (AsyncHTTPConnector): Connector to execute the HTTP requests.
            scheduler (PeriodicScheduler): Scheduler to run the polling cycles periodically.
            time_interval_seconds (int, optional): Polling frequency interval in
                seconds. Defaults to 15.
            logger (LoggingUtils): Utility to handle system logs and telemetry.
            endpoint_builder (EndpointBuilderBase, optional): Dynamic endpoint builder.
        """
        super().__init__(logger)

        # --- Guard Clause: Validate topology_setup exists ---
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

        # --- Guard Clause: Validate time_interval_seconds ---
        if not isinstance(time_interval_seconds, (int, float)) or time_interval_seconds <= 0:
            error_msg = "Initialization error: 'time_interval_seconds' must be a positive number."
            self._logger.error(f"{self._LOG_PREFIX} {error_msg}")
            raise ValueError(error_msg)

        self._topology_setup = topology_setup
        self._endpoint_builder = endpoint_builder
        self._time_interval_seconds: int = int(time_interval_seconds)

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

        self._http_connector = connector

        # --- Guard Clause: Validate scheduler dependency ---
        if not scheduler:
            error_msg = "Initialization error: 'scheduler' dependency is missing."
            self._logger.error(f"{self._LOG_PREFIX} {error_msg}")
            raise ValueError(error_msg)

        self._scheduler = scheduler

        # Extract static endpoints
        self._endpoints: List[dict] = topology_setup.get("endpoints", [])

        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        """
        Instantiates and registers the periodic polling job directly into the async scheduler engine.
        """
        if self._stop_event.is_set():
            self._logger.warning(
                f"{self._LOG_PREFIX} Start aborted: Handler is running under shutdown constraints."
            )
            return

        self._scheduler.create_job(
            time_interval=self._time_interval_seconds,
            trigger_callback=self._poll_all_endpoints,
            job_id="http_polling_ingest"
        )
        self._scheduler.start()

    async def _poll_all_endpoints(self) -> None:
        """
        Dispatches grouped or flat endpoint fetching routines concurrently.
        Each endpoint group runs in its own task and dispatches its callback
        independently upon completion.
        """
        if self._stop_event.is_set():
            self._logger.warning(
                f"{self._LOG_PREFIX} Poll Endpoints aborted: Handler is running under shutdown constraints."
            )
            return

        # 1. Resolve dynamic or static endpoints
        if self._endpoint_builder:
            target_endpoints = await self._endpoint_builder.build_endpoints()
        else:
            target_endpoints = self._endpoints

        if not target_endpoints:
            self._logger.debug(f"{self._LOG_PREFIX} No target endpoints resolved for current cycle.")
            return

        # 2. Normalize targets into a map of {group_key: [endpoint_list]}
        grouped_targets: Dict[str, List[dict]] = {}

        if isinstance(target_endpoints, dict):
            grouped_targets = target_endpoints
        elif isinstance(target_endpoints, list):
            # Flat list fallback mapped to a generic single group
            grouped_targets = {"default": target_endpoints}
        else:
            self._logger.error(
                f"{self._LOG_PREFIX} Unexpected endpoint structure format: {type(target_endpoints)}"
            )
            return

        self._logger.debug(
            f"{self._LOG_PREFIX} Spawning parallel fetch tasks for {len(grouped_targets)} groups: "
            f"{list(grouped_targets.keys())}"
        )

        # 3. Spawn an independent background task for each group so they resolve and dispatch in parallel
        for group_id, endpoints in grouped_targets.items():
            if endpoints:
                asyncio.create_task(self._fetch_group_and_dispatch(group_id, endpoints))

    async def _fetch_group_and_dispatch(self, group_id: str, endpoints: List[dict]) -> None:
        """
        Fetches all endpoints belonging to a specific group concurrently and invokes
        the system callback immediately once this group finishes.

        Expected Payload Delivered to Callback (`results`):
        ---------------------------------------------------
        [
            {
                "protocol": "HTTP",
                "source": "/data/request/Instant?instantType=avg&tags=35927&from=...&to=...",
                "status": 200,
                "content_type": "application/json",
                "payload": {
                    "data": [
                        {
                            "tag": "35927",
                            "timestamp": "2026-08-19 17:55:00",
                            "value": 12.45
                        }
                    ]
                },
                "metadata": {
                    "entity_id": "B01",
                    "parameter": "energy_in"
                }
            },
            {
                "protocol": "HTTP",
                "source": "/data/request/Instant?instantType=avg&tags=33690&from=...&to=...",
                "status": 200,
                "content_type": "application/json",
                "payload": {
                    "data": [
                        {
                            "tag": "33690",
                            "timestamp": "2026-08-19 17:55:00",
                            "value": 85.0
                        }
                    ]
                },
                "metadata": {
                    "entity_id": "B01",
                    "parameter": "SoC"
                }
            }
        ]
        """
        if self._stop_event.is_set():
            return

        self._logger.debug(
            f"{self._LOG_PREFIX} Fetching group '{group_id}' with {len(endpoints)} endpoint(s)."
        )

        # Gather only the endpoints for this specific entity group
        tasks = [self._fetch_endpoint(ep) for ep in endpoints]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        if self._stop_event.is_set():
            self._logger.warning(
                f"{self._LOG_PREFIX} Discarding payload for group '{group_id}': Shutdown signaled mid-flight."
            )
            return

        for envelope in results:
            if envelope is None:
                self._logger.warning(
                    f"{self._LOG_PREFIX} Skipping null result in group '{group_id}': endpoint fetch aborted mid-flight."
                )
                continue

            if isinstance(envelope, Exception):
                self._logger.error(
                    f"{self._LOG_PREFIX} Skipping failed fetch in group '{group_id}': {envelope}",
                    exc_info=envelope
                )
                continue

            try:
                if asyncio.iscoroutinefunction(self._on_message_callback):
                    await self._on_message_callback(envelope)
                else:
                    await asyncio.to_thread(self._on_message_callback, envelope)
            except Exception as e:
                self._logger.error(
                    f"{self._LOG_PREFIX} Callback execution failed for group '{group_id}': {e}",
                    exc_info=True
                )

    async def _fetch_endpoint(self, endpoint_block: dict) -> Optional[dict]:
        """
        Asynchronously fetches a single HTTP endpoint, delegates parsing or exception
        encapsulation to the PayloadNormalizer, and returns a canonical envelope.
        """
        metadata = endpoint_block.get("metadata", {})
        path_name = endpoint_block.get("path", "")
        if not path_name:
            error_msg = f"{self._LOG_PREFIX} Endpoint block initialization failed: Missing mandatory 'path' key."
            self._logger.error(error_msg)
            raise ValueError(error_msg)

        if self._stop_event.is_set():
            self._logger.warning(
                f"{self._LOG_PREFIX} Poll Endpoint {path_name} aborted: Handler is running under shutdown constraints."
            )
            return None

        method = endpoint_block.get("method", "GET").upper()

        if method not in self._ALLOWED_METHODS:
            error_msg = (
                f"{self._LOG_PREFIX} Unsupported HTTP method '{method}' requested for path '{path_name}'. "
                f"Allowed methods: {self._ALLOWED_METHODS}"
            )
            self._logger.error(error_msg)
            raise ValueError(error_msg)

        timeout_ms = endpoint_block.get("timeout_ms", 5000)

        self._logger.debug(f"{self._LOG_PREFIX} Issuing request -> [{method} {path_name}]")

        try:
            # The headers of the endpoint (e.g., its Authorization) go in its own request, and are added to the ones of
            # the session. They are not kept for the other endpoints.
            response = await self._http_connector.request(
                method=method,
                endpoint=path_name,
                headers=endpoint_block.get("headers"),
                timeout=timeout_ms / 1000
            )

            return await PayloadNormalizer.from_http(
                response=response,
                endpoint=path_name,
                metadata=metadata
            )

        except Exception as network_exception:
            self._logger.error(
                f"{self._LOG_PREFIX} Network exception detected for [{path_name}]: {network_exception}. "
                f"Normalizing error state."
            )

            return await PayloadNormalizer.from_http(
                endpoint=path_name,
                exception=network_exception,
                metadata=metadata
            )

    async def stop(self) -> None:
        """
        Graceful shutdown pipeline hook. Stops the scheduler engine, signals termination,
        and safely closes the underlying HTTP connector session.
        """
        self._logger.debug(f"{self._LOG_PREFIX} Stopping logical consumer message ingestion processing pipelines.")

        # 1. Signal background routines to halt
        self._stop_event.set()

        # 2. Stop scheduling new recurring jobs
        if self._scheduler:
            try:
                self._scheduler.stop()
            except Exception as e:
                self._logger.warning(f"{self._LOG_PREFIX} Warning during scheduler stop: {e}")

        # 3. Close the physical transport connector session
        try:
            if self._http_connector and hasattr(self._http_connector, "close"):
                is_open = getattr(self._http_connector, "is_connected", None)
                # Handle callable method or boolean property
                is_active = is_open() if callable(is_open) else bool(is_open) if is_open is not None else True

                if is_active:
                    await self._http_connector.close()
        except Exception as e:
            self._logger.error(f"{self._LOG_PREFIX} Failed to close AsyncHTTP connector safely: {e}")

    @staticmethod
    def _validate_topology_schema(topology_setup: dict) -> None:
        """
        Validates the architectural schema structure of the HTTP topology configuration dictionary.
        """
        endpoints = topology_setup.get("endpoints")
        if not isinstance(endpoints, list) or not endpoints:
            raise ValueError("Topology validation error: 'endpoints' must be a non-empty list.")

        for idx, endpoint_block in enumerate(endpoints):
            if not isinstance(endpoint_block, dict):
                raise ValueError(
                    f"Topology validation error: Endpoint item at index {idx} must be a dictionary."
                )

            path = endpoint_block.get("path")
            if not path or not isinstance(path, str) or not path.strip():
                raise ValueError(
                    f"Topology validation error: Mandatory string 'path' is missing or empty at index {idx}."
                )

            method = endpoint_block.get("method", "GET")
            if not isinstance(method, str) or method.upper() not in HTTPInputHandler._ALLOWED_METHODS:
                raise ValueError(
                    f"Topology validation error: Method '{method}' at index {idx} is unsupported. "
                    f"Allowed methods: {HTTPInputHandler._ALLOWED_METHODS}"
                )

            if "timeout_ms" in endpoint_block:
                timeout_ms = endpoint_block["timeout_ms"]
                if not isinstance(timeout_ms, (int, float)) or timeout_ms <= 0:
                    raise ValueError(
                        f"Topology validation error: 'timeout_ms' for path '{path}' must be a positive number."
                    )

            if "headers" in endpoint_block and not isinstance(endpoint_block["headers"], dict):
                raise ValueError(
                    f"Topology validation error: 'headers' for path '{path}' must be a dictionary."
                )