# --- Standard Library Imports ---
import asyncio  # Used for running asynchronous tasks concurrently
from enum import Enum  # Used to define structured execution routing modes
import json  # Used for data serialization and pretty logging formats
import threading  # Used for thread-safe status flag synchronization
from typing import Any, Dict, List, Optional  # Standard type annotations for static analysis boundaries

# --- Internal Framework Imports ---
from app.core.connectors.async_http_connector import AsyncHTTPConnector  # Low-level network HTTP session manager (async)
from app.core.output_handlers.output_handler_base import OutputHandlerBase  # Abstract baseline contract
from app.core.utils.logger import LoggingUtils  # Standardized telemetry logger instance


class HttpPublishMode(Enum):
    """
    Specifies how dynamic endpoint parameters are combined with the static
    topology setup during outbound transmission.
    """
    STATIC = "static"  # Ignores dynamic inputs; targets only pre-configured paths
    OVERRIDE = "override"  # Bypasses static paths; targets ONLY dynamic inputs
    ADDITIVE = "additive"  # Targets both static paths and dynamic inputs merged


class AsyncHTTPOutputHandler(OutputHandlerBase):
    """
    Handles outbound HTTP data delivery across modular API paths
    with native asynchronous request-response synchronization.
    """

    # --- Class Attribute & Type Declarations ---
    _LOG_PREFIX: str  # Standardized logging identifier prefix
    _topology_setup: dict  # Configured routes, methods and layout mappings
    _connector: AsyncHTTPConnector  # Active persistent connection pipeline dedicated to egress (async)
    _stop_event: threading.Event  # Thread-safe event signal indicating shutdown
    _ALLOWED_METHODS: set  # Set of supported HTTP verbs for validation

    _LOG_PREFIX = "Async HTTP Output Handler |"
    _ALLOWED_METHODS = {"POST", "PUT"}

    def __init__(
            self,
            topology_setup: dict,
            connector: AsyncHTTPConnector,
            logger: LoggingUtils
    ) -> None:
        """
        Initializes the logical HTTP publisher boundary with pre-built connection assets.
        """
        super().__init__(logger)

        if topology_setup:
            # --- Guard Clause: Validate topology schema structure ---
            try:
                self._validate_topology_schema(topology_setup)
            except ValueError as val_err:
                self._logger.error(f"{self._LOG_PREFIX} {val_err}")
                raise

            self._topology_setup = topology_setup
        else:
            self._topology_setup = {"endpoints": []}

        # --- Guard Clause: Validate connector dependency ---
        if not connector:
            error_msg = "Initialization error: 'connector' dependency is missing."
            self._logger.error(f"{self._LOG_PREFIX} {error_msg}")
            raise ValueError(error_msg)

        self._publisher_connector = connector

        self._stop_event = threading.Event()

    async def send(
            self,
            message: dict,
            dynamic_endpoints: Optional[List[dict]] = None,
            mode: HttpPublishMode = HttpPublishMode.STATIC
    ) -> Optional[dict]:
        """
        Publishes a payload frame asynchronously. Resolves target endpoints dynamically
        based on the chosen execution mode and fires all requests concurrently.
        """
        # --- Variable Declarations & Type Hints ---
        collected_responses: Dict[str, Any]
        static_endpoints: list
        resolved_endpoints: List[dict]
        tasks: List[asyncio.Task]
        results: List[Any]
        index: int
        endpoint_block: dict
        path_name: str

        collected_responses = {}

        if self._stop_event.is_set():
            self._logger.warning(
                f"{self._LOG_PREFIX} Rejecting transmission: Handler is running under shutdown constraints."
            )
            return None

        # 1. Resolve configuration boundaries by extracting static topology mappings
        static_endpoints = self._topology_setup.get("endpoints", [])
        dynamic_endpoints = dynamic_endpoints or []
        # TODO VALIDAR ENDPOINTS
        # 2. Compute the targeting matrix based on the specified publication mode
        if mode == HttpPublishMode.OVERRIDE:
            if not dynamic_endpoints:
                self._logger.warning(
                    f"{self._LOG_PREFIX} Override mode requested but no dynamic endpoints were provided. Falling back to static."
                )
                resolved_endpoints = static_endpoints
            else:
                resolved_endpoints = dynamic_endpoints
        elif mode == HttpPublishMode.ADDITIVE:
            resolved_endpoints = static_endpoints + dynamic_endpoints
        else:
            resolved_endpoints = static_endpoints

        resolved_endpoints = [ep for ep in resolved_endpoints if ep.get("path", "").strip()]

        # 3. Build a list of concurrent async tasks for each resolved endpoint
        tasks = []
        for endpoint_block in resolved_endpoints:
            # Wrap each individual target request execution into an independent coroutine task
            tasks.append(
                asyncio.create_task(
                    self._dispatch_to_endpoint(endpoint_block, message)
                )
            )

        if not tasks:
            return None

        # 4. Dispatch ALL requests concurrently to the Event Loop
        # We set 'return_exceptions=True' so that one failing request does not crash the entire batch.
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 5. Collate all the results safely
        for index, endpoint_block in enumerate(resolved_endpoints):
            path_name = endpoint_block.get("path", "")
            result = results[index]

            if isinstance(result, Exception):
                # Handle unexpected exceptions raised outside internal handler try/except boundaries
                self._logger.error(f"{self._LOG_PREFIX} Unhandled task exception on path [{path_name}]: {result}")
                collected_responses[path_name] = {
                    "status": "error",
                    "error": str(result)
                }
            elif result is not None:
                # Store valid RPC dictionary responses returned by the dispatch method
                collected_responses[path_name] = result

        return collected_responses if collected_responses else None

    async def _dispatch_to_endpoint(self, endpoint_block: dict, message: dict) -> Optional[dict]:
        """
        Orchestrates request serialization and routing strategy for a single endpoint context.
        """
        # --- Variable Declarations & Type Hints ---
        path_name: str
        method: str
        timeout_ms: int
        headers: dict
        rpc_properties: dict
        rpc_enabled: bool
        content_type: str
        prepared_data: Any
        response: Any
        response_data: Any
        response_text: str
        pretty_msg: str

        path_name = endpoint_block.get("path", "")
        method = endpoint_block.get("method", "POST").upper()

        # --- Method Validation Layer ---
        if method not in self._ALLOWED_METHODS:
            self._logger.warning(
                f"{self._LOG_PREFIX} Unsupported HTTP method requested: '{method}'. Defaulting to POST."
            )
            method = "POST"

        timeout_ms = endpoint_block.get("timeout_ms", 5000)
        headers = endpoint_block.get("headers", {})
        rpc_properties = endpoint_block.get("rpc_properties", {})
        rpc_enabled = rpc_properties.get("enabled", False)

        # Content-Type adaptation layer
        content_type = headers.get("Content-Type", "").lower()
        prepared_data = message

        # Routing Execution Strategy (Awaited Sync vs. Async Fire-and-Forget)
        if rpc_enabled:
            # --- CASE A: RPC ENABLED ---
            try:
                response = await self._perform_request(method, path_name, prepared_data, headers, timeout_ms)
                pretty_msg = json.dumps(message, indent=4)
                self._logger.info(
                    f"{self._LOG_PREFIX} Request sent to [{method} {path_name}] with content-type [{content_type}]:\n{pretty_msg}"
                )

                try:
                    response_data = await response.json()
                    return {
                        "status": "success",
                        "code": response.status,
                        "data": response_data
                    }
                except Exception:
                    response_text = await response.text()
                    return {
                        "status": "success",
                        "code": response.status,
                        "data": response_text
                    }
            except Exception as e:
                self._logger.error(f"{self._LOG_PREFIX} Request failure on path [{path_name}]: {e}")
                return {
                    "status": "error",
                    "error": str(e)
                }
        else:
            # --- CASE B: RPC DISABLED (Optimized Fire-and-Forget) ---
            # Spawn a task inside the event loop background context and exit immediately.
            asyncio.create_task(
                self._fire_and_forget_request(
                    method=method,
                    endpoint=path_name,
                    data=prepared_data,
                    headers=headers,
                    timeout_ms=timeout_ms
                )
            )
            return None  # No result collected for Fire-and-Forget targets

    async def _fire_and_forget_request(
            self,
            method: str,
            endpoint: str,
            data: Optional[Any],
            headers: Optional[dict],
            timeout_ms: int
    ) -> None:
        """
        Executes an isolated, asynchronous HTTP request in background without blocking
        or returning results to the central egress pipeline thread context.
        """
        try:
            response = await self._perform_request(method, endpoint, data, headers, timeout_ms)
            self._logger.info(
                f"{self._LOG_PREFIX} [Background Fire-and-Forget] Request successfully sent to "
                f"[{method} {endpoint}] with status [{response.status}]."
            )
        except Exception as e:
            self._logger.error(
                f"{self._LOG_PREFIX} [Background Fire-and-Forget] Outbound task failed "
                f"on path [{endpoint}]: {e}"
            )

    async def _perform_request(
            self,
            method: str,
            endpoint: str,
            data: Optional[Any] = None,
            headers: Optional[dict] = None,
            timeout_ms: int = 5000
    ) -> Any:
        """
        Unified internal dispatcher. Prepares serialization contexts and routes the call
        to specific HTTP verb handlers using transient execution keyword parameters.
        """
        request_kwargs = {
            "timeout": timeout_ms / 1000
        }

        if headers:
            request_kwargs["headers"] = headers

        if data is not None:
            if isinstance(data, dict):
                request_kwargs["json"] = data
            else:
                request_kwargs["data"] = data

        response = await self._publisher_connector.request(method=method, endpoint=endpoint, **request_kwargs)

        return response

    async def stop(self) -> None:
        """
        Graceful shutdown pipeline hook. Halts logical transmission pipelines
        and safely closes the underlying HTTP publisher connector session.
        """
        self._logger.info(f"{self._LOG_PREFIX} Stopping outbound message dispatcher and closing transport pipelines.")

        # 1. Signal background consumption loops to reject/stop processing new frames
        self._stop_event.set()

        # 2. Close physical HTTP client session and transport connections
        try:
            if hasattr(self._publisher_connector, "is_connected") and self._publisher_connector.is_connected():
                await self._publisher_connector.close()
        except Exception as e:
            self._logger.error(f"{self._LOG_PREFIX} Failed to close AsyncHTTP connector safely: {e}")

    @staticmethod
    def _validate_topology_schema(topology_setup: dict) -> None:
        """
        Validates the architectural schema structure of the HTTP output topology configuration dictionary.

        Ensures that the endpoints list contains valid mandatory fields, supported HTTP methods,
        positive timeouts, and correct property types.

        Args:
            topology_setup (dict): Egress topology configurations to validate.

        Raises:
            ValueError: If mandatory keys are missing, types are invalid, or methods/timeouts are out of bounds.
        """
        endpoints = topology_setup.get("endpoints")
        if not isinstance(endpoints, list) or not endpoints:
            raise ValueError("Topology validation error: 'endpoints' must be a non-empty list.")

        for idx, endpoint_block in enumerate(endpoints):
            if not isinstance(endpoint_block, dict):
                raise ValueError(
                    f"Topology validation error: Endpoint item at index {idx} must be a dictionary."
                )

            # Mandatory 'path' string validation
            path = endpoint_block.get("path")
            if not path or not isinstance(path, str) or not path.strip():
                raise ValueError(
                    f"Topology validation error: Mandatory string 'path' is missing or empty at index {idx}."
                )

            # Optional 'method' validation (must be within _ALLOWED_METHODS)
            method = endpoint_block.get("method", "POST")
            if not isinstance(method, str) or method.upper() not in AsyncHTTPOutputHandler._ALLOWED_METHODS:
                raise ValueError(
                    f"Topology validation error: Method '{method}' at index {idx} for path '{path}' is unsupported. "
                    f"Allowed methods: {AsyncHTTPOutputHandler._ALLOWED_METHODS}"
                )

            # Optional 'timeout_ms' validation
            if "timeout_ms" in endpoint_block:
                timeout_ms = endpoint_block["timeout_ms"]
                if not isinstance(timeout_ms, (int, float)) or timeout_ms <= 0:
                    raise ValueError(
                        f"Topology validation error: 'timeout_ms' for path '{path}' must be a positive number."
                    )

            # Optional 'headers' dictionary validation
            if "headers" in endpoint_block and not isinstance(endpoint_block["headers"], dict):
                raise ValueError(
                    f"Topology validation error: 'headers' for path '{path}' must be a dictionary."
                )

            # Optional 'rpc_properties' dictionary validation
            if "rpc_properties" in endpoint_block:
                rpc_props = endpoint_block["rpc_properties"]
                if not isinstance(rpc_props, dict):
                    raise ValueError(
                        f"Topology validation error: 'rpc_properties' for path '{path}' must be a dictionary."
                    )
                if "enabled" in rpc_props and not isinstance(rpc_props["enabled"], bool):
                    raise ValueError(
                        f"Topology validation error: 'enabled' in 'rpc_properties' for path '{path}' must be a boolean."
                    )

        # TODO CORRIGIR ISTO PARA NÃO MANDAR ERROS ATOA, APENAS IGNORAR