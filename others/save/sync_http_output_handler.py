# --- Standard Library Imports ---
from enum import Enum  # Used to define structured execution routing modes
import json  # Used for data serialization and pretty logging formats
import threading  # Used for thread-safe synchronization guards and status flags
from typing import Any, Dict, List, Optional  # Standard type annotations for static analysis boundaries

# --- Third-Party Imports ---
import requests  # Used for physical response reference and type hinting inside signatures

# --- Internal Framework Imports ---
from others.save.sync_http_connector import HTTPConnector  # Low-level network HTTP session manager
from app.core.output_handlers import OutputHandlerBase  # Abstract baseline contract
from app.core.utils.logger import LoggingUtils  # Standardized telemetry logger instance


class HttpPublishMode(Enum):
    """
    Specifies how dynamic endpoint parameters are combined with the static
    topology setup during outbound transmission.
    """
    STATIC = "static"      # Ignores dynamic inputs; targets only pre-configured paths
    OVERRIDE = "override"  # Bypasses static paths; targets ONLY dynamic inputs
    ADDITIVE = "additive"  # Targets both static paths and dynamic inputs merged


class HTTPOutputHandler(OutputHandlerBase):
    """
    Handles outbound HTTP data delivery across modular API paths
    with native synchronous request-response synchronization.
    """

    # --- Class Attribute & Type Declarations ---
    _LOG_PREFIX: str  # Standardized logging identifier prefix
    _topology_setup: dict  # Configured routes, methods and layout mappings
    _publisher_connector: HTTPConnector  # Active persistent connection pipeline dedicated to egress
    _stop_event: threading.Event  # Thread-safe event signal indicating shutdown
    _ALLOWED_METHODS: set  # Set of supported HTTP verbs for validation

    _LOG_PREFIX = "HTTP Output Handler |"
    _ALLOWED_METHODS = {"POST", "PUT"}

    def __init__(
        self,
        topology_setup: dict,
        publisher_connector: HTTPConnector,
        logger: LoggingUtils
    ) -> None:
        """
        Initializes the logical HTTP publisher boundary with pre-built connection assets.
        """
        super().__init__(logger)
        self._topology_setup = topology_setup
        self._publisher_connector = publisher_connector
        self._stop_event = threading.Event()

    def send(
        self,
        message: dict,
        dynamic_endpoints: Optional[List[dict]] = None,
        mode: HttpPublishMode = HttpPublishMode.STATIC
    ) -> Optional[dict]:
        """
        Publishes a payload frame. Resolves target endpoints dynamically
        based on the chosen execution mode (STATIC, OVERRIDE, or ADDITIVE).
        Adapts the payload representation based on the Content-Type header configured
        for each resolved endpoint.
        """
        # --- Variable Declarations & Type Hints ---
        collected_responses: Dict[str, Any]
        static_endpoints: list
        resolved_endpoints: List[dict]
        endpoint_block: dict
        path_name: str
        method: str
        timeout_ms: int
        headers: dict
        rpc_properties: dict
        content_type: str
        prepared_data: Any
        response: requests.Response
        pretty_msg: str

        collected_responses = {}

        if self._stop_event.is_set():
            self._logger.warning(
                f"{self._LOG_PREFIX} Rejecting transmission: Handler is running under shutdown constraints."
            )
            return None

        # 1. Resolve configuration boundaries by extracting static topology mappings
        static_endpoints = self._topology_setup.get("endpoints", [])
        dynamic_endpoints = dynamic_endpoints or []

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

        # 3. Sequential egress broadcast loop firing requests to all resolved targets
        for endpoint_block in resolved_endpoints:
            path_name = endpoint_block.get("path", "")
            if not path_name:
                continue

            method = endpoint_block.get("method", "POST").upper()

            # --- Method Validation Layer ---
            if method not in self._ALLOWED_METHODS:
                error_msg = f"{self._LOG_PREFIX} Unsupported HTTP method '{method}' requested for path '{path_name}'. Allowed methods: {self._ALLOWED_METHODS}"
                self._logger.error(error_msg)
                raise ValueError(error_msg)

            timeout_ms = endpoint_block.get("timeout_ms", 5000)
            headers = endpoint_block.get("headers", {})
            rpc_properties = endpoint_block.get("rpc_properties", {})

            # 4. Content-Type adaptation layer
            # Resolve Content-Type to determine the correct serialization format.
            # Look first in routing-specific headers; fallback to global connector session settings if not specified.
            content_type = headers.get("Content-Type", "").lower()
            prepared_data = message

            # Convert standard application dictionary payloads using physical connector conversion helpers
            if "application/xml" in content_type or "text/xml" in content_type:
                prepared_data = self._publisher_connector.dict_to_xml(message)
            elif "application/x-www-form-urlencoded" in content_type:
                prepared_data = self._publisher_connector.dict_to_form_urlencoded(message)

            try:
                # Route request executing specific verb helpers
                response = self._perform_request(method, path_name, prepared_data, headers, timeout_ms)
                pretty_msg = json.dumps(message, indent=4)
                self._logger.info(
                    f"{self._LOG_PREFIX} Request sent to [{method} {path_name}] with content-type [{content_type}]:\n{pretty_msg}"
                )

                # Step 5: Handle synchronous RPC reply collection in-place if flagged
                if rpc_properties.get("enabled", False):
                    try:
                        # Extract and structure the dynamic server response payload
                        response_data = response.json()
                        collected_responses[path_name] = {
                            "status": "success",
                            "code": response.status_code,
                            "data": response_data
                        }
                    except json.JSONDecodeError:
                        collected_responses[path_name] = {
                            "status": "success",
                            "code": response.status_code,
                            "data": response.text
                        }
            except Exception as e:
                self._logger.error(f"{self._LOG_PREFIX} Request failure on path [{path_name}]: {e}")
                if rpc_properties.get("enabled", False):
                    collected_responses[path_name] = {
                        "status": "error",
                        "error": str(e)
                    }

        return collected_responses if collected_responses else None

    def _perform_request(
            self,
            method: str,
            endpoint: str,
            data: Optional[Any] = None,
            headers: Optional[dict] = None,
            timeout_ms: int = 5000
    ) -> requests.Response:
        """
        Unified internal dispatcher. Prepares serialization contexts and routes the call
        to specific HTTP verb handlers using transient execution keyword parameters.
        """
        # --- Variable Declarations & Type Hints ---
        request_kwargs: Dict[str, Any]
        response: requests.Response

        # Build dynamic requests session configuration arguments
        request_kwargs = {
            "timeout": timeout_ms / 1000
        }

        # Handle local headers safely without overriding global session contexts
        if headers:
            request_kwargs["headers"] = headers

        # Map prepared payloads to their designated request storage slots
        if data is not None:
            if isinstance(data, dict):
                request_kwargs["json"] = data  # Let requests handle JSON serialization
            else:
                request_kwargs["data"] = data  # Treat converted strings (XML/Form) or bytes as raw body

        # Route the final request kwargs dict dynamically using python's double star dict expansion
        if method == "POST":
            response = self._post(endpoint=endpoint, **request_kwargs)
        elif method == "PUT":
            response = self._put(endpoint=endpoint, **request_kwargs)
        else:
            self._logger.warning(
                f"{self._LOG_PREFIX} Unsupported HTTP method requested: '{method}'. Defaulting to POST."
            )
            response = self._post(endpoint=endpoint, **request_kwargs)

        return response

    def _post(self, endpoint: str, **kwargs: Any) -> requests.Response:
        """
        Performs a synchronous POST request via the underlying HTTP transport connector,
        unpacking all dynamically built configuration parameters.
        """
        return self._publisher_connector.request(
            method="POST",
            endpoint=endpoint,
            **kwargs  # Expands kwargs directly (headers, timeout, json/data) into the connector's method
        )

    def _put(self, endpoint: str, **kwargs: Any) -> requests.Response:
        """
        Performs a synchronous PUT request via the underlying HTTP transport connector,
        unpacking all dynamically built configuration parameters.
        """
        return self._publisher_connector.request(
            method="PUT",
            endpoint=endpoint,
            **kwargs  # Expands kwargs directly (headers, timeout, json/data) into the connector's method
        )


    def stop(self) -> None:
        """
        Polymorphic interface shutdown hook. Transitions internal logical flags.
        """
        self._logger.info(f"{self._LOG_PREFIX} Rejection constraints triggered. Flushing logical boundaries.")
        self._stop_event.set()