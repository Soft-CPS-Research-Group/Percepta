# --- Standard Library Imports ---
from typing import Any, Optional, cast

# --- Third-Party Imports ---
import requests
from requests.exceptions import ConnectionError, Timeout

# --- Internal Framework Imports ---
from app.core.utils.logger import LoggingUtils


class HTTPErrorWrapper(Exception):
    """Custom exception boundary for handling HTTP-related transmission errors."""
    pass


class HTTPConnector:
    """
    Wrapper class for managing an HTTP connection/session using the `requests` library.
    Fully instrumented with framework logging capabilities for deep observability.
    """

    # --- Class Constants & Type Declarations ---
    _LOG_PREFIX: str = "Sync HTTP Connector |"

    _session: Optional[requests.Session]  # Persistent connection pooling session
    _base_url: str  # Resolved target base server URL
    _verify_ssl: bool
    _server_configurations: dict
    _logger: LoggingUtils

    def __init__(self, server_configurations: dict, logger: LoggingUtils) -> None:
        """
        Initializes the HTTP transport parameters, logger reference, and base URL setup.

        Args:
            server_configurations (dict): Configuration mapping containing host, port,
                scheme, URL, authentication, and TLS/SSL verification parameters.
            logger (LoggingUtils): Central logging facility infrastructure dependency.

        Raises:
            HTTPErrorWrapper: If the configuration lacks both 'url' and 'host' parameters.
        """
        self._logger = logger
        self._server_configurations = server_configurations

        _host = server_configurations.get("host")
        _port = server_configurations.get("port")
        _scheme: str = server_configurations.get("scheme", "http")

        # Fallback layer: If 'url' is explicitly provided, use it directly.
        # Otherwise, dynamically reconstruct the target endpoint using host and port.
        if server_configurations.get("url"):
            self._base_url = server_configurations.get("url").rstrip("/")
        elif _host:
            if _port:
                self._base_url = f"{_scheme}://{_host}:{_port}"
            else:
                self._base_url = f"{_scheme}://{_host}"
        else:
            error_msg = "Error initializing HTTP setup: Either 'url' or 'host' must be provided."
            self._logger.error(f"{self._LOG_PREFIX} {error_msg}")
            raise HTTPErrorWrapper(error_msg)

        self._verify_ssl = server_configurations.get("verify_ssl", False)
        self._session = None

        self._logger.info(f"{self._LOG_PREFIX} Transport initialized with base URL: '{self._base_url}'")

    def connect(self) -> None:
        """
        Establishes a persistent requests Session and applies baseline client configurations.

        Raises:
            HTTPErrorWrapper: If the session pool context cannot be successfully created.
        """
        self._logger.info(f"{self._LOG_PREFIX} Establishing persistent requests Session...")
        try:
            # 1. Materialize the active physical session resource
            self._session = requests.Session()
            self._session.verify = self._verify_ssl

            # 2. Extract user credentials for Basic Authentication
            _auth_config: dict = self._server_configurations.get("auth", {})
            _username = _auth_config.get("username")
            _password = _auth_config.get("password")

            if _username and _password:
                self._session.auth = (_username, _password)
                self._logger.info(f"{self._LOG_PREFIX} Basic authentication applied to session context.")

            # 3. Load baseline fallback headers
            _headers = {
                "Content-Type": "application/json",
                "Accept": "application/json"
            }

            # 4. Merge and override with global headers defined in server_configurations
            _custom_global_headers = self._server_configurations.get("headers") or {}
            _headers.update(_custom_global_headers)

            # Enforce the unified headers into the session pool
            self._session.headers.update(_headers)

            self._logger.info(f"{self._LOG_PREFIX} Persistent HTTP requests Session established successfully.")

        except Exception as e:
            error_msg = f"Error establishing HTTP persistent link context: {e}"
            self._logger.error(f"{self._LOG_PREFIX} {error_msg}")
            raise HTTPErrorWrapper(error_msg) from e

    def request(self, method: str, endpoint: str, **kwargs: Any) -> requests.Response:
        """
        Performs HTTP requests with robust error handling and status code validation.

        Args:
            method (str): HTTP REST method (e.g., 'GET', 'POST', 'PUT').
            endpoint (str): Target API route to call (appended to the base URL).
            **kwargs: Arbitrary keyword arguments passed directly to the internal request engine
                (e.g., params, data, json, timeout).

        Returns:
            requests.Response: The successful HTTP response object from the downstream server.

        Raises:
            HTTPErrorWrapper: If the session is disconnected, a network boundary error occurs,
                or the downstream server returns an operational error code (300-599).
        """
        if not self.is_connected():
            error_msg = "ClientSession is closed or unallocated."
            self._logger.error(f"{self._LOG_PREFIX} {error_msg}")
            raise HTTPErrorWrapper(f"{self._LOG_PREFIX} {error_msg}")

        url = f"{self._base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        method_upper = method.upper()

        self._logger.info(f"{self._LOG_PREFIX} Issuing HTTP request -> [{method_upper}] {url}")

        try:
            # Safe access to session via cast to appease static typing checkers
            session = cast(requests.Session, self._session)

            # Dispatch the physical network request using the session pool
            response = session.request(method=method_upper, url=url, **kwargs)

            # Check status boundaries (300-599 are treated as operational/server failures)
            if 300 <= response.status_code < 600:
                error_msg = (
                    f"HTTP error during {method_upper} request for {url}: "
                    f"status code {response.status_code}, response: {response.text[:500]}, kwargs = {kwargs}"
                )
                self._logger.error(f"{self._LOG_PREFIX} {error_msg}")
                raise HTTPErrorWrapper(error_msg)

            self._logger.debug(
                f"{self._LOG_PREFIX} Successful response received from {url} with code {response.status_code}."
            )
            return response

        except ConnectionError as e:
            error_msg = f"Connection error during {method_upper} request for {url}: {e}"
            self._logger.error(f"{self._LOG_PREFIX} {error_msg}")
            raise HTTPErrorWrapper(error_msg) from e

        except Timeout as e:
            error_msg = f"Timeout during {method_upper} request for {url}: {e}"
            self._logger.error(f"{self._LOG_PREFIX} {error_msg}")
            raise HTTPErrorWrapper(error_msg) from e

        except HTTPErrorWrapper:
            # Re-raise explicitly constructed HTTP exceptions to prevent redundant log nesting
            raise

        except Exception as e:
            error_msg = f"Unexpected error during {method_upper} request for {url}: {e}"
            self._logger.error(f"{self._LOG_PREFIX} {error_msg}")
            raise HTTPErrorWrapper(error_msg) from e

    def get(self, endpoint: str, timeout: float, params: Optional[dict] = None) -> requests.Response:
        """
        Performs a synchronous GET request using the active session payload.

        Args:
            endpoint (str): Target API route to call (appended to the base URL).
            timeout (float): Request threshold timeout in seconds.
            params (Optional[dict]): Dictionary of URL query parameters. Defaults to None.

        Returns:
            requests.Response: The successful HTTP response object.
        """
        return self.request("GET", endpoint, params=params, timeout=timeout)

    def update_headers(self, new_headers: dict) -> None:
        """
        Dynamically patches and merges global headers into the active session block.

        Args:
            new_headers (dict): Dictionary mapping containing headers to add or overwrite.

        Raises:
            HTTPErrorWrapper: If the configuration structure is invalid or the session is down.
        """
        if not self.is_connected():
            error_msg = "Session is not established."
            self._logger.error(f"{self._LOG_PREFIX} {error_msg}")
            raise HTTPErrorWrapper(f"{self._LOG_PREFIX} {error_msg}")

        try:
            # Enforce strict typing on runtime payload delivery
            if not isinstance(new_headers, dict):
                raise ValueError("Headers must be provided as a dictionary")

            session = cast(requests.Session, self._session)

            # Only trigger an active update block if differences are detected
            if new_headers != dict(session.headers):
                session.headers.update(new_headers)  # Merge/overwrite headers in the session
                self._logger.info(f"{self._LOG_PREFIX} Session global headers updated dynamically.")

        except Exception as e:
            error_msg = f"Failed to update session headers: {e}"
            self._logger.error(f"{self._LOG_PREFIX} {error_msg}")
            raise HTTPErrorWrapper(error_msg) from e

    def close(self) -> None:
        """
        Gracefully terminates the HTTP session and releases internal socket buffers.

        Raises:
            HTTPErrorWrapper: If the connection teardown encounters operational lockups.
        """
        if self._session:
            self._logger.info(f"{self._LOG_PREFIX} Initiating clean shutdown of requests Session pool...")
            try:
                self._session.close()
                self._logger.info(f"{self._LOG_PREFIX} Persistent connections successfully discarded.")
            except Exception as e:
                error_msg = f"Failed to close HTTP session: {e}"
                self._logger.error(f"{self._LOG_PREFIX} {error_msg}")
                raise HTTPErrorWrapper(error_msg) from e
            finally:
                self._session = None

    def is_connected(self) -> bool:
        """
        Validates the state of the active connection pointer.

        Returns:
            bool: True if the session object is allocated, otherwise False.
        """
        return self._session is not None

    def dict_to_xml(self, data: dict, root_element: str = "root") -> str:
        """
        Helper utility to recursively serialize a standard dictionary into a plain XML string.

        Args:
            data (dict): The target dictionary payload to process.
            root_element (str): The name of the outermost XML wrapper tag. Defaults to "root".

        Returns:
            str: The serialized XML structure represented as a string.
        """

        def _build_xml(xml_dict: dict) -> str:
            sub_elements = []
            for key, val in xml_dict.items():
                if isinstance(val, dict):
                    sub_elements.append(f"<{key}>{_build_xml(val)}</{key}>")
                else:
                    sub_elements.append(f"<{key}>{val}</{key}>")
            return "".join(sub_elements)

        return f"<{root_element}>{_build_xml(data)}</{root_element}>"

    def dict_to_form_urlencoded(self, data: dict) -> str:
        """
        Helper utility to convert a dictionary mapping into a URL-encoded string layout.

        Args:
            data (dict): The key-value dictionary payload to translate.

        Returns:
            str: The sanitized and structured URL-encoded string.
        """
        from urllib.parse import urlencode
        return urlencode(data)