# --- Standard Library Imports ---
import asyncio
from typing import Any, Optional, cast

# --- Third-Party Imports ---
import aiohttp  # Async HTTP client session manager (non-blocking)

# --- Internal Framework Imports ---
from app.core.utils.logger import LoggingUtils


class HTTPErrorWrapper(Exception):
    """Custom exception for HTTP-related errors."""
    pass

# todo verificador de headers, QUER DIZER SINCERAMENTE SERA QUE FAZ SENTIDO TER UM VERIFICADOR? eXISTEM MIL HEADERS E O HTTP DA ERRO SE O HEADER FOR ERRADO E POSSO SIMPLESMENTE PROPAGAR ESSE ERRO.

class AsyncHTTPConnector:
    """
    Asynchronous wrapper class for managing an HTTP connection/session using aiohttp.
    Fully instrumented with framework logging capabilities for deep observability.
    """

    _LOG_PREFIX: str = "Async HTTP Connector |"

    _session: Optional[aiohttp.ClientSession]  # Persistent async session pool
    _base_url: str  # Resolved target base server URL
    _verify_ssl: bool
    _server_configurations: dict
    _logger: LoggingUtils

    def __init__(self, server_configurations: dict, logger: LoggingUtils) -> None:
        """
        Initializes the HTTP transport parameters, logger reference, and base URL setup.
        """
        self._logger = logger
        self._server_configurations = server_configurations

        _host = server_configurations.get("host")
        _port = server_configurations.get("port")
        _schema: str = server_configurations.get("schema", "http")

        if server_configurations.get("url"):
            self._base_url = server_configurations.get("url").rstrip("/")
        elif _host:
            if _port:
                self._base_url = f"{_schema}://{_host}:{_port}"
            else:
                self._base_url = f"{_schema}://{_host}"
        else:
            error_msg = f"{self._LOG_PREFIX} Error initializing HTTP setup: Either 'url' or 'host' must be provided."
            self._logger.error(error_msg)
            raise HTTPErrorWrapper(error_msg)

        self._verify_ssl = server_configurations.get("verify_ssl", False)
        self._session = None

        self._logger.info(f"{self._LOG_PREFIX} Transport initialized with base URL: '{self._base_url}'")

    async def connect(self) -> None:
        """
        Establishes the persistent, non-blocking HTTP ClientSession.
        """
        try:
            # 1. Setup SSL/TLS verification preferences
            connector = aiohttp.TCPConnector(ssl=self._verify_ssl)

            # 2. Extract baseline credentials
            _auth_config: dict = self._server_configurations.get("auth", {})
            _username = _auth_config.get("username")
            _password = _auth_config.get("password")
            auth = aiohttp.BasicAuth(_username, _password) if _username and _password else None

            # 3. Construct base session headers (Accepting JSON, Content-Type is route-specific)
            _headers = {
                "Accept": "application/json"
            }
            _custom_global_headers = self._server_configurations.get("headers") or {}
            _headers.update(_custom_global_headers)

            # 4. Materialize the active physical session resource
            self._session = aiohttp.ClientSession(
                connector=connector,
                auth=auth,
                headers=_headers
            )

        except Exception as e:
            error_msg = f"{self._LOG_PREFIX} Error establishing HTTP persistent link context: {e}"
            self._logger.error(error_msg)
            raise HTTPErrorWrapper(error_msg) from e

    async def request(self, method: str, endpoint: str, **kwargs: Any) -> aiohttp.ClientResponse:
        """
        Executes a non-blocking, asynchronous HTTP request.
        """
        if not self.is_connected():
            error_msg = "ClientSession is closed or unallocated."
            self._logger.error(f"{self._LOG_PREFIX} {error_msg}")
            raise HTTPErrorWrapper(f"{self._LOG_PREFIX} {error_msg}")

        url = f"{self._base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        method_upper = method.upper()

        if "timeout" in kwargs and isinstance(kwargs["timeout"], (int, float)):
            kwargs["timeout"] = aiohttp.ClientTimeout(total=kwargs["timeout"])

        try:
            session = cast(aiohttp.ClientSession, self._session)

            # Executa o request mantendo a response aberta para consumo posterior
            response = await session.request(method=method_upper, url=url, **kwargs)

            # Checa status boundaries
            if 300 <= response.status < 600:
                response_text = await response.text()
                error_msg = (
                    f"HTTP error during async {method_upper} for {url}: "
                    f"status code {response.status}, response: {response_text[:500]}"
                )
                self._logger.error(f"{self._LOG_PREFIX} {error_msg}")
                raise HTTPErrorWrapper(error_msg)

            return response

        except aiohttp.ClientConnectorError as e:
            error_msg = f"{self._LOG_PREFIX} Connection error during async {method_upper} for {url}: {e}"
            self._logger.error(error_msg)
            raise HTTPErrorWrapper(error_msg) from e
        except asyncio.TimeoutError as e:
            error_msg = f"{self._LOG_PREFIX} Timeout during async {method_upper} for {url}: {e}"
            self._logger.error(error_msg)
            raise HTTPErrorWrapper(error_msg) from e
        except HTTPErrorWrapper:
            raise
        except Exception as e:
            error_msg = f"{self._LOG_PREFIX} Unexpected error during async {method_upper} for {url}: {e}"
            self._logger.error(error_msg)
            raise HTTPErrorWrapper(error_msg) from e

    def update_headers(self, new_headers: dict) -> None:
        """
        Updates active ClientSession headers dynamically.
        """
        if not self.is_connected():
            error_msg = f"{self._LOG_PREFIX} Session is not established."
            self._logger.error(error_msg)
            raise HTTPErrorWrapper(error_msg)

        # aiohttp sessions hold a mutable multidict representing global headers
        session = cast(aiohttp.ClientSession, self._session)
        session.headers.update(new_headers)
        self._logger.info(f"{self._LOG_PREFIX} ClientSession global headers updated dynamically.")

    async def close(self) -> None:
        """Closes the HTTP ClientSession cleanly."""
        if self._session:
            self._logger.info(f"{self._LOG_PREFIX} Initiating clean shutdown of the ClientSession context pool...")
            try:
                await self._session.close()
                self._logger.info(f"{self._LOG_PREFIX} Persistent connections successfully discarded.")
            except Exception as e:
                error_msg = f"{self._LOG_PREFIX} Failed to close HTTP session: {e}"
                raise HTTPErrorWrapper(error_msg) from e
            finally:
                self._session = None

    def is_connected(self) -> bool:
        """Returns True if the session exists and is not closed."""
        return self._session is not None and not self._session.closed