# --- Standard Library Imports ---
import asyncio
import datetime
from typing import Any, Dict, List, Optional, Tuple

# --- Internal Framework Imports ---
from app.core.output_handlers.async_http_output_handler import AsyncHTTPOutputHandler, HttpPublishMode
from app.core.output_handlers.output_handler_base import OutputHandlerBase
from app.core.utils.logger import LoggingUtils


class CWSession:
    """Manages asynchronous authentication and access token lifecycle for the Cleanwatts API.

    One instance holds one session (credentials, handler and token). Each launcher creates its own and shares it
    between its environments.

    Executes login and token refresh requests via an injected HTTP Egress OutputHandler
    using endpoint definitions extracted directly from the topology configuration.

    Expected `auth` Configuration Schema (`auth_config`):
    -----------------------------------------------------
    {
        "refresh_interval_seconds": 3300,              # (int) Refresh job interval in seconds (e.g., 55 min)
        "handler_type": "AsyncHTTP",                   # Egress transport identifier
        "connection": {                                # Server connection settings
            "url": "https://innov.cleanwattsdigital.com/api/2.0",
            "verify_ssl": true,
            "headers": {
                "Content-Type": "application/json"
            },
            "credentials": {                           # (dict) Cleanwatts authentication credentials
                "username": "OPEVA-demo-09",              # (str) Account username/login
                "password": "secret_password"          # (str) Account password
            }
        },
        "topology": {
            "endpoints": [
                {
                    "path": "/sessions",
                    "method": "POST",
                    "timeout_ms": 5000,
                    "headers": {
                        "Content-Type": "application/json"
                    },
                    "rpc_properties": {
                        "enabled": true
                    }
                },
                {
                    "path": "/session/refresh?token={token}&refresh_token={refresh_token}",
                    "method": "PUT",
                    "timeout_ms": 5000,
                    "headers": {
                        "Content-Type": "application/json"
                    },
                    "rpc_properties": {
                        "enabled": true
                    }
                }
            ]
        }
    }
    """

    _LOG_PREFIX: str = "CWSession |"

    def __init__(self) -> None:
        """Creates an empty session. Call `initialize` and then `login` to use it.

        Each Cleanwatts launcher (e.g., the Telemetry System and the Target System) creates its own session and shares
        it between all the environments it launches, so each launcher logs in with its own credentials and keeps
        its own token.
        """
        self.token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.token_valid: bool = False

        self._output_handler: Optional[OutputHandlerBase] = None
        self._logger: Optional[LoggingUtils] = None
        self._auth_config: dict = {}
        self._credentials: dict = {}
        self._login_endpoint_template: Optional[dict] = None
        self._refresh_endpoint_template: Optional[dict] = None

    def initialize(
        self,
        output_handler: OutputHandlerBase,
        auth_config: dict,
        logger: LoggingUtils
    ) -> None:
        """Initializes dependencies and resolves endpoint templates from topology.

        Args:
            output_handler (OutputHandlerBase): Mandatory HTTP RPC output handler.
            auth_config (dict): Mandatory authentication configuration dictionary.
            logger (LoggingUtils): Mandatory structured operational logger instance.

        Raises:
            ValueError: If mandatory configuration fields or endpoints are missing.
            TypeError: If input arguments violate expected types.
        """
        # --- Guard Clause 1: Validate logger dependency ---
        if not logger:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'logger' dependency is mandatory.")

        # --- Guard Clause 2: Validate output_handler dependency ---
        if not output_handler:
            error_msg = f"{self._LOG_PREFIX} Initialization error: 'output_handler' dependency is mandatory."
            logger.error(error_msg)
            raise ValueError(error_msg)

        if not isinstance(output_handler, OutputHandlerBase):
            error_msg = (
                f"{self._LOG_PREFIX} Initialization error: 'output_handler' must be an instance of OutputHandlerBase, "
                f"got {type(output_handler).__name__}."
            )
            logger.error(error_msg)
            raise TypeError(error_msg)

        # --- Guard Clause 3: Validate auth_config dictionary ---
        if not auth_config or not isinstance(auth_config, dict):
            error_msg = (
                f"{self._LOG_PREFIX} Initialization error: 'auth_config' must be a non-empty dictionary, "
                f"got {type(auth_config).__name__}."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Extract credentials from 'connection.credentials' (with top-level fallback)
        connection = auth_config.get("connection", {})
        credentials = connection.get("credentials") if isinstance(connection, dict) else None
        if not credentials:
            credentials = auth_config.get("credentials")

        if not credentials or not isinstance(credentials, dict):
            error_msg = (
                f"{self._LOG_PREFIX} Initialization error: Mandatory 'credentials' dictionary missing "
                f"under 'connection.credentials' in 'auth_config'."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        endpoints: List[dict] = auth_config.get("topology", {}).get("endpoints", [])
        if not endpoints or not isinstance(endpoints, list):
            error_msg = f"{self._LOG_PREFIX} Initialization error: Missing or empty 'topology.endpoints' in 'auth_config'."
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Resolve Login (POST) and Refresh (PUT) endpoint templates from topology
        login_ep = None
        refresh_ep = None
        for ep in endpoints:
            if not isinstance(ep, dict):
                continue
            method = ep.get("method", "POST").upper()
            path = ep.get("path", "")
            if method == "POST" and not login_ep:
                login_ep = ep
            elif (method == "PUT" or "refresh" in path) and not refresh_ep:
                refresh_ep = ep

        if not login_ep:
            error_msg = f"{self._LOG_PREFIX} Initialization error: No valid POST login endpoint found in 'topology.endpoints'."
            logger.error(error_msg)
            raise ValueError(error_msg)

        self._output_handler = output_handler
        self._auth_config = auth_config
        self._credentials = credentials
        self._logger = logger
        self._login_endpoint_template = login_ep
        self._refresh_endpoint_template = refresh_ep

    def get_token(self) -> Optional[str]:
        """Returns the current active access token."""
        return self.token

    def is_token_valid(self) -> bool:
        """Checks whether the session holds a valid access token."""
        return self.token_valid

    @staticmethod
    def _extract_tokens(data: Any) -> Tuple[Optional[str], Optional[str]]:
        """Recursively traverses the response payload to extract 'Token' and 'RefreshToken'."""
        if isinstance(data, dict):
            if "Token" in data:
                return str(data.get("Token")), str(data.get("RefreshToken")) if data.get("RefreshToken") else None
            for v in data.values():
                token, refresh = CWSession._extract_tokens(v)
                if token:
                    return token, refresh
        elif isinstance(data, list):
            for item in data:
                token, refresh = CWSession._extract_tokens(item)
                if token:
                    return token, refresh
        return None, None

    async def login(self) -> None:
        """Performs asynchronous authentication using the configured POST login endpoint."""
        username = self._credentials.get("username")

        password = self._credentials.get("password")

        login_payload = {
            "Login": username,
            "Password": password
        }

        login_endpoint = dict(self._login_endpoint_template)
        login_path = login_endpoint.get("path", "/sessions")

        self.token_valid = False
        self._logger.debug(f"{self._LOG_PREFIX} Authenticating with Cleanwatts server via [{login_path}]...")

        try:
            if isinstance(self._output_handler, AsyncHTTPOutputHandler):
                response = await self._output_handler.send(
                    message=login_payload,
                    dynamic_endpoints=[login_endpoint],
                    mode=HttpPublishMode.OVERRIDE
                )

                token, refresh_token = CWSession._extract_tokens(response)
                if not token:
                    raise ValueError(f"No Token present in login response: {response}")

                self.token = token
                self.refresh_token = refresh_token
                self.token_valid = True
                self._logger.info(f"{self._LOG_PREFIX} Authentication successful. Access token acquired.")
                if self.refresh_token:
                    self._logger.debug(f"{self._LOG_PREFIX} Refresh token acquired: {self.refresh_token}")

        except Exception as err:
            self._logger.error(f"{self._LOG_PREFIX} Authentication failed: {err}")
            raise

    async def refresh_tokens(self) -> None:
        """Refreshes the active access token using the PUT refresh endpoint template."""
        if self._output_handler is None or self._logger is None:
            return

        now = datetime.datetime.now()
        # Avoid race condition during active ingestions in top 10 seconds of the minute
        if now.second < 10:
            sleep_time = 10 - now.second
            self._logger.debug(f"{self._LOG_PREFIX} Waiting {sleep_time}s before refresh to avoid ingestion contention.")
            await asyncio.sleep(sleep_time)

        # If no refresh endpoint template is configured, fall back to full login
        if not self._refresh_endpoint_template or not self.refresh_token:
            self._logger.info(f"{self._LOG_PREFIX} No refresh template or refresh token present. Performing full login...")
            await self.login()
            return

        self.token_valid = False
        raw_path = self._refresh_endpoint_template.get("path", "")
        resolved_path = raw_path.replace("{token}", str(self.token)).replace("{refresh_token}", str(self.refresh_token))

        refresh_endpoint = dict(self._refresh_endpoint_template)
        refresh_endpoint["path"] = resolved_path

        self._logger.debug(f"{self._LOG_PREFIX} Refreshing access token via [{resolved_path}]...")

        try:
            if isinstance(self._output_handler, AsyncHTTPOutputHandler):
                response = await self._output_handler.send(
                    message={},
                    dynamic_endpoints=[refresh_endpoint],
                    mode=HttpPublishMode.OVERRIDE
                )

                token, refresh_token = CWSession._extract_tokens(response)
                if not token:
                    raise ValueError(f"No Token present in refresh response: {response}")

                self.token = token
                self.refresh_token = refresh_token
                self.token_valid = True
                self._logger.info(f"{self._LOG_PREFIX} Token refreshed successfully.")
                if self.refresh_token:
                    self._logger.debug(f"{self._LOG_PREFIX} New refresh token: {self.refresh_token}")

        except Exception as err:
            self._logger.warning(f"{self._LOG_PREFIX} Token refresh failed ({err}). Falling back to full login...")
            await self.login()

    async def run_refresh_job(self) -> None:
        """Periodic job callback invoked by the AsyncPeriodicScheduler."""
        try:
            await self.refresh_tokens()
        except Exception as err:
            if self._logger:
                self._logger.error(f"{self._LOG_PREFIX} Scheduled refresh job error: {err}")