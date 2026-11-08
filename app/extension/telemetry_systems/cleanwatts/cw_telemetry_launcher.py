# --- Standard Library Imports ---
import asyncio
from typing import Callable, Optional
from zoneinfo import ZoneInfo

# --- Internal Framework Imports ---
from app.core.launchers.external_systems_launchers.telemetry_system_launcher_base import (
    TelemetrySystemComponents,
    TelemetrySystemLauncherBase
)
from app.core.output_handlers.output_handler_base import OutputHandlerBase
from app.core.utils.async_periodic_scheduler import AsyncPeriodicScheduler
from app.core.utils.data import DataSet
from app.core.utils.labels import OutputHandlerLabel
from app.core.utils.logger import LoggingUtils
from app.extension.telemetry_systems.cleanwatts.cw_telemetry_adapter import CWTelemetryAdapter
from app.extension.telemetry_systems.cleanwatts.cw_endpoint_builder import CWEndpointBuilder
from app.extension.telemetry_systems.cleanwatts.cw_session import CWSession


class CWTelemetryLauncher(TelemetrySystemLauncherBase):
    """Launcher of the Cleanwatts Telemetry System.

    Logs in to Cleanwatts once (shared by all environments) and keeps the token
    fresh. For each environment, it returns the Cleanwatts Adapter and the HTTP
    Input Handler that polls the data.
    """

    _LOG_PREFIX: str = "CW Telemetry Launcher |"

    def __init__(self, input_launcher, output_launcher, logger) -> None:
        """Initializes the Cleanwatts launcher.

        Args:
            input_launcher (InputHandlersLauncher): Factory of Input Handlers.
            output_launcher (OutputHandlersLauncher): Factory of Output Handlers (used for the login requests).
            logger (LoggingUtils): Logging interface.
        """
        super().__init__(input_launcher, output_launcher, logger)
        self._auth_scheduler: Optional[AsyncPeriodicScheduler] = None
        self._auth_output_handler: Optional[OutputHandlerBase] = None
        self._auth_lock = asyncio.Lock()
        # The session of this launcher, shared by all the environments it launches
        self._session = CWSession()

    async def launch(
        self,
        telemetry_system_id: str,
        connection_configuration: dict,
        environment_name: str,
        environment_specs: dict,
        source_mapping: dict,
        settings: dict,
        callback: Callable,
        logger: LoggingUtils
    ) -> TelemetrySystemComponents:
        """Prepares the Cleanwatts Adapter and Input Handler for one environment.

        Expected `connection_configuration` Data Structure:
        ---------------------------------------------------
        {
            "protocol_id": "AsyncHTTP",                 # Type of the Input Handler and of the login Output Handler
            "connection_info": {
                "timezone": "Europe/Lisbon",            # (str, optional) Timezone the API reads 'from' and 'to' in. Defaults to the instance's
                "refresh_interval_seconds": 3300,       # (int, optional) Token refresh interval. Defaults to 3300
                "lookback_seconds": 1800,               # (int, optional) How far back every data request goes. Must exceed Cleanwatts' publishing delay. Defaults to 1800
                "credentials": {"username": "user", "password": "secret_password"},
                "connection": {
                    "url": "https://innov.cleanwattsdigital.com/api/2.0",
                    "verify_ssl": True
                },
                "topology": {
                    "endpoints": [
                        # Authentication endpoints (marked with 'rpc_properties.enabled')
                        {
                            "path": "/sessions",
                            "method": "POST",
                            "timeout_ms": 5000,
                            "headers": {"Content-Type": "application/json"},
                            "rpc_properties": {"enabled": True}
                        },
                        {
                            "path": "/session/refresh?token={token}&refresh_token={refresh_token}",
                            "method": "PUT",
                            "timeout_ms": 5000,
                            "headers": {"Content-Type": "application/json"},
                            "rpc_properties": {"enabled": True}
                        },
                        # Data endpoint (polled by the Input Handler)
                        {
                            "path": "/data/request/Instant?instantType=avg&tags={param_id}&from={from}&to={to}",
                            "method": "GET",
                            "timeout_ms": 4000,
                            "headers": {"Authorization": "CW {token}"}
                        }
                    ]
                }
            }
        }

        The 'topology' is shared: the endpoints marked with 'rpc_properties.enabled' are
        given to the authentication handler and all the others to the Input Handler.

        Expected `source_mapping` Data Structure:
        -----------------------------------------
        {
            "37507": {"fetch_key": "37507", "match_key": "37507"},      # Parameter id -> Cleanwatts tag id
            "41836": {"fetch_key": "41836", "match_key": "41836"},
            "connected_vehicle": None                                   # Derived by the Adapter
        }

        Args:
            telemetry_system_id (str): Id of the Telemetry System.
            connection_configuration (dict): How to connect to Cleanwatts, as detailed above.
            environment_name (str): Name of the environment (e.g., "R-H-01").
            environment_specs (dict): Schema of the environment, with only the parameters handled by Cleanwatts.
            source_mapping (dict): Source mapping of Cleanwatts for this environment.
            settings (dict): Instance settings ("timezone" and "frequency").
            callback (Callable): Function the Input Handler calls with each message it receives.
            logger (LoggingUtils): Logger of the environment, given to the Adapter, the Endpoint Builder
                and the Input Handler, so their logs show the environment.

        Returns:
            TelemetrySystemComponents: The Adapter and the Input Handler (not started).

        Raises:
            ValueError: If the connection configuration or the source mapping is invalid.
        """
        connection_info = connection_configuration.get("connection_info")
        protocol_id = connection_configuration.get("protocol_id")

        if not isinstance(connection_info, dict):
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: 'connection_info' is missing for "
                f"telemetry system '{telemetry_system_id}'."
            )
        if not isinstance(connection_info.get("credentials"), dict):
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: 'connection_info.credentials' is missing for "
                f"telemetry system '{telemetry_system_id}'."
            )
        if not isinstance(connection_info.get("connection"), dict):
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: 'connection_info.connection' is missing for "
                f"telemetry system '{telemetry_system_id}'."
            )

        auth_topology, ingestion_topology = self._split_topology(connection_info.get("topology"))
        if not auth_topology["endpoints"] or not ingestion_topology["endpoints"]:
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: 'connection_info.topology.endpoints' of telemetry system "
                f"'{telemetry_system_id}' needs authentication endpoints (with 'rpc_properties.enabled') "
                f"and at least one data endpoint."
            )

        # --- Guard Clause: the window of the requests must be a positive number ---
        # The Endpoint Builder does not validate it, so it is checked here (there is no default)
        lookback_seconds = connection_info.get("lookback_seconds")
        if isinstance(lookback_seconds, bool) or not isinstance(lookback_seconds, (int, float)) or lookback_seconds <= 0:
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: 'connection_info.lookback_seconds' must be a positive number "
                f"for telemetry system '{telemetry_system_id}', got {lookback_seconds!r}."
            )

        rules = source_mapping
        if not isinstance(rules, dict) or not rules:
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: missing or empty source mapping "
                f"for environment '{environment_name}'."
            )

        timezone = settings.get("timezone", "UTC")

        # Cleanwatts reads the 'from' and 'to' of its requests in its own timezone, which may not be the instance's one
        api_timezone = connection_info.get("timezone", timezone)

        time_interval = DataSet.calculate_interval(settings.get("frequency"))

        # Login once, even if several environments are launched at the same time
        async with self._auth_lock:
            if not self._session.is_token_valid():
                auth_config = {
                    "handler_type": protocol_id,
                    "refresh_interval_seconds": connection_info.get("refresh_interval_seconds", 3300),
                    "connection": {**connection_info["connection"], "credentials": connection_info["credentials"]},
                    "topology": auth_topology
                }
                await self._initialize_auth(auth_config, timezone)

        adapter = CWTelemetryAdapter(
            environment=environment_name,
            environment_specs=environment_specs,
            source_mapping_rules=rules,
            logger=logger,
            timezone=timezone
        )

        topology = ingestion_topology
        endpoint_builder = CWEndpointBuilder(
            base_topology_template=topology,
            environment_name=environment_name,
            environment_specs=environment_specs,
            source_mapping_rules=rules,
            logger=logger,
            lookback_seconds=lookback_seconds,
            timezone=api_timezone,
            cw_session=self._session
        )

        input_handler = await self._input_launcher.build_input_handler(
            handler_type=protocol_id,
            topology_setup=topology,
            callback=callback,
            server_configurations=connection_info["connection"],
            time_interval_seconds=time_interval,
            endpoint_builder=endpoint_builder,
            logger=logger
        )

        return TelemetrySystemComponents(adapter=adapter, input_handler=input_handler)

    @staticmethod
    def _split_topology(topology) -> tuple:
        """Splits the shared topology into the authentication and the data topologies.

        Endpoints marked with 'rpc_properties.enabled' are authentication endpoints;
        all the others are data endpoints. Other keys of the topology are kept in both.

        Args:
            topology (dict): The shared topology with an 'endpoints' list.

        Returns:
            tuple: (authentication topology, data topology). The 'endpoints' lists are empty
                if the topology is missing or invalid.
        """
        topology = topology if isinstance(topology, dict) else {}
        endpoints = topology.get("endpoints")
        endpoints = endpoints if isinstance(endpoints, list) else []

        def is_rpc(endpoint) -> bool:
            return isinstance(endpoint, dict) and bool(endpoint.get("rpc_properties", {}).get("enabled"))

        base = {key: value for key, value in topology.items() if key != "endpoints"}
        auth_topology = {**base, "endpoints": [e for e in endpoints if is_rpc(e)]}
        data_topology = {**base, "endpoints": [e for e in endpoints if isinstance(e, dict) and not is_rpc(e)]}
        return auth_topology, data_topology

    async def _initialize_auth(self, auth_config: dict, timezone: str) -> None:
        """Bootstraps the RPC Output Handler for authentication and schedules periodic token refreshes.

        If authentication was already initialized, the previous scheduler and handler
        are stopped first, so they are not left running without an owner.

        Args:
            auth_config (dict): Login configuration with 'handler_type', 'refresh_interval_seconds',
                'connection' (including the credentials) and 'topology' (authentication endpoints).
            timezone (str): Timezone of the refresh scheduler.
        """
        await self._stop_auth()

        # 1. Build dedicated RPC Output Handler for Auth requests
        self._auth_output_handler, _ = await self._output_launcher.build_output_handler(
            handler_type=auth_config.get("handler_type", OutputHandlerLabel.ASYNC_HTTP.value),
            topology_setup=auth_config.get("topology", {}),
            server_configurations=auth_config["connection"]
        )

        # 2. Inject dependencies into the session of this launcher
        self._session.initialize(
            output_handler=self._auth_output_handler,
            auth_config=auth_config,
            logger=self._logger
        )

        # 3. Perform initial asynchronous login
        await self._session.login()

        # 4. Schedule periodic async token refresh
        self._auth_scheduler = AsyncPeriodicScheduler(timezone=ZoneInfo(timezone), logger=self._logger)
        self._auth_scheduler.create_job(
            time_interval=auth_config.get("refresh_interval_seconds", 3300),
            trigger_callback=self._session.run_refresh_job,
            job_id="cw_token_refresh_job"
        )
        self._auth_scheduler.start()

    async def _stop_auth(self) -> None:
        """Stops the auth scheduler and the auth output handler, if they exist."""
        if self._auth_scheduler:
            self._auth_scheduler.stop()
            self._auth_scheduler = None

        if self._auth_output_handler:
            result = self._auth_output_handler.stop()
            if hasattr(result, "__await__"):
                await result
            self._auth_output_handler = None

    async def stop(self) -> None:
        """Stops the token refresh scheduler and the authentication handler."""
        await self._stop_auth()