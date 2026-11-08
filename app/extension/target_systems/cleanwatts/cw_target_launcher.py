# --- Standard Library Imports ---
import asyncio
from typing import Optional
from zoneinfo import ZoneInfo

# --- Internal Framework Imports ---
from app.core.integrated_logic.actuation_error_handling_logic_base import ActuationErrorHandlingLogicBase
from app.core.launchers.external_systems_launchers.target_system_launcher_base import (
    TargetSystemComponents,
    TargetSystemLauncherBase
)
from app.core.output_handlers.output_handler_base import OutputHandlerBase
from app.core.utils.async_periodic_scheduler import AsyncPeriodicScheduler
from app.core.utils.labels import OutputHandlerLabel
from app.core.utils.logger import LoggingUtils
from app.extension.target_systems.cleanwatts.cw_target_adapter import CWTargetAdapter
from app.extension.target_systems.default_actuation_error_handling_logic import DefaultActuationErrorHandlingLogic
from app.extension.telemetry_systems.cleanwatts.cw_session import CWSession


class CWTargetLauncher(TargetSystemLauncherBase):
    """Launcher of the Cleanwatts Target System (actuation of batteries and EV chargers over HTTP).

    It has its own login session, independent of the one of the Cleanwatts Telemetry System: it logs in once, shares
    the session between all the environments it launches, and keeps the token fresh. For each environment, it returns
    the Cleanwatts Adapter, the HTTP Output Handler that sends the actuations and the Actuation Error Handling Logic
    (the default one, unless the entry of the system names another).
    """

    _LOG_PREFIX: str = "CW Target Launcher |"

    def __init__(self, output_launcher, logger) -> None:
        """Initializes the Cleanwatts launcher.

        Args:
            output_launcher (OutputHandlersLauncher): Factory of Output Handlers (used for the login requests and for
                the actuations).
            logger (LoggingUtils): Logging interface.
        """
        super().__init__(output_launcher, logger)
        self._auth_scheduler: Optional[AsyncPeriodicScheduler] = None
        self._auth_output_handler: Optional[OutputHandlerBase] = None
        self._auth_lock = asyncio.Lock()
        # The session of this launcher, shared by all the environments it launches
        self._session = CWSession()

    async def launch(
        self,
        target_system_id: str,
        target_system_configuration: dict,
        environment_name: str,
        environment: dict,
        logger: LoggingUtils
    ) -> TargetSystemComponents:
        """Prepares the Cleanwatts Adapter, Output Handler and logic for one environment.

        Expected `target_system_configuration` Data Structure:
        ------------------------------------------------------
        {
            "path": "app.extension.target_systems.cleanwatts.cw_target_launcher.CWTargetLauncher",
            "connection_configuration": {
                "protocol_id": "AsyncHTTP",                 # Output Handler type
                "connection_info": {
                    "timezone": "Europe/Lisbon",            # (optional, default "UTC") Of the token refresh job
                    "refresh_interval_seconds": 3300,       # (optional, default 3300)
                    "credentials": {"username": "user", "password": "secret"},
                    "connection": {"url": "https://innov.cleanwattsdigital.com/api/2.0", "verify_ssl": True},
                    "authentication": {                     # The requests to log in and to renew the token
                        "topology": {
                            "endpoints": [
                                {"path": "/sessions", "method": "POST", "timeout_ms": 5000,
                                 "headers": {"Content-Type": "application/json"},
                                 "rpc_properties": {"enabled": True}},
                                {"path": "/session/refresh?token={token}&refresh_token={refresh_token}",
                                 "method": "PUT", "timeout_ms": 5000,
                                 "headers": {"Content-Type": "application/json"},
                                 "rpc_properties": {"enabled": True}}
                            ]
                        }
                    },
                    "topology": {                           # The actuation. The response is needed to detect errors
                        "endpoints": [
                            {"path": "/actuations", "method": "POST", "timeout_ms": 5000,
                             "headers": {"Content-Type": "application/json"},
                             "rpc_properties": {"enabled": True}}
                        ]
                    }
                }
            },
            "actuation_error_handling_logic": {             # (dict, optional) Default: 3 attempts, 1 second apart
                "path": "app.extension.target_systems.cleanwatts.my_logic.MyLogic"
            }
        }

        Args:
            target_system_id (str): Id of the Target System.
            target_system_configuration (dict): The entry of the system, as detailed above.
            environment_name (str): Name of the environment (e.g., "R-H-01").
            environment (dict): The environment, as returned by the Schema Storage.
            logger (LoggingUtils): Logger of the environment, given to the Adapter, the Output Handler and the logic, so
                their logs show the environment.

        Returns:
            TargetSystemComponents: The Adapter, the Output Handler (already connected) and the Actuation Error
                Handling Logic.

        Raises:
            ValueError: If the configuration is invalid, or the login fails.
        """
        connection_configuration = target_system_configuration.get("connection_configuration")
        connection_info = (
            connection_configuration.get("connection_info") if isinstance(connection_configuration, dict) else None
        )

        # Guard Clause 1: Checks that the protocol is HTTP, which is how Cleanwatts is reached.
        protocol_id = connection_configuration.get("protocol_id") if isinstance(connection_configuration, dict) else None
        if protocol_id != OutputHandlerLabel.ASYNC_HTTP.value:
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: the 'protocol_id' of Target System '{target_system_id}' must be "
                f"'{OutputHandlerLabel.ASYNC_HTTP.value}', and it is {protocol_id!r}."
            )

        # Guard Clause 2: Checks that the connection, the credentials and the two topologies exist.
        if not isinstance(connection_info, dict):
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: 'connection_info' is missing for Target System '{target_system_id}'."
            )
        for key in ("connection", "credentials"):
            if not isinstance(connection_info.get(key), dict):
                raise ValueError(
                    f"{self._LOG_PREFIX} Launch error: 'connection_info.{key}' is missing for Target System "
                    f"'{target_system_id}'."
                )

        authentication = connection_info.get("authentication")
        auth_topology = authentication.get("topology") if isinstance(authentication, dict) else None
        auth_endpoints = auth_topology.get("endpoints") if isinstance(auth_topology, dict) else None
        if not isinstance(auth_endpoints, list) or not auth_endpoints:
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: 'connection_info.authentication.topology.endpoints' must be a "
                f"non-empty list for Target System '{target_system_id}'."
            )

        topology = connection_info.get("topology")
        endpoints = topology.get("endpoints") if isinstance(topology, dict) else None

        # Guard Clause 3: Checks that there is an actuation endpoint with a path, and that it returns the response,
        # because that is how the Decision Dispatching Service finds out that an actuation failed.
        if not isinstance(endpoints, list) or not endpoints or not isinstance(endpoints[0], dict) \
                or not isinstance(endpoints[0].get("path"), str) or not endpoints[0]["path"].strip():
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: 'connection_info.topology.endpoints' must be a list whose first "
                f"endpoint has a 'path' for Target System '{target_system_id}'."
            )
        if not endpoints[0].get("rpc_properties", {}).get("enabled", False):
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: the actuation endpoint '{endpoints[0]['path']}' of Target System "
                f"'{target_system_id}' must have 'rpc_properties.enabled' set to true, so that errors are detected."
            )

        # Guard Clause 4: Checks that Cleanwatts can control every actuator of the environment that it receives, so that
        # a wrong 'actuator_mapping' (e.g., a charger in amperes with no voltage) is found when the application
        # starts, and not when the first decision arrives.
        mapping = (environment.get("target_systems") or {}).get(target_system_id, {}).get("actuator_mapping", {})
        for actuator_id, device in mapping.items():
            if isinstance(device, dict):
                try:
                    CWTargetAdapter.check_device(device.get("destination_measurement_unit"), device.get("voltage"))
                except ValueError as err:
                    raise ValueError(
                        f"{self._LOG_PREFIX} Launch error: actuator '{actuator_id}' of environment "
                        f"'{environment_name}': {err}."
                    ) from err

        # Login once, even if several environments are launched at the same time
        async with self._auth_lock:
            if not self._session.is_token_valid():
                await self._initialize_auth({
                    "handler_type": protocol_id,
                    "refresh_interval_seconds": connection_info.get("refresh_interval_seconds", 3300),
                    "connection": {**connection_info["connection"], "credentials": connection_info["credentials"]},
                    "topology": auth_topology
                }, connection_info.get("timezone", "UTC"))

        adapter = CWTargetAdapter(
            target_system_id=target_system_id,
            environment_name=environment_name,
            environment=environment,
            logger=logger,
            session=self._session,
            endpoint=endpoints[0]
        )

        error_handling_logic = self._build_logic(
            target_system_configuration, "actuation_error_handling_logic",
            ActuationErrorHandlingLogicBase, environment, logger
        ) or DefaultActuationErrorHandlingLogic(environment, logger)

        output_handler, _ = await self._output_launcher.build_output_handler(
            handler_type=protocol_id,
            topology_setup=topology,
            server_configurations=connection_info["connection"],
            logger=logger
        )

        return TargetSystemComponents(
            adapter=adapter, output_handler=output_handler, error_handling_logic=error_handling_logic
        )

    async def _initialize_auth(self, auth_config: dict, timezone: str) -> None:
        """Bootstraps the Output Handler for authentication, logs in and schedules periodic token refreshes.

        If authentication was already initialized, the previous scheduler and handler are stopped first, so they are
        not left running without an owner.

        Args:
            auth_config (dict): Login configuration with 'handler_type', 'refresh_interval_seconds', 'connection'
                (including the credentials) and 'topology' (authentication endpoints).
            timezone (str): Timezone of the refresh scheduler.
        """
        await self._stop_auth()

        # 1. Build dedicated RPC Output Handler for Auth requests
        self._auth_output_handler, _ = await self._output_launcher.build_output_handler(
            handler_type=auth_config["handler_type"],
            topology_setup=auth_config["topology"],
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
            time_interval=auth_config["refresh_interval_seconds"],
            trigger_callback=self._session.run_refresh_job,
            job_id="cw_target_token_refresh_job"
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
