# --- Internal Framework Imports ---
from app.core.decision_dispatching.actuation_error_handling_logic_base import ActuationErrorHandlingLogicBase
from app.core.decision_dispatching.target_system_launcher_base import (
    TargetSystemComponents,
    TargetSystemLauncherBase
)
from app.core.utils.labels import OutputHandlerLabel
from app.core.utils.logger import LoggingUtils
from app.extension.target_systems.default_actuation_error_handling_logic import DefaultActuationErrorHandlingLogic
from app.extension.target_systems.softcps.softcps_target_adapter import SoftCPSTargetAdapter


class SoftCPSTargetLauncher(TargetSystemLauncherBase):
    """Launcher of the SoftCPS Target System (actuation of batteries over HTTP).

    For each environment, it returns the SoftCPS Adapter, the HTTP Output Handler that sends the requests and the
    Actuation Error Handling Logic (the default one, unless the entry of the system names another). SoftCPS needs no
    login.
    """

    _LOG_PREFIX: str = "SoftCPS Target Launcher |"

    async def launch(
        self,
        target_system_id: str,
        target_system_configuration: dict,
        environment_name: str,
        environment: dict,
        logger: LoggingUtils
    ) -> TargetSystemComponents:
        """Prepares the SoftCPS Adapter, Output Handler and logic for one environment.

        Expected `target_system_configuration` Data Structure:
        ------------------------------------------------------
        {
            "path": "app.extension.target_systems.softcps.softcps_target_launcher.SoftCPSTargetLauncher",
            "connection_configuration": {
                "protocol_id": "AsyncHTTP",                 # Output Handler type
                "connection_info": {
                    "connection": {"url": "http://193.136.62.78:8003"},
                    "topology": {
                        "endpoints": [
                            {
                                "path": "/assets/{asset_id}/actions",   # '{asset_id}' is the destination of the actuator
                                "method": "POST",
                                "timeout_ms": 5000,
                                "headers": {"Content-Type": "application/json"},
                                "rpc_properties": {"enabled": True}     # The response is needed to detect errors
                            }
                        ]
                    }
                }
            },
            "actuation_error_handling_logic": {             # (dict, optional) Default: 3 attempts, 1 second apart
                "path": "app.extension.target_systems.softcps.my_logic.MyLogic"
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
            ValueError: If the configuration is invalid.
        """
        connection_configuration = target_system_configuration.get("connection_configuration")
        protocol_id = connection_configuration.get("protocol_id") if isinstance(connection_configuration, dict) else None
        connection_info = (
            connection_configuration.get("connection_info") if isinstance(connection_configuration, dict) else None
        )

        # Guard Clause 1: Checks that the protocol is HTTP, which is how SoftCPS is reached.
        if protocol_id != OutputHandlerLabel.ASYNC_HTTP.value:
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: the 'protocol_id' of Target System '{target_system_id}' must be "
                f"'{OutputHandlerLabel.ASYNC_HTTP.value}', and it is {protocol_id!r}."
            )

        # Guard Clause 2: Checks that the connection exists.
        if not isinstance(connection_info, dict) or not isinstance(connection_info.get("connection"), dict):
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: 'connection_info.connection' is missing for Target System "
                f"'{target_system_id}'."
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

        adapter = SoftCPSTargetAdapter(
            target_system_id=target_system_id,
            environment_name=environment_name,
            environment=environment,
            logger=logger,
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
