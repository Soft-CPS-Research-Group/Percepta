# --- Standard Library Imports ---
from typing import Callable

# --- Internal Framework Imports ---
from app.core.data_ingestion.telemetry_system_launcher_base import (
    TelemetrySystemComponents,
    TelemetrySystemLauncherBase
)
from app.core.utils.data import DataSet
from app.core.utils.logger import LoggingUtils
from app.extension.telemetry_systems.softcps.softcps_telemetry_adapter import SoftCPSTelemetryAdapter
from app.extension.telemetry_systems.softcps.softcps_endpoint_builder import SoftCPSEndpointBuilder


class SoftCPSTelemetryLauncher(TelemetrySystemLauncherBase):
    """Launcher of the SoftCPS Telemetry System (battery and grid meter data over HTTP).

    For each environment, it returns the SoftCPS Adapter and the HTTP Input Handler that
    polls the observations of each asset. SoftCPS needs no login.
    """

    _LOG_PREFIX: str = "SoftCPS Telemetry Launcher |"

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
        """Prepares the SoftCPS Adapter and Input Handler for one environment.

        Expected `connection_configuration` Data Structure:
        ---------------------------------------------------
        {
            "protocol_id": "AsyncHTTP",                 # Input Handler type
            "connection_info": {
                "connection": {
                    "url": "http://193.136.62.78:8003"
                },
                "topology": {
                    # Endpoint id -> endpoint. Each one is requested for every asset.
                    "endpoints": {
                        "observations": {
                            "path": "/assets/{asset_id}/observations?window_seconds={time_interval}",
                            "method": "GET",
                            "timeout_ms": 4000
                        }
                    }
                }
            }
        }

        Expected `source_mapping` Data Structure:
        -----------------------------------------
        {
            # Parameter id -> keys. The 'fetch_key' is the SoftCPS asset id, requested only once even if
            # several parameters share it. The 'match_key' is the key of the payload that holds the value.
            "b01_energy_in": {"fetch_key": "battery_id_1", "match_key": "energy_charged_kwh_window"},
            "b01_soc": {"fetch_key": "battery_id_1", "match_key": "soc_pct"},
            "gr01_in_total": {"fetch_key": "hq-grid-meter", "match_key": "energy_in_total"}
        }

        In the path, '{asset_id}' is replaced by the 'fetch_key' and '{time_interval}' by the polling
        interval in seconds, which is the window of observations requested.

        Args:
            telemetry_system_id (str): Id of the Telemetry System.
            connection_configuration (dict): How to connect to SoftCPS, as detailed above.
            environment_name (str): Name of the environment (e.g., "R-H-03").
            environment_specs (dict): Schema of the environment, with only the parameters handled by SoftCPS.
            source_mapping (dict): Source mapping of SoftCPS for this environment.
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

        if not isinstance(connection_info.get("connection"), dict):
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: 'connection_info.connection' is missing for "
                f"telemetry system '{telemetry_system_id}'."
            )

        topology = connection_info.get("topology")
        endpoints = topology.get("endpoints") if isinstance(topology, dict) else None
        if not isinstance(endpoints, dict) or not endpoints:
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: 'connection_info.topology.endpoints' must be a non-empty "
                f"dictionary, keyed by endpoint id, for telemetry system '{telemetry_system_id}'."
            )

        # --- Guard Clause: every endpoint needs an id and a path ---
        # The Endpoint Builder does not validate them, so they are checked here
        for endpoint_id, endpoint in endpoints.items():
            if not isinstance(endpoint_id, str) or not endpoint_id.strip() \
                    or not isinstance(endpoint, dict) or not isinstance(endpoint.get("path"), str):
                raise ValueError(
                    f"{self._LOG_PREFIX} Launch error: endpoint '{endpoint_id}' of telemetry system "
                    f"'{telemetry_system_id}' must have a non-empty id and be a dictionary with a 'path'."
                )

        rules = source_mapping
        if not isinstance(rules, dict) or not rules:
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: missing or empty source mapping "
                f"for environment '{environment_name}'."
            )

        timezone = settings.get("timezone", "UTC")
        time_interval = DataSet.calculate_interval(settings.get("frequency"))

        adapter = SoftCPSTelemetryAdapter(
            environment=environment_name,
            environment_specs=environment_specs,
            source_mapping_rules=rules,
            logger=logger,
            timezone=timezone
        )

        endpoint_builder = SoftCPSEndpointBuilder(
            base_topology_template=topology,
            environment_name=environment_name,
            environment_specs=environment_specs,
            source_mapping_rules=rules,
            logger=logger,
            time_interval_seconds=time_interval,
            timezone=timezone
        )

        # The Input Handler takes the endpoints as a list, so the ids are only used up to here
        handler_topology = {**topology, "endpoints": list(endpoints.values())}

        input_handler = await self._input_launcher.build_input_handler(
            handler_type=protocol_id,
            topology_setup=handler_topology,
            callback=callback,
            server_configurations=connection_info["connection"],
            time_interval_seconds=time_interval,
            endpoint_builder=endpoint_builder,
            logger=logger
        )

        return TelemetrySystemComponents(adapter=adapter, input_handler=input_handler)