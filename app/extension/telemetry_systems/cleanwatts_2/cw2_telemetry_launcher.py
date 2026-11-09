# --- Standard Library Imports ---
from typing import Callable

# --- Internal Framework Imports ---
from app.core.data_ingestion.telemetry_system_launcher_base import (
    TelemetrySystemComponents,
    TelemetrySystemLauncherBase
)
from app.core.utils.logger import LoggingUtils
from app.extension.telemetry_systems.cleanwatts_2.cw2_telemetry_adapter import CW2TelemetryAdapter
from app.extension.telemetry_systems.cleanwatts_2.cw2_endpoint_builder import CW2EndpointBuilder


class CW2TelemetryLauncher(TelemetrySystemLauncherBase):
    """Launcher of the Cleanwatts v2 Telemetry System (Shelly data over RabbitMQ).

    For each environment, it returns the Cleanwatts v2 Adapter and a function that
    builds the RabbitMQ Input Handler, with the topology resolved for that environment.
    """

    _LOG_PREFIX: str = "CW2 Telemetry Launcher |"

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
        """Prepares the Cleanwatts v2 Adapter and Input Handler for one environment.

        Expected `connection_configuration` Data Structure:
        ---------------------------------------------------
        {
            "protocol_id": "AsyncRabbitMQ",             # Input Handler type
            "connection_info": {
                "connection": {
                    "host": "rabbit.cleanwatts.energy",
                    "port": 5671,
                    "vhost": "iEnergyInnov",
                    "auth": {"username": "user", "password": "secret_password"},
                    "ssl_conf": {"enabled": True, "check_hostname": False, "verify_mode": None}
                },
                "topology": {
                    "exchanges": [
                        {
                            "exchange_name": "{exchange_name}",
                            "exchange_properties": {"exchange_type": "fanout", "durable": False},
                            "queues": [
                                {
                                    "queue_name": "isep_consumer_{random_hash}",
                                    "routing_key": "",
                                    "consume_properties": {"no_ack": False, "requeue": False},
                                    "queue_properties": {"durable": True, "exclusive": True}
                                }
                            ]
                        }
                    ]
                }
            }
        }

        Expected `source_mapping` Data Structure:
        -----------------------------------------
        {
            # Parameter id -> keys. All the parameters of an entity share the same exchange,
            # which is subscribed to only once.
            "GR02_active_power": {"fetch_key": "<exchange name>", "match_key": "<exchange name>"}
        }

        Args:
            telemetry_system_id (str): Id of the Telemetry System.
            connection_configuration (dict): How to connect to RabbitMQ, as detailed above.
            environment_name (str): Name of the environment (e.g., "R-H-02").
            environment_specs (dict): Schema of the environment, with only the parameters handled by this system.
            source_mapping (dict): Source mapping of this Telemetry System for this environment.
            settings (dict): Instance settings ("timezone").
            callback (Callable): Function the Input Handler calls with each message it receives.
            logger (LoggingUtils): Logger of the environment, given to the Adapter, the Endpoint Builder
                and the Input Handler, so their logs show the environment.

        Returns:
            TelemetrySystemComponents: The Adapter and the Input Handler (not started).

        Raises:
            ValueError: If the connection, the topology or the source mapping is missing or invalid.
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

        # --- Guard Clause: connection_info must have a 'topology' dictionary ---
        # The Endpoint Builder does not validate or complete it, so it has to be in the configuration
        if not isinstance(connection_info.get("topology"), dict):
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: 'connection_info.topology' is missing for "
                f"telemetry system '{telemetry_system_id}'."
            )

        rules = source_mapping
        if not isinstance(rules, dict) or not rules:
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: missing or empty source mapping "
                f"for environment '{environment_name}'."
            )

        timezone = settings.get("timezone", "UTC")

        adapter = CW2TelemetryAdapter(
            environment=environment_name,
            environment_specs=environment_specs,
            source_mapping_rules=rules,
            logger=logger,
            timezone=timezone
        )

        endpoint_builder = CW2EndpointBuilder(
            base_topology_template=connection_info["topology"],
            environment_name=environment_name,
            environment_specs=environment_specs,
            source_mapping_rules=rules,
            logger=logger,
            timezone=timezone
        )

        # The final RabbitMQ topology (exchanges + generated consumer queues) for this environment
        resolved_topology = await endpoint_builder.build_endpoints()

        input_handler = await self._input_launcher.build_input_handler(
            handler_type=protocol_id,
            topology_setup=resolved_topology,
            callback=callback,
            server_configurations=connection_info["connection"],
            logger=logger
        )

        return TelemetrySystemComponents(adapter=adapter, input_handler=input_handler)