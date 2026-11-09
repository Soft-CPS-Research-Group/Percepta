# --- Standard Library Imports ---
from typing import Callable

# --- Internal Framework Imports ---
from app.core.data_ingestion.telemetry_system_launcher_base import (
    TelemetrySystemComponents,
    TelemetrySystemLauncherBase
)
from app.core.utils.logger import LoggingUtils
from app.extension.telemetry_systems.predictor.predictor_endpoint_builder import PredictorEndpointBuilder
from app.extension.telemetry_systems.predictor.predictor_telemetry_adapter import PredictorTelemetryAdapter


class PredictorTelemetryLauncher(TelemetrySystemLauncherBase):
    """Launcher of the Predictor Telemetry System (consumption and solar production forecasts over RabbitMQ).

    For each environment, it returns the Predictor Adapter and the RabbitMQ Input
    Handler, with the topology resolved for that environment.
    """

    _LOG_PREFIX: str = "Predictor Telemetry Launcher |"

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
        """Prepares the Predictor Adapter and Input Handler for one environment.

        Expected `connection_configuration` Data Structure:
        ---------------------------------------------------
        {
            "protocol_id": "AsyncRabbitMQ",
            "connection_info": {
                "connection": {
                    "host": "rabbit.example.com",
                    "port": 5672,
                    "vhost": "/",
                    "heartbeat": 660,
                    "auth": {"username": "user", "password": "secret_password"}
                },
                "topology": {
                    "exchanges": [
                        {
                            "exchange_name": "pred.{environment}",
                            "exchange_properties": {"exchange_type": "direct", "durable": True},
                            "queues": [
                                {
                                    "queue_name": "percepta_local_forecast_{environment}",
                                    "routing_key": "q.pred.{environment}",
                                    "consume_properties": {"no_ack": False, "requeue": False},
                                    "queue_properties": {"durable": True, "exclusive": False}
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
            "pr01_consumption": {"match_key": "consumption"},
            "pr01_production": {"match_key": "production"}
        }

        The topology is passed to the Endpoint Builder as it is. The Endpoint Builder
        replaces '{environment}' in the exchange name, the queue names and the routing keys.

        Args:
            telemetry_system_id (str): Id of the Telemetry System.
            connection_configuration (dict): How to connect to RabbitMQ, as detailed above.
            environment_name (str): Name of the environment (e.g., "R-H-03").
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

        # --- Guard Clause 1: connection_info must be a dictionary ---
        if not isinstance(connection_info, dict):
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: 'connection_info' is missing for "
                f"telemetry system '{telemetry_system_id}'."
            )

        # --- Guard Clause 2: connection_info must have a 'connection' dictionary ---
        if not isinstance(connection_info.get("connection"), dict):
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: 'connection_info.connection' is missing for "
                f"telemetry system '{telemetry_system_id}'."
            )

        # --- Guard Clause 3: connection_info must have a 'topology' dictionary ---
        if not isinstance(connection_info.get("topology"), dict):
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: 'connection_info.topology' is missing for "
                f"telemetry system '{telemetry_system_id}'."
            )

        # --- Guard Clause 4: the source mapping must not be empty ---
        rules = source_mapping
        if not isinstance(rules, dict) or not rules:
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: missing or empty source mapping "
                f"for environment '{environment_name}'."
            )

        timezone = settings.get("timezone", "UTC")

        adapter = PredictorTelemetryAdapter(
            environment=environment_name,
            environment_specs=environment_specs,
            source_mapping_rules=rules,
            logger=logger,
            timezone=timezone
        )

        endpoint_builder = PredictorEndpointBuilder(
            base_topology_template=connection_info["topology"],
            environment_name=environment_name,
            environment_specs=environment_specs,
            source_mapping_rules=rules,
            logger=logger,
            timezone=timezone
        )

        # The RabbitMQ topology for this environment
        resolved_topology = await endpoint_builder.build_endpoints()

        input_handler = await self._input_launcher.build_input_handler(
            handler_type=protocol_id,
            topology_setup=resolved_topology,
            callback=callback,
            server_configurations=connection_info["connection"],
            logger=logger
        )

        return TelemetrySystemComponents(adapter=adapter, input_handler=input_handler)