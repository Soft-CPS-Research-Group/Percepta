# --- Standard Library Imports ---
from typing import Optional, Tuple

# --- Internal Framework Imports ---
from app.core.input_handlers.input_handler_base import InputHandlerBase
from app.core.input_handlers.input_handlers_launcher import InputHandlersLauncher
from app.core.output_handlers.output_handler_base import OutputHandlerBase
from app.core.output_handlers.output_handlers_launcher import OutputHandlersLauncher
from app.core.services.telemetry_pipeline_service import TelemetryPipelineService
from app.core.utils.logger import LoggingUtils
from app.core.utils.providers import Provider
from others.adapters.ic_adapter import ICTelemetryAdapter
from others.endpoints_builders.ic_endpoint_builder import ICEndpointBuilder


class ICAdapterLauncher:
    """Master orchestrator and composition root for the i-charging provider pipeline.

    Bootstraps isolated environment pipelines consisting of:
    - Input Transport (AsyncRabbitMQInputHandler via ICEndpointBuilder)
    - Domain Adapter (ICAdapter)
    - Application Orchestration Service (TelemetryPipelineService)
    - Output Transport (AsyncLocalOutputHandler / AsyncRabbitMQOutputHandler)
    """

    provider: str = Provider.ICHARGING.value if hasattr(Provider, "ICHARGING") else "icharging"
    _LOG_PREFIX: str = "ICLauncher |"

    def __init__(
        self,
        configurations: dict,
        input_launcher: InputHandlersLauncher,
        output_launcher: OutputHandlersLauncher
    ) -> None:
        """Initializes the i-charging provider launcher factory.

        Expected `configurations` Data Structure:
        -----------------------------------------
        {
            "timezone": "UTC",                      # (str) Target execution timezone
            # --- INGRESS (Input Transport Setup) ---
            "ingress": {
                "handler_type": "AsyncRabbitMQ",    # Transport protocol identifier
                "connection": {
                    "host": "localhost",
                    "port": 5672,
                    "virtual_host": "/",
                    "auth": {
                        "username": "guest",
                        "password": "guest"
                    }
                },
                "topology": {
                    "exchanges": [
                        {
                            "exchange_name": "{exchange_name}",
                            "exchange_properties": {
                                "exchange_type": "fanout",
                                "durable": True
                            },
                            "queues": [
                                {
                                    "queue_name": "{queue_name}",
                                    "routing_key": "",
                                    "consume_properties": {
                                        "no_ack": False,
                                        "requeue": False
                                    },
                                    "queue_properties": {
                                        "durable": True
                                    }
                                }
                            ]
                        }
                    ]
                }
            },
            # --- EGRESS (Output Transport Setup) ---
            "egress": {
                "handler_type": "AsyncLocal",       # "AsyncLocal" | "AsyncRabbitMQ" | "AsyncHTTP"
                "connection": {
                    # For AsyncLocal: {"target_callable": <callable_sink_function>}
                    # For AsyncRabbitMQ: {"host": "localhost", "port": 5672, "auth": {...}}
                },
                "topology": {
                    "rpc_properties": {"enabled": False}
                }
            },
            "source_mapping": {
                "R-H-01": "community_porto_telemetry" # (str: str) Maps internal env to remote AMQP exchange
            },
            "log_files": {
                "max_size": 10485760
            }
        }

        Args:
            configurations (dict): Master provider configuration settings as detailed above.
            input_launcher (InputHandlersLauncher): Ingress transport factory instance.
            output_launcher (OutputHandlersLauncher): Egress transport factory instance.

        Raises:
            ValueError: If mandatory configuration blocks or launcher dependencies are missing.
            TypeError: If configurations is not a dictionary.
        """
        # --- Guard Clause 1: Validate configurations ---
        if not configurations:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'configurations' is missing or empty.")

        if not isinstance(configurations, dict):
            raise TypeError(
                f"{self._LOG_PREFIX} Initialization error: 'configurations' must be a dictionary, "
                f"got {type(configurations).__name__}."
            )

        # --- Guard Clause 2: Validate launcher dependencies ---
        if not input_launcher:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'input_launcher' dependency is missing.")

        if not output_launcher:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'output_launcher' dependency is missing.")

        self._configurations = configurations
        self._input_launcher = input_launcher
        self._output_launcher = output_launcher

    async def launch_environment(
        self,
        environment_name: str,
        environment_specs: dict
    ) -> Tuple[InputHandlerBase, OutputHandlerBase, Optional[InputHandlerBase]]:
        """Bootstraps a complete Ingress -> Service -> Adapter -> Egress pipeline for a SINGLE environment.

        Expected `environment_specs` Data Structure:
        -------------------------------------------
        {
            "group": "porto_chargers",              # (str, optional) Environmental cluster identifier
            "entities": {                           # (dict) Entity schema definitions keyed by entity_id
                "SN0000_0": {                       # (str) Unique entity identifier (serialnumber_plug)
                    "label": "ev_charger",          # (str) Domain classification label
                    "parameters": {                 # (dict) Internal parameter schema definitions
                        "power": {
                            "id": "power",
                            "label": "power",
                            "type": "EV Charging Power",
                            "measurementUnit": "kW"
                        }
                    }
                }
            }
        }

        Args:
            environment_name (str): Identifier of the environment (e.g., "R-H-01").
            environment_specs (dict): Entity specifications for this specific environment.

        Returns:
            Tuple[InputHandlerBase, OutputHandlerBase, Optional[InputHandlerBase]]:
                - Primary Ingress InputHandler (AsyncRabbitMQInputHandler)
                - Primary Egress OutputHandler (AsyncLocalOutputHandler / AsyncRabbitMQOutputHandler)
                - Optional RPC Listener InputHandler (if RPC is configured on egress)

        Raises:
            ValueError: If environment_name or environment_specs are invalid or missing.
        """
        # --- Guard Clause 1: Validate environment_name ---
        if not environment_name or not isinstance(environment_name, str) or not environment_name.strip():
            raise ValueError(f"{self._LOG_PREFIX} Launch error: 'environment_name' must be a non-empty string.")

        # --- Guard Clause 2: Validate environment_specs ---
        if not isinstance(environment_specs, dict) or "entities" not in environment_specs:
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: 'environment_specs' must be a dictionary "
                f"containing an 'entities' key for environment '{environment_name}'."
            )

        logger_env = LoggingUtils(f"{self.provider}_pipeline", self._configurations, environment_name)
        timezone: str = self._configurations.get("timezone", "UTC")

        # Extract environment mapping rule (if present)
        source_mapping = self._configurations.get("source_mapping", {})
        env_mapping_rules = source_mapping if isinstance(source_mapping, dict) else {}

        # Extract Ingress transport settings
        ingress_config: dict = self._configurations.get("ingress", {})
        input_handler_type: str = ingress_config.get("handler_type", "AsyncRabbitMQ")
        raw_topology_template: dict = ingress_config.get("topology", {})
        ingress_connection: dict = ingress_config.get("connection", {})

        # Extract Egress transport settings
        egress_config: dict = self._configurations.get("egress", {})
        output_handler_type: str = egress_config.get("handler_type", "AsyncLocal")
        egress_connection: dict = egress_config.get("connection", egress_config.get("server_configurations", {}))
        egress_topology: dict = egress_config.get("topology", {})

        # ---------------------------------------------------------------------
        # Step 1: Materialize Egress Output Handler (+ optional RPC listener)
        # ---------------------------------------------------------------------
        output_handler, rpc_listener = await self._output_launcher.build_output_handler(
            handler_type=output_handler_type,
            topology_setup=egress_topology,
            server_configurations=egress_connection
        )
        if rpc_listener:
            await rpc_listener.start()

        # ---------------------------------------------------------------------
        # Step 2: Materialize Pure Domain Adapter (No Network I/O)
        # ---------------------------------------------------------------------
        adapter = ICTelemetryAdapter(
            environment=environment_name,
            environment_specs=environment_specs,
            configurations=self._configurations,
            logger=logger_env
        )

        # ---------------------------------------------------------------------
        # Step 3: Materialize Application Service (Orchestrator: Adapt -> Send)
        # ---------------------------------------------------------------------
        pipeline_service = TelemetryPipelineService(
            adapter=adapter,
            output_handler=output_handler,
            logger=logger_env
        )

        # ---------------------------------------------------------------------
        # Step 4: Materialize AMQP Topology Builder
        # ---------------------------------------------------------------------
        endpoint_builder = ICEndpointBuilder(
            base_topology_template=raw_topology_template,
            environment_name=environment_name,
            environment_specs=environment_specs,
            source_mapping_rules=env_mapping_rules,
            logger=logger_env,
            timezone=timezone
        )

        # ---------------------------------------------------------------------
        # Step 5: Materialize & Start Ingress Input Handler (Bound to Service)
        # ---------------------------------------------------------------------
        input_handler = await self._input_launcher.build_input_handler(
            handler_type=input_handler_type,
            topology_setup=raw_topology_template,
            callback=pipeline_service.process_message,
            server_configurations=ingress_connection,
            endpoint_builder=endpoint_builder
        )

        await input_handler.start()

        return input_handler, output_handler, rpc_listener

    def stop(self, timeout: float = 5.0) -> None:
        """Lifecycle hook for provider-level cleanups."""
        pass