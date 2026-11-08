# --- Standard Library Imports ---
import copy
# --- Internal Framework Imports ---
from app.core.endpoint_builders.endpoint_builder_base import EndpointBuilderBase
from app.core.utils.logger import LoggingUtils


class PCEndpointBuilder(EndpointBuilderBase):
    """PulseCharge (PC) AMQP/RabbitMQ topology resolution strategy.

    PulseCharge publishes the data of all the users of an environment on one fanout
    exchange. The name of that exchange comes from the exchange template of the topology
    (e.g. "building_{environment}"), where '{environment}' is replaced by the environment
    name. This builder produces the static AMQP topology used by the AsyncRabbitMQInputHandler.

    The builder only creates the endpoints: it replaces the environment placeholders of the exchange and queue
    names, and changes nothing else. It sets no default values and validates nothing. Every property must be in
    the configuration, and the topology is validated by the launcher and by the Input Handler.
    """

    _LOG_PREFIX: str = "PCEndpointBuilder |"

    def __init__(
        self,
        base_topology_template: dict,
        environment_name: str,
        environment_specs: dict,
        source_mapping_rules: dict,
        logger: LoggingUtils,
        timezone: str
    ) -> None:
        """Initializes the PulseCharge AMQP topology builder.

        Expected `base_topology_template` Data Structure:
        -------------------------------------------------
        {
            "exchanges": [
                {
                    "exchange_name": "building_{environment}",      # '{environment}' is replaced by the environment name
                    "exchange_properties": {"exchange_type": "fanout", "durable": True, "passive": True},
                    "queues": [
                        {
                            "queue_name": "percepta_local_pulsecharge_{environment}",
                            "routing_key": "",
                            "consume_properties": {"no_ack": False, "requeue": False},
                            "queue_properties": {"durable": True, "exclusive": False}
                        }
                    ]
                }
            ]
        }

        Expected `source_mapping_rules` Data Structure:
        ----------------------------------------------
        {
            "ev01_soc": {"match_key": "user-123"},
            "ev01_flexibility": {"match_key": "user-123"}
        }

        Args:
            base_topology_template (dict): Base topology blueprint dictionary containing the 'exchanges' list.
            environment_name (str): Unique identifier of the target environment (e.g., "R-H-03").
            environment_specs (dict): Specification dictionary describing environment entities.
            source_mapping_rules (dict): For each parameter id, its keys, or None if the parameter is derived.
            logger (LoggingUtils): Structured telemetry logging interface.
            timezone (str): Target execution timezone (e.g., "UTC").

        """
        super().__init__(
            base_topology_template=base_topology_template,
            environment_name=environment_name,
            environment_specs=environment_specs,
            source_mapping_rules=source_mapping_rules,
            logger=logger,
            timezone=timezone
        )

        self._static_topology = self._precalculate_topology()

        self._logger.debug(
            f"{self._LOG_PREFIX} Topology constructed for environment '{self._environment_name}' "
            f"with {len(self._static_topology['exchanges'])} AMQP exchange binding(s)."
        )

    def _precalculate_topology(self) -> dict:
        """Builds the RabbitMQ topology definition of the environment.

        Copies the exchanges of the template and replaces the environment placeholders of the exchange and
        queue names. Nothing else is added or changed.

        Expected Output Structure (environment "R-H-03"):
        -------------------------------------------------
        {
            "exchanges": [
                {
                    "exchange_name": "building_R-H-03",
                    "exchange_properties": {"exchange_type": "fanout", "durable": True, "passive": True},
                    "queues": [
                        {
                            "queue_name": "percepta_local_pulsecharge_R-H-03",
                            "routing_key": "",
                            "consume_properties": {"no_ack": False, "requeue": False},
                            "queue_properties": {"durable": True, "exclusive": False}
                        }
                    ]
                }
            ]
        }
        """
        # Deep copy, so that the changes below never change the template
        exchanges = copy.deepcopy(self._base_template["exchanges"])

        for exchange in exchanges:
            # Example: "building_{environment}" -> "building_R-H-03"
            exchange["exchange_name"] = self._replace_environment(exchange["exchange_name"])

            for queue in exchange["queues"]:
                # Example: "percepta_local_pulsecharge_{environment}" -> "percepta_local_pulsecharge_R-H-03"
                queue["queue_name"] = self._replace_environment(queue["queue_name"])

        return {"exchanges": exchanges}

    def _replace_environment(self, template: str) -> str:
        """Replaces the environment placeholders of a name template.

        Both '{environment}' and '{environment_name}' are replaced by the environment
        name, with spaces turned into underscores.

        Examples:
        ---------
        "building_{environment}" with environment "R-H-03" -> "building_R-H-03"
        "building_{environment}" with environment "R H 03" -> "building_R_H_03"

        Args:
            template (str): Name with placeholders.

        Returns:
            str: Name with the environment filled in.
        """
        environment = self._environment_name.replace(" ", "_")
        return template.replace("{environment_name}", environment).replace("{environment}", environment)

    async def build_endpoints(self) -> dict:
        """Returns the pre-calculated static AMQP topology definition.

        Returns:
            dict: Static AMQP topology structure ready for the AsyncRabbitMQInputHandler.
        """
        return self._static_topology