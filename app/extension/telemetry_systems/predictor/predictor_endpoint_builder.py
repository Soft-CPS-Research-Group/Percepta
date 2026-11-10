# --- Standard Library Imports ---
import copy

# --- Internal Framework Imports ---
from app.core.endpoint_builders.endpoint_builder_base import EndpointBuilderBase
from app.core.utils.logger import LoggingUtils


class PredictorEndpointBuilder(EndpointBuilderBase):
    """Predictor AMQP/RabbitMQ topology resolution strategy.

    The Predictor publishes the forecasts of an environment on one direct exchange, named after the
    environment. The name of that exchange comes from the exchange template of the topology
    (e.g. "pred.{environment}"), where '{environment}' is replaced by the environment name. As the exchange is
    direct, the queue is bound with a routing key that also depends on the environment (e.g. "q.pred.{environment}").
    This builder produces the static AMQP topology used by the AsyncRabbitMQInputHandler.

    The builder only creates the endpoints: it replaces the environment placeholders of the exchange name,
    the queue names and the routing keys, and changes nothing else. It sets no default values and validates nothing. Every property must be in the
    configuration, and the topology is validated by the Input Handler.
    """

    _LOG_PREFIX: str = "Predictor Telemetry Endpoint Builder |"

    def __init__(
        self,
        base_topology_template: dict,
        environment_name: str,
        environment_specs: dict,
        source_mapping_rules: dict,
        logger: LoggingUtils,
        timezone: str
    ) -> None:
        """Initializes the Predictor AMQP topology builder.

        Expected `base_topology_template` Data Structure:
        -------------------------------------------------
        {
            "exchanges": [
                {
                    "exchange_name": "pred.{environment}",          # '{environment}' is replaced by the environment name
                    "exchange_properties": {"exchange_type": "direct", "durable": True},
                    "queues": [
                        {
                            "queue_name": "percepta_local_forecast_{environment}",
                            "routing_key": "q.pred.{environment}",
                            "consume_properties": {"no_ack": False, "acknowledgement_properties": {"requeue": False}},
                            "queue_properties": {"durable": True, "exclusive": False}
                        }
                    ]
                }
            ]
        }

        Args:
            base_topology_template (dict): Base topology blueprint dictionary containing the 'exchanges' list.
            environment_name (str): Unique identifier of the target environment (e.g., "R-H-03").
            environment_specs (dict): Specification dictionary describing environment entities (not used here).
            source_mapping_rules (dict): Source mapping rules of the system (not used here: the Predictor
                channel depends on the environment, not on the parameters).
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

        Copies the exchanges of the template and replaces the environment placeholders of the exchange name,
        the queue names and the routing keys. Nothing else is added or changed.

        Expected Output Structure (environment "R-H-03"):
        -------------------------------------------------
        {
            "exchanges": [
                {
                    "exchange_name": "pred.R-H-03",
                    "exchange_properties": {"exchange_type": "direct", "durable": True},
                    "queues": [
                        {
                            "queue_name": "percepta_local_forecast_R-H-03",
                            "routing_key": "q.pred.R-H-03",
                            "consume_properties": {"no_ack": False, "acknowledgement_properties": {"requeue": False}},
                            "queue_properties": {"durable": True, "exclusive": False}
                        }
                    ]
                }
            ]
        }
        """
        # Deep copy, so that the changes below never change the template
        exchanges = copy.deepcopy(self._base_template.get("exchanges", []))

        for exchange in exchanges:
            # Example: "pred.{environment}" -> "pred.R-H-03"
            exchange["exchange_name"] = self._replace_environment(exchange["exchange_name"])

            for queue in exchange.get("queues", []):
                # Example: "percepta_local_forecast_{environment}" -> "percepta_local_forecast_R-H-03"
                queue["queue_name"] = self._replace_environment(queue["queue_name"])

                # Example: "q.pred.{environment}" -> "q.pred.R-H-03" (the exchange is direct, so the key is used)
                queue["routing_key"] = self._replace_environment(queue["routing_key"])

        return {"exchanges": exchanges}

    def _replace_environment(self, template: str) -> str:
        """Replaces the environment placeholders of a name template.

        Both '{environment}' and '{environment_name}' are replaced by the environment
        name, with spaces turned into underscores.

        Examples:
        ---------
        "pred.{environment}" with environment "R-H-03" -> "pred.R-H-03"
        "percepta_local_forecast_{environment}" with environment "R H 03" -> "percepta_local_forecast_R_H_03"
        "q.pred.{environment}" with environment "R-H-03" -> "q.pred.R-H-03"

        Args:
            template (str): Name or routing key with placeholders.

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