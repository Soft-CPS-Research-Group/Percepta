# --- Standard Library Imports ---
from typing import Any, Dict, List

# --- Internal Framework Imports ---
from app.core.endpoint_builders.endpoint_builder_base import EndpointBuilderBase
from app.core.utils.logger import LoggingUtils


class ICEndpointBuilder(EndpointBuilderBase):
    """i-charging AMQP/RabbitMQ topology and routing resolution strategy.

    Resolves exchange names using external mapping rules per environment, sets up
    fanout topology structures with dedicated consumer queues, and pre-calculates
    the static AMQP topology used by the RabbitMQ Input Handler.
    """

    _LOG_PREFIX: str = "ICEndpointBuilder |"

    def __init__(
        self,
        base_topology_template: dict,
        environment_name: str,
        environment_specs: dict,
        source_mapping_rules: dict,
        logger: LoggingUtils,
        timezone: str = "UTC"
    ) -> None:
        """Initializes the i-charging AMQP topology builder with strict dependency & schema validations.

        Expected `environment_specs` Data Structure:
        -------------------------------------------
        {
            "group": "porto_chargers",              # (str, optional) Environmental cluster/group tag
            "entities": {                           # (dict) Map of entity unique identifiers
                "EV_FAST_01": {                     # (str) Unique entity identifier
                    "label": "ev_charger",          # (str) Domain classification label
                    "parameters": {
                        "power": {
                            "id": "active_power",
                            "label": "active_power",
                            "type": "EV Charging Power",
                            "measurementUnit": "kW"
                        }
                    }
                }
            }
        }

        Expected `source_mapping_rules` Data Structure:
        ----------------------------------------------
        {
            # Maps internal environment name to remote exchange/environment identifier
            "R-H-01": "community_porto_telemetry"
        }

        Expected `base_topology_template` Data Structure:
        -------------------------------------------------
        {
            "exchanges": [
                {
                    "exchange_name": "{exchange_name}",
                    "exchange_properties": {
                        "exchange_type": "fanout",
                        "durable": true
                    },
                    "queues": [
                        {
                            "queue_name": "{queue_name}",
                            "routing_key": "",
                            "consume_properties": {
                                "no_ack": false,
                                "requeue": false
                            },
                            "queue_properties": {
                                "durable": true
                            }
                        }
                    ]
                }
            ]
        }

        Args:
            base_topology_template (dict): Base AMQP blueprint template dictionary.
            environment_name (str): Unique identifier of the target environment.
            environment_specs (dict): Specification dictionary describing environment entities.
            source_mapping_rules (dict): Dictionary mapping entity identifiers to remote exchange names.
            logger (LoggingUtils): Structured telemetry logging interface.
            timezone (str, optional): Target execution timezone. Defaults to "UTC".
        """
        super().__init__(
            base_topology_template=base_topology_template,
            environment_name=environment_name,
            environment_specs=environment_specs,
            source_mapping_rules=source_mapping_rules,
            logger=logger,
            timezone=timezone
        )

        if not isinstance(self._base_template, dict):
            error_msg = (
                f"{self._LOG_PREFIX} Initialization error: 'base_topology_template' must be a dictionary, "
                f"got {type(self._base_template).__name__}."
            )
            self._logger.error(error_msg)
            raise TypeError(error_msg)

        self._exchanges = self._base_template.get("exchanges")
        if not isinstance(self._exchanges, list) or not self._exchanges:
            error_msg = (
                f"{self._LOG_PREFIX} Initialization error: 'base_topology_template' must contain a non-empty "
                f"'exchanges' list for environment '{self._environment_name}'."
            )
            self._logger.error(error_msg)
            raise ValueError(error_msg)

        # Resolve remote AMQP Exchange Name from source mapping rules or fallback to environment_name
        self._resolved_exchange_name = self._source_mapping_rules.get(self._environment_name, self._environment_name)
        if not isinstance(self._resolved_exchange_name, str) or not self._resolved_exchange_name.strip():
            error_msg = (
                f"{self._LOG_PREFIX} Resolution error: Resolved exchange name for environment "
                f"'{self._environment_name}' must be a non-empty string."
            )
            self._logger.error(error_msg)
            raise ValueError(error_msg)

        # Build dedicated consumer queue name
        self._resolved_queue_name = f"percepta_{self._resolved_exchange_name}_consumer"

        # Pre-calculate static AMQP topology
        self._static_topology = self._precalculate_topology()


    def _precalculate_topology(self) -> dict:
        """Pre-computes the static AMQP topology structure.

        Expected Output Structure:
        --------------------------
        {
            "exchanges": [
                {
                    "exchange_name": "community_porto_telemetry",
                    "exchange_properties": {
                        "exchange_type": "fanout",
                        "durable": true
                    },
                    "queues": [
                        {
                            "queue_name": "percepta_community_porto_telemetry_consumer",
                            "routing_key": "",
                            "consume_properties": {
                                "no_ack": false,
                                "requeue": false
                            },
                            "queue_properties": {
                                "durable": true
                            }
                        }
                    ],
                    "metadata": {
                        "environment": "R-H-01",
                        "entities": ["EV_FAST_01"]
                    }
                }
            ]
        }

        Returns:
            dict: Complete RabbitMQ topology configuration dictionary.
        """
        resolved_exchanges: List[Dict[str, Any]] = []

        for ex in self._exchanges:
            ex_copy = ex.copy()
            ex_copy["exchange_name"] = str(ex_copy.get("exchange_name", "")).replace("{exchange_name}", self._resolved_exchange_name)

            resolved_queues: List[Dict[str, Any]] = []
            for q in ex_copy.get("queues", []):
                q_copy = q.copy()
                q_copy["queue_name"] = str(q_copy.get("queue_name", "")).replace("{queue_name}", self._resolved_queue_name)
                resolved_queues.append(q_copy)

            ex_copy["queues"] = resolved_queues
            ex_copy["metadata"] = {
                "environment": self._environment_name,
                "entities": list(self._entities.keys())
            }
            resolved_exchanges.append(ex_copy)

        return {"exchanges": resolved_exchanges}

    async def build_endpoints(self) -> dict:
        """Returns the pre-calculated static AMQP topology dictionary.

        Since RabbitMQ bindings are persistent and event-driven (no polling time window),
        this method returns the static structure without temporal variations.

        Returns:
            dict: Static AMQP topology structure ready for the AsyncRabbitMQInputHandler.
        """
        return self._static_topology