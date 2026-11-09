# --- Standard Library Imports ---
import copy
import random
from typing import Any, Dict, List, Set

# --- Internal Framework Imports ---
from app.core.endpoint_builders.endpoint_builder_base import EndpointBuilderBase
from app.core.utils.logger import LoggingUtils


class CW2EndpointBuilder(EndpointBuilderBase):
    """Cleanwatts v2 (Shelly) AMQP/RabbitMQ multi-exchange topology resolution strategy.

    Resolves target Shelly fanout exchanges from the 'fetch_key' of the source mapping
    rules (each exchange once, even if several parameters use it), replaces dynamic
    placeholders in queue templates (e.g., '{random_hash}'), and produces the static
    AMQP topology used by the AsyncRabbitMQInputHandler.

    The builder only creates the endpoints. It sets no default values and validates nothing: every property
    (for example 'passive', 'durable', 'exclusive' and 'no_ack') must be in the configuration, and the topology
    is validated by the launcher and by the Input Handler.
    """

    _LOG_PREFIX: str = "CW2EndpointBuilder |"

    def __init__(
        self,
        base_topology_template: dict,
        environment_name: str,
        environment_specs: dict,
        source_mapping_rules: dict,
        logger: LoggingUtils,
        timezone: str
    ) -> None:
        """Initializes the Cleanwatts v2 AMQP topology builder.

        Expected `base_topology_template` Data Structure:
        -------------------------------------------------
        {
            "exchanges": [
                {
                    "exchange_name": "{exchange_name}",             # Replaced by each resolved exchange
                                                                    # ('passive': the exchange belongs to Cleanwatts, so it is only checked, never created)
                    "exchange_properties": {"exchange_type": "fanout", "durable": False, "passive": True},
                    "queues": [
                        {
                            "queue_name": "isep_consumer_{random_hash}",    # Hash is generated for each queue
                            "routing_key": "",
                            "consume_properties": {"no_ack": True, "requeue": False},
                            "queue_properties": {"durable": True, "exclusive": True}
                        }
                    ]
                }
            ]
        }

        Only the first exchange is used, as a prototype for every exchange that is resolved.

        Expected `environment_specs` Data Structure:
        -------------------------------------------
        {
            "group": "living lab",
            "entities": {
                "GR01": {
                    "entity_type_id": "grid_meter",
                    "parameters": {
                        "energy_in_l1": {
                            "parameter_type_id": "energy_in_l1",
                            "measurement_unit": "kWh",
                            "temporal_behavior": {
                                "cumulative": True,
                                "periodicity": {"value": 1, "unit": "min"},
                                "aggregation_operation": "sum",
                                "fill_operation": "linear"
                            },
                            "necessary": True
                        },
                        "energy_in_total": {
                            "parameter_type_id": "energy_in_total",
                            "measurement_unit": "kWh",
                            "temporal_behavior": {
                                "cumulative": True,
                                "periodicity": {"value": 1, "unit": "min"},
                                "aggregation_operation": "sum",
                                "fill_operation": "linear"
                            },
                            "necessary": True
                        }
                    }
                }
            }
        }

        Expected `source_mapping_rules` Data Structure:
        ----------------------------------------------
        {
            "energy_in_l1": {
                "fetch_key": "Shelly-shellypro3em-0cb815fd4324",    # AMQP exchange to consume from
                "match_key": "a_total_act_energy",                  # Key of the value in the Shelly message
                "topic": "/status/emdata:0",                        # Topic the Shelly message is published on
                "shelly_accumulation": "lifetime"                   # "lifetime" (running total) or "interval"
            },
            "energy_in_total": {
                "fetch_key": "Shelly-shellypro3em-0cb815fd4324",
                "match_key": "total_act",
                "topic": "/status/emdata:0",
                "shelly_accumulation": "lifetime"
            }
        }

        Both parameters above share an exchange, so a single exchange is consumed.
        A derived parameter, which has no exchange, is listed with the value None.

        Args:
            base_topology_template (dict): Base topology blueprint dictionary containing the 'exchanges' list.
            environment_name (str): Unique identifier of the target environment (e.g., "R-H-01").
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

        self._exchanges = self._base_template["exchanges"]

        # Pre-compute static multi-exchange topology for all entities in this environment
        self._static_topology = self._precalculate_topology()

        self._logger.debug(
            f"{self._LOG_PREFIX} Topology successfully constructed for environment '{self._environment_name}' "
            f"with {len(self._static_topology.get('exchanges', []))} AMQP exchange binding(s)."
        )

    def _precalculate_topology(self) -> dict:
        """Builds the complete multi-exchange RabbitMQ topology definition.

        Resolves the remote exchanges of the configured entities, each one once, and
        replaces the placeholders of the exchange and queue names. Nothing else is added or changed.
        """
        resolved_exchanges: List[Dict[str, Any]] = []
        exchange_prototype = self._exchanges[0]

        for target_exchange in self._resolve_exchanges():
            # 1. Deep copy of the exchange prototype (so the template is never changed), with its name filled in.
            # Example: "{exchange_name}" -> "Shelly-shellypro3em-0cb815fd4324"
            ex_copy = copy.deepcopy(exchange_prototype)
            ex_copy["exchange_name"] = ex_copy["exchange_name"].replace("{exchange_name}", str(target_exchange))

            # 2. Each queue of the copy gets its own random hash. Example: "isep_consumer_{random_hash}" -> "isep_consumer_048213"
            for queue in ex_copy["queues"]:
                random_hash = f"{random.randint(0, 999999):06d}"
                queue["queue_name"] = queue["queue_name"].replace("{random_hash}", random_hash)

            resolved_exchanges.append(ex_copy)

        return {"exchanges": resolved_exchanges}

    def _resolve_exchanges(self) -> List[str]:
        """Finds the remote exchange to consume for each entity, from the 'fetch_key' of its parameters.

        An exchange used by several parameters (or entities) is returned once.
        Derived parameters (rule None) have no exchange.

        Expected Output Structure:
        --------------------------
        ["Shelly-shellypro3em-0cb815fd4324"]

        Returns:
            List[str]: Exchange names, without repetitions.
        """
        resolved: List[str] = []
        seen_exchanges: Set[str] = set()

        for entity_id, entity_data in self._entities.items():
            entity_exchanges = [
                str(self._source_mapping_rules[param_id]["fetch_key"])
                for param_id in entity_data.get("parameters", {})
                if self._source_mapping_rules.get(param_id) is not None
            ]

            if not entity_exchanges:
                self._logger.warning(
                    f"{self._LOG_PREFIX} Entity '{entity_id}' has no matching remote exchange "
                    f"in source_mapping_rules. Skipping."
                )
                continue

            for exchange in entity_exchanges:
                if exchange not in seen_exchanges:
                    seen_exchanges.add(exchange)
                    resolved.append(exchange)

        return resolved

    async def build_endpoints(self) -> dict:
        """Returns the pre-calculated static AMQP topology definition.

        Returns:
            dict: Static AMQP topology structure ready for the AsyncRabbitMQInputHandler.
        """
        return self._static_topology