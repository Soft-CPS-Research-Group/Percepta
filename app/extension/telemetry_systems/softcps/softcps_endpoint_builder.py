# --- Standard Library Imports ---
import copy
from typing import Any, Dict, List

# --- Internal Framework Imports ---
from app.core.endpoint_builders.endpoint_builder_base import EndpointBuilderBase
from app.core.utils.logger import LoggingUtils


class SoftCPSEndpointBuilder(EndpointBuilderBase):
    """SoftCPS-specific endpoint resolution strategy for HTTP Polling.

    Pre-computes one endpoint per SoftCPS asset (the 'fetch_key' of the rules) for each endpoint
    of the topology, which is a dictionary keyed by endpoint id, grouped by entity ID during initialization. The endpoints do not change between polling cycles, as
    SoftCPS returns the observations of the last `window_seconds` seconds, so there is nothing
    to interpolate at runtime.

    The builder only creates the endpoints. It sets no default values and validates nothing: the topology,
    the polling interval and the timezone must be given, and the launcher checks them.
    """

    _LOG_PREFIX: str = "SoftCPS Endpoint Builder |"

    def __init__(
        self,
        base_topology_template: dict,
        environment_name: str,
        environment_specs: dict,
        source_mapping_rules: dict,
        logger: LoggingUtils,
        time_interval_seconds: int,
        timezone: str
    ) -> None:
        """Initializes the SoftCPS endpoint builder.

        Expected `environment_specs` Data Structure:
        -------------------------------------------
        {
            "group": "living lab",
            "entities": {
                "B01": {
                    "entity_type_id": "battery",
                    "parameters": {
                        "b01_energy_in": {"parameter_type_id": "energy_in", "measurement_unit": "kWh"},
                        "b01_soc": {"parameter_type_id": "state_of_charge", "measurement_unit": "%"}
                    }
                }
            }
        }

        Expected `source_mapping_rules` Data Structure:
        ----------------------------------------------
        {
            "b01_energy_in": {"fetch_key": "battery_id_1", "match_key": "energy_charged_kwh_window"},
            "b01_soc": {"fetch_key": "battery_id_1", "match_key": "soc_pct"}
        }

        The 'fetch_key' is the SoftCPS asset id requested in the endpoint. Each asset is requested
        only once, even if several parameters use it.

        Expected `base_topology_template` Data Structure:
        -------------------------------------------------
        {
            # Endpoint id -> endpoint. Each one is requested for every asset.
            "endpoints": {
                "observations": {
                    "path": "/assets/{asset_id}/observations?window_seconds={time_interval}",
                    "method": "GET",
                    "timeout_ms": 4000
                }
            }
        }

        Args:
            base_topology_template (dict): Base topology blueprint dictionary containing the 'endpoints' dictionary, keyed by endpoint id.
            environment_name (str): Unique identifier of the target environment.
            environment_specs (dict): Specification dictionary describing environment entities.
            source_mapping_rules (dict): For each parameter id, its keys, or None if the parameter is derived.
            logger (LoggingUtils): Structured telemetry logging interface.
            time_interval_seconds (int): Polling frequency interval in seconds, which is also the
                window of observations requested.
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

        self._endpoints = self._base_template["endpoints"]

        # SoftCPS expects the window in whole seconds
        self._time_interval = int(time_interval_seconds)

        self._static_grouped_endpoints = self._precalculate_static_endpoints()

    def _precalculate_static_endpoints(self) -> Dict[str, List[Dict[str, Any]]]:
        """Resolves the endpoints of each entity, one for each asset it uses.

        Expected Output Structure:
        --------------------------
        {
            "B01": [
                {
                    "path": "/assets/battery_id_1/observations?window_seconds=15",
                    "method": "GET",
                    "timeout_ms": 4000
                }
            ]
        }

        Returns:
            Dict[str, List[Dict[str, Any]]]: The resolved endpoints, by entity id.
        """
        grouped_endpoints: Dict[str, List[Dict[str, Any]]] = {}
        total_endpoints = 0

        # Entity that requested each asset, so none is requested twice
        entity_by_asset: Dict[str, str] = {}

        for entity_id, entity_data in self._entities.items():
            entity_endpoints: List[Dict[str, Any]] = []

            for param_id in entity_data["parameters"]:
                rule = self._source_mapping_rules[param_id]

                # A derived parameter is not requested, as the Adapter builds it from the others
                if rule is None:
                    continue

                # str() accepts numeric ids, and strip() removes spaces at the ends, so the id is requested as the Adapter compares it
                asset_id = str(rule["fetch_key"]).strip()

                # An asset is requested only once. It is normal for several parameters of the same entity to
                # share it (a battery has energy and state of charge); another entity using it is a mistake
                known_entity = entity_by_asset.get(asset_id)
                if known_entity is not None:
                    if known_entity != entity_id:
                        self._logger.warning(
                            f"{self._LOG_PREFIX} Asset '{asset_id}' of parameter '{param_id}' on entity "
                            f"'{entity_id}' is already requested for entity '{known_entity}'. Skipping."
                        )
                    continue
                entity_by_asset[asset_id] = entity_id

                for template in self._endpoints.values():
                    # Deep copy, so resolving the placeholders never changes the shared template
                    endpoint = copy.deepcopy(template)
                    endpoint["path"] = (
                        endpoint["path"]
                        .replace("{asset_id}", asset_id)
                        .replace("{time_interval}", str(self._time_interval))
                    )

                    entity_endpoints.append(endpoint)
                    total_endpoints += 1

            if entity_endpoints:
                grouped_endpoints[entity_id] = entity_endpoints

        self._logger.debug(
            f"{self._LOG_PREFIX} Pre-calculated {total_endpoints} static endpoints across {len(grouped_endpoints)} entities."
        )

        return grouped_endpoints

    async def build_endpoints(self) -> Dict[str, List[Dict[str, Any]]]:
        """Returns the endpoints to poll in this cycle, grouped by entity.

        They are the same in every cycle. A copy is returned, so whoever receives them cannot
        change the ones kept here.

        Expected Output Structure:
        --------------------------
        {
            "B01": [
                {
                    "path": "/assets/battery_id_1/observations?window_seconds=15",
                    "method": "GET",
                    "timeout_ms": 4000
                }
            ]
        }

        Returns:
            Dict[str, List[Dict[str, Any]]]: The endpoints, by entity id.
        """
        return copy.deepcopy(self._static_grouped_endpoints)