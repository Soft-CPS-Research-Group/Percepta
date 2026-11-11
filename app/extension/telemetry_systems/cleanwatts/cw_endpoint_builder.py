# --- Standard Library Imports ---
import datetime
from typing import Any, Dict, List, Set, Type

# --- Internal Framework Imports ---
from app.core.endpoint_builders.endpoint_builder_base import EndpointBuilderBase
from app.core.utils.logger import LoggingUtils
from app.extension.telemetry_systems.cleanwatts.cw_session import CWSession


class CWEndpointBuilder(EndpointBuilderBase):
    """Cleanwatts-specific dynamic endpoint resolution strategy for HTTP Polling.

    Pre-computes one static endpoint per Cleanwatts tag ({param_id}), grouped by
    entity ID during initialization, and dynamically interpolates sliding time
    window query parameters ({from}, {to}) and authorization session tokens ({token})
    during runtime polling cycles.

    The builder only creates the endpoints. It sets no default values and validates nothing: the topology,
    the window size ('lookback_seconds') and the timezone must be given, and the launcher checks them.
    """

    _LOG_PREFIX: str = "CWEndpointBuilder |"

    def __init__(
        self,
        base_topology_template: dict,
        environment_name: str,
        environment_specs: dict,
        source_mapping_rules: dict,
        logger: LoggingUtils,
        lookback_seconds: int,
        timezone: str,
        cw_session: CWSession
    ) -> None:
        """Initializes the Cleanwatts endpoint builder.

        Expected `environment_specs` Data Structure:
        -------------------------------------------
        {
            "group": "living lab",
            "entities": {
                "PV01": {
                    "entity_type_id": "pv_panel",
                    "parameters": {
                        "33711": {
                            "parameter_type_id": "energy",
                            "measurement_unit": "kWh",
                            "temporal_behavior": {
                                "cumulative": true,
                                "periodicity": {"value": 5, "unit": "min"},
                                "aggregation_operation": "sum",
                                "fill_operation": "linear"
                            },
                            "necessary": true
                        }
                    }
                },
                "B01": {
                    "entity_type_id": "battery",
                    "parameters": {
                        "35927": {
                            "parameter_type_id": "energy_in",
                            "measurement_unit": "kWh",
                            "necessary": true
                        },
                        "33690": {
                            "parameter_type_id": "state_of_charge",
                            "measurement_unit": "%",
                            "necessary": true
                        }
                    }
                }
            }
        }

        Expected `source_mapping_rules` Data Structure:
        ----------------------------------------------
        {
            "33711": {"fetch_key": "33711", "match_key": "33711"},
            "35927": {"fetch_key": "35927", "match_key": "35927"},
            "33690": {"fetch_key": "33690", "match_key": "33690"}
        }

        The 'connected_vehicle' parameter is in the schema but not in the source mapping: it is derived from another
        parameter (see 'derived_from'), so the Adapter builds it and there is nothing to request.

        The 'fetch_key' is the Cleanwatts tag requested in the endpoint. Each tag is requested
        only once, even if several parameters use it.

        Expected `base_topology_template` Data Structure:
        -------------------------------------------------
        {
            "endpoints": [
                {
                    "path": "/data/request/Instant?instantType=avg&tags={param_id}&from={from}&to={to}",
                    "method": "GET",
                    "timeout_ms": 5000,
                    "headers": {"Authorization": "CW {token}"}
                }
            ]
        }

        Args:
            base_topology_template (dict): Base topology blueprint dictionary containing the 'endpoints' list.
            environment_name (str): Unique identifier of the target environment.
            environment_specs (dict): Specification dictionary describing environment entities.
            source_mapping_rules (dict): For each parameter id that Cleanwatts provides, its keys.
            logger (LoggingUtils): Structured telemetry logging interface.
            lookback_seconds (int): How far back from "now" every request asks for data, in seconds.
                It must be longer than the longest delay Cleanwatts takes to publish a reading, because a
                reading that arrives late is only picked up while it is still inside this window.
                Example: 1800 -> at 12:00:00 the request is from=11:30:00 to=12:00:00.
            timezone (str): Target execution timezone (e.g., "UTC").
            cw_session (CWSession): Session holding the active token.
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

        self._lookback = datetime.timedelta(seconds=lookback_seconds)
        self._cw_session = cw_session

        self._static_grouped_endpoints = self._precalculate_static_endpoints()

    def _precalculate_static_endpoints(
        self
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Expected Output Structure:
        --------------------------
        {
            "PV01": [
                {
                    "path": "/data/request/Instant?instantType=avg&tags=33711&from={from}&to={to}",
                    "method": "GET",
                    "timeout_ms": 5000,
                    "headers": {"Authorization": "CW {token}"}
                }
            ],
            "B01": [
                {
                    "path": "/data/request/Instant?instantType=avg&tags=35927&from={from}&to={to}",
                    "method": "GET",
                    "timeout_ms": 5000,
                    "headers": {"Authorization": "CW {token}"}
                }
            ]
        }
        """
        grouped_endpoints: Dict[str, List[Dict[str, Any]]] = {}
        total_endpoints = 0

        # Tags already requested, so none is requested twice
        requested_tags: Set[str] = set()

        for entity_id, entity_data in self._entities.items():
            entity_endpoints: List[Dict[str, Any]] = []
            parameters = entity_data["parameters"]

            for param_id in parameters:
                # A parameter that is not in the source mapping (a derived one) is not requested, as the Adapter builds
                # it from the others
                rule = self._source_mapping_rules.get(param_id)
                if rule is None:
                    continue

                tag = str(rule["fetch_key"])
                if tag in requested_tags:
                    self._logger.warning(
                        f"{self._LOG_PREFIX} Tag '{tag}' of parameter '{param_id}' on entity '{entity_id}' "
                        f"is already requested. Skipping."
                    )
                    continue
                requested_tags.add(tag)

                for template in self._endpoints:
                    ep_copy = template.copy()

                    # Resolve static hardware placeholder {param_id} in path
                    if "path" in ep_copy and isinstance(ep_copy["path"], str):
                        ep_copy["path"] = ep_copy["path"].replace("{param_id}", tag)

                    # Deep copy headers template to avoid cross-endpoint state pollution
                    if "headers" in ep_copy and isinstance(ep_copy["headers"], dict):
                        ep_copy["headers"] = ep_copy["headers"].copy()

                    entity_endpoints.append(ep_copy)
                    total_endpoints += 1

            if entity_endpoints:
                grouped_endpoints[entity_id] = entity_endpoints

        self._logger.debug(
            f"{self._LOG_PREFIX} Pre-calculated {total_endpoints} static endpoints across {len(grouped_endpoints)} entities."
        )

        return grouped_endpoints

    async def build_endpoints(self) -> Dict[str, List[Dict[str, Any]]]:
        """Interpolates dynamic sliding window timestamps and active tokens into grouped endpoints.

        Internal State / Source Data Used:
        -----------------------------------
        - `self._static_grouped_endpoints`: Pre-calculated templates organized by entity ID.
        - `self._lookback`: Size of the window. It always ends at the current time (e.g., 1800 s at 14:30:00 -> 14:00:00 to 14:30:00).
        - `self._cw_session.get_token()`: Active JWT session token string.

        Expected Output Structure:
        --------------------------
        {
            "PV01": [
                {
                    "path": "/data/request/Instant?instantType=avg&tags=33711&from=2026-08-19T14:00:00&to=2026-08-19T14:30:00",
                    "method": "GET",
                    "timeout_ms": 5000,
                    "headers": {
                        "Authorization": "CW eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
                    }
                }
            ]
        }
        """
        # The window always ends now, so it follows the clock even if some polling cycles were late or missed.
        # Cleanwatts publishes readings with a long delay, so asking only for the latest seconds would come back empty.
        # Asking for the same long window every time picks up the late readings as soon as they appear.
        now = datetime.datetime.now(self._tz).replace(microsecond=0)
        from_str = (now - self._lookback).strftime("%Y-%m-%dT%H:%M:%S")
        to_str = now.strftime("%Y-%m-%dT%H:%M:%S")

        self._logger.debug(f"{self._LOG_PREFIX} Building dynamic endpoints for interval [{from_str} -> {to_str}]")

        # Resolve token and common authorization header once per polling cycle
        token = self._cw_session.get_token()
        resolved_headers = {"Authorization": f"CW {token}"}

        # Construct resolved endpoints maintaining entity groups
        resolved_grouped_endpoints = {
            entity_id: [
                {
                    **ep,
                    "path": ep["path"].replace("{from}", from_str).replace("{to}", to_str),
                    "headers": resolved_headers
                }
                for ep in ep_list
            ]
            for entity_id, ep_list in self._static_grouped_endpoints.items()
        }

        return resolved_grouped_endpoints