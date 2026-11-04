# --- Standard Library Imports ---
from typing import Any, Dict, List

# --- Internal Framework Imports ---
from app.core.adapters.target_system_adapter_base import MappedDecision, TargetSystemAdapterBase
from app.core.output_handlers.async_http_output_handler import HttpPublishMode
from app.core.utils.logger import LoggingUtils
from app.extension.utils.labels import Label


class SoftCPSTargetAdapter(TargetSystemAdapterBase):
    """Adapter of the SoftCPS Target System.

    SoftCPS receives one request for each asset, with the power in kW. The decision of a battery is sent to the asset
    that is the 'destination' of its actuator (the 'actuator_mapping' of the environment gives it), so the address of the
    request depends on the actuator: the placeholder '{asset_id}' of the path of the endpoint is replaced by the
    destination.

    Only batteries are supported.
    """

    _LOG_PREFIX: str = "SoftCPS Target Adapter |"

    # The placeholder of the path of the endpoint that is replaced by the asset
    _ASSET_PLACEHOLDER: str = "{asset_id}"

    def __init__(
        self,
        target_system_id: str,
        environment_name: str,
        environment: Dict[str, Any],
        logger: LoggingUtils,
        endpoint: Dict[str, Any]
    ) -> None:
        """Initializes the Adapter.

        Expected `endpoint` Data Structure (the actuation endpoint of the topology):
        ----------------------------------------------------------------------------
        {
            "path": "/assets/{asset_id}/actions",
            "method": "POST",
            "timeout_ms": 5000,
            "rpc_properties": {"enabled": True}
        }

        Args:
            target_system_id (str): Id of the Target System.
            environment_name (str): Name of the environment.
            environment (Dict[str, Any]): The environment, as returned by the Schema Storage.
            logger (LoggingUtils): Logger of the environment.
            endpoint (Dict[str, Any]): The actuation endpoint, as detailed above.

        Raises:
            ValueError: If a dependency is missing or invalid.
        """
        super().__init__(target_system_id, environment_name, environment, logger)

        # Guard Clause 1: Checks that the endpoint has a path with the placeholder of the asset.
        path = endpoint.get("path") if isinstance(endpoint, dict) else None
        if not isinstance(path, str) or self._ASSET_PLACEHOLDER not in path:
            raise ValueError(
                f"{self._LOG_PREFIX} Initialization error: the 'path' of the endpoint must have the placeholder "
                f"'{self._ASSET_PLACEHOLDER}'."
            )

        self._endpoint = endpoint

    def adapt_decisions(self, decisions: List[Dict[str, Any]]) -> List[MappedDecision]:
        """Builds one request for each actuator of the decisions, addressed to the asset of the actuator.

        An actuator that cannot be mapped (its entity is not a battery, or its value or destination is invalid) is left
        out with a warning, so the other actuators are still sent.

        Expected Input Structure:
        -------------------------
        decisions = [
            {
                "id": "B01",
                "entity_type_id": "battery",
                "actuators": {
                    "act_power_1": {
                        "actuator_type_id": "power", "value": -2.5, "destination": "ASSET_1"
                    }
                },
                "timestamp": "2026-08-19 11:15:00 +0000"
            }
        ]

        Expected Output Structure:
        --------------------------
        [
            MappedDecision(
                message={"power_kw": -2.5},
                send_arguments={
                    "dynamic_endpoints": [{"path": "/assets/ASSET_1/actions", "method": "POST", ...}],
                    "mode": HttpPublishMode.OVERRIDE
                }
            )
        ]

        Args:
            decisions (List[Dict[str, Any]]): The decisions directed at SoftCPS, as detailed above.

        Returns:
            List[MappedDecision]: The requests to send. Empty if no actuator could be mapped.
        """
        mapped_decisions: List[MappedDecision] = []
        for decision in decisions:
            entity_type_id = decision.get("entity_type_id")
            for actuator_id, actuator in decision.get("actuators", {}).items():
                try:
                    # Guard Clause 1: Checks that the entity is a battery, the only one SoftCPS supports.
                    if entity_type_id != Label.BATTERY.value:
                        raise ValueError(f"SoftCPS has no actuation for the entity type {entity_type_id!r}")

                    # Guard Clause 2: Checks that the device is controlled in kW, the only unit SoftCPS accepts.
                    unit = actuator.get("measurement_unit")
                    if unit not in (None, "kW"):
                        raise ValueError(f"SoftCPS only accepts kW, and the actuator is in {unit!r}")

                    # Guard Clause 3: Checks that the power is a number (a boolean is not).
                    power_kw = actuator["value"]
                    if isinstance(power_kw, bool) or not isinstance(power_kw, (int, float)):
                        raise ValueError(f"the value must be a number, got {power_kw!r}")

                    # Guard Clause 4: Checks that the actuator has a destination.
                    destination = actuator["destination"]
                    asset_id = str(destination).strip() if destination is not None else ""
                    if not asset_id:
                        raise ValueError("the destination is empty")
                except (KeyError, TypeError, ValueError) as err:
                    self._logger.warning(
                        f"{self._LOG_PREFIX} Ignoring actuator '{actuator_id}' of entity '{decision.get('id')}' in "
                        f"environment '{self._environment_name}': {type(err).__name__}: {err}"
                    )
                    continue

                endpoint = {**self._endpoint, "path": self._endpoint["path"].replace(self._ASSET_PLACEHOLDER, asset_id)}
                mapped_decisions.append(MappedDecision(
                    message={"power_kw": float(power_kw)},
                    send_arguments={"dynamic_endpoints": [endpoint], "mode": HttpPublishMode.OVERRIDE}
                ))

        return mapped_decisions
