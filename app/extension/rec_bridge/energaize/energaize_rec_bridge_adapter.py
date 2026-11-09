# --- Standard Library Imports ---
import copy
from typing import Any, Dict, List

# --- Internal Framework Imports ---
from app.extension.rec_bridge.rec_bridge_adapter_base import RECBridgeAdapterBase
from app.core.utils.logger import LoggingUtils
from app.extension.utils.labels import ParamLabel


class EnergAIzeRECBridgeAdapter(RECBridgeAdapterBase):
    """Adapter of the REC Bridge for EnergAIze.

    What is shared with the other installations is decided by the environment:
    - A parameter is shared if it has "rec_relevant": true in the entity it belongs to.
    - A derived datum is shared if its name is in 'rec.derived_data_to_share' (e.g., ["solar_generation"]). An empty
      list, or none, means that no derived data is shared.

    What the community did in the period is the sum of what every installation shared: the installation itself and the
    ones that sent their data in time. For each installation, the 'final_values' of the shared parameters of the types
    'energy_in_total' and 'energy_out_total' are added up (the first value of each parameter; a parameter without a
    value adds nothing). The result is added to the message in the key "community"; the rest of the message is not
    changed.

    Expected `environment` structure (only the parts used here):
    ------------------------------------------------------------
    {
        "entities": {
            "GR01": {"entity_type_id": "grid_meter", "parameters": {
                "energy_in_total": {"parameter_type_id": "energy_in_total", "rec_relevant": true},
                "energy_out_total": {"parameter_type_id": "energy_out_total", "rec_relevant": true}}}
        },
        "rec": {"community_id": "porto", "installation_id": "lab-01", "peers": ["lab-02"],
                "derived_data_to_share": []}
    }

    Data shared by an installation (what 'extract_rec_data' returns):
    -----------------------------------------------------------------
    {
        "entities": {"GR01": {"entity_type_id": "grid_meter", "parameters": {"energy_in_total": {<the parameter>}}}},
        "derived_data": {"solar_generation": {<the derived datum of the message>}}
    }

    Key added to the message (what 'combine_with_received_data' adds):
    ------------------------------------------------------------------
    message["community"] = {"energy_in_total": 0.018354421768707493, "energy_out_total": 0.00705}
    """

    _LOG_PREFIX: str = "EnergAIze REC Bridge Adapter |"

    # The types of parameter that are added up, and the name of the total in the "community" key
    _COMMUNITY_TOTALS: Dict[str, str] = {
        ParamLabel.ENERGY_IN_TOTAL.value: "energy_in_total",
        ParamLabel.ENERGY_OUT_TOTAL.value: "energy_out_total"
    }

    def __init__(self, environment_name: str, environment: Dict[str, Any], logger: LoggingUtils) -> None:
        super().__init__(environment_name, environment, logger)
        rec = environment.get("rec") if isinstance(environment.get("rec"), dict) else {}
        derived_data_to_share = rec.get("derived_data_to_share", [])
        self._shared_derived_data: List[str] = derived_data_to_share if isinstance(derived_data_to_share, list) else []

    def extract_rec_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Extracts the shared parameters and derived data from the unified message.

        Args:
            message (Dict[str, Any]): The unified message of the period.

        Returns:
            Dict[str, Any]: The shared data, in the structure above. The keys without anything to share are left out.
        """
        shared_entities: Dict[str, Any] = {}
        environment_entities = self._environment.get("entities", {})
        for entity_id, entity in (message.get("entities") or {}).items():
            if not isinstance(entity, dict):
                continue
            environment_parameters = (environment_entities.get(entity_id) or {}).get("parameters", {})
            shared_parameters = {
                parameter_id: copy.deepcopy(parameter)
                for parameter_id, parameter in (entity.get("parameters") or {}).items()
                if (environment_parameters.get(parameter_id) or {}).get("rec_relevant") is True
            }
            if shared_parameters:
                shared_entities[entity_id] = {
                    "entity_type_id": entity.get("entity_type_id"),
                    "parameters": shared_parameters
                }

        derived_data = message.get("derived_data") or {}
        shared_derived_data = {
            name: copy.deepcopy(derived_data[name]) for name in self._shared_derived_data if name in derived_data
        }

        rec_data: Dict[str, Any] = {}
        if shared_entities:
            rec_data["entities"] = shared_entities
        if shared_derived_data:
            rec_data["derived_data"] = shared_derived_data
        return rec_data

    def combine_with_received_data(
            self, message: Dict[str, Any], received_data: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Adds to the message the totals of the community: this installation plus the ones that sent their data.

        Args:
            message (Dict[str, Any]): The unified message of the period. It is not changed.
            received_data (Dict[str, Dict[str, Any]]): The data shared by each other installation, by installation.

        Returns:
            Dict[str, Any]: A copy of the message with "community": {"energy_in_total": x, "energy_out_total": y}.
        """
        installations = [self.extract_rec_data(message), *received_data.values()]
        totals: Dict[str, float] = {name: 0.0 for name in self._COMMUNITY_TOTALS.values()}
        for shared in installations:
            for entity in (shared.get("entities") or {}).values():
                for parameter in ((entity or {}).get("parameters") or {}).values():
                    total_name = self._COMMUNITY_TOTALS.get((parameter or {}).get("parameter_type_id"))
                    if total_name is None:
                        continue
                    final_values = parameter.get("final_values")
                    value = final_values[0] if isinstance(final_values, list) and final_values else None
                    if isinstance(value, (int, float)) and not isinstance(value, bool):
                        totals[total_name] += value

        combined = copy.deepcopy(message)
        combined["community"] = totals
        return combined
