# --- Standard Library Imports ---
import copy
from typing import Any, Dict, List

# --- Internal Framework Imports ---
from app.extension.rec_bridge.rec_bridge_adapter_base import RECBridgeAdapterBase
from app.core.utils.logger import LoggingUtils
from app.core.utils.unit_utils import convert
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

    Each total has its "value", "generated", "start_date" and "measurement_unit" (always "kWh"):
    - "value" is the sum of the values that exist, or None if no installation has one (None is not 0.0: it means
      "no data").
    - "generated" is True if the sum is not complete: a value is None, or an installation has no value for it, or an
      installation of the REC did not send its data in time. Otherwise it is False.
    - The totals are in kWh. Before adding, each value is checked: if the 'measurement_unit' of its parameter is not
      kWh, the value is converted (e.g., 500 Wh -> 0.5 kWh). A parameter without 'measurement_unit' is taken as
      already in kWh. A value whose unit cannot be converted to kWh is not used, a warning is logged and the total is
      "generated".
    - Each total has its own "generated": a None in "energy_in_total" does not mark "energy_out_total".
    - "start_date" is the start of the period. It is the one of the parameter of the installation itself (all the
      installations of the REC are in the same period, as their messages have the same timestamp), or, if it has none,
      the first one found in the data received.

    Examples (the installation itself plus one peer, "B"; only "value" and "generated" are shown):
    - Both have a value: 0.5 + 0.25 -> {"value": 0.75, "generated": False}.
    - Both are None -> {"value": None, "generated": True}.
    - 500 Wh in one installation and 0.25 kWh in B -> {"value": 0.75, "generated": False}.
    - The installation itself is None and B has 0.25 -> {"value": 0.25, "generated": True}.
    - B did not send its data in time, and the installation has 0.5 -> {"value": 0.5, "generated": True}.
    - Only the "energy_in_total" of B is None -> "energy_in_total" is generated, "energy_out_total" is not.

    Notes:
    - A total is a {"value", "generated", "start_date", "measurement_unit"} dictionary, and not a number, so that each
      total can say if it is complete, to which period it belongs and in which unit it is. It follows the structure of the parameters of the message.
    - The "generated" of the parameter that each installation shared (e.g., a value filled by the Missing Data
      Approach) is not used: a total is generated only for the reasons above.
    - None is not 0.0: 0.0 is a real value, None means that there is no data.

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
    message["community"] = {
        "energy_in_total": {
            "value": 0.018354421768707493, "generated": False, "start_date": "2026-11-09 16:19:15 +0000",
            "measurement_unit": "kWh"
        },
        "energy_out_total": {
            "value": 0.00705, "generated": False, "start_date": "2026-11-09 16:19:15 +0000",
            "measurement_unit": "kWh"
        }
    }
    """

    _LOG_PREFIX: str = "EnergAIze REC Bridge Adapter |"

    # The unit of the totals of the community
    _MEASUREMENT_UNIT: str = "kWh"

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
        peers = rec.get("peers", [])
        self._peers: List[str] = peers if isinstance(peers, list) else []

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
            Dict[str, Any]: A copy of the message with "community": {"energy_in_total": {"value": x, "generated": g,
                "start_date": d}, "energy_out_total": {...}}. The value is None if no installation has one.
        """
        installations = [self.extract_rec_data(message), *received_data.values()]

        # An installation of the REC that did not send its data in time makes every total incomplete
        incomplete = {name: any(peer not in received_data for peer in self._peers) for name in self._COMMUNITY_TOTALS.values()}
        totals: Dict[str, float | None] = {name: None for name in self._COMMUNITY_TOTALS.values()}
        start_dates: Dict[str, str | None] = {name: None for name in self._COMMUNITY_TOTALS.values()}

        for shared in installations:
            found = {name: False for name in self._COMMUNITY_TOTALS.values()}
            for entity in (shared.get("entities") or {}).values():
                for parameter in ((entity or {}).get("parameters") or {}).values():
                    total_name = self._COMMUNITY_TOTALS.get((parameter or {}).get("parameter_type_id"))
                    if total_name is None:
                        continue
                    found[total_name] = True
                    if start_dates[total_name] is None:
                        start_dates[total_name] = parameter.get("start_date")
                    final_values = parameter.get("final_values")
                    value = final_values[0] if isinstance(final_values, list) and final_values else None
                    if isinstance(value, (int, float)) and not isinstance(value, bool):
                        # Every value is added in kWh
                        unit = parameter.get("measurement_unit")
                        try:
                            value = value if unit is None else convert(value, unit, self._MEASUREMENT_UNIT)
                        except ValueError as err:
                            self._logger.warning(
                                f"{self._LOG_PREFIX} A value of '{total_name}' is in '{unit}', which cannot be "
                                f"converted to {self._MEASUREMENT_UNIT}, so it is not used: {err}"
                            )
                            incomplete[total_name] = True
                            continue
                        totals[total_name] = (totals[total_name] or 0.0) + value
                    else:
                        incomplete[total_name] = True

            # An installation that has no parameter for a total leaves that sum incomplete
            for total_name, was_found in found.items():
                if not was_found:
                    incomplete[total_name] = True

        combined = copy.deepcopy(message)
        combined["community"] = {
            name: {
                "value": totals[name],
                "generated": incomplete[name],
                "start_date": start_dates[name],
                "measurement_unit": self._MEASUREMENT_UNIT
            }
            for name in self._COMMUNITY_TOTALS.values()
        }
        return combined
