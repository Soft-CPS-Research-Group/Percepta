# --- Standard Library Imports ---
from typing import Any, Dict, List, Tuple

# --- Internal Framework Imports ---
from app.core.utils.logger import LoggingUtils


class TargetSystemResolver:
    """Determines the Target System that each decision is directed at.

    A decision of the normalised format has the actuators of an entity, and each actuator is controlled by a Target
    System. The environment says which one in the 'actuator_mapping' of each of its Target Systems: it links the id of
    each actuator to its destination in that system (e.g., the id of a tag or of an asset). So, to resolve a decision,
    it is enough to look for each actuator in the mappings. This is the same for every use case, so there is nothing to
    configure.

    A decision can have actuators of different Target Systems. The resolver splits it: each Target System gets a copy
    of the decision with only its actuators.

    One instance is created for each environment, and the same instance is used in every period.
    """

    _LOG_PREFIX: str = "Target System Resolver |"

    def __init__(self, environment_name: str, environment: Dict[str, Any], logger: LoggingUtils) -> None:
        """Initializes the resolver and indexes the actuators of the environment.

        Expected Input Structure:
        -------------------------
        environment = {
            "target_systems": {
                "cleanwatts": {"actuator_mapping": {
                    "act_power_1": "35447",                                                 # The destination alone
                    "act_power_2": {"destination": "39052", "destination_measurement_unit": "A", "voltage": 230}
                }},
                "softcps": {"actuator_mapping": {"act_power_3": "ASSET_3"}}
            },
            ...                                 # The rest of the environment, as returned by the Schema Storage
        }

        Args:
            environment_name (str): Name of the environment.
            environment (Dict[str, Any]): The environment, as returned by the Schema Storage.
            logger (LoggingUtils): Logger of the environment.

        Raises:
            ValueError: If a dependency is invalid, or an actuator is mapped by more than one Target System (it would
                not be possible to know which one receives its decisions).
        """
        # Guard Clause 1: Checks that the logger exists, because it is needed to report the other errors.
        if not logger:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'logger' dependency is missing.")

        # Guard Clause 2: Checks that the environment is a dictionary.
        if not isinstance(environment, dict):
            error_msg = f"{self._LOG_PREFIX} Initialization error: 'environment' must be a dictionary."
            logger.error(error_msg)
            raise ValueError(error_msg)

        self._environment_name = environment_name
        self._logger = logger

        # Actuator id -> (id of the Target System, what the mapping says about the actuator in that system: its
        # 'destination', the 'destination_measurement_unit' and, if it has it, the 'voltage' of the real device)
        self._actuators: Dict[str, Tuple[str, Dict[str, Any]]] = {}
        for target_system_id, target_system in (environment.get("target_systems") or {}).items():
            for actuator_id, mapping in target_system["actuator_mapping"].items():
                # The destination alone is the same as a dictionary with only the destination
                device = dict(mapping) if isinstance(mapping, dict) else {"destination": mapping}
                # Guard Clause 3: Checks that the actuator is not already mapped by another Target System.
                if actuator_id in self._actuators:
                    error_msg = (
                        f"{self._LOG_PREFIX} Initialization error: actuator '{actuator_id}' of environment "
                        f"'{environment_name}' is mapped by the Target Systems '{self._actuators[actuator_id][0]}' "
                        f"and '{target_system_id}'."
                    )
                    logger.error(error_msg)
                    raise ValueError(error_msg)
                self._actuators[actuator_id] = (target_system_id, device)

    def resolve(self, decisions: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Groups the actuators of the decisions by Target System.

        An actuator that no Target System maps cannot be applied, so it is left out and a warning is logged, to not
        stop the decisions of the other actuators. A decision left with no actuators is not returned. The decisions
        received are not changed.

        Expected Input Structure:
        -------------------------
        decisions = [
            {
                "id": "EVC01",
                "entity_type_id": "ev_charger",
                "actuators": {
                    "act_power_2": {"actuator_type_id": "power", "value": 4.6},
                    "unmapped_actuator": {"actuator_type_id": "power", "value": 1}
                },
                "timestamp": "2026-08-19 11:15:00 +0000"
            }
        ]

        Expected Output Structure:
        --------------------------
        {
            "cleanwatts": [                                 # Id of the Target System
                {
                    "id": "EVC01",
                    "entity_type_id": "ev_charger",
                    "actuators": {
                        "act_power_2": {
                            "actuator_type_id": "power",
                            "value": 4.6,
                            "destination": "39052",         # What the 'actuator_mapping' of the Target System says
                            "destination_measurement_unit": "A",   # about the actuator (the voltage only if
                                                                  # it has it)
                            "voltage": 230
                        }
                    },
                    "timestamp": "2026-08-19 11:15:00 +0000"
                }
            ]
        }

        Args:
            decisions (List[Dict[str, Any]]): The decisions, in the normalised format.

        Returns:
            Dict[str, List[Dict[str, Any]]]: The decisions of each Target System, by its id. Only the Target Systems
                that have decisions are in the result.
        """
        resolved: Dict[str, List[Dict[str, Any]]] = {}

        for decision in decisions:
            # Guard Clause 1: Checks that the decision has the structure that the resolution needs.
            if not isinstance(decision, dict) or not isinstance(decision.get("actuators"), dict):
                self._logger.warning(
                    f"{self._LOG_PREFIX} Ignoring a decision of environment '{self._environment_name}' with no "
                    f"'actuators': {decision!r}"
                )
                continue

            # Each Target System gets a copy of the decision with only its own actuators
            by_target_system: Dict[str, Dict[str, Any]] = {}
            for actuator_id, actuator in decision["actuators"].items():
                # Guard Clause 2: Checks that a Target System maps the actuator.
                if actuator_id not in self._actuators:
                    self._logger.warning(
                        f"{self._LOG_PREFIX} Ignoring actuator '{actuator_id}' of entity '{decision.get('id')}' in "
                        f"environment '{self._environment_name}': no Target System maps it."
                    )
                    continue

                target_system_id, device = self._actuators[actuator_id]
                part = by_target_system.setdefault(
                    target_system_id, {**{key: value for key, value in decision.items() if key != "actuators"},
                                       "actuators": {}}
                )
                part["actuators"][actuator_id] = {**actuator, **device}

            for target_system_id, part in by_target_system.items():
                resolved.setdefault(target_system_id, []).append(part)

        return resolved
