# --- Standard Library Imports ---
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# --- Internal Framework Imports ---
from app.core.adapters.ml_model_adapter_base import MLModelAdapterBase
from app.core.utils.time_utils import format_timestamp
from app.extension.utils.labels import ActLabel


class EnergAIzeMLModelAdapter(MLModelAdapterBase):
    """Adapter of the EnergAIze ML Model.

    Input: EnergAIze receives the data in the exact format produced by the Combination Logic and by the
    Parameter-Derivation Logic (and, when the installation belongs to a REC, by the REC Bridge container), so the
    message is not transformed.

    Output: EnergAIze controls the energy operation of the installation by producing decisions for its entities. In
    the current version it only produces power decisions, so every value of its output is a power setpoint. The Adapter
    finds the power actuator of each entity and puts the setpoint in the normalised format of the decisions.

    NOTE: If an entity has more than one power actuator, the decision goes to the FIRST one that appears in the
    environment.
    """

    def adapt_model_input(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Returns the message as it is, because EnergAIze already expects the unified message.

        Expected Input Structure:
        -------------------------
        See `EnergAIzeParameterDerivationLogic` (the message already has the derived parameters).

        Expected Output Structure:
        --------------------------
        The same message.

        Args:
            message (Dict[str, Any]): The unified message of a period.

        Returns:
            Dict[str, Any]: The same message.
        """
        self._logger.debug(
            f"{self._LOG_PREFIX} EnergAIze expects the unified message, so the input is not adapted."
        )
        return message

    def adapt_model_decisions(self, decisions: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Maps the power setpoints of EnergAIze into the normalised format of the decisions.

        The timestamp of every decision is the moment when this method maps them, in UTC.

        An entity is left out of the result (with a warning) if it is not in the environment, if it has no power
        actuator, or if its setpoint is not a number.

        Expected Input Structure:
        -------------------------
        decisions = {
            "AC000001_1": 3                     # entity id -> power setpoint
        }

        Expected Output Structure:
        --------------------------
        [
            {
                "id": "AC000001_1",
                "entity_type_id": "ev_charger",
                "actuators": {
                    "actuator_1": {"actuator_type_id": "power", "value": 3}
                },
                "timestamp": "2026-08-19 11:15:00 +0000"
            }
        ]

        Args:
            decisions (Dict[str, Any]): The output of EnergAIze: the power setpoint of each entity.

        Returns:
            List[Dict[str, Any]]: The decisions in the normalised format. Empty if there are none.
        """
        # Guard Clause 1: Checks that the output of EnergAIze is a dictionary.
        if not isinstance(decisions, dict):
            self._logger.warning(
                f"{self._LOG_PREFIX} The output of EnergAIze must be a dictionary, and it is "
                f"{type(decisions).__name__}, so there are no decisions."
            )
            return []

        entities_schema: Dict[str, Any] = self._environment.get("entities", {})
        formatted_timestamp: str = format_timestamp(datetime.now(timezone.utc))

        normalised_decisions: List[Dict[str, Any]] = []
        for entity_id, value in decisions.items():
            # Guard Clause 2: Ignores the entities that are not in the environment.
            entity_schema = entities_schema.get(entity_id)
            if entity_schema is None:
                self._logger.warning(f"{self._LOG_PREFIX} Decision for '{entity_id}' ignored: it is not in the environment.")
                continue

            # Guard Clause 3: Ignores the entities that have no power actuator.
            actuator_id: Optional[str] = self._find_power_actuator(entity_schema)
            if actuator_id is None:
                self._logger.warning(
                    f"{self._LOG_PREFIX} Decision for '{entity_id}' ignored: the entity has no power actuator."
                )
                continue

            # Guard Clause 4: Ignores the setpoints that are not numbers ('bool' is a number in Python, but not here).
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                self._logger.warning(
                    f"{self._LOG_PREFIX} Decision for '{entity_id}' ignored: the setpoint {value!r} is not a number."
                )
                continue

            normalised_decisions.append({
                "id": entity_id,
                "entity_type_id": entity_schema.get("entity_type_id"),
                "actuators": {actuator_id: {"actuator_type_id": ActLabel.POWER.value, "value": value}},
                "timestamp": formatted_timestamp,
            })

        return normalised_decisions

    @staticmethod
    def _find_power_actuator(entity_schema: Dict[str, Any]) -> Optional[str]:
        """Finds the first power actuator of an entity.

        Args:
            entity_schema (Dict[str, Any]): The entity, as it is in the environment.

        Returns:
            Optional[str]: The id of the first actuator whose type is power, or None if there is none.
        """
        for actuator_id, actuator in entity_schema.get("actuators", {}).items():
            if actuator.get("actuator_type_id") == ActLabel.POWER.value:
                return actuator_id
        return None
