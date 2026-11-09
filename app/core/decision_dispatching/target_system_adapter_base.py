# --- Standard Library Imports ---
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List

# --- Internal Framework Imports ---
from app.core.utils.logger import LoggingUtils


@dataclass
class MappedDecision:
    """A decision in the format that a Target System expects, ready to be sent.

    Attributes:
        message (Any): What is sent to the Target System (e.g., the body of a request).
        send_arguments (Dict[str, Any]): Extra keyword arguments for the 'send' method of the Output Handler, for what
            changes from one message to the next and cannot be in the configuration. It is empty if the Output Handler
            sends every message in the same way.
            Example for an HTTP Output Handler, whose address depends on the actuator:
            {"dynamic_endpoints": [{"path": "/assets/ASSET_1/actions", "method": "POST"}],
             "mode": HttpPublishMode.OVERRIDE}
    """
    message: Any
    send_arguments: Dict[str, Any] = field(default_factory=dict)


class TargetSystemAdapterBase(ABC):
    """Abstract base class of the Adapter of a Target System.

    Each Target System expects the decisions in its own format. The Adapter translates the decisions that are directed
    at the Target System, which arrive in the normalised format of Percepta, into the messages that the Target System
    expects.

    The decisions that the Adapter receives were already resolved: each one only has the actuators that belong to this
    Target System, and each actuator carries what the 'actuator_mapping' of the Target System says about it: its
    'destination' (e.g., the id of a tag or of an asset), the 'destination_measurement_unit' (mandatory) and, if the
    mapping has it, the 'voltage' of the real device (e.g., an EV charger that is controlled in amperes). The
    decisions are always in kW, and the Adapter converts them to the unit of the device.

    One instance of the Adapter is created for each Target System of each environment, and the same instance is used in
    every period. Nothing is shared between environments.

    To integrate an Adapter, create a class that inherits from this one and implement 'adapt_decisions'. The Adapter is
    built by the launcher of the Target System (see 'TargetSystemLauncherBase').
    """

    _LOG_PREFIX: str = "Target System Adapter |"

    # The unit of the power of the decisions. Every Adapter converts it to the 'destination_measurement_unit' of the
    # actuator (see `app.core.utils.unit_utils`).
    DECISIONS_UNIT: str = "kW"

    def __init__(
        self,
        target_system_id: str,
        environment_name: str,
        environment: Dict[str, Any],
        logger: LoggingUtils
    ) -> None:
        """Initializes the Adapter.

        Expected Input Structure:
        -------------------------
        environment = {
            "group": "group_1",
            "entities": {
                "B01": {
                    "entity_type_id": "battery",
                    "actuators": {"act_power_1": {"actuator_type_id": "power", ...}}
                }
            },
            "target_systems": {"cleanwatts": {"actuator_mapping": {"act_power_1": "35447"}}},
            ...                                 # The rest of the environment, as returned by the Schema Storage
        }

        Args:
            target_system_id (str): Id of the Target System this Adapter translates for.
            environment_name (str): Name of the environment.
            environment (Dict[str, Any]): The environment, as returned by the Schema Storage.
            logger (LoggingUtils): Logger of the environment.

        Raises:
            ValueError: If a dependency is missing or invalid.
        """
        # Guard Clause 1: Checks that the logger exists, because it is needed to report the other errors.
        if not logger:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'logger' dependency is missing.")

        # Guard Clause 2: Checks that the ids are non-empty strings.
        for name, value in (("target_system_id", target_system_id), ("environment_name", environment_name)):
            if not isinstance(value, str) or not value.strip():
                error_msg = f"{self._LOG_PREFIX} Initialization error: '{name}' must be a non-empty string."
                logger.error(error_msg)
                raise ValueError(error_msg)

        # Guard Clause 3: Checks that the environment is a dictionary.
        if not isinstance(environment, dict):
            error_msg = f"{self._LOG_PREFIX} Initialization error: 'environment' must be a dictionary."
            logger.error(error_msg)
            raise ValueError(error_msg)

        self._target_system_id = target_system_id
        self._environment_name = environment_name
        self._environment = environment
        self._logger = logger

    @abstractmethod
    def adapt_decisions(self, decisions: List[Dict[str, Any]]) -> List[MappedDecision]:
        """Translates the decisions directed at the Target System into the messages that it expects.

        Each item of the result is sent on its own, and an error in one does not stop the others. The Adapter chooses
        how many messages to build: one for each decision, one for each actuator, or only one for all of them,
        depending on what the Target System accepts.

        Expected Input Structure:
        -------------------------
        decisions = [
            {
                "id": "B01",                                # The entity that the decision is directed at
                "entity_type_id": "battery",
                "actuators": {
                    "act_power_1": {                        # Only the actuators of this Target System
                        "actuator_type_id": "power",
                        "value": -2.5,                      # kW
                        "destination": "35447",             # What the 'actuator_mapping' says about the actuator
                        "destination_measurement_unit": "A",   # Unit of the real device (mandatory)
                        "voltage": 230                      # (only if the mapping has it) Volts
                    }
                },
                "timestamp": "2026-08-19 11:15:00 +0000"
            }
        ]

        Expected Output Structure:
        --------------------------
        Specific to the Target System. For example, for a Target System that receives one request for each actuator:
        [MappedDecision(message={"Action": 2, "Value": -2.5, "ValueFormat": 0, "TagIds": [35445]})]

        Args:
            decisions (List[Dict[str, Any]]): The decisions directed at this Target System, as detailed above.

        Returns:
            List[MappedDecision]: The messages to send. Empty if there is nothing to send.
        """
        raise NotImplementedError()
