# --- Standard Library Imports ---
from typing import Any, Dict, List

# --- Internal Framework Imports ---
from app.core.adapters.target_system_adapter_base import MappedDecision, TargetSystemAdapterBase
from app.core.output_handlers.async_http_output_handler import HttpPublishMode
from app.core.utils.logger import LoggingUtils
from app.core.utils.unit_utils import build_converter, unit_family
from app.extension.telemetry_systems.cleanwatts.cw_session import CWSession


class CWTargetAdapter(TargetSystemAdapterBase):
    """Adapter of the Cleanwatts Target System.

    Cleanwatts receives one actuation request for each actuator. The decisions have powers in kW, and the request needs
    the value in the unit that the real device is controlled in, which is the mandatory 'destination_measurement_unit'
    of the 'actuator_mapping' of the environment:
    - a unit of power ("W", "kW" or "MW"): the power is converted from kW to that unit (e.g., a battery in "kW":
      positive charges, negative discharges);
    - "A": the current in amperes, calculated from the power with the 'voltage' of the actuator,
      `kW * 1000 / voltage` (e.g., an EV charger, which Cleanwatts controls in amperes). Amperes are not a unit of
      power, so they cannot be converted with `app.core.utils.unit_utils`.
    The tag that receives the actuation is the 'destination' of the actuator.

    The requests need the token of the session of the Cleanwatts launcher, which changes every time it is renewed, so it
    is read when the decisions are mapped and sent in the request.
    """

    _LOG_PREFIX: str = "Cleanwatts Target Adapter |"

    # The type of actuation that sets a numeric value
    _ACTION_NUMERIC: int = 2
    # The value is given as a plain number
    _VALUE_FORMAT_NUMBER: int = 0
    # The unit in which Cleanwatts controls a device that is not controlled in a unit of power
    UNIT_AMPERE: str = "A"

    def __init__(
        self,
        target_system_id: str,
        environment_name: str,
        environment: Dict[str, Any],
        logger: LoggingUtils,
        session: CWSession,
        endpoint: Dict[str, Any]
    ) -> None:
        """Initializes the Adapter.

        Expected `endpoint` Data Structure (the actuation endpoint of the topology):
        ----------------------------------------------------------------------------
        {
            "path": "/actuations",
            "method": "POST",
            "timeout_ms": 5000,
            "headers": {"Content-Type": "application/json"},
            "rpc_properties": {"enabled": True}
        }

        Args:
            target_system_id (str): Id of the Target System.
            environment_name (str): Name of the environment.
            environment (Dict[str, Any]): The environment, as returned by the Schema Storage.
            logger (LoggingUtils): Logger of the environment.
            session (CWSession): The session of the launcher, which has the token that authorises the requests.
            endpoint (Dict[str, Any]): The actuation endpoint, as detailed above. The token is added to its headers in
                each request.

        Raises:
            ValueError: If a dependency is missing or invalid.
        """
        super().__init__(target_system_id, environment_name, environment, logger)

        # Guard Clause 1: Checks that the session and the endpoint exist.
        if not session:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'session' dependency is missing.")
        if not isinstance(endpoint, dict) or not endpoint.get("path"):
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'endpoint' must be a dictionary with a 'path'.")

        self._session = session
        self._endpoint = endpoint

    def adapt_decisions(self, decisions: List[Dict[str, Any]]) -> List[MappedDecision]:
        """Builds one actuation request for each actuator of the decisions.

        An actuator that cannot be mapped (its value or destination is invalid, or its unit is not supported, or it
        is in amperes and has no voltage) is left out with a warning, so the other actuators are still sent.

        Expected Input Structure:
        -------------------------
        decisions = [
            {
                "id": "B01",
                "entity_type_id": "battery",
                "actuators": {
                    "act_power_1": {"actuator_type_id": "power", "value": -2.5, "destination": "35447"}
                },
                "timestamp": "2026-08-19 11:15:00 +0000"
            },
            {
                "id": "EVC01",
                "entity_type_id": "ev_charger",
                "actuators": {
                    "act_power_2": {
                        "actuator_type_id": "power", "value": 4.6, "destination": "39052",
                        "destination_measurement_unit": "A", "voltage": 230
                    }
                },
                "timestamp": "2026-08-19 11:15:00 +0000"
            }
        ]

        Expected Output Structure:
        --------------------------
        [
            MappedDecision(
                message={"Action": 2, "Value": "-2.5", "ValueFormat": 0, "TagIds": [35447]},
                send_arguments={"dynamic_endpoints": [<the actuation endpoint, with the Authorization header>],
                                "mode": HttpPublishMode.OVERRIDE}
            ),
            MappedDecision(
                message={"Action": 2, "Value": "20.0", "ValueFormat": 0, "TagIds": [39052]},     # 4.6 * 1000 / 230 A
                send_arguments={...}
            )
        ]
        The "Value" is text and the "TagIds" are integers, as in the Cleanwatts documentation.

        Args:
            decisions (List[Dict[str, Any]]): The decisions directed at Cleanwatts, as detailed above.

        Returns:
            List[MappedDecision]: The requests to send. Empty if no actuator could be mapped.

        Raises:
            RuntimeError: If the session has no token, as no request would be authorised.
        """
        # Guard Clause 1: Checks that the session has a token, because every request needs it.
        token = self._session.get_token()
        if not token:
            raise RuntimeError(f"{self._LOG_PREFIX} The Cleanwatts session has no token, so nothing can be sent.")

        # The same endpoint is used for every request, with the token of the session in the headers
        endpoint = {**self._endpoint, "headers": {**self._endpoint.get("headers", {}), "Authorization": f"CW {token}"}}

        mapped_decisions: List[MappedDecision] = []
        for decision in decisions:
            for actuator_id, actuator in decision.get("actuators", {}).items():
                try:
                    value = self._convert_value(
                        actuator["value"], actuator.get("destination_measurement_unit"), actuator.get("voltage")
                    )
                    tag_id = int(actuator["destination"])
                except (KeyError, TypeError, ValueError) as err:
                    self._logger.warning(
                        f"{self._LOG_PREFIX} Ignoring actuator '{actuator_id}' of entity '{decision.get('id')}' in "
                        f"environment '{self._environment_name}': {type(err).__name__}: {err}"
                    )
                    continue

                mapped_decisions.append(MappedDecision(
                    message={
                        "Action": self._ACTION_NUMERIC,
                        "Value": str(value),        # Cleanwatts expects the value as text, as in its documentation
                        "ValueFormat": self._VALUE_FORMAT_NUMBER,
                        "TagIds": [tag_id]
                    },
                    send_arguments={"dynamic_endpoints": [endpoint], "mode": HttpPublishMode.OVERRIDE}
                ))

        return mapped_decisions

    @classmethod
    def check_device(cls, destination_measurement_unit: Any, voltage: Any) -> None:
        """Checks that Cleanwatts can control a device with this unit and voltage.

        Args:
            destination_measurement_unit (Any): The unit of the device, from the 'actuator_mapping'. It is mandatory.
            voltage (Any): The voltage of the device, from the 'actuator_mapping'. Only needed for amperes.

        Raises:
            ValueError: If there is no unit, or it is not a unit of power or A, or it is A and there is no voltage.
        """
        # Guard Clause 1: Checks that the unit is there, and is one that Cleanwatts controls: a unit of power, or
        # amperes.
        if destination_measurement_unit != cls.UNIT_AMPERE and unit_family(destination_measurement_unit) != "power":
            raise ValueError(
                f"the 'destination_measurement_unit' {destination_measurement_unit!r} is missing or is not supported "
                f"by Cleanwatts (supported: a unit of power such as 'W', 'kW' or 'MW', or {cls.UNIT_AMPERE!r})"
            )

        # Guard Clause 2: Checks that a device in amperes has the voltage to convert the power.
        if destination_measurement_unit == cls.UNIT_AMPERE and (
                isinstance(voltage, bool) or not isinstance(voltage, (int, float)) or voltage <= 0):
            raise ValueError(f"a device in {cls.UNIT_AMPERE!r} needs a positive 'voltage', got {voltage!r}")

    def _convert_value(self, power_kw: Any, destination_measurement_unit: Any, voltage: Any) -> float:
        """Converts the power of a decision into the value that Cleanwatts expects for the device.

        Args:
            power_kw (Any): The power of the decision, in kW.
            destination_measurement_unit (Any): The unit of the device, from the 'actuator_mapping'.
            voltage (Any): The voltage of the device, from the 'actuator_mapping'.

        Returns:
            float: The power in the unit of the device, or the current in amperes (`kW * 1000 / voltage`) if the device
                is in amperes.

        Raises:
            ValueError: If the power is not a number, or the device is not supported (see `check_device`).
        """
        # Guard Clause 1: Checks that the power is a number (a boolean is not).
        if isinstance(power_kw, bool) or not isinstance(power_kw, (int, float)):
            raise ValueError(f"the value must be a number, got {power_kw!r}")

        self.check_device(destination_measurement_unit, voltage)

        if destination_measurement_unit == self.UNIT_AMPERE:
            return power_kw * 1000 / voltage

        return float(build_converter(self.DECISIONS_UNIT, destination_measurement_unit)(power_kw))
