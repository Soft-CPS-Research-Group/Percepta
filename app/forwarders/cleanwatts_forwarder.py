import threading
from app.forwarders.forwarder_base import ForwarderBase
from app.utils.providers import Provider
from app.utils.cwlogin import CWSession
from app.http_forwarding_protocol import HTTPForwardingProtocol
from app.utils.labels import Label
from app.utils.logger import LoggingUtils

# Strategy handlers: links JSON strategy names to their respective logic methods
_LABEL_STRATEGIES = {}

def register_label_strategy(name):
    """Decorator to register a label strategy method into the class mapping."""

    def decorator(func):
        _LABEL_STRATEGIES[name] = func
        return func

    return decorator


class CWForwarder(ForwarderBase):
    provider = Provider.CLEANWATTS.value

    def __init__(self, environment, environment_specs, configurations, logger):
        super().__init__(environment, environment_specs, configurations, logger)

        server_config = configurations.get("cleanwatts").get("publisher_server")
        self._raw_endpoint = server_config.get("resources").get("actuation")

        self._protocol = HTTPForwardingProtocol(server_config, logger)

        logger_cw_session = LoggingUtils(f"{self.provider}_login", configurations)

        cw_session = threading.Thread(
            target=CWSession.start_token_refresher_service,
            args=(logger_cw_session, configurations),
            daemon=True
        )
        cw_session.start()

    @register_label_strategy(Label.EV_CHARGER.value)
    def _ev_charger(self, result, actuators):
        target_actuator_id = ""

        voltage = 230
        current_amps = result / voltage

        for actuator_id, actuator_values in actuators.items():
            if actuator_values.get('label') == "power_actuation":
                target_actuator_id = self._resources_rules.get(actuator_id)

        return {
            "Action": 2,
            "Value": current_amps,
            "ValueFormat": 0,
            "TagIds": [target_actuator_id]
        }

    @register_label_strategy(Label.BATTERY.value)
    def _battery(self, result, actuators):
        target_actuator_id = ""
        for actuator_id, actuator_values in actuators.items():
            if actuator_values.get('label') == "power_actuation":
                target_actuator_id = self._resources_rules.get(actuator_id)

        return {
            "Action": 2,
            "Value": result,
            "ValueFormat": 0,
            "TagIds": [target_actuator_id]
        }

    def to_forward(self, entity_id, result, entity_specs):
        label = entity_specs.get("label")
        actuators = entity_specs.get("actuators")
        handler = _LABEL_STRATEGIES.get(label)

        if not handler:
            self._logger.error(f"Unsupported strategy: '{label}'")
            raise ValueError(f"Mapping strategy '{label}' is not supported.")

        message = handler(self, result, actuators)
        self._logger.info(f"Message to Cleanwatts: {message}")

        self._protocol.send_message(message, self._raw_endpoint, self._header_updater())

    def _header_updater(self) -> dict:
        """
        Sets the authorization header using the current CWSession token.

        """
        token = CWSession.get_token()
        if token is None:
            raise RuntimeError(f"Token is None.")

        return {'Authorization': f"CW {token}"}