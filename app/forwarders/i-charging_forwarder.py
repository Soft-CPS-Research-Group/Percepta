from app.forwarders.forwarder_base import ForwarderBase
from app.utils.providers import Provider
from app.ic_actuation_request import ICActuationRequest

def _ev_charger(result, entity_specs):
    result = float(f"{result:.1f}")

    return {
        "type": "setlimit",
        "value": {
            "serialnumber": entity_specs.get('serialNumber'),
            "plug":  entity_specs.get('plug'),
            "power": result
        }
    }


class ICForwarder(ForwarderBase):
    provider = Provider.ICHARGING.value

    def __init__(self, environment, environment_specs, configurations, logger):
        super().__init__(environment, environment_specs, configurations, logger)

        self._ic_actuation_request = ICActuationRequest(configurations, logger)

        self._labels_functions_mapper = {
            "ev_charger": _ev_charger
        }

    # Se eu passar as specs assim por parâmetro secalhar é mais fácil depois tornar o Percepta dinâmico
    def to_forward(self, entity, result, entity_specs):
        message = self._labels_functions_mapper[entity_specs['label']](result, entity_specs)
        self._logger.info(f"Message to IC: {message}")

        self._ic_actuation_request.send_message(message)


    def stop(self):
        self._ic_actuation_request.stop()