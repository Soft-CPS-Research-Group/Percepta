from app.forwarders.forwarder_base import ForwarderBase
from app.utils.providers import Provider
from app.http_forwarding_protocol import HTTPForwardingProtocol
from app.utils.labels import Label

# Strategy handlers: links JSON strategy names to their respective logic methods
_LABEL_STRATEGIES = {}

def register_label_strategy(name):
    """Decorator to register a label strategy method into the class mapping."""

    def decorator(func):
        _LABEL_STRATEGIES[name] = func
        return func

    return decorator


class SoftCPSForwarder(ForwarderBase):
    provider = Provider.SOFTCPS.value

    def __init__(self, environment, environment_specs, configurations, logger):
        super().__init__(environment, environment_specs, configurations, logger)

        server_config = configurations.get("softCPS").get("publisher_server")

        self._protocol = HTTPForwardingProtocol(server_config, logger)
        self._raw_endpoint = server_config.get("resources").get("data")


    @register_label_strategy(Label.BATTERY.value)
    def _battery(self, result):
        return {
            "power_kw": result
        }

    # Se eu passar as specs assim por parâmetro secalhar é mais fácil depois tornar o Percepta dinâmico
    def to_forward(self, entity, result, entity_specs):
        label = entity_specs.get("label")

        handler = _LABEL_STRATEGIES.get(label)

        if not handler:
            self._logger.error(f"Unsupported strategy: '{label}'")
            raise ValueError(f"Mapping strategy '{label}' is not supported.")


        message = handler(self, result)
        self._logger.info(f"Message to SoftCPS: {message}")
        endpoint = self._raw_endpoint.format(entity_id=self._resources_rules.get(entity))
        self._protocol.send_message(message, endpoint)


    def stop(self):
        self._protocol.stop()