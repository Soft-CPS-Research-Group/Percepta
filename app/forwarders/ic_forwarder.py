from app.forwarders.forwarder_base import ForwarderBase
from app.utils.providers import Provider
from app.rabbitMQ_forwarding_protocol import RabbitMQForwardingProtocol



# Strategy handlers: links JSON strategy names to their respective logic methods
_LABEL_STRATEGIES = {}

def register_label_strategy(name):
    """Decorator to register a label strategy method into the class mapping."""

    def decorator(func):
        _LABEL_STRATEGIES[name] = func
        return func

    return decorator


class ICForwarder(ForwarderBase):
    provider = Provider.ICHARGING.value

    def __init__(self, configurations, logger):
        super().__init__(configurations, logger)

        server_config = configurations.get("i-charging").get("receiver_server")
        self._protocol = RabbitMQForwardingProtocol(server_config, logger)


    @register_label_strategy("ev_charger")
    def _ev_charger(self, result, entity_specs):
        result = float(f"{result:.1f}")

        return {
            "type": "setlimit",
            "value": {
                "serialnumber": entity_specs.get('serialNumber'),
                "plug": entity_specs.get('plug'),
                "power": result
            }
        }

    # Se eu passar as specs assim por parâmetro secalhar é mais fácil depois tornar o Percepta dinâmico
    def to_forward(self, entity, result, entity_specs):
        label = entity_specs.get("label")

        handler = _LABEL_STRATEGIES.get(label)

        if not handler:
            self._logger.error(f"Unsupported strategy: '{label}'")
            raise ValueError(f"Mapping strategy '{label}' is not supported.")


        message = handler(result, entity_specs)
        self._logger.info(f"Message to IC: {message}")

        self._protocol.send_message(message)


    def stop(self):
        self._protocol.stop()