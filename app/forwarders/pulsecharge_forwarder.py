from app.forwarders.forwarder_base import ForwarderBase
from app.utils.providers import Provider

class PCForwarder(ForwarderBase):
    provider = Provider.PULSECHARGE.value

    def __init__(self, configurations, logger):
        super().__init__(configurations, logger)

    def to_forward(self, entity_id, result, entity_specs):
        #print(result)
        pass
    