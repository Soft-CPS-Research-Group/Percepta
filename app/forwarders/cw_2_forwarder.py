from app.forwarders.forwarder_base import ForwarderBase
from app.utils.providers import Provider

class CW2Forwarder(ForwarderBase):
    provider = Provider.CLEANWATTS_2.value

    def __init__(self, environment, environment_specs, configurations, logger):
        super().__init__(environment, environment_specs, configurations, logger)

    def to_forward(self, entity_id, result, entity_specs):
        #print(result)
        pass
    