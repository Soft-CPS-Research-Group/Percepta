from app.forwarders.forwarder_base import ForwarderBase
from app.utils.providers import Provider

class ICForwarder(ForwarderBase):
    provider = Provider.ICHARGING.value

    def __init__(self, configurations, logger):
        super().__init__(configurations, logger)

    def to_forward(self, result):
        #print(result)
        pass
    