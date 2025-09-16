from app.forwarders.forwarder_base import ForwarderBase

class ICForwarder(ForwarderBase):
    provider = "i-charging"

    def __init__(self, configurations, logger):
        super().__init__(configurations, logger)

    def to_forward(self, result):
        #print(result)
        pass
    