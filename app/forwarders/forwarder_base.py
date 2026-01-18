from abc import ABC, abstractmethod

class ForwarderBase(ABC):
    provider : str

    def __init__(self, configurations, logger):
        self._configurations = configurations
        self._logger = logger

    @abstractmethod
    def to_forward(self, entity_id, result, entity_specs):
        """
        Determines where the given result should be forwarded for further processing.

        result -> output object from the model
        """
        raise NotImplementedError()

   
