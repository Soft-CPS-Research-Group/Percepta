from abc import ABC, abstractmethod
from app.receivers.source_mapper import SourceMapper

class ForwarderBase(ABC):
    provider : str

    def __init__(self, environment, environment_specs, configurations, logger):
        self.environment = environment
        self.environment_specs = environment_specs
        self._configurations = configurations
        self._logger = logger

        source_mapper = SourceMapper(environment, environment_specs, configurations.get("source_mapping", {}), logger)

        self._resources_rules = source_mapper.resolve_address()

    @abstractmethod
    def to_forward(self, entity_id, result, entity_specs):
        """
        Determines where the given result should be forwarded for further processing.

        result -> output object from the model
        """
        raise NotImplementedError()

   
