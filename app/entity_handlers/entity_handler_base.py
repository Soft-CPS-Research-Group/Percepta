from abc import ABC, abstractmethod
from app.utils.data import DataSet

class EntityHandlerBase(ABC):
    label : str

    def __init__(self, repository, entities_ids, configurations, logger):
        self._repository = repository
        self._logger = logger
        self._entity_ids = entities_ids
        self._time_interval = DataSet.calculate_interval(configurations.get('frequency'))

    @abstractmethod
    def process(self, message, all_data):
        """
        Updates the `message` object using the entity logic.

        message     -> final structured message to send to the model, for example
        all_data    -> full dictionary of all received data in the current cycle
        """
        raise NotImplementedError

    @abstractmethod
    def fallback(self, entity_id, last_known_data):
        """
        Provides fallback data if no data was received for the entity.

        entity_id        -> unique identifier for the entity
        last_known_data  -> dictionary containing previous values per entity
        """
        raise NotImplementedError

