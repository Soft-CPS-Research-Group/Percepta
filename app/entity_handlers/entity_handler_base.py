from abc import ABC, abstractmethod
from zoneinfo import ZoneInfo
from app.repositories.file_environment_repository import FileEnvironmentRepository
from app.utils.data import DataSet
from app.utils.logger import LoggingUtils


class EntityHandlerBase(ABC):
    label : str

    _repository: FileEnvironmentRepository
    _logger : LoggingUtils
    _entities_ids : dict
    _time_interval : int
    _tz : ZoneInfo

    def __init__(self, repository : FileEnvironmentRepository, entities_ids : dict, environment_specs: dict, configurations : dict, logger : LoggingUtils):
        self._repository = repository
        self._logger = logger
        self._entities_ids = entities_ids
        self._environment_specs = environment_specs
        self._configurations = configurations
        self._time_interval = DataSet.calculate_interval(configurations.get('frequency'))
        self._tz = self._set_time_zone()

    def _set_time_zone(self) -> ZoneInfo:
        # Get the current timestamp in UTC without microseconds
        tz_name = self._configurations.get("timezone", "UTC")
        try:
            return ZoneInfo(tz_name)
        except Exception:
            self._logger.warning(f"Invalid timezone '{tz_name}', falling back to UTC")
            return ZoneInfo("UTC")

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

    def generic_period_harmonizer(self):
        """Generic harmonizer.py to resample or aggregate data across different time periods.

        For example, this function can take data points recorded every 5 minutes
        and aggregate them into 15-minute intervals. It is meant to provide a
        standardized temporal resolution for downstream processing.

        This method can be overridden by subclasses with the specific
        aggregation or resampling logic required for the data.
        """
        pass