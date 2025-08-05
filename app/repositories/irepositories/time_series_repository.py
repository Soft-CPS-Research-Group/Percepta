from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, List, Dict


class TimeSeriesRepository(ABC):
    def __init__(self, group, environment, logger):
        self._group = group
        self._environment = environment
        self._logger = logger

    @abstractmethod
    def write(self, value: Any) -> None:
        """
        Writes a single value into the time series for a given entity and timestamp.

        environment_id:
            str -> Identifier of the environment the value belongs to
        timestamp:
            datetime -> Timestamp representing when the value occurred
        value:
            Any -> The data point to store (e.g., float, int, dict)

        Returns:
            None
        """
        pass

    @abstractmethod
    def read(self, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """
        Reads the time series values for a specific entity between two timestamps.

        start_time:
            datetime -> Lower bound of the time range (inclusive)
        end_time:
            datetime -> Upper bound of the time range (inclusive)

        Returns:
            List[Dict] -> A chronologically ordered list of data points within the specified time range
        """
        pass

    @abstractmethod
    def latest(self) -> Dict[str, Any]:
        """
        Retrieves the latest available value for a given entity.

        entity_id:
            str -> Identifier of the entity to query

        Returns:
            Dict -> A dictionary containing the latest timestamp and value for the entity
        """
        pass
