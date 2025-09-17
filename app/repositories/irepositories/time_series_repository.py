from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any
from app.utils.logger import LoggingUtils


class TimeSeriesRepository(ABC):

    _group : str                # Name of the database group to connect to (used to select the MongoDB database)
    _environment : str          # Environment identifier used to construct the collection name
    _logger : LoggingUtils      # Logger instance

    def __init__(self, group : str, environment : str, logger : LoggingUtils):
        self._group = group
        self._environment = environment
        self._logger = logger

    @abstractmethod
    def write(self, value: Any) -> None:
        """
        Inserts a single time series value into the collection.
        """
        pass

    @abstractmethod
    def read(self, start_time: datetime, end_time: datetime) -> list:
        """
        Reads all time series entries from the collection between two timestamps.
        Results are sorted chronologically in ascending order.
        """
        pass

    @abstractmethod
    def latest(self) -> dict:
        """
        Retrieves the most recent time series entry from the collection.
        """
        pass
