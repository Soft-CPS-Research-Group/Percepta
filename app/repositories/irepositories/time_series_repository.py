from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any
from app.utils.logger import LoggingUtils


class TimeSeriesRepository(ABC):
    """
       Abstract base class for time series repositories.

       This class defines the interface for storing and retrieving time series data.
       Implementations must provide concrete methods for writing new entries,
       reading entries within a time range, and retrieving the latest entry.
    """

    @abstractmethod
    def write(self, value: Any) -> None:
        """
        Inserts a single time series value into the collection.

         Args:
            value (Any): Time series value to be inserted.
        """
        pass

    @abstractmethod
    def read(self, start_time: datetime, end_time: datetime) -> list:
        """
        Reads all time series entries from the collection between two timestamps.
        Results are sorted chronologically in ascending order.

        Args:
            start_time (datetime): Start time of the time series entries to be read.
            end_time (datetime): End time of the time series entries to be read.

        Returns:
            list: List of time series entries between start_time and end_time.
        """
        pass

    @abstractmethod
    def latest(self) -> dict:
        """
        Retrieves the most recent time series entry from the collection.

        Returns:
            dict: Latest time series entry retrieved from MongoDB.
        """
        pass
