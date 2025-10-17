from app.utils.logger import LoggingUtils
from abc import ABC, abstractmethod


class AggregatorBase(ABC):
    def __init__(self, logger : LoggingUtils =None):
        self._logger = logger

    @abstractmethod
    def aggregate(self, message):
        raise NotImplementedError
