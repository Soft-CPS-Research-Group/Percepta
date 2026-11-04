from abc import ABC, abstractmethod

from app.core.utils.logger import LoggingUtils


class LauncherBase(ABC):

    def __init__(self, logger : LoggingUtils):
        self._logger = logger

    @abstractmethod
    def launch(self):
        raise NotImplementedError()
