import threading
from abc import ABC, abstractmethod
from app.utils.data import DataSet

class ReceiverBase(ABC, threading.Thread):
    provider : str

    def __init__(self, environment, environment_specs, configurations, logger):
        threading.Thread.__init__(self)
        self._environment = environment
        self._entities = environment_specs.get('entities')
        self._configurations = configurations
        self._logger = logger
        self._max_reconnect_attempts = configurations.get('maxReconnectAttempts')
        self._time_interval = DataSet.calculate_interval(configurations.get('frequency'))

    @abstractmethod
    def run(self):
        raise NotImplementedError

    @abstractmethod
    def stop(self):
        """Stop the receiver thread or process."""
        raise NotImplementedError

    @classmethod
    def pre_start(cls):
        pass

    @classmethod
    def post_start(cls, environments, configurations, logger):
        """Optional hook to run after receiver starts. Defaults to no-op."""
        pass
