from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor


class OutputHandlerBase(ABC):
    _LOG_PREFIX = "Output Handler |"

    def __init__(self, environment, environment_specs, configurations, logger):

        # Thread pool to handle async publishing without blocking the main flow
        self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="OutputHandler")

        self._environment = environment
        self._environment_specs = environment_specs
        self._logger = logger
        self._configurations = configurations

    def message_handler(self, message):
        future = self._executor.submit(self._handler, message)

        def _callback(f):
            try:
                f.result()
            except Exception as e:
                self._logger.error(f"{self._LOG_PREFIX} Async Publish Error: {e}")

        future.add_done_callback(_callback)

    def stop(self):
        self._executor.shutdown(wait=False)

    @abstractmethod
    def _handler(self, message):
        raise NotImplementedError()