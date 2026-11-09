from abc import ABC, abstractmethod
from typing import Any
from app.core.utils.logger import LoggingUtils


class OutputHandlerBase(ABC):
    """
    Abstract Base Class defining the contract for all egress pipeline operations.
    Every specialized output handler (Network, Local, File, RPC) must inherit
    from this class and implement the 'handle' orchestration strategy.
    """

    def __init__(self, logger: LoggingUtils) -> None:
        """
        Initializes the abstract output handler infrastructure boundary.

        Args:
            logger (LoggingUtils): Framework logger instance for telemetry tracking.
        """
        self._logger = logger

    @abstractmethod
    def send(self, message: Any) -> Any:
        """
        Abstract orchestration strategy method. Must be implemented by concrete classes
        to process or publish outbound messages down the line.

        Args:
            message (Any): The core data packet payload emitted by the domain layers.

        Returns:
            Any: Can return an RPC response payload dictionary, a delivery confirmation, or None.
        """
        raise NotImplementedError()

    @abstractmethod
    def stop(self) -> None:
        """
        Abstract lifecycle method. Guarantees that thread pools, connection loops,
        and background tasks can be cleanly and gracefully dismantled on shutdown.
        """
        raise NotImplementedError()