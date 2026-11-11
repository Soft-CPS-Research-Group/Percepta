# --- Standard Library Imports ---
from typing import Any

# --- Internal Framework Imports ---
from app.core.output_handlers.output_handler_base import OutputHandlerBase
from app.core.utils.logger import LoggingUtils


class LogOutputHandler(OutputHandlerBase):
    """Output Handler that does not send the messages anywhere: it only writes them in the log.

    It is used when a container has nowhere to send its messages. For example, when the link from Data Ingestion to
    Data Processing is not in the Instance Configuration, so that the Data Ingestion can still run (receive, adapt and
    show the messages) without the next container.

    Unlike the other Output Handlers, it has no connector, no topology and no configuration: there is nothing to
    connect to, so nothing can fail when it is built or when it sends. It is not registered in the Output Handlers
    Launcher, and it is not a 'protocol_id' that a configuration can ask for: whoever needs it creates it directly.

    Nothing is stored or retried: a message that is logged is gone once the log is rotated.
    """

    _LOG_PREFIX: str = "Log Output Handler |"

    def __init__(self, logger: LoggingUtils) -> None:
        """Initializes the Output Handler that only logs.

        Args:
            logger (LoggingUtils): Logger where the messages are written. It is the logger of the environment, so
                that each line shows which environment the message belongs to.

        Raises:
            ValueError: If 'logger' is missing.
        """
        # --- Guard Clause: without a logger the messages would be lost, and that is the only thing it does. ---
        if not logger:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'logger' dependency is missing.")

        super().__init__(logger)

    async def send(self, message: Any) -> None:
        """Writes the message in the log (level INFO) instead of sending it.

        It is a coroutine function ('async def') like the 'send' of the other Output Handlers, so that the pipeline
        awaits it in the same way, even though nothing here waits.

        Expected `message` Data Structure (the canonical message of the pipeline):
        --------------------------------------------------------------------------
        {
            "EVC01": {
                "entity_type_id": "ev_charger",
                "parameters": {"energy_in": {"parameter_type_id": "energy_in", "values": [{...}]}},
                "timestamp": "2026-08-19 11:00:05 +0000"
            }
        }

        Args:
            message (Any): The message that would be sent to the next container.
        """
        # An empty message has nothing to show, so it is skipped (the other Output Handlers do the same).
        if not message:
            self._logger.debug(f"{self._LOG_PREFIX} Skipping empty message.")
            return

        # Level INFO, and not DEBUG, because the point of this handler is to make the messages visible: with DEBUG
        # they would only show in the log if that level was on, and the messages would seem to vanish.
        self._logger.info(f"{self._LOG_PREFIX} No next container to send to. Message: {message}")

    async def stop(self) -> None:
        """Nothing to release: there is no connection, task or buffer. It exists because every Output Handler has to
        be stoppable in the same way."""
        return None
