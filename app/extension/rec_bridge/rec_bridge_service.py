# --- Standard Library Imports ---
import asyncio
import json
import time
from datetime import timezone
from typing import Any, Dict, List, Optional

# --- Internal Framework Imports ---
from app.extension.rec_bridge.rec_bridge_adapter_base import RECBridgeAdapterBase
from app.core.output_handlers.output_handler_base import OutputHandlerBase
from app.core.utils.logger import LoggingUtils
from app.core.utils.time_utils import format_timestamp, parse_timestamp


class RECBridgeService:
    """Application Service of the REC Bridge container of ONE environment.

    The REC Bridge sits between Data Processing and Inference. Every time the unified message of a period arrives from
    Data Processing (the Input Handler calls 'receive_message'), the service shares data with the other installations
    of the REC and adds what they shared to the message:

        1. The Adapter extracts from the message the data relevant to the REC.
        2. That data is published, through the Output Handler of the REC, to the other installations.
        3. The service waits for the data of the other installations for the same period, until all the peers
           have sent it or the collection window ends.
        4. The Adapter combines the data received with the original message.
        5. The resulting message is sent to Inference through the Output Handler.

    The data of the other installations arrives at any time, through a second Input Handler that calls
    'receive_from_installation'. That Input Handler is always listening: it has one queue of this installation, bound
    to the exchange of each other installation. While the service waits
    for the period, only the data of that period is kept: the data of any other period is discarded. Data that arrives
    when no period is being waited for is kept, because the other installation may be ahead, and it is only used (or
    discarded) when the message of the period arrives.

    If the environment does not belong to a REC (no Adapter), the message goes to Inference as it arrived. If the
    other installations do not send their data in time, the message goes with the data that did arrive (an installation
    that is late or down never blocks the decisions).
    """

    _LOG_PREFIX: str = "REC Bridge Service |"

    def __init__(
            self,
            adapter: Optional[RECBridgeAdapterBase],
            rec_output_handler: Optional[OutputHandlerBase],
            output_handler: OutputHandlerBase,
            installation_id: Optional[str],
            peers: List[str],
            collection_window_seconds: float,
            logger: LoggingUtils
    ) -> None:
        """Initializes the service.

        Args:
            adapter (RECBridgeAdapterBase, optional): The Adapter. None if the environment does not belong to a REC,
                and then the message is only passed on.
            rec_output_handler (OutputHandlerBase, optional): Publishes the data to the other installations. None if
                the environment does not belong to a REC.
            output_handler (OutputHandlerBase): Sends the resulting message to Inference.
            installation_id (str, optional): Id of this installation in the REC. None if there is no REC.
            peers (List[str]): Ids of the other installations whose data is waited for.
            collection_window_seconds (float): The most it waits for the other installations, in seconds.
            logger (LoggingUtils): Logger of the environment.

        Raises:
            ValueError: If a dependency is missing, or the REC is only half configured.
        """
        if not output_handler:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'output_handler' dependency is missing.")
        if not logger:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'logger' dependency is missing.")
        if (adapter is None) != (rec_output_handler is None):
            raise ValueError(
                f"{self._LOG_PREFIX} Initialization error: 'adapter' and 'rec_output_handler' must be given together."
            )
        if adapter is not None and (not isinstance(installation_id, str) or not installation_id.strip()):
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'installation_id' is missing.")

        self._adapter = adapter
        self._rec_output_handler = rec_output_handler
        self._output_handler = output_handler
        self._installation_id = installation_id
        self._peers: List[str] = list(peers)
        self._collection_window_seconds = collection_window_seconds
        self._logger = logger

        # The data of the other installations, by period and then by installation: {"<timestamp>": {"lab-02": {...}}}
        self._received: Dict[str, Dict[str, Dict[str, Any]]] = {}
        # The period being waited for (None when the service is not waiting): data of other periods is discarded then
        self._current_period: Optional[str] = None
        # Switched on every time the data of an installation arrives, so the wait of a period can check if it is done
        self._arrival = asyncio.Event()

    async def receive_message(self, envelope: Dict[str, Any]) -> None:
        """Shares the data of the period with the REC and sends the resulting message to Inference.

        The Input Handler calls this for every message that arrives from Data Processing.

        Args:
            envelope (Dict[str, Any]): The message, as given by the Input Handler. Its 'payload' is the unified message
                (a dictionary, or JSON text or bytes).
        """
        message = self._read_payload(envelope, "unified message")
        if message is None:
            return

        period_id = self._period_id(message)
        try:
            result = await self._share_and_combine(message, period_id) if self._adapter else message
            await self._output_handler.send(result)
        except Exception as err:
            self._logger.error(
                f"{self._LOG_PREFIX} Could not process the period '{period_id}', so nothing was sent to Inference: "
                f"{type(err).__name__}: {err}"
            )

    async def receive_from_installation(self, envelope: Dict[str, Any]) -> None:
        """Keeps the data that another installation of the REC shared for a period.

        The second Input Handler calls this for every message that arrives from the other installations.

        Expected Input Structure (the payload):
        ---------------------------------------
        {"installation_id": "lab-02", "timestamp": "2026-08-19 11:15:00 +0000", "data": {...}}

        Args:
            envelope (Dict[str, Any]): The message, as given by the Input Handler.
        """
        payload = self._read_payload(envelope, "message of another installation")
        if payload is None:
            return

        installation_id, timestamp, data = payload.get("installation_id"), payload.get("timestamp"), payload.get("data")

        # Guard Clause 1: only well formed messages of the known peers are kept
        if not isinstance(installation_id, str) or not isinstance(timestamp, str) or not isinstance(data, dict):
            self._logger.warning(
                f"{self._LOG_PREFIX} Skipping a message of another installation: it needs 'installation_id', "
                f"'timestamp' and 'data'."
            )
            return
        if installation_id == self._installation_id:
            return
        if installation_id not in self._peers:
            self._logger.warning(
                f"{self._LOG_PREFIX} Skipping data of installation '{installation_id}' for the period '{timestamp}': "
                f"it is not one of the peers {self._peers}."
            )
            return

        # The same moment can be written in different ways (e.g., with another UTC offset), so the timestamps are
        # compared as moments: the data is of the period of this installation if its timestamp is the same one.
        timestamp = self._same_moment_key(timestamp)

        # Guard Clause 2: while a period is being waited for, data from another period is discarded
        if self._current_period is not None and timestamp != self._current_period:
            self._logger.debug(
                f"{self._LOG_PREFIX} Discarding data of installation '{installation_id}' for the period "
                f"'{timestamp}': the period being waited for is '{self._current_period}'."
            )
            return

        self._received.setdefault(timestamp, {})[installation_id] = data
        self._logger.debug(
            f"{self._LOG_PREFIX} Received the data of installation '{installation_id}' for the period '{timestamp}'."
        )
        self._arrival.set()

    async def _share_and_combine(self, message: Dict[str, Any], period_id: str) -> Dict[str, Any]:
        """Publishes the REC data of the period, waits for the data of the peers and combines everything."""
        rec_data = self._adapter.extract_rec_data(message)
        await self._rec_output_handler.send(
            {"installation_id": self._installation_id, "timestamp": message.get("timestamp"), "data": rec_data}
        )
        self._logger.info(
            f"{self._LOG_PREFIX} Shared the data of the period '{period_id}' with the REC "
            f"({len(rec_data.get('entities', {}))} entity(ies) and {len(rec_data.get('derived_data', {}))} derived "
            f"datum(s)). Waiting up to {self._collection_window_seconds:g} s for {self._peers}."
        )

        received = await self._collect(period_id)
        missing = [peer for peer in self._peers if peer not in received]
        if missing:
            self._logger.warning(
                f"{self._LOG_PREFIX} For the period '{period_id}', no data arrived in time from {missing}. "
                f"The message goes on without it."
            )

        result = self._adapter.combine_with_received_data(message, received)
        self._logger.info(
            f"{self._LOG_PREFIX} Resulting message of the period '{period_id}', with the data of "
            f"{sorted(received)}:\n{json.dumps(result, default=str, indent=2)}"
        )
        return result

    async def _collect(self, period_id: str) -> Dict[str, Dict[str, Any]]:
        """Waits until every peer has sent the data of the period or the collection window ends.

        Returns:
            Dict[str, Dict[str, Any]]: The data of the peers that sent it in time, by installation.
        """
        # Only the data of this period is kept: whatever arrived before for other periods is discarded
        for timestamp in [timestamp for timestamp in self._received if timestamp != period_id]:
            del self._received[timestamp]
        self._current_period = period_id

        try:
            deadline = time.monotonic() + self._collection_window_seconds
            while True:
                received = self._received.get(period_id, {})
                remaining = deadline - time.monotonic()
                if all(peer in received for peer in self._peers) or remaining <= 0:
                    break
                self._arrival.clear()
                try:
                    await asyncio.wait_for(self._arrival.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    break
        finally:
            # The window is over: the service stops filtering, even if the wait was interrupted
            self._current_period = None

        # The data of the period is taken and forgotten
        return self._received.pop(period_id, {})

    def _read_payload(self, envelope: Any, what: str) -> Optional[Dict[str, Any]]:
        """Gets the dictionary in the payload of an envelope (a dictionary, or JSON as text or bytes)."""
        payload = envelope.get("payload") if isinstance(envelope, dict) else None
        if isinstance(payload, (bytes, str)):
            try:
                payload = json.loads(payload)
            except ValueError as err:
                self._logger.warning(f"{self._LOG_PREFIX} Skipping a {what} that is not valid JSON: {err}")
                return None
        if not isinstance(payload, dict):
            self._logger.warning(
                f"{self._LOG_PREFIX} Skipping a {what} whose payload is not a dictionary, got "
                f"{type(payload).__name__}."
            )
            return None
        return payload

    @staticmethod
    def _period_id(message: Dict[str, Any]) -> str:
        """Identifies the period of a message by its 'timestamp' ('?' if it has none)."""
        timestamp = message.get("timestamp")
        if isinstance(timestamp, str) and timestamp.strip():
            return RECBridgeService._same_moment_key(timestamp)
        return "?"

    @staticmethod
    def _same_moment_key(timestamp: str) -> str:
        """Writes a timestamp in UTC, so that equal moments have equal keys (e.g., 11:00 +0100 and 10:00 +0000).

        A timestamp that cannot be read is returned as it is, so it only matches an identical text.
        """
        try:
            return format_timestamp(parse_timestamp(timestamp).astimezone(timezone.utc))
        except (ValueError, TypeError):
            return timestamp
